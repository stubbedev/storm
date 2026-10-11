package schema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/stubbedev/storm/orm"
)

// docStore resolves the builder's connection to a document store's schema
// operations when it is one; SQL connections report false. In pretend mode
// the store's changes are recorded instead of run.
func (s *Builder) docStore(ctx context.Context) (orm.DocSchema, bool, error) {
	c, err := orm.Connection(ctx, s.conn)
	if err != nil {
		return nil, false, fmt.Errorf("schema: resolving connection %q: %w", s.conn, err)
	}
	if c.Doc == nil {
		return nil, false, nil
	}
	ds, ok := c.Doc.(orm.DocSchema)
	if !ok {
		return nil, false, fmt.Errorf("schema: document store %T on connection %q has no schema operations (orm.DocSchema)", c.Doc, s.conn)
	}
	if s.pretend != nil {
		return pretendDocs{ds, s.pretend}, true, nil
	}
	return ds, true, nil
}

// thenRun is err when resolving the store failed, otherwise what run
// returns.
func thenRun(err error, run func() error) error {
	if err != nil {
		return err
	}
	return run()
}

// docRefusal turns a store's refusal into the UnsupportedError SQL engines
// report, and gives every other error its context.
func docRefusal(ds orm.DocSchema, table, op string, err error) error {
	if err == nil {
		return nil
	}
	if nt, ok := errors.AsType[*orm.ErrNotTranslatable](err); ok {
		return &UnsupportedError{Engine: ds.Engine(), Table: table, Operation: op, Reason: nt.Reason}
	}
	return fmt.Errorf("schema: %s %s on %s: %w", op, table, ds.Engine(), err)
}

func docUnsupported(ds orm.DocSchema, table, op, reason string) error {
	return &UnsupportedError{Engine: ds.Engine(), Table: table, Operation: op, Reason: reason}
}

// docCollection translates a create blueprint into a collection: columns
// become declared fields, indexes and keys become indexes, and an HNSW
// index names the vector's metric. What documents cannot hold — generated
// keys, defaults, generated columns, foreign keys — is refused.
func docCollection(ds orm.DocSchema, bp *Blueprint) (orm.Collection, error) {
	if err := bp.err(); err != nil {
		return orm.Collection{}, err
	}
	c := orm.Collection{Name: bp.table}
	for _, col := range bp.columns {
		if col.Changed {
			return orm.Collection{}, fmt.Errorf("schema: cannot change column %s while creating collection %s", col.Name, bp.table)
		}
		f, err := docField(ds, bp.table, col)
		if err != nil {
			return orm.Collection{}, err
		}
		c.Fields = append(c.Fields, f)
	}
	for _, cmd := range bp.commands {
		switch {
		case cmd.kind == cmdHnsw:
			v, err := docVector(ds, bp, cmd)
			if err != nil {
				return orm.Collection{}, err
			}
			c.Vector = v
		case cmd.kind == cmdPrimary || cmd.kind.createsIndex():
			idx, err := docIndex(ds, bp.table, cmd, c.Fields)
			if err != nil {
				return orm.Collection{}, err
			}
			c.Indexes = append(c.Indexes, idx)
		default:
			return orm.Collection{}, docUnsupported(ds, bp.table, cmd.kind.String(), "documents have no such constraint")
		}
	}
	return c, nil
}

// docField declares a column as a document field.
func docField(ds orm.DocSchema, table string, col *Column) (orm.DocField, error) {
	op := "add column " + col.Name
	switch {
	case col.IsAutoIncrement:
		return orm.DocField{}, docUnsupported(ds, table, op, "documents have no generated integer keys; use a uuid, ulid or manual key")
	case col.HasDefault || col.OnUpdateNow:
		return orm.DocField{}, docUnsupported(ds, table, op, "documents have no column defaults; set the value in the model")
	case col.StoredExpr != "" || col.VirtualExpr != "" || col.Identity != "":
		return orm.DocField{}, docUnsupported(ds, table, op, "documents have no generated columns")
	case col.Type == TypeSet:
		return orm.DocField{}, docUnsupported(ds, table, op, "set columns exist only on MySQL and MariaDB")
	}
	return orm.DocField{Name: col.Name, Kind: docKind(col.Type), Nullable: col.IsNullable, Unsigned: col.IsUnsigned, Allowed: col.Allowed}, nil
}

// docKind is the field kind a column type's values have once the ORM
// stores them in a document.
func docKind(t string) orm.FieldKind {
	switch t {
	case TypeString, TypeChar, TypeTinyText, TypeText, TypeMediumText, TypeLongText,
		TypeUUID, TypeULID, TypeIPAddress, TypeMACAddress, TypeEnum, TypeTime, TypeTimeTz:
		return orm.KindString
	case TypeTinyInteger, TypeSmallInteger, TypeMediumInteger, TypeInteger, TypeBigInteger, TypeYear:
		return orm.KindInt
	case TypeDecimal, TypeFloat, TypeDouble:
		return orm.KindFloat
	case TypeBoolean:
		return orm.KindBool
	case TypeDate, TypeDateTime, TypeDateTimeTz, TypeTimestamp, TypeTimestampTz:
		return orm.KindTime
	case TypeJSON, TypeJSONB:
		return orm.KindJSON
	case TypeVector:
		return orm.KindVector
	}
	return orm.KindAny
}

func docIndex(ds orm.DocSchema, table string, cmd *command, fields []orm.DocField) (orm.DocIndex, error) {
	switch cmd.kind {
	case cmdSpatial:
		return orm.DocIndex{}, docUnsupported(ds, table, cmd.kind.String(), "spatial indexes are not supported on document stores")
	case cmdForeign:
		return orm.DocIndex{}, docUnsupported(ds, table, cmd.kind.String(), "documents have no foreign keys")
	}
	if cmd.index.Expression != "" || cmd.index.Predicate != "" {
		return orm.DocIndex{}, docUnsupported(ds, table, cmd.kind.String(), "expression and partial indexes are SQL")
	}
	idx := orm.DocIndex{
		Name:    cmd.index.IndexName,
		Unique:  cmd.kind == cmdUnique || cmd.kind == cmdPrimary,
		Primary: cmd.kind == cmdPrimary,
		Text:    cmd.kind == cmdFullText,
	}
	for _, name := range cmd.columns {
		f := orm.DocField{Name: name}
		if i := slices.IndexFunc(fields, func(f orm.DocField) bool { return f.Name == name }); i >= 0 {
			f = fields[i]
		}
		idx.Fields = append(idx.Fields, f)
	}
	return idx, nil
}

// docVector reads the collection vector off an HNSW index: the indexed
// vector column's size and the index's metric.
func docVector(ds orm.DocSchema, bp *Blueprint, cmd *command) (*orm.DocVector, error) {
	i := slices.IndexFunc(bp.columns, func(c *Column) bool { return c.Name == cmd.columns[0] })
	if i < 0 || bp.columns[i].Type != TypeVector {
		return nil, docUnsupported(ds, bp.table, cmd.kind.String(), cmd.columns[0]+" is not a vector column of this blueprint")
	}
	metric, ok := metricOfOpClass(cmd.index.VectorOp)
	if !ok {
		return nil, fmt.Errorf("schema: unknown vector operator class %q", cmd.index.VectorOp)
	}
	return &orm.DocVector{Field: bp.columns[i].Name, Size: bp.columns[i].Length, Metric: metric}, nil
}

func metricOfOpClass(op string) (orm.VectorMetric, bool) {
	for _, m := range []orm.VectorMetric{orm.L2, orm.Cosine, orm.InnerProduct} {
		if vectorOpClass(m) == op {
			return m, true
		}
	}
	return 0, false
}

func (s *Builder) docCreate(ctx context.Context, ds orm.DocSchema, bp *Blueprint) error {
	c, err := docCollection(ds, bp)
	if err != nil {
		return err
	}
	if bp.ifNot {
		exists, err := docHas(ctx, ds, bp.table)
		if err != nil || exists {
			return err
		}
	}
	return docRefusal(ds, bp.table, "create collection", ds.CreateCollection(ctx, c))
}

func docHas(ctx context.Context, ds orm.DocSchema, name string) (bool, error) {
	names, err := docCollections(ctx, ds)
	return slices.Contains(names, name), err
}

func docCollections(ctx context.Context, ds orm.DocSchema) ([]string, error) {
	names, err := ds.Collections(ctx)
	if err != nil {
		return nil, fmt.Errorf("schema: listing %s collections: %w", ds.Engine(), err)
	}
	return names, nil
}

// docAlter applies an alter blueprint to a collection. It plans the whole
// change first — the resulting fields and the indexes to drop and create —
// so a refused declaration fails before any document is touched, then
// applies it: renamed fields, dropped ones, the field declarations, then
// the index changes.
func (s *Builder) docAlter(ctx context.Context, ds orm.DocSchema, bp *Blueprint) error {
	if err := bp.err(); err != nil {
		return err
	}
	current, err := ds.DescribeCollection(ctx, bp.table)
	if err != nil {
		return docRefusal(ds, bp.table, "describe collection", err)
	}
	plan, err := planDocAlter(ds, bp, current)
	if err != nil {
		return err
	}
	for _, cmd := range plan.renames {
		if err := ds.RenameField(ctx, bp.table, cmd.from, cmd.to); err != nil {
			return docRefusal(ds, bp.table, cmd.kind.String(), err)
		}
	}
	for _, cmd := range plan.drops {
		if err := ds.DropFields(ctx, bp.table, cmd.columns); err != nil {
			return docRefusal(ds, bp.table, cmd.kind.String(), err)
		}
	}
	if !slices.EqualFunc(plan.fields, current.Fields, docFieldEqual) {
		if err := ds.SetFields(ctx, bp.table, plan.fields); err != nil {
			return docRefusal(ds, bp.table, "declare fields", err)
		}
	}
	for _, idx := range plan.dropIndexes {
		if err := ds.DropIndex(ctx, bp.table, idx); err != nil {
			return docRefusal(ds, bp.table, cmdDropIndex.String(), err)
		}
	}
	for _, idx := range plan.createIndexes {
		if err := ds.CreateIndex(ctx, bp.table, idx); err != nil {
			return docRefusal(ds, bp.table, "create index", err)
		}
	}
	return nil
}

// docAlterPlan is a validated collection change, ready to apply.
type docAlterPlan struct {
	renames, drops             []*command
	fields                     []orm.DocField
	dropIndexes, createIndexes []orm.DocIndex
}

func planDocAlter(ds orm.DocSchema, bp *Blueprint, current orm.Collection) (docAlterPlan, error) {
	var plan docAlterPlan
	fields := slices.Clone(current.Fields)
	at := func(name string) int {
		return slices.IndexFunc(fields, func(f orm.DocField) bool { return f.Name == name })
	}
	for _, cmd := range bp.commands {
		switch cmd.kind {
		case cmdRenameColumn:
			if at(cmd.to) >= 0 {
				return plan, fmt.Errorf("schema: cannot rename column %s to %s on collection %s: %s is already declared", cmd.from, cmd.to, bp.table, cmd.to)
			}
			if i := at(cmd.from); i >= 0 {
				fields[i].Name = cmd.to
			}
			plan.renames = append(plan.renames, cmd)
		case cmdDropColumn:
			fields = slices.DeleteFunc(fields, func(f orm.DocField) bool { return slices.Contains(cmd.columns, f.Name) })
			plan.drops = append(plan.drops, cmd)
		}
	}
	for _, col := range bp.columns {
		f, err := docField(ds, bp.table, col)
		if err != nil {
			return plan, err
		}
		switch i := at(col.Name); {
		case col.Changed && i < 0:
			return plan, fmt.Errorf("schema: cannot change column %s on collection %s: it is not declared", col.Name, bp.table)
		case col.Changed:
			fields[i] = f
		case i >= 0:
			return plan, fmt.Errorf("schema: cannot add column %s to collection %s: it is already declared", col.Name, bp.table)
		default:
			fields = append(fields, f)
		}
	}
	plan.fields = fields
	for _, cmd := range bp.commands {
		switch {
		case cmd.kind == cmdRenameColumn || cmd.kind == cmdDropColumn:
		case cmd.kind == cmdDropIndex:
			idx, ok := docFindIndex(current.Indexes, bp.table, cmd.index)
			if !ok {
				return plan, fmt.Errorf("schema: cannot drop index %s on collection %s: no such index", cmd.index.IndexName, bp.table)
			}
			plan.dropIndexes = append(plan.dropIndexes, idx)
		case cmd.kind == cmdPrimary || cmd.kind.createsIndex():
			idx, err := docIndex(ds, bp.table, cmd, fields)
			if err != nil {
				return plan, err
			}
			plan.createIndexes = append(plan.createIndexes, idx)
		default:
			return plan, docUnsupported(ds, bp.table, cmd.kind.String(), "documents have no such constraint")
		}
	}
	return plan, nil
}

func docFieldEqual(a, b orm.DocField) bool {
	return a.Name == b.Name && a.Kind == b.Kind && a.Nullable == b.Nullable && a.Unsigned == b.Unsigned && slices.Equal(a.Allowed, b.Allowed)
}

// docFindIndex finds the index a drop names: by its name, or — where a
// store names indexes by field, as Qdrant does — by the Laravel-style name
// the blueprint would have given an index on that field.
func docFindIndex(idxs []orm.DocIndex, table string, want *Index) (orm.DocIndex, bool) {
	for _, idx := range idxs {
		if idx.Name == want.IndexName || len(want.Columns) > 0 && slices.Equal(idx.FieldNames(), want.Columns) {
			return idx, true
		}
		for _, suffix := range []string{suffixIndex, suffixUnique, suffixFullText, suffixPrimary} {
			if indexName(table, suffix, idx.FieldNames()) == want.IndexName {
				return idx, true
			}
		}
	}
	return orm.DocIndex{}, false
}

func (s *Builder) docDrop(ctx context.Context, ds orm.DocSchema, table string, ifExists bool) error {
	if !ifExists {
		exists, err := docHas(ctx, ds, table)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("schema: cannot drop collection %s on %s: it does not exist", table, ds.Engine())
		}
	}
	return docRefusal(ds, table, "drop collection", ds.DropCollection(ctx, table))
}

func (s *Builder) docColumns(ctx context.Context, ds orm.DocSchema, table string) ([]ColumnInfo, error) {
	c, err := ds.DescribeCollection(ctx, table)
	if err != nil {
		return nil, docRefusal(ds, table, "describe collection", err)
	}
	cols := make([]ColumnInfo, len(c.Fields))
	for i, f := range c.Fields {
		cols[i] = ColumnInfo{Name: f.Name, Type: f.Kind.String(), Nullable: f.Nullable}
	}
	return cols, nil
}

func (s *Builder) docIndexes(ctx context.Context, ds orm.DocSchema, table string) ([]IndexInfo, error) {
	c, err := ds.DescribeCollection(ctx, table)
	if err != nil {
		return nil, docRefusal(ds, table, "describe collection", err)
	}
	idxs := make([]IndexInfo, len(c.Indexes))
	for i, idx := range c.Indexes {
		idxs[i] = IndexInfo{Name: idx.Name, Columns: idx.FieldNames(), Unique: idx.Unique, Primary: idx.Primary}
	}
	return idxs, nil
}

// pretendDocs records a document store's schema changes instead of
// running them; reads still reach the store.
type pretendDocs struct {
	orm.DocSchema
	log *[]string
}

func (p pretendDocs) record(format string, args ...any) error {
	*p.log = append(*p.log, fmt.Sprintf(format, args...))
	return nil
}

// DescribeCollection reads a collection that exists; one that does not
// was created by an earlier pretended step, so it reads as empty.
func (p pretendDocs) DescribeCollection(ctx context.Context, name string) (orm.Collection, error) {
	exists, err := docHas(ctx, p.DocSchema, name)
	if err != nil || !exists {
		return orm.Collection{Name: name}, err
	}
	return p.DocSchema.DescribeCollection(ctx, name)
}

func (p pretendDocs) CreateCollection(_ context.Context, c orm.Collection) error {
	fields := make([]string, len(c.Fields))
	for i, f := range c.Fields {
		fields[i] = f.Name + " " + f.Kind.String()
	}
	return p.record("create collection %s (%s)", c.Name, strings.Join(fields, ", "))
}

func (p pretendDocs) DropCollection(_ context.Context, name string) error {
	return p.record("drop collection %s", name)
}

func (p pretendDocs) RenameCollection(_ context.Context, from, to string) error {
	return p.record("rename collection %s to %s", from, to)
}

func (p pretendDocs) SetFields(_ context.Context, collection string, fields []orm.DocField) error {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return p.record("declare fields of %s: %s", collection, strings.Join(names, ", "))
}

func (p pretendDocs) DropFields(_ context.Context, collection string, fields []string) error {
	return p.record("drop fields %s from %s", strings.Join(fields, ", "), collection)
}

func (p pretendDocs) RenameField(_ context.Context, collection, from, to string) error {
	return p.record("rename field %s to %s in %s", from, to, collection)
}

func (p pretendDocs) CreateIndex(_ context.Context, collection string, idx orm.DocIndex) error {
	return p.record("create index %s on %s (%s)", idx.Name, collection, strings.Join(idx.FieldNames(), ", "))
}

func (p pretendDocs) DropIndex(_ context.Context, collection string, idx orm.DocIndex) error {
	return p.record("drop index %s on %s", idx.Name, collection)
}
