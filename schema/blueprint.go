// Package schema defines tables and migrations, Laravel style:
//
//	schema.Create(ctx, "users", func(t *schema.Blueprint) {
//		t.ID()
//		t.ForeignID("country_id").Nullable().Constrained().NullOnDelete()
//		t.String("email").Unique()
//		t.Boolean("active").Default(true)
//		t.JSON("settings").Nullable()
//		t.Timestamps()
//		t.SoftDeletes()
//	})
//
// DDL is compiled by a grammar chosen from the connection's dialect, so the
// same blueprint produces SQLite, Postgres, MySQL, MariaDB, DuckDB or
// ClickHouse statements. A blueprint the engine cannot express fails with an
// *UnsupportedError naming the engine and the operation; nothing is
// approximated.
package schema

import (
	"cmp"
	"errors"
	"fmt"
	"strings"

	"github.com/stubbedev/eloquent-go/orm"
)

// Blueprint collects column definitions and commands for one table.
type Blueprint struct {
	table     string
	ifNot     bool
	temporary bool
	engine    string
	charset   string
	collation string
	comment   string
	columns   []*Column
	commands  []*command
	sorting   sorting
	errs      []error
}

// sorting holds the ClickHouse MergeTree storage clauses.
type sorting struct {
	orderBy     []string
	unsorted    bool
	partitionBy string
	ttl         string
	settings    []setting
}

type setting struct {
	name  string
	value any
}

func (s sorting) declared() bool {
	return len(s.orderBy) > 0 || s.unsorted || s.partitionBy != "" || s.ttl != "" || len(s.settings) > 0
}

type commandKind int

const (
	cmdPrimary commandKind = iota
	cmdUnique
	cmdIndex
	cmdFullText
	cmdSpatial
	cmdHnsw
	cmdForeign
	cmdDropColumn
	cmdRenameColumn
	cmdRenameIndex
	cmdDropIndex
	cmdDropForeign
	cmdDropPrimary
)

func (k commandKind) String() string {
	return [...]string{
		cmdPrimary:      "add a primary key",
		cmdUnique:       "create a unique index",
		cmdIndex:        "create an index",
		cmdFullText:     "create a full text index",
		cmdSpatial:      "create a spatial index",
		cmdHnsw:         "create an hnsw vector index",
		cmdForeign:      "add a foreign key",
		cmdDropColumn:   "drop a column",
		cmdRenameColumn: "rename a column",
		cmdRenameIndex:  "rename an index",
		cmdDropIndex:    "drop an index",
		cmdDropForeign:  "drop a foreign key",
		cmdDropPrimary:  "drop the primary key",
	}[k]
}

func (k commandKind) createsIndex() bool {
	switch k {
	case cmdUnique, cmdIndex, cmdFullText, cmdSpatial, cmdHnsw:
		return true
	}
	return false
}

type command struct {
	kind    commandKind
	index   *Index
	columns []string
	from    string
	to      string
	foreign *Foreign
}

// Column is a column definition with chainable modifiers.
type Column struct {
	Name            string
	Type            string // see the Type constants in grammar.go
	Length          int
	Precision       int
	Scale           int
	Allowed         []string // enum / set values
	Subtype         string   // geometry subtype
	SRID            int
	IsNullable      bool
	IsUnsigned      bool
	IsAutoIncrement bool
	StartingValue   int
	HasDefault      bool
	DefaultValue    any
	OnUpdateNow     bool
	CommentText     string
	CharsetName     string
	CollationName   string
	AfterColumn     string
	IsFirst         bool
	IsInvisible     bool
	StoredExpr      string
	VirtualExpr     string
	Identity        string // "BY DEFAULT" or "ALWAYS" (Postgres identity columns)
	Changed         bool   // modify an existing column

	bp        *Blueprint
	refTable  string // set by ForeignIDFor
	refColumn string // set by ForeignIDFor
}

// Expr is a raw SQL default value, e.g. schema.Expr("CURRENT_TIMESTAMP").
type Expr string

// currentTimestamp is the default UseCurrent sets; each grammar renders
// its engine's spelling.
type currentTimestamp struct{}

func (b *Blueprint) add(name, typ string) *Column {
	c := &Column{Name: name, Type: typ, bp: b}
	b.columns = append(b.columns, c)
	return c
}

func (b *Blueprint) fail(err error) { b.errs = append(b.errs, err) }

func (b *Blueprint) err() error { return errors.Join(b.errs...) }

// ---------------------------------------------------------------------------
// Table options
// ---------------------------------------------------------------------------

// Engine sets the table engine: MySQL's (InnoDB, ...) or ClickHouse's
// (MergeTree by default; ReplacingMergeTree(ver), Memory, Log, ...).
func (b *Blueprint) Engine(engine string) { b.engine = engine }

// Charset sets the MySQL table character set.
func (b *Blueprint) Charset(charset string) { b.charset = charset }

// Collation sets the MySQL table collation.
func (b *Blueprint) Collation(coll string) { b.collation = coll }

// Comment sets the table comment.
func (b *Blueprint) Comment(comment string) { b.comment = comment }

// Temporary creates a temporary table.
func (b *Blueprint) Temporary() { b.temporary = true }

// OrderBy sets a ClickHouse MergeTree table's sorting key (ORDER BY). It
// defaults to the primary key.
func (b *Blueprint) OrderBy(column string, more ...string) {
	b.sorting.orderBy = append([]string{column}, more...)
}

// Unsorted declares a ClickHouse MergeTree table without a sorting key
// (ORDER BY tuple()).
func (b *Blueprint) Unsorted() { b.sorting.unsorted = true }

// PartitionBy sets a ClickHouse MergeTree partition key expression, e.g.
// "toYYYYMM(created_at)".
func (b *Blueprint) PartitionBy(expr string) { b.sorting.partitionBy = expr }

// TTL sets a ClickHouse MergeTree table TTL expression, e.g.
// "created_at + INTERVAL 1 MONTH".
func (b *Blueprint) TTL(expr string) { b.sorting.ttl = expr }

// Setting adds a ClickHouse table setting (SETTINGS name = value), in call order.
func (b *Blueprint) Setting(name string, value any) {
	b.sorting.settings = append(b.sorting.settings, setting{name, value})
}

// ---------------------------------------------------------------------------
// Column types
// ---------------------------------------------------------------------------

// ID is an auto-incrementing big integer primary key named "id".
func (b *Blueprint) ID(name ...string) *Column { return b.BigIncrements(first(name, "id")) }

func (b *Blueprint) increments(name, typ string) *Column {
	c := b.add(name, typ)
	c.IsAutoIncrement, c.IsUnsigned = true, true
	return c
}

func (b *Blueprint) BigIncrements(name string) *Column { return b.increments(name, TypeBigInteger) }
func (b *Blueprint) Increments(name string) *Column    { return b.increments(name, TypeInteger) }
func (b *Blueprint) MediumIncrements(name string) *Column {
	return b.increments(name, TypeMediumInteger)
}

func (b *Blueprint) SmallIncrements(name string) *Column { return b.increments(name, TypeSmallInteger) }

func (b *Blueprint) TinyIncrements(name string) *Column { return b.increments(name, TypeTinyInteger) }

func (b *Blueprint) String(name string, length ...int) *Column {
	c := b.add(name, TypeString)
	c.Length = first(length, 255)
	return c
}

func (b *Blueprint) Char(name string, length ...int) *Column {
	c := b.add(name, TypeChar)
	c.Length = first(length, 255)
	return c
}

func (b *Blueprint) TinyText(name string) *Column   { return b.add(name, TypeTinyText) }
func (b *Blueprint) Text(name string) *Column       { return b.add(name, TypeText) }
func (b *Blueprint) MediumText(name string) *Column { return b.add(name, TypeMediumText) }
func (b *Blueprint) LongText(name string) *Column   { return b.add(name, TypeLongText) }

func (b *Blueprint) TinyInteger(name string) *Column   { return b.add(name, TypeTinyInteger) }
func (b *Blueprint) SmallInteger(name string) *Column  { return b.add(name, TypeSmallInteger) }
func (b *Blueprint) MediumInteger(name string) *Column { return b.add(name, TypeMediumInteger) }
func (b *Blueprint) Integer(name string) *Column       { return b.add(name, TypeInteger) }
func (b *Blueprint) BigInteger(name string) *Column    { return b.add(name, TypeBigInteger) }

func (b *Blueprint) UnsignedTinyInteger(name string) *Column { return b.TinyInteger(name).Unsigned() }

func (b *Blueprint) UnsignedSmallInteger(name string) *Column { return b.SmallInteger(name).Unsigned() }

func (b *Blueprint) UnsignedMediumInteger(name string) *Column {
	return b.MediumInteger(name).Unsigned()
}
func (b *Blueprint) UnsignedInteger(name string) *Column    { return b.Integer(name).Unsigned() }
func (b *Blueprint) UnsignedBigInteger(name string) *Column { return b.BigInteger(name).Unsigned() }

func (b *Blueprint) Boolean(name string) *Column { return b.add(name, TypeBoolean) }

func (b *Blueprint) Decimal(name string, precision, scale int) *Column {
	c := b.add(name, TypeDecimal)
	c.Precision, c.Scale = precision, scale
	return c
}

func (b *Blueprint) Float(name string, precision ...int) *Column {
	c := b.add(name, TypeFloat)
	c.Precision = first(precision, 53)
	return c
}

func (b *Blueprint) Double(name string) *Column { return b.add(name, TypeDouble) }

func (b *Blueprint) Date(name string) *Column        { return b.add(name, TypeDate) }
func (b *Blueprint) Time(name string) *Column        { return b.add(name, TypeTime) }
func (b *Blueprint) TimeTz(name string) *Column      { return b.add(name, TypeTimeTz) }
func (b *Blueprint) DateTime(name string) *Column    { return b.add(name, TypeDateTime) }
func (b *Blueprint) DateTimeTz(name string) *Column  { return b.add(name, TypeDateTimeTz) }
func (b *Blueprint) Timestamp(name string) *Column   { return b.add(name, TypeTimestamp) }
func (b *Blueprint) TimestampTz(name string) *Column { return b.add(name, TypeTimestampTz) }
func (b *Blueprint) Year(name string) *Column        { return b.add(name, TypeYear) }

func (b *Blueprint) JSON(name string) *Column   { return b.add(name, TypeJSON) }
func (b *Blueprint) JSONB(name string) *Column  { return b.add(name, TypeJSONB) }
func (b *Blueprint) Binary(name string) *Column { return b.add(name, TypeBinary) }

func (b *Blueprint) UUID(name ...string) *Column { return b.add(first(name, "uuid"), TypeUUID) }
func (b *Blueprint) ULID(name ...string) *Column { return b.add(first(name, "ulid"), TypeULID) }

// UUIDPrimary is a UUID primary key (HasUuids models).
func (b *Blueprint) UUIDPrimary(name ...string) *Column { return b.UUID(first(name, "id")).Primary() }

// ULIDPrimary is a ULID primary key (HasUlids models).
func (b *Blueprint) ULIDPrimary(name ...string) *Column { return b.ULID(first(name, "id")).Primary() }

func (b *Blueprint) IPAddress(name ...string) *Column {
	return b.add(first(name, "ip_address"), TypeIPAddress)
}

func (b *Blueprint) MACAddress(name ...string) *Column {
	return b.add(first(name, "mac_address"), TypeMACAddress)
}

func (b *Blueprint) Enum(name string, allowed ...string) *Column {
	c := b.add(name, TypeEnum)
	c.Allowed = allowed
	return c
}

// Set is MySQL's SET type; other engines refuse it, as Laravel does.
func (b *Blueprint) Set(name string, allowed ...string) *Column {
	c := b.add(name, TypeSet)
	c.Allowed = allowed
	return c
}

// Geometry is a spatial column; subtype ("point", "polygon", ...) and SRID are optional.
func (b *Blueprint) Geometry(name string, subtype string, srid int) *Column {
	c := b.add(name, TypeGeometry)
	c.Subtype, c.SRID = subtype, srid
	return c
}

func (b *Blueprint) Geography(name string, subtype string, srid int) *Column {
	c := b.add(name, TypeGeography)
	c.Subtype, c.SRID = subtype, cmp.Or(srid, 4326)
	return c
}

// Vector is a pgvector / MySQL 9 vector column.
func (b *Blueprint) Vector(name string, dimensions int) *Column {
	c := b.add(name, TypeVector)
	c.Length = dimensions
	return c
}

// ForeignID is an unsigned big integer meant for a foreign key; chain
// Constrained() to add the constraint.
func (b *Blueprint) ForeignID(name string) *Column   { return b.UnsignedBigInteger(name) }
func (b *Blueprint) ForeignUUID(name string) *Column { return b.UUID(name) }
func (b *Blueprint) ForeignULID(name string) *Column { return b.ULID(name) }

// ForeignIDFor adds a key column referencing a model's table, typed after
// its key: ForeignIDFor(Users.Table) adds user_id (foreignIdFor), and
// Constrained() then references the model's primary key column. Models with
// manual keys have no known column type; declare their column explicitly.
func (b *Blueprint) ForeignIDFor[M any](t *orm.Table[M], column ...string) *Column {
	if t.PrimaryKey == "" {
		b.fail(fmt.Errorf("schema: ForeignIDFor(%s) on table %s: the model has no primary key to reference", t.Name, b.table))
	}
	name := b.keyColumn(t.Name, t.PrimaryKey, column)
	var c *Column
	switch t.KeyType {
	case orm.KeyAutoIncrement:
		c = b.ForeignID(name)
	case orm.KeyUUID:
		c = b.ForeignUUID(name)
	case orm.KeyULID:
		c = b.ForeignULID(name)
	default:
		b.fail(fmt.Errorf("schema: ForeignIDFor(%s) on table %s: the model's key is set manually, so its column type is unknown; declare %s with its type and Constrained(%q, %q)",
			t.Name, b.table, name, t.Name, t.PrimaryKey))
		c = &Column{Name: name, bp: b}
	}
	c.refTable, c.refColumn = t.Name, t.PrimaryKey
	return c
}

// keyColumn is the column given, or the one a model's key is stored in
// elsewhere: user_id.
func (b *Blueprint) keyColumn(table, primaryKey string, column []string) string {
	if len(column) > 0 && column[0] != "" {
		return column[0]
	}
	if primaryKey == "" {
		b.fail(fmt.Errorf("schema: cannot name the key column for table %s on %s: it has no primary key; pass the column", table, b.table))
	}
	return singular(table) + "_" + primaryKey
}

// Timestamps adds nullable created_at and updated_at.
func (b *Blueprint) Timestamps() {
	b.Timestamp("created_at").Nullable()
	b.Timestamp("updated_at").Nullable()
}

func (b *Blueprint) TimestampsTz() {
	b.TimestampTz("created_at").Nullable()
	b.TimestampTz("updated_at").Nullable()
}

// NullableTimestamps is an alias of Timestamps.
func (b *Blueprint) NullableTimestamps() { b.Timestamps() }

// Datetimes adds nullable created_at and updated_at datetime columns.
func (b *Blueprint) Datetimes() {
	b.DateTime("created_at").Nullable()
	b.DateTime("updated_at").Nullable()
}

// SoftDeletes adds a nullable deleted_at.
func (b *Blueprint) SoftDeletes(name ...string) *Column {
	return b.Timestamp(first(name, "deleted_at")).Nullable()
}

func (b *Blueprint) SoftDeletesTz(name ...string) *Column {
	return b.TimestampTz(first(name, "deleted_at")).Nullable()
}

// Morphs adds {name}_id and {name}_type with a composite index.
func (b *Blueprint) Morphs(name string)     { b.morphs(name, b.UnsignedBigInteger, false) }
func (b *Blueprint) UUIDMorphs(name string) { b.morphs(name, b.uuidCol, false) }
func (b *Blueprint) ULIDMorphs(name string) { b.morphs(name, b.ulidCol, false) }
func (b *Blueprint) NullableMorphs(name string) {
	b.morphs(name, b.UnsignedBigInteger, true)
}
func (b *Blueprint) NullableUUIDMorphs(name string) { b.morphs(name, b.uuidCol, true) }
func (b *Blueprint) NullableULIDMorphs(name string) { b.morphs(name, b.ulidCol, true) }

func (b *Blueprint) uuidCol(name string) *Column { return b.UUID(name) }
func (b *Blueprint) ulidCol(name string) *Column { return b.ULID(name) }

func (b *Blueprint) morphs(name string, id func(string) *Column, nullable bool) {
	idc, typ := id(name+"_id"), b.String(name+"_type")
	if nullable {
		idc.Nullable()
		typ.Nullable()
	}
	b.Index(name+"_type", name+"_id")
}

// RememberToken adds a nullable remember_token string(100).
func (b *Blueprint) RememberToken() *Column { return b.String("remember_token", 100).Nullable() }

// ---------------------------------------------------------------------------
// Column modifiers
// ---------------------------------------------------------------------------

func (c *Column) Nullable(nullable ...bool) *Column {
	c.IsNullable = len(nullable) == 0 || nullable[0]
	return c
}
func (c *Column) Unsigned() *Column              { c.IsUnsigned = true; return c }
func (c *Column) Default(v any) *Column          { c.HasDefault, c.DefaultValue = true, v; return c }
func (c *Column) Comment(text string) *Column    { c.CommentText = text; return c }
func (c *Column) Charset(charset string) *Column { c.CharsetName = charset; return c }
func (c *Column) Collation(coll string) *Column  { c.CollationName = coll; return c }
func (c *Column) After(column string) *Column    { c.AfterColumn = column; return c } // MySQL
func (c *Column) First() *Column                 { c.IsFirst = true; return c }       // MySQL
func (c *Column) Invisible() *Column             { c.IsInvisible = true; return c }   // MySQL

// AutoIncrement makes the column an auto-incrementing primary key.
func (c *Column) AutoIncrement() *Column { c.IsAutoIncrement = true; return c }

// From sets the starting value of an auto-incrementing column.
func (c *Column) From(start int) *Column { c.StartingValue = start; return c }

// StoredAs makes the column a stored generated column (MATERIALIZED on ClickHouse).
func (c *Column) StoredAs(expr string) *Column { c.StoredExpr = expr; return c }

// VirtualAs makes the column a virtual generated column (ALIAS on ClickHouse).
func (c *Column) VirtualAs(expr string) *Column { c.VirtualExpr = expr; return c }

// GeneratedAs makes the column a Postgres identity column (GENERATED BY DEFAULT AS IDENTITY).
func (c *Column) GeneratedAs() *Column { c.Identity = "BY DEFAULT"; return c }

// Always makes an identity column GENERATED ALWAYS.
func (c *Column) Always() *Column { c.Identity = "ALWAYS"; return c }

// UseCurrent defaults the column to the current timestamp.
func (c *Column) UseCurrent() *Column { return c.Default(currentTimestamp{}) }

// UseCurrentOnUpdate sets the column to CURRENT_TIMESTAMP on update (MySQL).
func (c *Column) UseCurrentOnUpdate() *Column { c.OnUpdateNow = true; return c }

// Change marks the definition as a modification of an existing column
// (SQLite rebuilds the table, like Laravel). On Postgres an enum's CHECK
// constraint, <table>_<column>_check, is replaced with the new values.
func (c *Column) Change() *Column { c.Changed = true; return c }

func (c *Column) Primary() *Column      { c.bp.Primary(c.Name); return c }
func (c *Column) Unique() *Column       { c.bp.Unique(c.Name); return c }
func (c *Column) Index() *Column        { c.bp.Index(c.Name); return c }
func (c *Column) FullText() *Column     { c.bp.FullText(c.Name); return c }
func (c *Column) SpatialIndex() *Column { c.bp.SpatialIndex(c.Name); return c }
func (c *Column) HnswIndex(metric orm.VectorMetric) *Column {
	c.bp.HnswIndex(c.Name, metric)
	return c
}

// Constrained adds a foreign key on the column. The table defaults to the
// model given to ForeignIDFor, or is guessed from the column name
// (category_id -> categories); the column defaults to the model's primary
// key, or "id".
func (c *Column) Constrained(tableAndColumn ...string) *Foreign {
	col := cmp.Or(c.refColumn, "id")
	if len(tableAndColumn) > 1 {
		col = tableAndColumn[1]
	}
	table := first(tableAndColumn, c.refTable)
	if table == "" {
		stem, ok := strings.CutSuffix(c.Name, "_"+col)
		if !ok || stem == "" {
			c.bp.fail(fmt.Errorf("schema: cannot guess the table %s.%s references: the name does not end in _%s; pass the table to Constrained",
				c.bp.table, c.Name, col))
		}
		table = pluralize(stem)
	}
	return c.bp.Foreign(c.Name).References(col).On(table)
}

// ---------------------------------------------------------------------------
// Indexes and foreign keys
// ---------------------------------------------------------------------------

// Index is an index definition with chainable modifiers.
type Index struct {
	IndexName string
	Columns   []string
	Algo      string // btree, hash, gin, gist, hnsw, ...; ClickHouse skip index type
	Lang      string // full text language (Postgres)
	VectorOp  string // pgvector operator class (vector_cosine_ops, ...)
}

func vectorOpClass(m orm.VectorMetric) string {
	switch m {
	case orm.Cosine:
		return "vector_cosine_ops"
	case orm.InnerProduct:
		return "vector_ip_ops"
	default:
		return "vector_l2_ops"
	}
}

func (i *Index) Name(name string) *Index { i.IndexName = name; return i }

// Algorithm sets the index method: USING on Postgres, MySQL and DuckDB; the
// data skipping index TYPE on ClickHouse (bloom_filter by default, or
// minmax, set(100), ...).
func (i *Index) Algorithm(algo string) *Index { i.Algo = algo; return i }
func (i *Index) Language(lang string) *Index  { i.Lang = lang; return i }

// Primary sets the primary key. On ClickHouse it is the MergeTree primary
// key, which also becomes the sorting key unless OrderBy sets one.
func (b *Blueprint) Primary(cols ...string) *Index { return b.index(cmdPrimary, "primary", cols) }

func (b *Blueprint) Unique(cols ...string) *Index   { return b.index(cmdUnique, "unique", cols) }
func (b *Blueprint) Index(cols ...string) *Index    { return b.index(cmdIndex, "index", cols) }
func (b *Blueprint) FullText(cols ...string) *Index { return b.index(cmdFullText, "fulltext", cols) }

func (b *Blueprint) SpatialIndex(cols ...string) *Index {
	return b.index(cmdSpatial, "spatialindex", cols)
}

// HnswIndex builds a pgvector HNSW index over one vector column, making
// Nearest ordering an index-backed search instead of a sequential scan.
func (b *Blueprint) HnswIndex(column string, metric orm.VectorMetric) *Index {
	idx := b.index(cmdHnsw, "hnsw", []string{column})
	idx.Algo = "hnsw"
	idx.VectorOp = vectorOpClass(metric)
	return idx
}

func (b *Blueprint) index(kind commandKind, suffix string, cols []string) *Index {
	if len(cols) == 0 {
		b.fail(fmt.Errorf("schema: cannot %s on table %s without columns", kind, b.table))
	}
	idx := &Index{IndexName: indexName(b.table, suffix, cols), Columns: cols}
	b.commands = append(b.commands, &command{kind: kind, index: idx, columns: cols})
	return idx
}

// RenameIndex renames an index.
func (b *Blueprint) RenameIndex(from, to string) {
	b.commands = append(b.commands, &command{kind: cmdRenameIndex, from: from, to: to})
}

// Foreign starts a foreign key on cols.
func (b *Blueprint) Foreign(cols ...string) *Foreign {
	f := &Foreign{Columns: cols, Name: indexName(b.table, "foreign", cols)}
	b.commands = append(b.commands, &command{kind: cmdForeign, foreign: f})
	return f
}

// Foreign is a foreign key definition with chainable modifiers.
type Foreign struct {
	Name       string
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDel      string
	OnUpd      string
	Defer      bool
	InitDefer  bool
}

func (f *Foreign) References(cols ...string) *Foreign { f.RefColumns = cols; return f }
func (f *Foreign) On(table string) *Foreign           { f.RefTable = table; return f }
func (f *Foreign) Named(name string) *Foreign         { f.Name = name; return f }
func (f *Foreign) OnDelete(action string) *Foreign    { f.OnDel = action; return f }
func (f *Foreign) OnUpdate(action string) *Foreign    { f.OnUpd = action; return f }
func (f *Foreign) CascadeOnDelete() *Foreign          { return f.OnDelete("cascade") }
func (f *Foreign) RestrictOnDelete() *Foreign         { return f.OnDelete("restrict") }
func (f *Foreign) NullOnDelete() *Foreign             { return f.OnDelete("set null") }
func (f *Foreign) NoActionOnDelete() *Foreign         { return f.OnDelete("no action") }
func (f *Foreign) CascadeOnUpdate() *Foreign          { return f.OnUpdate("cascade") }
func (f *Foreign) RestrictOnUpdate() *Foreign         { return f.OnUpdate("restrict") }
func (f *Foreign) NullOnUpdate() *Foreign             { return f.OnUpdate("set null") }
func (f *Foreign) NoActionOnUpdate() *Foreign         { return f.OnUpdate("no action") }

// Deferrable makes the constraint deferrable (Postgres).
func (f *Foreign) Deferrable() *Foreign { f.Defer = true; return f }

// InitiallyDeferred makes the constraint deferrable and initially deferred (Postgres).
func (f *Foreign) InitiallyDeferred() *Foreign { f.Defer, f.InitDefer = true, true; return f }

// ---------------------------------------------------------------------------
// Alter commands
// ---------------------------------------------------------------------------

func (b *Blueprint) DropColumn(column string, more ...string) {
	b.commands = append(b.commands, &command{kind: cmdDropColumn, columns: append([]string{column}, more...)})
}

func (b *Blueprint) RenameColumn(from, to string) {
	b.commands = append(b.commands, &command{kind: cmdRenameColumn, from: from, to: to})
}

// DropIndex drops the index named name (dropIndex('name')).
func (b *Blueprint) DropIndex(name string) { b.dropNamed(cmdDropIndex, name) }

// DropIndexOn drops the index Index(columns...) created (dropIndex(['a', 'b'])).
func (b *Blueprint) DropIndexOn(column string, more ...string) {
	b.dropOn(cmdDropIndex, "index", column, more)
}

// DropUnique drops the unique index named name.
func (b *Blueprint) DropUnique(name string) { b.dropNamed(cmdDropIndex, name) }

// DropUniqueOn drops the unique index Unique(columns...) created.
func (b *Blueprint) DropUniqueOn(column string, more ...string) {
	b.dropOn(cmdDropIndex, "unique", column, more)
}

// DropFullText drops the full text index named name.
func (b *Blueprint) DropFullText(name string) { b.dropNamed(cmdDropIndex, name) }

// DropFullTextOn drops the full text index FullText(columns...) created.
func (b *Blueprint) DropFullTextOn(column string, more ...string) {
	b.dropOn(cmdDropIndex, "fulltext", column, more)
}

// DropSpatialIndex drops the spatial index named name.
func (b *Blueprint) DropSpatialIndex(name string) { b.dropNamed(cmdDropIndex, name) }

// DropSpatialIndexOn drops the spatial index SpatialIndex(columns...) created.
func (b *Blueprint) DropSpatialIndexOn(column string, more ...string) {
	b.dropOn(cmdDropIndex, "spatialindex", column, more)
}

// DropForeign drops the foreign key named name.
func (b *Blueprint) DropForeign(name string) { b.dropNamed(cmdDropForeign, name) }

// DropForeignOn drops the foreign key Foreign(columns...) created.
func (b *Blueprint) DropForeignOn(column string, more ...string) {
	b.dropOn(cmdDropForeign, "foreign", column, more)
}

func (b *Blueprint) DropPrimary() {
	b.commands = append(b.commands, &command{kind: cmdDropPrimary, index: &Index{IndexName: indexName(b.table, "primary", nil)}})
}

// DropConstrainedForeignID drops the foreign key on column and the column.
func (b *Blueprint) DropConstrainedForeignID(column string) {
	b.DropForeignOn(column)
	b.DropColumn(column)
}

// DropForeignIDFor drops the key column ForeignIDFor added; drop its
// foreign key first, or use DropConstrainedForeignIDFor.
func (b *Blueprint) DropForeignIDFor[M any](t *orm.Table[M], column ...string) {
	b.DropColumn(b.keyColumn(t.Name, t.PrimaryKey, column))
}

// DropConstrainedForeignIDFor drops the foreign key and key column that
// ForeignIDFor(...).Constrained() added.
func (b *Blueprint) DropConstrainedForeignIDFor[M any](t *orm.Table[M], column ...string) {
	b.DropConstrainedForeignID(b.keyColumn(t.Name, t.PrimaryKey, column))
}

func (b *Blueprint) DropTimestamps()    { b.DropColumn("created_at", "updated_at") }
func (b *Blueprint) DropTimestampsTz()  { b.DropTimestamps() }
func (b *Blueprint) DropSoftDeletes()   { b.DropColumn("deleted_at") }
func (b *Blueprint) DropSoftDeletesTz() { b.DropSoftDeletes() }
func (b *Blueprint) DropRememberToken() { b.DropColumn("remember_token") }
func (b *Blueprint) DropMorphs(name string) {
	b.DropIndexOn(name+"_type", name+"_id")
	b.DropColumn(name+"_type", name+"_id")
}

func (b *Blueprint) dropNamed(kind commandKind, name string) {
	if name == "" {
		b.fail(fmt.Errorf("schema: cannot %s on table %s: no name given", kind, b.table))
	}
	b.commands = append(b.commands, &command{kind: kind, index: &Index{IndexName: name}})
}

func (b *Blueprint) dropOn(kind commandKind, suffix, column string, more []string) {
	cols := append([]string{column}, more...)
	b.commands = append(b.commands, &command{kind: kind, index: &Index{IndexName: indexName(b.table, suffix, cols), Columns: cols}})
}

// indexName follows Laravel's convention: users_email_unique.
func indexName(table, suffix string, cols []string) string {
	parts := append(append([]string{table}, cols...), suffix)
	return strings.ToLower(strings.NewReplacer("-", "_", ".", "_").Replace(strings.Join(parts, "_")))
}

func first[T comparable](vs []T, def T) T {
	var zero T
	if len(vs) > 0 && vs[0] != zero {
		return vs[0]
	}
	return def
}
