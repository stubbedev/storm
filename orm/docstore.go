package orm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// DocStore is a non-SQL backend behind the same builder: MongoDB and Qdrant
// implement it, and every typed query method runs against it unchanged
// when its connection is a document store. Methods that have no translation
// (joins, unions, raw SQL) fail with ErrNotTranslatable naming the part.
type DocStore interface {
	// FindDocs returns documents (column name to value) matching the plan.
	// Plan.Vector makes it a similarity search; a store without vectors
	// returns ErrNotTranslatable for it.
	FindDocs(ctx context.Context, p Plan) ([]map[string]any, error)
	// InsertDocs stores new documents, each identified by its key column;
	// a key that is already taken fails with a DuplicateKeyError.
	InsertDocs(ctx context.Context, table, key string, docs []map[string]any) error
	// UpsertDocs inserts documents, or, where one matching on all of
	// uniqueBy exists, overwrites only its update columns.
	UpsertDocs(ctx context.Context, table, key string, docs []map[string]any, uniqueBy, update []string) error
	// UpdateDocs applies u to matching documents and reports how many
	// matched.
	UpdateDocs(ctx context.Context, p Plan, u DocUpdate) (int64, error)
	// DeleteDocs removes matching documents and reports how many.
	DeleteDocs(ctx context.Context, p Plan) (int64, error)
	// CountDocs counts matching documents.
	CountDocs(ctx context.Context, p Plan) (int64, error)
	// AggregateDocs computes fn over column across matching documents,
	// returning nil when none has a value. A store that cannot aggregate
	// server-side returns ErrNotTranslatable.
	AggregateDocs(ctx context.Context, p Plan, fn Aggregate, column string) (any, error)
}

// DuplicateKeyError reports an insert whose document key is already
// taken; IsUniqueViolation recognises it.
type DuplicateKeyError struct {
	Table string
	Key   string
	Err   error
}

func (e *DuplicateKeyError) Error() string {
	return fmt.Sprintf("orm: duplicate %s in %s: %v", e.Key, e.Table, e.Err)
}

func (e *DuplicateKeyError) Unwrap() error { return e.Err }

// Aggregate is a column aggregate: Sum, Avg, Min or Max.
type Aggregate int

const (
	AggregateSum Aggregate = iota + 1
	AggregateAvg
	AggregateMin
	AggregateMax
)

// String is the aggregate's SQL function name.
func (a Aggregate) String() string {
	switch a {
	case AggregateSum:
		return "SUM"
	case AggregateAvg:
		return "AVG"
	case AggregateMin:
		return "MIN"
	case AggregateMax:
		return "MAX"
	}
	return fmt.Sprintf("Aggregate(%d)", int(a))
}

// DocUpdate is the change UpdateDocs applies: Set overwrites fields (nil
// clears one), Inc adds to numeric fields (a negative amount subtracts).
// A store that cannot add atomically refuses Inc with ErrNotTranslatable.
type DocUpdate struct {
	Set map[string]any
	Inc map[string]any
}

// AddDocStore registers a document store as a connection. Tables directed
// at it with connection= (or Query.On) run their typed queries against the
// store instead of SQL.
func AddDocStore(name string, ds DocStore) {
	connMu.Lock()
	conns[name] = Conn{Doc: ds}
	connMu.Unlock()
}

func (q Query[M]) docstore(ctx context.Context) (DocStore, bool, error) {
	c, err := lookup(ctx, q.connName())
	if err != nil {
		return nil, false, err
	}
	return c.Doc, c.Doc != nil, nil
}

func docStoreFor(ctx context.Context, conn string) (DocStore, bool, error) {
	c, err := lookup(ctx, conn)
	if err != nil {
		return nil, false, err
	}
	return c.Doc, c.Doc != nil, nil
}

// plan compiles the query for a document store, refusing the parts that
// only exist in SQL.
func (q Query[M]) plan() (Plan, error) {
	p := Plan{Table: q.table.Name, Key: q.table.PrimaryKey, Limit: q.limit, Offset: q.offset}
	notSupported := func(what string) (Plan, error) {
		return Plan{}, &ErrNotTranslatable{Reason: what}
	}
	if len(q.joins) > 0 {
		return notSupported("joins")
	}
	if len(q.groups) > 0 || len(q.having) > 0 {
		return notSupported("groupBy / having")
	}
	if len(q.unions) > 0 {
		return notSupported("unions")
	}
	if q.distinct {
		return notSupported("distinct")
	}
	if len(q.extras) > 0 {
		return notSupported("addSelect aggregates (withCount and friends)")
	}
	if len(q.where) > 0 {
		ir := And(q.where...).ir
		if ir == nil {
			return notSupported("a raw or SQL-specific condition (whereRaw, whereExists, subqueries)")
		}
		if err := checkSameTable(ir, q.table.Name); err != nil {
			return Plan{}, err
		}
		p.Where = ir
	}
	for _, o := range q.orders {
		switch {
		case o.vec != nil:
			if p.Vector != nil {
				return notSupported("more than one vector order")
			}
			p.Vector = o.vec
		case o.col != nil:
			p.Orders = append(p.Orders, PlanOrder{Column: o.col.Name(), Desc: o.desc})
		default:
			return notSupported("orderByRaw or ordering by an expression")
		}
	}
	for _, c := range q.selects {
		p.Columns = append(p.Columns, c.Name())
	}
	return p, nil
}

// planOf is plan with the error wrapped for the calling method's context.
func (q Query[M]) planOf(method string) (Plan, error) {
	p, err := q.plan()
	if err != nil {
		return p, fmt.Errorf("orm: %s on %s: %w", method, q.table.Name, err)
	}
	return p, nil
}

func (q Query[M]) findDocs(ctx context.Context) ([]M, error) {
	ds, ok, err := q.docstore(ctx)
	if err != nil || !ok {
		return nil, err
	}
	p, err := q.planOf("query")
	if err != nil {
		return nil, err
	}
	start := time.Now()
	docs, err := ds.FindDocs(ctx, p)
	emitDoc(q.connName(), "find", p, time.Since(start), err)
	if err != nil {
		return nil, err
	}
	models := make([]M, 0, len(docs))
	for _, doc := range docs {
		m, err := hydrate(q.table, doc)
		if err != nil {
			return nil, err
		}
		q.table.sync(&m)
		models = append(models, m)
	}
	if err := q.finish(ctx, models); err != nil {
		return nil, err
	}
	return models, nil
}

func (q Query[M]) countDocs(ctx context.Context) (int64, bool, error) {
	ds, ok, err := q.docstore(ctx)
	if err != nil || !ok {
		return 0, false, err
	}
	p, err := q.planOf("count")
	if err != nil {
		return 0, true, err
	}
	start := time.Now()
	n, err := ds.CountDocs(ctx, p)
	emitDoc(q.connName(), "count", p, time.Since(start), err)
	return n, true, err
}

func aggregateDocs[V any](ctx context.Context, ds DocStore, conn string, p Plan, fn Aggregate, column string) (V, error) {
	var zero V
	start := time.Now()
	raw, err := ds.AggregateDocs(ctx, p, fn, column)
	emitDoc(conn, strings.ToLower(fn.String())+" "+column+" of", p, time.Since(start), err)
	if err != nil {
		return zero, fmt.Errorf("orm: %s of %s on %s: %w", fn, column, p.Table, err)
	}
	var v *V
	if err := (nullable{&v}).Scan(raw); err != nil {
		return zero, fmt.Errorf("orm: %s of %s on %s: %w", fn, column, p.Table, err)
	}
	return aggregated(fn, v, p.Table)
}

func (q Query[M]) docMaps(method string, models []*M) ([]map[string]any, error) {
	t := q.table
	docs := make([]map[string]any, len(models))
	for i, m := range models {
		doc := make(map[string]any, len(t.Columns))
		for _, c := range t.Columns {
			if v := t.dbValue(m, c); v != nil {
				doc[c] = v
			}
		}
		if t.PrimaryKey != "" && t.KeyType == KeyAutoIncrement && isZero(t.key(m)) {
			return nil, fmt.Errorf("orm: %s: %s needs a uuid, ulid or manual key on a document store", method, t.Name)
		}
		docs[i] = doc
	}
	return docs, nil
}

func (q Query[M]) writeDocs(ctx context.Context, method string, models []*M) error {
	ds, ok, err := q.docstore(ctx)
	if err != nil || !ok {
		return err
	}
	docs, err := q.docMaps(method, models)
	if err != nil {
		return err
	}
	start := time.Now()
	err = ds.InsertDocs(ctx, q.table.Name, q.table.PrimaryKey, docs)
	emitDocRaw(q.connName(), fmt.Sprintf("insert %s (%d documents)", q.table.Name, len(docs)), time.Since(start), err)
	return err
}

// insertOrIgnoreDocs inserts only the documents whose key combination is
// neither stored yet nor repeated earlier in docs; like a NULL in SQL, a
// document missing a key column never collides. The read-then-write gap
// means a concurrent writer can still insert the same key; a unique index
// on keys makes that safe.
func insertOrIgnoreDocs(ctx context.Context, ds DocStore, table, docKeyCol string, docs []map[string]any, keys []string) error {
	key := func(d map[string]any) (string, bool) {
		for _, k := range keys {
			if _, ok := d[k]; !ok {
				return "", false
			}
		}
		return docKey(keys, d), true
	}
	var keyed []map[string]any
	for _, d := range docs {
		if _, ok := key(d); ok {
			keyed = append(keyed, d)
		}
	}
	taken := map[string]bool{}
	if len(keyed) > 0 {
		parts := make([]Node, len(keys))
		for i, k := range keys {
			vals := make([]any, len(keyed))
			for j, d := range keyed {
				vals[j] = d[k]
			}
			parts[i] = List{Table: table, Column: k, Values: vals}
		}
		existing, err := ds.FindDocs(ctx, Plan{Table: table, Key: docKeyCol, Columns: keys, Where: Composite{Parts: parts}})
		if err != nil {
			return err
		}
		for _, d := range existing {
			taken[docKey(keys, d)] = true
		}
	}
	fresh, _ := splitByKey(docs, key, taken)
	return ds.InsertDocs(ctx, table, docKeyCol, fresh)
}

func docKey(keys []string, doc map[string]any) string {
	vals := make([]any, len(keys))
	for i, k := range keys {
		vals[i] = doc[k]
	}
	return keyString(vals)
}

func (q Query[M]) updateDocs(ctx context.Context, sets []colSet) (int64, bool, error) {
	ds, ok, err := q.docstore(ctx)
	if err != nil || !ok {
		return 0, false, err
	}
	p, err := q.planOf("update")
	if err != nil {
		return 0, true, err
	}
	n, err := updateDocs(ctx, ds, q.connName(), p, sets)
	return n, true, err
}

func updateDocs(ctx context.Context, ds DocStore, conn string, p Plan, sets []colSet) (int64, error) {
	u, err := docUpdate(sets)
	if err != nil {
		return 0, fmt.Errorf("orm: update on %s: %w", p.Table, err)
	}
	if p, err = mutationPlan(ctx, ds, conn, p); err != nil {
		return 0, fmt.Errorf("orm: update on %s: %w", p.Table, err)
	}
	start := time.Now()
	n, err := ds.UpdateDocs(ctx, p, u)
	emitDoc(conn, "update", p, time.Since(start), err)
	return n, err
}

func deleteDocs(ctx context.Context, ds DocStore, conn string, p Plan) (int64, error) {
	p, err := mutationPlan(ctx, ds, conn, p)
	if err != nil {
		return 0, fmt.Errorf("orm: delete on %s: %w", p.Table, err)
	}
	start := time.Now()
	n, err := ds.DeleteDocs(ctx, p)
	emitDoc(conn, "delete", p, time.Since(start), err)
	return n, err
}

// mutationPlan resolves a limited or offset mutation to the keys of the
// documents it selects, since stores update and delete every match.
func mutationPlan(ctx context.Context, ds DocStore, conn string, p Plan) (Plan, error) {
	if p.Limit == 0 && p.Offset == 0 {
		p.Orders = nil
		return p, nil
	}
	key := p.Key
	if key == "" {
		return p, errors.New("a limited update or delete needs a key column")
	}
	p.Columns = []string{key}
	start := time.Now()
	docs, err := ds.FindDocs(ctx, p)
	emitDoc(conn, "find keys", p, time.Since(start), err)
	if err != nil {
		return p, err
	}
	keys := make([]any, len(docs))
	for i, d := range docs {
		keys[i] = d[key]
	}
	return Plan{Table: p.Table, Key: key, Where: List{Column: key, Values: keys}}, nil
}

// docUpdate is sets as a document update: literals are set, increments
// added; other expressions (SetExpr) have no document form.
func docUpdate(sets []colSet) (DocUpdate, error) {
	u := DocUpdate{Set: map[string]any{}, Inc: map[string]any{}}
	for _, s := range sets {
		switch {
		case s.inc != nil:
			u.Inc[s.col] = s.inc
		case s.expr != nil:
			return DocUpdate{}, &ErrNotTranslatable{Reason: "an expression assigned to " + s.col}
		default:
			u.Set[s.col] = s.raw
		}
	}
	return u, nil
}

// docFields is sets as the fields of a new document, which only literals
// can fill.
func docFields(sets []colSet) (map[string]any, error) {
	fields := make(map[string]any, len(sets))
	for _, s := range sets {
		if s.expr != nil {
			return nil, &ErrNotTranslatable{Reason: "an expression inserted into " + s.col}
		}
		fields[s.col] = s.raw
	}
	return fields, nil
}

func (q Query[M]) deleteDocs(ctx context.Context) (int64, bool, error) {
	ds, ok, err := q.docstore(ctx)
	if err != nil || !ok {
		return 0, false, err
	}
	p, err := q.planOf("delete")
	if err != nil {
		return 0, true, err
	}
	n, err := deleteDocs(ctx, ds, q.connName(), p)
	return n, true, err
}

// checkSameTable rejects conditions on other tables' columns (joins),
// which document stores cannot evaluate.
func checkSameTable(n Node, table string) error {
	foreign := func(t string) bool { return t != "" && t != table }
	switch c := n.(type) {
	case Field:
		if foreign(c.Table) {
			return &ErrNotTranslatable{Reason: "conditions on a column of " + c.Table}
		}
	case List:
		if foreign(c.Table) {
			return &ErrNotTranslatable{Reason: "conditions on a column of " + c.Table}
		}
	case Range:
		if foreign(c.Table) {
			return &ErrNotTranslatable{Reason: "conditions on a column of " + c.Table}
		}
	case NullTest:
		if foreign(c.Table) {
			return &ErrNotTranslatable{Reason: "conditions on a column of " + c.Table}
		}
	case Composite:
		for _, part := range c.Parts {
			if err := checkSameTable(part, table); err != nil {
				return err
			}
		}
	case Negate:
		return checkSameTable(c.Part, table)
	case notTranslatable:
		return &ErrNotTranslatable{Reason: c.reason}
	}
	return nil
}

// hydrate fills a model from a document's column values, reusing the
// driver-value conversion SQL scanning uses.
func hydrate[M any](t *Table[M], doc map[string]any) (M, error) {
	var m M
	for _, c := range t.Columns {
		v, ok := doc[c]
		if !ok {
			continue
		}
		if err := (nullable{t.Ptr(&m, c)}).Scan(v); err != nil {
			return m, fmt.Errorf("orm: hydrating %s.%s: %w", t.Name, c, err)
		}
	}
	return m, nil
}

// emitDoc reports a document-store operation to listeners and the query
// log, the same funnel SQL statements go through; the SQL field carries a
// readable rendering of the plan.
func emitDoc(conn, op string, p Plan, d time.Duration, err error) {
	emitDocRaw(conn, planSummary(op, p), d, err)
}

func emitDocRaw(conn, summary string, d time.Duration, err error) {
	Emit(QueryEvent{Connection: conn, SQL: summary, Duration: d, Err: err})
}

// planSummary renders a plan deterministically: find documents where
// (views > 1) order by views desc limit 10.
func planSummary(op string, p Plan) string {
	var b strings.Builder
	b.WriteString(op)
	b.WriteString(" ")
	b.WriteString(p.Table)
	if p.Where != nil {
		b.WriteString(" where ")
		writeNode(&b, p.Where)
	}
	if p.Vector != nil {
		b.WriteString(" nearest ")
		b.WriteString(p.Vector.Column)
	}
	for _, o := range p.Orders {
		b.WriteString(" order by ")
		b.WriteString(o.Column)
		if o.Desc {
			b.WriteString(" desc")
		}
	}
	if p.Limit > 0 {
		fmt.Fprintf(&b, " limit %d", p.Limit)
	}
	if p.Offset > 0 {
		fmt.Fprintf(&b, " offset %d", p.Offset)
	}
	return b.String()
}

func writeNode(b *strings.Builder, n Node) {
	switch c := n.(type) {
	case Field:
		fmt.Fprintf(b, "%s %s %s", c.Column, strings.ToLower(c.Op), fmt.Sprint(c.Value))
	case List:
		if c.Not {
			b.WriteString("not ")
		}
		fmt.Fprintf(b, "%s in [%s]", c.Column, joinValues(c.Values))
	case Range:
		if c.Not {
			b.WriteString("not ")
		}
		fmt.Fprintf(b, "%s between %s and %s", c.Column, fmt.Sprint(c.Lo), fmt.Sprint(c.Hi))
	case NullTest:
		if c.Not {
			fmt.Fprintf(b, "%s is not null", c.Column)
		} else {
			fmt.Fprintf(b, "%s is null", c.Column)
		}
	case Composite:
		sep := " and "
		if c.Or {
			sep = " or "
		}
		for i, p := range c.Parts {
			if i > 0 {
				b.WriteString(sep)
			}
			b.WriteString("(")
			writeNode(b, p)
			b.WriteString(")")
		}
	case Negate:
		b.WriteString("not (")
		writeNode(b, c.Part)
		b.WriteString(")")
	}
}

func joinValues(vs []any) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, ", ")
}
