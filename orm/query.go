package orm

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Query is an immutable query builder for model M; every method returns a
// new Query, so partially built queries can be shared. SQL is rendered only
// when the query runs, in the dialect of the connection it runs on.
type Query[M any, K comparable] struct {
	bound
	table    *Table[M, K]
	onConn   string // On(): explicit connection name
	alias    string // table alias, for self-referencing subqueries
	distinct bool
	selects  []AnyColumn[M] // nil: all persisted columns
	extras   []selectAs     // AddSelectAs / withCount / withSum ...
	joins    []frag
	where    []Cond[M]
	groups   []AnyExpr
	having   []Cond[M]
	orders   []Order[M]
	limit    int
	offset   int
	unions   []union[M, K]
	lock     rowLock
	eager    []eagerFn[M]

	trashed    trashedMode
	without    []string // removed global scopes
	withoutAll bool
	scoped     bool    // global scopes already applied
	err        error   // the first invalid builder call, reported when the query runs
	perParent  AnyExpr // set by eager loads: limit and offset apply per value of it
}

type selectAs struct {
	dest string
	f    frag
}

type union[M any, K comparable] struct {
	all bool
	q   Query[M, K]
}

type eagerFn[M any] func(ctx context.Context, conn string, models []M) error

type trashedMode int

const (
	withoutTrashed trashedMode = iota
	withTrashed
	onlyTrashed
)

// Query starts a builder for the table (Model::query()).
func (t *Table[M, K]) Query() Query[M, K] { return Query[M, K]{table: t} }

// ---------------------------------------------------------------------------
// Where clauses
// ---------------------------------------------------------------------------

// Where adds conditions joined with AND. Group with orm.Or / orm.And for
// nested clauses.
func (q Query[M, K]) Where(conds ...Cond[M]) Query[M, K] {
	q.where = append(slices.Clip(q.where), conds...)
	return q
}

// OrWhere ORs the given conditions (ANDed together) with everything so far.
func (q Query[M, K]) OrWhere(conds ...Cond[M]) Query[M, K] {
	if len(q.where) == 0 {
		return q.Where(conds...)
	}
	q.where = []Cond[M]{Or(And(q.where...), And(conds...))}
	return q
}

func (q Query[M, K]) WhereNot(conds ...Cond[M]) Query[M, K]   { return q.Where(Not(And(conds...))) }
func (q Query[M, K]) OrWhereNot(conds ...Cond[M]) Query[M, K] { return q.OrWhere(Not(And(conds...))) }

// WhereOf adds conditions on a joined model: after
// Join(Posts.Table, ...), WhereOf(Posts.Published.Eq(true)).
func (q Query[M, K]) WhereOf[O any](conds ...Cond[O]) Query[M, K] {
	return q.Where(Cond[M]{f: And(conds...).f})
}

// WhereKey filters by primary key.
func (q Query[M, K]) WhereKey(ids ...K) Query[M, K] { return q.keyIn("IN", ids) }

func (q Query[M, K]) WhereKeyNot(ids ...K) Query[M, K] { return q.keyIn("NOT IN", ids) }

func (q Query[M, K]) keyIn(op string, ids []K) Query[M, K] {
	if q.table.PrimaryKey == "" {
		q.err = cmp.Or(q.err, fmt.Errorf("orm: %s has no primary key to look up by", q.table.Name))
		return q
	}
	return q.Where(q.table.keyColumn().in(op, ids))
}

// bound is the context a builder runs its queries with: the one
// WithContext gave it, or the background context (no transaction, no
// deadline) without one.
type bound struct{ ctx context.Context }

func (b bound) context() context.Context {
	if b.ctx == nil {
		return context.Background()
	}
	return b.ctx
}

func boundTo(ctx context.Context) bound {
	if ctx == nil {
		panic("orm: WithContext(nil); pass context.Background() for no transaction or deadline")
	}
	return bound{ctx}
}

// WithContext runs the query with ctx: inside the transaction ctx carries,
// under its deadline and cancellation, and with its options (Sticky,
// WithoutEvents, WithoutTimestamps, WithConn).
func (q Query[M, K]) WithContext(ctx context.Context) Query[M, K] {
	q.bound = boundTo(ctx)
	return q
}

// When applies fn only if cond is true (conditional clauses).
func (q Query[M, K]) When(cond bool, fn func(Query[M, K]) Query[M, K]) Query[M, K] {
	if cond {
		return fn(q)
	}
	return q
}

// Unless applies fn only if cond is false.
func (q Query[M, K]) Unless(cond bool, fn func(Query[M, K]) Query[M, K]) Query[M, K] {
	return q.When(!cond, fn)
}

// Scope applies local scopes: Users.Query().Scope(Active, Popular(10)).
func (q Query[M, K]) Scope(scopes ...func(Query[M, K]) Query[M, K]) Query[M, K] {
	for _, s := range scopes {
		q = s(q)
	}
	return q
}

// ---------------------------------------------------------------------------
// Selects, joins, grouping
// ---------------------------------------------------------------------------

// Select limits the selected columns.
func (q Query[M, K]) Select(cols ...AnyColumn[M]) Query[M, K] {
	q.selects = slices.Clone(cols)
	return q
}

// AddSelect adds columns to the selection.
func (q Query[M, K]) AddSelect(cols ...AnyColumn[M]) Query[M, K] {
	if q.selects == nil {
		q.selects = q.allColumns()
	}
	q.selects = append(slices.Clip(q.selects), cols...)
	return q
}

// AddSelectAs selects any expression of type V — a subquery, an aggregate,
// a column of a joined model — into a (usually virtual) column of M.
func (q Query[M, K]) AddSelectAs[V any](into Column[M, K, V], e ValueExpr[V]) Query[M, K] {
	q.extras = append(slices.Clip(q.extras), selectAs{into.name, e.build})
	return q
}

func (q Query[M, K]) Distinct() Query[M, K] { q.distinct = true; return q }

func (q Query[M, K]) Join[O any](t TableRef[O], on ...Cond[O]) Query[M, K] {
	q.joins = append(slices.Clip(q.joins), joinOn("JOIN", t, on))
	return q
}

func (q Query[M, K]) LeftJoin[O any](t TableRef[O], on ...Cond[O]) Query[M, K] {
	q.joins = append(slices.Clip(q.joins), joinOn("LEFT JOIN", t, on))
	return q
}

func (q Query[M, K]) RightJoin[O any](t TableRef[O], on ...Cond[O]) Query[M, K] {
	q.joins = append(slices.Clip(q.joins), joinOn("RIGHT JOIN", t, on))
	return q
}

func (q Query[M, K]) CrossJoin[O any](t TableRef[O]) Query[M, K] {
	q.joins = append(slices.Clip(q.joins), joinOn[O]("CROSS JOIN", t, nil))
	return q
}

// joinOn renders a join of t on the ANDed conditions over its columns.
func joinOn[O any](kind string, t TableRef[O], on []Cond[O]) frag {
	name := t.tableRef().Name
	return func(b *SQL) {
		b.Write(" ", kind, " ")
		b.Ident(name)
		if len(on) > 0 {
			b.Write(" ON ")
			And(on...).f(b)
		}
	}
}

// JoinSub joins a subquery under alias; on conditions use the subquery's
// model columns, which are rendered against the alias (joinSub).
func (q Query[M, K]) JoinSub[O any, OK comparable](sub Query[O, OK], alias string, on ...Cond[O]) Query[M, K] {
	return q.joinSub("JOIN", sub, alias, on)
}

func (q Query[M, K]) LeftJoinSub[O any, OK comparable](sub Query[O, OK], alias string, on ...Cond[O]) Query[M, K] {
	return q.joinSub("LEFT JOIN", sub, alias, on)
}

func (q Query[M, K]) joinSub[O any, OK comparable](kind string, sub Query[O, OK], alias string, on []Cond[O]) Query[M, K] {
	q.joins = append(slices.Clip(q.joins), func(b *SQL) {
		b.Write(" ", kind, " (")
		sub.renderSelect(b, nil)
		b.Write(") AS ")
		b.Ident(alias)
		if len(on) > 0 {
			b.Write(" ON ")
			b.withAlias(sub.table.Name, alias, And(on...).f)
		}
	})
	return q
}

// JoinLateral joins a subquery that may refer to the outer row's columns
// (joinLateral: JOIN LATERAL (...) ON TRUE), e.g. each user's three latest
// posts. Read its columns with Column.From(alias). Postgres, MySQL 8.0.14+
// and DuckDB have lateral joins; elsewhere the query fails.
func (q Query[M, K]) JoinLateral[O any, OK comparable](sub Query[O, OK], alias string) Query[M, K] {
	return q.joinLateral("JOIN", sub, alias)
}

// LeftJoinLateral is JoinLateral keeping outer rows the subquery has no
// rows for.
func (q Query[M, K]) LeftJoinLateral[O any, OK comparable](sub Query[O, OK], alias string) Query[M, K] {
	return q.joinLateral("LEFT JOIN", sub, alias)
}

func (q Query[M, K]) joinLateral[O any, OK comparable](kind string, sub Query[O, OK], alias string) Query[M, K] {
	q.joins = append(slices.Clip(q.joins), func(b *SQL) {
		if l, ok := b.Dialect.(lateralJoins); !ok || !l.lateralJoins() {
			b.fail(fmt.Errorf("orm: %s has no lateral joins; select from a correlated subquery instead", b.Dialect.Name()))
			return
		}
		b.Write(" ", kind, " LATERAL (")
		sub.renderSelect(b, nil)
		b.Write(") AS ")
		b.Ident(alias)
		b.Write(" ON TRUE")
	})
	return q
}

// lateralJoins reports whether a dialect has JOIN LATERAL.
type lateralJoins interface{ lateralJoins() bool }

// WhereFullText adds a full text match (see FullTextMatch).
func (q Query[M, K]) WhereFullText(term string, cols ...AnyColumn[M]) Query[M, K] {
	return q.Where(FullTextMatch(term, cols...))
}

func (q Query[M, K]) GroupBy(exprs ...AnyExpr) Query[M, K] {
	q.groups = append(slices.Clip(q.groups), exprs...)
	return q
}

func (q Query[M, K]) Having(conds ...Cond[M]) Query[M, K] {
	q.having = append(slices.Clip(q.having), conds...)
	return q
}

func (q Query[M, K]) OrHaving(conds ...Cond[M]) Query[M, K] {
	if len(q.having) == 0 {
		return q.Having(conds...)
	}
	q.having = []Cond[M]{Or(And(q.having...), And(conds...))}
	return q
}

// ---------------------------------------------------------------------------
// Ordering, limits, unions, locks
// ---------------------------------------------------------------------------

func (q Query[M, K]) OrderBy(orders ...Order[M]) Query[M, K] {
	q.orders = append(slices.Clip(q.orders), orders...)
	return q
}

// OrderByOf orders by a joined model's columns.
func (q Query[M, K]) OrderByOf[O any](orders ...Order[O]) Query[M, K] {
	for _, o := range orders {
		q.orders = append(slices.Clip(q.orders), Order[M]{f: o.f, e: o.e, desc: o.desc, nulls: o.nulls})
	}
	return q
}

// Latest orders by cols descending, or by the created-at column.
func (q Query[M, K]) Latest(cols ...AnyColumn[M]) Query[M, K] { return q.byTimestamp(true, cols) }

// Oldest orders by cols ascending, or by the created-at column.
func (q Query[M, K]) Oldest(cols ...AnyColumn[M]) Query[M, K] { return q.byTimestamp(false, cols) }

func (q Query[M, K]) byTimestamp(desc bool, cols []AnyColumn[M]) Query[M, K] {
	dir := map[bool]string{true: " DESC", false: " ASC"}[desc]
	if len(cols) == 0 {
		if q.table.CreatedAt == "" {
			q.err = cmp.Or(q.err, fmt.Errorf("orm: Latest or Oldest on %s needs a column: the table maintains no created-at column", q.table.Name))
			return q
		}
		cols = []AnyColumn[M]{tableColumn[M, K]{q.table, q.table.CreatedAt}}
	}
	for _, c := range cols {
		q = q.OrderBy(Order[M]{f: func(b *SQL) { c.build(b); b.Write(dir) }, e: c, col: c, desc: desc})
	}
	return q
}

func (q Query[M, K]) InRandomOrder() Query[M, K] {
	return q.OrderBy(Order[M]{f: func(b *SQL) { b.Write(b.Dialect.Random()) }})
}

// Reorder drops existing orders, optionally replacing them.
func (q Query[M, K]) Reorder(orders ...Order[M]) Query[M, K] {
	q.orders = slices.Clone(orders)
	return q
}

// Limit takes at most n rows, n > 0; Offset skips n >= 0 rows. Other
// values fail the query when it runs.
func (q Query[M, K]) Limit(n int) Query[M, K] {
	q.limit, q.err = n, cmp.Or(q.err, limitErr(n))
	return q
}

func (q Query[M, K]) Offset(n int) Query[M, K] {
	q.offset, q.err = n, cmp.Or(q.err, offsetErr(n))
	return q
}
func (q Query[M, K]) Take(n int) Query[M, K] { return q.Limit(n) }
func (q Query[M, K]) Skip(n int) Query[M, K] { return q.Offset(n) }

// ForPage sets limit/offset for a 1-based page number.
func (q Query[M, K]) ForPage(page, perPage int) Query[M, K] {
	return q.Limit(perPage).Offset(max(page-1, 0) * perPage)
}

func (q Query[M, K]) Union(other Query[M, K]) Query[M, K]    { return q.union(false, other) }
func (q Query[M, K]) UnionAll(other Query[M, K]) Query[M, K] { return q.union(true, other) }

func (q Query[M, K]) union(all bool, other Query[M, K]) Query[M, K] {
	q.unions = append(slices.Clip(q.unions), union[M, K]{all, other})
	return q
}

func (q Query[M, K]) LockForUpdate() Query[M, K] { q.lock.mode = ForUpdate; return q }
func (q Query[M, K]) SharedLock() Query[M, K]    { q.lock.mode = ForShare; return q }

// NoWait makes a locking read fail at once on a row another transaction has
// locked, instead of waiting for it (NOWAIT).
func (q Query[M, K]) NoWait() Query[M, K] { q.lock.wait = NoWait; return q }

// SkipLocked makes a locking read leave out the rows another transaction
// has locked (SKIP LOCKED): workers of a job queue each take other jobs.
func (q Query[M, K]) SkipLocked() Query[M, K] { q.lock.wait = SkipLocked; return q }

// ---------------------------------------------------------------------------
// Scopes and connections
// ---------------------------------------------------------------------------

func (q Query[M, K]) WithTrashed() Query[M, K] { q.trashed = withTrashed; return q }
func (q Query[M, K]) OnlyTrashed() Query[M, K] { q.trashed = onlyTrashed; return q }

func (q Query[M, K]) WithoutGlobalScope(names ...string) Query[M, K] {
	q.without = append(slices.Clip(q.without), names...)
	return q
}

func (q Query[M, K]) WithoutGlobalScopes() Query[M, K] { q.withoutAll = true; return q }

// On runs the query on a named connection instead of the table's (Model::on()).
func (q Query[M, K]) On(conn string) Query[M, K] { q.onConn = conn; return q }

// prepared applies global scopes and the soft delete constraint once.
func (q Query[M, K]) prepared() Query[M, K] {
	if q.scoped {
		return q
	}
	q.scoped = true
	if q.withoutAll {
		return q
	}
	for _, s := range q.table.globalScopes() {
		if !slices.Contains(q.without, s.name) {
			q = s.fn(q)
			q.scoped = true
		}
	}
	if q.table.softDeletes() && !slices.Contains(q.without, softDeleteScope) {
		t := q.table
		deleted := t.column(t.DeletedAt)
		switch q.trashed {
		case withoutTrashed:
			q.where = append(slices.Clip(q.where), deleted.IsNull())
		case onlyTrashed:
			q.where = append(slices.Clip(q.where), deleted.IsNotNull())
		}
	}
	return q
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

// ToSQL renders the SELECT for the dialect of the query's connection.
func (q Query[M, K]) ToSQL() (string, []any, error) { return q.ToSQLFor(dialectOf(q.connName())) }

// ToSQLFor renders the SELECT for a specific dialect.
func (q Query[M, K]) ToSQLFor(d Dialect) (string, []any, error) {
	return render(d, func(b *SQL) { q.renderSelect(b, nil) })
}

// ToRawSQL renders the SELECT with arguments inlined, for debugging only.
func (q Query[M, K]) ToRawSQL() (string, error) {
	b := &SQL{Dialect: dialectOf(q.connName()), inline: true}
	q.renderSelect(b, nil)
	return b.String(), b.err
}

// Subquery turns the query into a single-column subquery, usable with
// InSub, Cmp/EqCol or AddSelectAs on another query.
func (q Query[M, K]) Subquery[V any](e Expr[M, V]) Subquery[V] {
	return Subquery[V]{func(b *SQL) { q.renderSelect(b, e.build) }}
}

func (q Query[M, K]) connName() string { return cmp.Or(q.onConn, q.table.connection()) }

func (q Query[M, K]) allColumns() []AnyColumn[M] {
	cols := make([]AnyColumn[M], len(q.table.Columns))
	for i, name := range q.table.Columns {
		cols[i] = tableColumn[M, K]{q.table, name}
	}
	return cols
}

// dests are the column names scanned from each row, in select order.
func (q Query[M, K]) dests() []string {
	var names []string
	if q.selects == nil {
		names = slices.Clone(q.table.Columns)
	} else {
		for _, c := range q.selects {
			names = append(names, c.Name())
		}
	}
	for _, e := range q.extras {
		names = append(names, e.dest)
	}
	return names
}

func (q Query[M, K]) renderSelect(b *SQL, list frag) {
	q = q.prepared()
	if q.alias != "" {
		b.withAlias(q.table.Name, q.alias, func(b *SQL) { q.renderBody(b, list) })
		return
	}
	q.renderBody(b, list)
}

func (q Query[M, K]) renderBody(b *SQL, list frag) {
	if q.err != nil {
		b.fail(q.err)
	}
	if q.perParent != nil {
		q.renderPerParent(b)
		return
	}
	b.Write("SELECT ")
	if q.distinct {
		b.Write("DISTINCT ")
	}
	if list != nil {
		list(b)
	} else {
		q.renderColumns(b)
	}
	q.renderFrom(b)
	for i, u := range q.unions {
		m := u.q.prepared()
		writeUnion(b, u.all, i, len(m.orders) > 0 || m.limit > 0 || m.offset > 0, func(b *SQL) { m.renderBody(b, list) })
	}
	if len(q.orders) > 0 {
		unionResult(b, len(q.unions) > 0, func(b *SQL) {
			b.Write(" ORDER BY ")
			for i, o := range q.orders {
				if i > 0 {
					b.Write(", ")
				}
				o.f(b)
			}
		})
	}
	b.Dialect.Limit(b, q.limit, q.offset)
	q.lock.render(b)
}

func limitErr(n int) error {
	if n > 0 {
		return nil
	}
	return fmt.Errorf("orm: Limit(%d): a limit must be positive", n)
}

func offsetErr(n int) error {
	if n >= 0 {
		return nil
	}
	return fmt.Errorf("orm: Offset(%d): an offset cannot be negative", n)
}

// writeUnion appends branch as the i-th union branch. A branch with its
// own order or limit becomes a derived table, the one form every engine
// accepts (SQLite allows neither inside a compound select).
func writeUnion(b *SQL, all bool, i int, limited bool, branch frag) {
	_, explicit := b.Dialect.(interface{ explicitUnionDistinct() })
	switch {
	case all:
		b.Write(" UNION ALL ")
	case explicit:
		b.Write(" UNION DISTINCT ")
	default:
		b.Write(" UNION ")
	}
	if !limited {
		branch(b)
		return
	}
	b.Write("SELECT * FROM (")
	branch(b)
	b.Write(") AS ")
	b.Ident(fmt.Sprintf("union_%d", i+1))
}

// unionResult renders f, the union-level ORDER BY, with unqualified
// columns: a union's result has no tables to qualify them with.
func unionResult(b *SQL, union bool, f frag) {
	plain := b.plain
	b.plain = b.plain || union
	f(b)
	b.plain = plain
}

// renderPerParent applies limit and offset to each parent's rows, not to
// all of them: the rows are numbered per q.perParent value in q's order
// (or by key) and filtered on that number.
func (q Query[M, K]) renderPerParent(b *SQL) {
	const row = "per_parent_row"
	inner := q
	inner.perParent, inner.orders, inner.limit, inner.offset = nil, nil, 0, 0
	orders := q.orders
	if len(orders) == 0 {
		orders = []Order[M]{q.table.keyOrder()}
	}
	writeOrders := func(b *SQL) {
		for i, o := range orders {
			if i > 0 {
				b.Write(", ")
			}
			o.f(b)
		}
	}
	b.Write("SELECT ")
	for i, c := range q.persistedColumns() {
		if i > 0 {
			b.Write(", ")
		}
		b.Col(q.table.Name, c)
	}
	for _, e := range q.extras {
		b.Write(", ")
		b.Ident(e.dest)
	}
	b.Write(" FROM (")
	inner.renderBody(b, func(b *SQL) {
		inner.renderColumns(b)
		b.Write(", ROW_NUMBER() OVER (PARTITION BY ")
		q.perParent.build(b)
		b.Write(" ORDER BY ")
		writeOrders(b)
		b.Write(") AS ")
		b.Ident(row)
	})
	b.Write(") AS ")
	b.Ident(q.table.Name)
	b.Write(" WHERE ")
	b.Ident(row)
	b.Write(" > ", strconv.Itoa(q.offset))
	if q.limit > 0 {
		b.Write(" AND ")
		b.Ident(row)
		b.Write(" <= ", strconv.Itoa(q.offset+q.limit))
	}
	b.Write(" ORDER BY ")
	writeOrders(b)
}

// persistedColumns are the columns the query selects from its table.
func (q Query[M, K]) persistedColumns() []string {
	if q.selects == nil {
		return q.table.Columns
	}
	return names(q.selects)
}

func (q Query[M, K]) renderColumns(b *SQL) {
	i := 0
	sep := func() {
		if i > 0 {
			b.Write(", ")
		}
		i++
	}
	if q.selects == nil {
		for _, c := range q.table.Columns {
			sep()
			b.Col(q.table.Name, c)
		}
	} else {
		for _, c := range q.selects {
			sep()
			c.build(b)
		}
	}
	for _, e := range q.extras {
		sep()
		e.f(b)
		b.Write(" AS ")
		b.Ident(e.dest)
	}
}

// renderFrom writes FROM, joins, WHERE, GROUP BY and HAVING.
func (q Query[M, K]) renderFrom(b *SQL) {
	b.Write(" FROM ")
	b.Ident(q.table.Name)
	if q.alias != "" {
		b.Write(" AS ")
		b.Ident(q.alias)
	}
	for _, j := range q.joins {
		j(b)
	}
	if len(q.where) > 0 {
		b.Write(" WHERE ")
		And(q.where...).f(b)
	}
	if len(q.groups) > 0 {
		b.Write(" GROUP BY ")
		for i, g := range q.groups {
			if i > 0 {
				b.Write(", ")
			}
			g.build(b)
		}
	}
	if len(q.having) > 0 {
		b.Write(" HAVING ")
		And(q.having...).f(b)
	}
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

func (q Query[M, K]) conn(ctx context.Context) (Conn, error) { return sqlConn(ctx, q.connName()) }

// query reads on the read pool, or the write pool for a locking read,
// whose lock means nothing on a replica.
func (q Query[M, K]) query(ctx context.Context, f frag) (*resultRows, error) {
	if q.lock.mode != NoLock {
		return writeRows(ctx, q.connName(), f)
	}
	return readRows(ctx, q.connName(), f)
}

func (q Query[M, K]) queryWrite(ctx context.Context, f frag) (*resultRows, error) {
	return writeRows(ctx, q.connName(), f)
}

func (q Query[M, K]) exec(ctx context.Context, f frag) (sql.Result, error) {
	return execOn(ctx, q.connName(), f)
}

// readRows runs a query on the named connection's read pool — or its write
// pool once a Sticky scope has written to it.
func readRows(ctx context.Context, name string, f frag) (*resultRows, error) {
	c, err := sqlConn(ctx, name)
	if err != nil {
		return nil, err
	}
	return runRows(ctx, name, c.Dialect, stickyDB(ctx, name, c), f)
}

// writeRows runs a statement that reads rows back but is a write, like
// INSERT ... RETURNING, on the write pool.
func writeRows(ctx context.Context, name string, f frag) (*resultRows, error) {
	if err := writable(ctx); err != nil {
		return nil, err
	}
	c, err := sqlConn(ctx, name)
	if err != nil {
		return nil, err
	}
	rows, err := runRows(ctx, name, c.Dialect, c.DB, f)
	if err == nil {
		markWritten(ctx, name)
	}
	return rows, err
}

func runRows(ctx context.Context, name string, d Dialect, db DB, f frag) (*resultRows, error) {
	stmt, args, err := render(d, f)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	rows, err := db.QueryContext(ctx, stmt, args...)
	Emit(QueryEvent{Connection: name, SQL: stmt, Args: args, Duration: time.Since(start), Err: err})
	if err != nil {
		return nil, &QueryError{Err: err, SQL: stmt}
	}
	return &resultRows{Rows: rows}, nil
}

// execOn runs a statement that returns no rows on the write pool.
func execOn(ctx context.Context, name string, f frag) (sql.Result, error) {
	if err := writable(ctx); err != nil {
		return nil, err
	}
	c, err := sqlConn(ctx, name)
	if err != nil {
		return nil, err
	}
	stmt, args, err := render(c.Dialect, f)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	res, err := c.DB.ExecContext(ctx, stmt, args...)
	Emit(QueryEvent{Connection: name, SQL: stmt, Args: args, Duration: time.Since(start), Err: err})
	if err != nil {
		return nil, &QueryError{Err: err, SQL: stmt}
	}
	markWritten(ctx, name)
	return res, nil
}

// scalarOn reads the first column of the first row; no row is V's zero.
func scalarOn[V any](ctx context.Context, name string, f frag) (V, error) {
	v, err := optionalOn[V](ctx, name, f)
	if err != nil || v == nil {
		var zero V
		return zero, err
	}
	return *v, nil
}

// optionalOn reads the first column of the first row; NULL or no row is nil.
func optionalOn[V any](ctx context.Context, name string, f frag) (*V, error) {
	rows, err := readRows(ctx, name, f)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var v *V
	if rows.Next() {
		if err := rows.Scan(nullable{&v}); err != nil {
			return nil, err
		}
	}
	return v, rows.Err()
}

// aggregated is an aggregate's value: SUM of nothing is zero, but MIN,
// MAX and AVG of nothing have no value, which is ErrNotFound rather than
// a zero indistinguishable from a real one.
func aggregated[V any](agg Aggregate, v *V, table string) (V, error) {
	var zero V
	switch {
	case v != nil:
		return *v, nil
	case agg == AggregateSum:
		return zero, nil
	}
	return zero, fmt.Errorf("orm: %s on %s: no value to aggregate: %w", strings.ToLower(agg.String()), table, ErrNotFound)
}

// QueryError wraps a database error with the SQL that caused it.
type QueryError struct {
	Err error
	SQL string
}

func (e *QueryError) Error() string { return "orm: " + e.Err.Error() + "\n  " + e.SQL }
func (e *QueryError) Unwrap() error { return e.Err }

// tableColumn is an untyped column reference used internally.
type tableColumn[M any, K comparable] struct {
	t    *Table[M, K]
	name string
}

func (c tableColumn[M, K]) build(b *SQL)      { b.Col(c.t.Name, c.name) }
func (c tableColumn[M, K]) Name() string      { return c.name }
func (c tableColumn[M, K]) anyPtr(m *M) any   { return c.t.Ptr(m, c.name) }
func (c tableColumn[M, K]) anyValue(m *M) any { return c.t.value(m, c.name) }
func (c tableColumn[M, K]) decode(raw json.RawMessage) (any, error) {
	return decodeInto(raw, c.t.Ptr(new(M), c.name))
}
