package orm

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"time"
)

// RowQuery is a query on a table without a model — Eloquent's
// DB::table('users'). Rows come back as maps; values are bound as
// arguments and never interpolated. Model-level features (events, casts,
// relations) do not apply.
//
//	rows, err := orm.From("users").
//		Where("active", orm.Eq, true).
//		WhereIn("country_id", 1, 2).
//		WhereNull("deleted_at").
//		OrderBy("name").
//		Get(ctx) // []map[string]any{{"id": int64(1), "name": "Alice"}, ...}
//
// Like Query, every method returns a new RowQuery, so builders can be shared.
type RowQuery struct {
	conn     string
	name     string
	source   frag   // a raw table source (FromRaw), rendered instead of name
	key      string // auto-increment key column for InsertGetID; default "id"
	distinct bool
	selects  []selectTerm
	joins    []frag
	where    []frag
	groups   []frag
	having   []frag
	nodes    []Node // document-store form of where, when translatable
	badNode  bool   // set by clauses with no document form (WhereRaw)
	orders   []keysetTerm
	unions   []rowUnion
	lock     LockMode
	limit    int
	offset   int
}

type rowUnion struct {
	all bool
	q   RowQuery
}

// From starts a row query on a table by name.
func From(name string) RowQuery { return RowQuery{name: name, key: "id"} }

// FromRaw starts a read-only row query on a raw table source, binding
// args to its ? placeholders — a table function such as DuckDB's
// orm.FromRaw("read_csv(?)", "events.csv").
func FromRaw(source string, args ...any) RowQuery {
	return RowQuery{name: source, key: "id", source: func(b *SQL) { b.Raw(source, args...) }}
}

// writable refuses writes through a raw table source.
func (r RowQuery) writable(method string) error {
	if r.source != nil {
		return fmt.Errorf("orm: %s on %s: a raw table source (FromRaw) is read-only", method, r.name)
	}
	return nil
}

// FromTable starts a row query on a model's table and connection, with its
// primary key as the InsertGetID column: orm.FromTable(Users).
func FromTable[M any](ref TableRef[M]) RowQuery {
	t := ref.tableRef()
	return RowQuery{name: t.Name, conn: t.Connection, key: cmp.Or(t.PrimaryKey, "id")}
}

// On targets a named connection (DB::table(...)->on('analytics')).
func (r RowQuery) On(conn string) RowQuery { r.conn = conn; return r }

// WithKey names the auto-increment column InsertGetID returns (Postgres
// needs it for RETURNING; on SQLite "rowid" works for keyless tables). On a
// document store it is the key InsertOrIgnore matches on.
func (r RowQuery) WithKey(col string) RowQuery { r.key = col; return r }

func (r RowQuery) connName() string { return cmp.Or(r.conn, DefaultConnection) }

func (r RowQuery) cond(col string, op Op, v any) (frag, Node) {
	return func(b *SQL) { compare(b, identExpr(col), op, v) }, Field{Column: col, Op: string(op), Value: v}
}

// Where adds a simple column condition (where('active', '=', true)); the
// operator is an orm.Op constant — Where("active", orm.Eq, true) — or a
// string literal.
func (r RowQuery) Where(col string, op Op, v any) RowQuery {
	return r.addWhere(r.cond(col, op, v))
}

// OrWhere ORs the condition with everything so far.
func (r RowQuery) OrWhere(col string, op Op, v any) RowQuery {
	return r.orWhere(r.cond(col, op, v))
}

// WhereCond adds typed conditions built from any model's columns, with
// values compile-checked against the column type:
// orm.From("users").WhereCond(Users.Karma.Gt(10), Users.Active.Eq(true)).
// Mixing models in one call needs the same model; chain calls to mix.
func (r RowQuery) WhereCond[M any](cs ...Cond[M]) RowQuery {
	for _, c := range cs {
		r = r.addWhere(c.f, c.ir)
	}
	return r
}

// OrWhereCond ORs typed conditions (ANDed together) with everything so
// far: WhereCond(Users.Active.Eq(true)).OrWhereCond(Users.Karma.Gt(90)).
func (r RowQuery) OrWhereCond[M any](cs ...Cond[M]) RowQuery {
	c := And(cs...)
	return r.orWhere(c.f, c.ir)
}

// WhereIn is whereIn on a raw column; the Or forms of every Where OR the
// condition with everything so far.
func (r RowQuery) WhereIn(col string, vs ...any) RowQuery { return r.addWhere(inCond(false, col, vs)) }

func (r RowQuery) WhereNotIn(col string, vs ...any) RowQuery {
	return r.addWhere(inCond(true, col, vs))
}

func (r RowQuery) OrWhereIn(col string, vs ...any) RowQuery { return r.orWhere(inCond(false, col, vs)) }

func (r RowQuery) OrWhereNotIn(col string, vs ...any) RowQuery {
	return r.orWhere(inCond(true, col, vs))
}

// WhereNull and WhereNotNull filter on column nullness.
func (r RowQuery) WhereNull(col string) RowQuery      { return r.addWhere(nullCond(false, col)) }
func (r RowQuery) WhereNotNull(col string) RowQuery   { return r.addWhere(nullCond(true, col)) }
func (r RowQuery) OrWhereNull(col string) RowQuery    { return r.orWhere(nullCond(false, col)) }
func (r RowQuery) OrWhereNotNull(col string) RowQuery { return r.orWhere(nullCond(true, col)) }

// WhereBetween filters a column between lo and hi; WhereNotBetween outside.
func (r RowQuery) WhereBetween(col string, lo, hi any) RowQuery {
	return r.addWhere(betweenCond(false, col, lo, hi))
}

func (r RowQuery) WhereNotBetween(col string, lo, hi any) RowQuery {
	return r.addWhere(betweenCond(true, col, lo, hi))
}

func (r RowQuery) OrWhereBetween(col string, lo, hi any) RowQuery {
	return r.orWhere(betweenCond(false, col, lo, hi))
}

func (r RowQuery) OrWhereNotBetween(col string, lo, hi any) RowQuery {
	return r.orWhere(betweenCond(true, col, lo, hi))
}

// WhereRaw adds a condition from SQL with ? placeholders (whereRaw).
func (r RowQuery) WhereRaw(sql string, args ...any) RowQuery {
	return r.addWhere(rawCond(sql, args), nil)
}

func (r RowQuery) OrWhereRaw(sql string, args ...any) RowQuery {
	return r.orWhere(rawCond(sql, args), nil)
}

// WhereColumn compares two columns (whereColumn); WhereCond with EqCol and
// friends is the typed form.
func (r RowQuery) WhereColumn(first string, op Op, second string) RowQuery {
	return r.addWhere(columnCond(first, op, second), nil)
}

func (r RowQuery) OrWhereColumn(first string, op Op, second string) RowQuery {
	return r.orWhere(columnCond(first, op, second), nil)
}

// WhereExists keeps rows for which sub returns any row (whereExists);
// correlate sub to the outer table with WhereColumn on qualified names.
func (r RowQuery) WhereExists(sub RowQuery) RowQuery { return r.addWhere(existsCond(false, sub), nil) }

func (r RowQuery) WhereNotExists(sub RowQuery) RowQuery {
	return r.addWhere(existsCond(true, sub), nil)
}

func (r RowQuery) OrWhereExists(sub RowQuery) RowQuery { return r.orWhere(existsCond(false, sub), nil) }

func (r RowQuery) OrWhereNotExists(sub RowQuery) RowQuery {
	return r.orWhere(existsCond(true, sub), nil)
}

func inCond(not bool, col string, vs []any) (frag, Node) {
	node := List{Column: col, Not: not, Values: vs}
	if len(vs) == 0 {
		return func(b *SQL) { b.Write(map[bool]string{false: "1 = 0", true: "1 = 1"}[not]) }, node
	}
	return func(b *SQL) {
		b.Ident(col)
		b.Write(map[bool]string{false: " IN (", true: " NOT IN ("}[not])
		for i, v := range vs {
			if i > 0 {
				b.Write(", ")
			}
			b.Arg(v)
		}
		b.Write(")")
	}, node
}

func nullCond(not bool, col string) (frag, Node) {
	return func(b *SQL) {
		b.Ident(col)
		b.Write(map[bool]string{false: " IS NULL", true: " IS NOT NULL"}[not])
	}, NullTest{Column: col, Not: not}
}

func betweenCond(not bool, col string, lo, hi any) (frag, Node) {
	return func(b *SQL) {
		b.Ident(col)
		b.Write(map[bool]string{false: " BETWEEN ", true: " NOT BETWEEN "}[not])
		b.Arg(lo)
		b.Write(" AND ")
		b.Arg(hi)
	}, Range{Column: col, Lo: lo, Hi: hi, Not: not}
}

func rawCond(sql string, args []any) frag {
	return func(b *SQL) { b.Raw(sql, args...) }
}

func columnCond(first string, op Op, second string) frag {
	return func(b *SQL) {
		if knownOp(b, op) {
			b.Ident(first)
			b.Write(" ", string(op), " ")
			b.Ident(second)
		}
	}
}

func existsCond(not bool, sub RowQuery) frag {
	return func(b *SQL) {
		b.Write(map[bool]string{false: "EXISTS (", true: "NOT EXISTS ("}[not])
		sub.render(b, selectOne)
		b.Write(")")
	}
}

// addWhere ANDs a condition; a nil node marks it SQL-only.
func (r RowQuery) addWhere(f frag, node Node) RowQuery {
	r.where = append(slices.Clip(r.where), f)
	if node == nil {
		r.badNode = true
	} else {
		r.nodes = append(slices.Clip(r.nodes), node)
	}
	return r
}

// orWhere makes the query (everything so far) OR (this condition), in the
// document-store form too.
func (r RowQuery) orWhere(f frag, node Node) RowQuery {
	if len(r.where) == 0 {
		return r.addWhere(f, node)
	}
	first := r.where
	r.where = []frag{func(b *SQL) {
		b.Write("(")
		writeAnd(b, first)
		b.Write(") OR (")
		f(b)
		b.Write(")")
	}}
	if node == nil {
		r.badNode = true
		return r
	}
	r.nodes = []Node{Composite{Or: true, Parts: []Node{Composite{Parts: r.nodes}, node}}}
	return r
}

func writeFrags(b *SQL, parts []frag) {
	for i, p := range parts {
		if i > 0 {
			b.Write(", ")
		}
		p(b)
	}
}

func writeAnd(b *SQL, conds []frag) {
	for i, c := range conds {
		if i > 0 {
			b.Write(" AND ")
		}
		c(b)
	}
}

// Select limits the selected columns; default is *.
func (r RowQuery) Select(cols ...string) RowQuery {
	r.selects = make([]selectTerm, len(cols))
	for i, c := range cols {
		r.selects[i] = selectTerm{f: func(b *SQL) { b.Ident(c) }, col: c}
	}
	return r
}

// SelectCols selects typed columns, from any model, table-qualified so
// joined tables cannot make them ambiguous — completion after Users. lists
// exactly the valid columns. Column.As renames one in the result.
func (r RowQuery) SelectCols(cols ...Selectable) RowQuery {
	r.selects = make([]selectTerm, len(cols))
	for i, c := range cols {
		r.selects[i] = c.term()
	}
	return r
}

// SelectRaw adds a raw expression to the select list (selectRaw):
// SelectRaw("COUNT(*) AS n"). Arguments bind to ? placeholders.
func (r RowQuery) SelectRaw(sql string, args ...any) RowQuery {
	r.selects = append(slices.Clip(r.selects), selectTerm{f: func(b *SQL) { b.Raw(sql, args...) }})
	return r
}

func (r RowQuery) Distinct() RowQuery { r.distinct = true; return r }

// GroupBy groups by columns named as strings; GroupByCols by typed
// columns, table-qualified.
func (r RowQuery) GroupBy(cols ...string) RowQuery {
	for _, c := range cols {
		r.groups = append(slices.Clip(r.groups), identExpr(c).build)
	}
	return r
}

func (r RowQuery) GroupByCols(cols ...QualifiedColumn) RowQuery {
	for _, c := range cols {
		r.groups = append(slices.Clip(r.groups), c.build)
	}
	return r
}

// Having filters groups on a column or select alias (having('n', '>', 1));
// Postgres cannot see select aliases here, so prefer HavingCond there.
func (r RowQuery) Having(col string, op Op, v any) RowQuery {
	f, _ := r.cond(col, op, v)
	r.having = append(slices.Clip(r.having), f)
	return r
}

// HavingCond filters groups on typed conditions, aggregates included:
// HavingCond(orm.Count[User]().Gt(1)).
func (r RowQuery) HavingCond[M any](cs ...Cond[M]) RowQuery {
	for _, c := range cs {
		r.having = append(slices.Clip(r.having), c.f)
	}
	return r
}

// HavingRaw filters groups on SQL with ? placeholders (havingRaw).
func (r RowQuery) HavingRaw(sql string, args ...any) RowQuery {
	r.having = append(slices.Clip(r.having), func(b *SQL) { b.Raw(sql, args...) })
	return r
}

// Join joins a model's table on typed conditions over its columns, as on
// a model query: Join(Countries, Countries.ID.EqCol(Users.CountryID)).
// Both sides of EqCol must hold the same value type.
func (r RowQuery) Join[O any](t TableRef[O], on ...Cond[O]) RowQuery {
	r.joins = append(slices.Clip(r.joins), joinOn("JOIN", t, on))
	return r
}

func (r RowQuery) LeftJoin[O any](t TableRef[O], on ...Cond[O]) RowQuery {
	r.joins = append(slices.Clip(r.joins), joinOn("LEFT JOIN", t, on))
	return r
}

func (r RowQuery) RightJoin[O any](t TableRef[O], on ...Cond[O]) RowQuery {
	r.joins = append(slices.Clip(r.joins), joinOn("RIGHT JOIN", t, on))
	return r
}

func (r RowQuery) CrossJoin[O any](t TableRef[O]) RowQuery {
	r.joins = append(slices.Clip(r.joins), joinOn[O]("CROSS JOIN", t, nil))
	return r
}

// QualifiedColumn is a column of any model or value type that renders
// table-qualified in SQL — every orm.Column satisfies it — letting orm.From
// queries order and group by typed columns from any schema.
type QualifiedColumn interface {
	AnyExpr
	Name() string
}

// identExpr is a column named by a (possibly table-qualified) string.
type identExpr string

func (c identExpr) build(b *SQL) { b.Ident(string(c)) }
func (c identExpr) Name() string { return string(c) }

// OrderBy sorts ascending; OrderByDesc descending.
func (r RowQuery) OrderBy(col string) RowQuery { return r.order(col, identExpr(col), " ASC") }

func (r RowQuery) OrderByDesc(col string) RowQuery { return r.order(col, identExpr(col), " DESC") }

// OrderByCol sorts by a typed column, table-qualified like SelectCols,
// ascending; OrderByColDesc descending.
func (r RowQuery) OrderByCol(c QualifiedColumn) RowQuery { return r.order(c.Name(), c, " ASC") }

func (r RowQuery) OrderByColDesc(c QualifiedColumn) RowQuery { return r.order(c.Name(), c, " DESC") }

func (r RowQuery) order(col string, e AnyExpr, dir string) RowQuery {
	r.orders = append(slices.Clip(r.orders), keysetTerm{e: e, col: col, desc: dir == " DESC"})
	return r
}

// Latest sorts by col (default created_at) descending; Oldest ascending.
func (r RowQuery) Latest(col ...string) RowQuery {
	return r.OrderByDesc(cmp.Or(slices.Concat(col, []string{"created_at"})...))
}

func (r RowQuery) Oldest(col ...string) RowQuery {
	return r.OrderBy(cmp.Or(slices.Concat(col, []string{"created_at"})...))
}

// InRandomOrder shuffles the rows; it has no document-store form.
func (r RowQuery) InRandomOrder() RowQuery {
	r.orders = append(slices.Clip(r.orders), keysetTerm{e: randomOrder{}})
	return r
}

type randomOrder struct{}

func (randomOrder) build(b *SQL) { b.Write(b.Dialect.Random()) }

// Reorder drops every order so far.
func (r RowQuery) Reorder() RowQuery { r.orders = nil; return r }

// When applies fn when cond holds; Unless when it does not.
func (r RowQuery) When(cond bool, fn func(RowQuery) RowQuery) RowQuery {
	if cond {
		return fn(r)
	}
	return r
}

func (r RowQuery) Unless(cond bool, fn func(RowQuery) RowQuery) RowQuery { return r.When(!cond, fn) }

// LockForUpdate and SharedLock lock the selected rows until the
// transaction ends.
func (r RowQuery) LockForUpdate() RowQuery { r.lock = ForUpdate; return r }
func (r RowQuery) SharedLock() RowQuery    { r.lock = ForShare; return r }

// Union appends other's rows, without duplicates; UnionAll keeps them.
// Orders and limits apply to the combined rows.
func (r RowQuery) Union(other RowQuery) RowQuery {
	r.unions = append(slices.Clip(r.unions), rowUnion{q: other})
	return r
}

func (r RowQuery) UnionAll(other RowQuery) RowQuery {
	r.unions = append(slices.Clip(r.unions), rowUnion{all: true, q: other})
	return r
}

func (r RowQuery) Limit(n int) RowQuery  { r.limit = n; return r }
func (r RowQuery) Offset(n int) RowQuery { r.offset = n; return r }

// ForPage limits the query to page (from 1) of perPage rows.
func (r RowQuery) ForPage(page, perPage int) RowQuery {
	return r.Limit(perPage).Offset(max(page-1, 0) * perPage)
}

// Paginate returns page (from 1) of perPage rows with the total count, on
// every engine, document stores included; with GroupBy or Distinct the
// total counts groups or distinct rows.
func (r RowQuery) Paginate(ctx context.Context, perPage, page int) (Page[map[string]any], error) {
	return paginate(perPage, page, func() (int64, error) { return r.Count(ctx) }, r.pageOf(ctx))
}

// SimplePaginate is Paginate without the count query: it only reports
// whether another page follows.
func (r RowQuery) SimplePaginate(ctx context.Context, perPage, page int) (SimplePage[map[string]any], error) {
	return simplePaginate(perPage, page, r.pageOf(ctx))
}

func (r RowQuery) pageOf(ctx context.Context) func(limit, offset int) ([]map[string]any, error) {
	return func(limit, offset int) ([]map[string]any, error) { return r.Limit(limit).Offset(offset).Get(ctx) }
}

// CursorPaginate pages by the values of the ORDER BY columns (keyset
// pagination), the key column ascending when there is none; the next
// page starts after NextCursor. It runs on document stores too.
func (r RowQuery) CursorPaginate(ctx context.Context, perPage int, cursor string) (CursorPage[map[string]any], error) {
	if len(r.orders) == 0 {
		r = r.OrderBy(r.keyColumn())
	}
	if slices.ContainsFunc(r.orders, func(o keysetTerm) bool { return o.col == "" }) {
		return CursorPage[map[string]any]{}, fmt.Errorf("orm: cursor pagination on %s needs column orders, not a random order", r.name)
	}
	return cursorPaginate(perPage, cursor, r.orders,
		func(_ int, raw json.RawMessage) (any, error) { return jsonScalar(raw) },
		func(after *Cond[rowModel], limit int) ([]map[string]any, error) {
			page := r.Limit(limit)
			if after != nil {
				page = page.addWhere(after.f, after.ir)
			}
			return page.Get(ctx)
		},
		func(row map[string]any, i int) any { return row[resultName(r.orders[i].col)] })
}

// rowModel is the model type of conditions a row query builds itself.
type rowModel struct{}

// resultName is the key a (possibly qualified) column has in a result row.
func resultName(col string) string { return col[strings.LastIndexByte(col, '.')+1:] }

// jsonScalar decodes a cursor value into a driver-ready value: integers
// stay integers.
func jsonScalar(raw json.RawMessage) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	if n, ok := v.(json.Number); ok {
		if i, err := n.Int64(); err == nil {
			return i, nil
		}
		return n.Float64()
	}
	return v, nil
}

// keyColumn is the key column, qualified when joins could make it
// ambiguous.
func (r RowQuery) keyColumn() string {
	if len(r.joins) > 0 {
		return r.name + "." + r.key
	}
	return r.key
}

// Chunk hands fn the rows size at a time, paging by offset; order the
// query for stable pages. Return ErrStop from fn to stop early.
func (r RowQuery) Chunk(ctx context.Context, size int, fn func([]map[string]any) error) error {
	return chunkPages(size, r.pageOf(ctx), fn)
}

// ChunkByID pages by the key column (WithKey, default "id"), which must be
// selected; safe when fn modifies the rows it gets.
func (r RowQuery) ChunkByID(ctx context.Context, size int, fn func([]map[string]any) error) error {
	return eachChunk(r.chunksByID(ctx, size), fn)
}

// Lazy streams rows chunk by chunk by key column (lazyById).
func (r RowQuery) Lazy(ctx context.Context, size int) iter.Seq2[map[string]any, error] {
	return flatten(r.chunksByID(ctx, size))
}

func (r RowQuery) chunksByID(ctx context.Context, size int) iter.Seq2[[]map[string]any, error] {
	col := r.keyColumn()
	page := r
	page.orders = nil
	page = page.OrderBy(col).Limit(size)
	return keysetChunks(size, func(after any) ([]map[string]any, error) {
		if after == nil {
			return page.Get(ctx)
		}
		return page.Where(col, Gt, after).Get(ctx)
	}, func(row map[string]any) (any, error) {
		if v := row[r.key]; v != nil {
			return v, nil
		}
		return nil, fmt.Errorf("orm: chunk %s by %s: the rows must include %s", r.name, r.key, r.key)
	})
}

// Cursor streams the rows of one query as they are read (cursor()).
func (r RowQuery) Cursor(ctx context.Context) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		if _, ok, err := r.docConn(ctx); err != nil || ok {
			flatten(func(yield func([]map[string]any, error) bool) { yield(r.Get(ctx)) })(yield)
			return
		}
		rows, err := readRows(ctx, r.connName(), func(b *SQL) { r.render(b, nil) })
		if err != nil {
			yield(nil, err)
			return
		}
		defer closeRows(rows)
		if err := eachMap(rows, func(m map[string]any) bool { return yield(m, nil) }); err != nil {
			yield(nil, err)
		}
	}
}

// ToSQL renders the SELECT for the builder's connection, with its arguments.
func (r RowQuery) ToSQL() (string, []any, error) {
	return render(dialectOf(r.connName()), func(b *SQL) { r.render(b, nil) })
}

// render writes the SELECT; list, when set, replaces the select list.
func (r RowQuery) render(b *SQL, list frag) {
	b.Write("SELECT ")
	if r.distinct {
		b.Write("DISTINCT ")
	}
	switch {
	case list != nil:
		list(b)
	case len(r.selects) == 0:
		b.Write("*")
	default:
		for i, t := range r.selects {
			if i > 0 {
				b.Write(", ")
			}
			t.build(b)
		}
	}
	b.Write(" FROM ")
	r.writeTable(b)
	for _, j := range r.joins {
		j(b)
	}
	r.renderWhere(b)
	if len(r.groups) > 0 {
		b.Write(" GROUP BY ")
		writeFrags(b, r.groups)
	}
	if len(r.having) > 0 {
		b.Write(" HAVING ")
		writeAnd(b, r.having)
	}
	for i, u := range r.unions {
		branch := u.q
		branch.lock = 0
		writeUnion(b, u.all, i, len(branch.orders) > 0 || branch.limit > 0 || branch.offset > 0, func(b *SQL) { branch.render(b, list) })
	}
	unionResult(b, len(r.unions) > 0, func(b *SQL) {
		for i, o := range r.orders {
			b.Write(map[bool]string{true: " ORDER BY ", false: ", "}[i == 0])
			if len(r.unions) > 0 && o.col != "" {
				identExpr(resultName(o.col)).build(b)
			} else {
				o.e.build(b)
			}
			b.Write(map[bool]string{false: " ASC", true: " DESC"}[o.desc])
		}
	})
	b.Dialect.Limit(b, r.limit, r.offset)
	b.Write(b.Dialect.Lock(r.lock))
}

func (r RowQuery) writeTable(b *SQL) {
	if r.source != nil {
		r.source(b)
		return
	}
	b.Ident(r.name)
}

func (r RowQuery) renderWhere(b *SQL) {
	if len(r.where) == 0 {
		return
	}
	b.Write(" WHERE ")
	writeAnd(b, r.where)
}

// docConn resolves the document store behind this builder's connection,
// if any.
func (r RowQuery) docConn(ctx context.Context) (DocStore, bool, error) {
	return docStoreFor(ctx, r.connName())
}

func (r RowQuery) plan(method string) (Plan, error) {
	p, err := r.planOf()
	if err != nil {
		return Plan{}, fmt.Errorf("orm: %s on %s: %w", method, r.name, err)
	}
	return p, nil
}

func (r RowQuery) planOf() (Plan, error) {
	if len(r.joins) > 0 {
		return Plan{}, &ErrNotTranslatable{Reason: "joins"}
	}
	if r.distinct {
		return Plan{}, &ErrNotTranslatable{Reason: "distinct"}
	}
	if len(r.groups) > 0 || len(r.having) > 0 {
		return Plan{}, &ErrNotTranslatable{Reason: "groupBy / having"}
	}
	if len(r.unions) > 0 {
		return Plan{}, &ErrNotTranslatable{Reason: "unions"}
	}
	if r.lock != 0 {
		return Plan{}, &ErrNotTranslatable{Reason: "row locks"}
	}
	if r.badNode {
		return Plan{}, &ErrNotTranslatable{Reason: "a raw or SQL-specific condition (whereRaw)"}
	}
	if r.source != nil {
		return Plan{}, &ErrNotTranslatable{Reason: "a raw table source (FromRaw)"}
	}
	p := Plan{Table: r.name, Key: r.key, Limit: r.limit, Offset: r.offset}
	for _, o := range r.orders {
		if o.col == "" {
			return Plan{}, &ErrNotTranslatable{Reason: "random order"}
		}
		p.Orders = append(p.Orders, PlanOrder{Column: unqualified(o.col, r.name), Desc: o.desc})
	}
	for _, t := range r.selects {
		if t.col == "" {
			return Plan{}, &ErrNotTranslatable{Reason: "raw or computed selects"}
		}
		if t.alias != "" {
			return Plan{}, &ErrNotTranslatable{Reason: "column aliases"}
		}
		p.Columns = append(p.Columns, unqualified(t.col, r.name))
	}
	if len(r.nodes) > 0 {
		p.Where = unqualifiedNode(Composite{Parts: r.nodes}, r.name)
		if err := checkSameTable(p.Where, r.name); err != nil {
			return Plan{}, err
		}
	}
	return p, nil
}

// Get runs the query and returns every row as a map of column to value.
func (r RowQuery) Get(ctx context.Context) ([]map[string]any, error) {
	if ds, ok, err := r.docConn(ctx); err != nil {
		return nil, err
	} else if ok {
		p, err := r.plan("query")
		if err != nil {
			return nil, err
		}
		start := time.Now()
		docs, err := ds.FindDocs(ctx, p)
		emitDoc(r.connName(), "find", p, time.Since(start), err)
		return docs, err
	}
	rows, err := readRows(ctx, r.connName(), func(b *SQL) { r.render(b, nil) })
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	return scanMaps(rows)
}

// Find returns the row whose key column (WithKey, default "id") is id, or
// ErrNotFound.
func (r RowQuery) Find(ctx context.Context, id any) (map[string]any, error) {
	return r.Where(r.keyColumn(), Eq, id).First(ctx)
}

// Sole returns the only matching row: ErrNotFound for none,
// ErrMultipleRecords for more.
func (r RowQuery) Sole(ctx context.Context) (map[string]any, error) { return sole(r.Limit(2).Get(ctx)) }

// Value returns col of the first row (value()), or ErrNotFound; ValueCol
// is the typed form.
func (r RowQuery) Value(ctx context.Context, col string) (any, error) {
	row, err := r.Select(col).First(ctx)
	if err != nil {
		return nil, err
	}
	return row[resultName(col)], nil
}

func (r RowQuery) ValueCol[M, V any](ctx context.Context, c Column[M, V]) (V, error) {
	vs, err := r.Limit(1).PluckCol(ctx, c)
	if err != nil || len(vs) == 0 {
		var zero V
		return zero, cmp.Or(err, ErrNotFound)
	}
	return vs[0], nil
}

// InsertUsing inserts the rows sub selects into cols (insertUsing).
func (r RowQuery) InsertUsing(ctx context.Context, cols []string, sub RowQuery) (int64, error) {
	if err := r.writable("insert"); err != nil {
		return 0, err
	}
	return insertUsing(ctx, r.connName(), r.name, cols, func(b *SQL) { sub.render(b, nil) })
}

// Explain returns the engine's plan for the query; document stores have
// none.
func (r RowQuery) Explain(ctx context.Context) ([]map[string]any, error) {
	return explainOn(ctx, r.connName(), func(b *SQL) { r.render(b, nil) })
}

// First returns the first row or ErrNotFound.
func (r RowQuery) First(ctx context.Context) (map[string]any, error) {
	rows, err := r.Limit(1).Get(ctx)
	if err != nil || len(rows) == 0 {
		return nil, cmp.Or(err, ErrNotFound)
	}
	return rows[0], nil
}

// Pluck returns one column per row.
func (r RowQuery) Pluck(ctx context.Context, col string) ([]any, error) {
	rows, err := r.Select(col).Get(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]any, len(rows))
	for i, row := range rows {
		out[i] = row[resultName(col)]
	}
	return out, nil
}

// PluckCol returns one typed column per row: PluckCol(ctx, Users.Email) is
// a []string.
func (r RowQuery) PluckCol[M, V any](ctx context.Context, c Column[M, V]) ([]V, error) {
	rows, err := r.SelectCols(c).Get(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]V, len(rows))
	for i, row := range rows {
		if err := (nullable{&out[i]}).Scan(row[c.name]); err != nil {
			return nil, fmt.Errorf("orm: pluck %s from %s: %w", c.name, r.name, err)
		}
	}
	return out, nil
}

// Count returns the number of matching rows; with Distinct, the number of
// distinct selected rows; with GroupBy, the number of groups.
func (r RowQuery) Count(ctx context.Context) (int64, error) {
	if ds, ok, err := r.docConn(ctx); err != nil {
		return 0, err
	} else if ok {
		p, err := r.plan("count")
		if err != nil {
			return 0, err
		}
		start := time.Now()
		n, err := ds.CountDocs(ctx, p)
		emitDoc(r.connName(), "count", p, time.Since(start), err)
		return n, err
	}
	r.orders, r.limit, r.offset = nil, 0, 0
	if r.distinct || len(r.groups) > 0 || len(r.unions) > 0 {
		var list frag
		if len(r.selects) == 0 && len(r.groups) > 0 {
			list = func(b *SQL) { writeFrags(b, r.groups) }
		}
		return scalarOn[int64](ctx, r.connName(), func(b *SQL) {
			b.Write("SELECT COUNT(*) FROM (")
			r.render(b, list)
			b.Write(") AS ")
			b.Ident("aggregate")
		})
	}
	return rowScalar[int64](ctx, r, func(b *SQL) { b.Write("COUNT(*)") })
}

// Sum, Avg, Min and Max aggregate a column; the result type is chosen at
// the call site: Sum[int64](ctx, "karma"). The *Col forms infer it from a
// typed column. Document stores aggregate server-side where they can.
func (r RowQuery) Sum[V Number](ctx context.Context, col string) (V, error) {
	return rowAggregate[V](ctx, r, AggregateSum, identExpr(col))
}

func (r RowQuery) Avg(ctx context.Context, col string) (float64, error) {
	return rowAggregate[float64](ctx, r, AggregateAvg, identExpr(col))
}

func (r RowQuery) Min[V any](ctx context.Context, col string) (V, error) {
	return rowAggregate[V](ctx, r, AggregateMin, identExpr(col))
}

func (r RowQuery) Max[V any](ctx context.Context, col string) (V, error) {
	return rowAggregate[V](ctx, r, AggregateMax, identExpr(col))
}

// SumCol, AvgCol, MinCol and MaxCol aggregate a typed column:
// SumCol(ctx, Users.Karma) is an int64.
func (r RowQuery) SumCol[M any, V Number](ctx context.Context, c Column[M, V]) (V, error) {
	return rowAggregate[V](ctx, r, AggregateSum, c)
}

func (r RowQuery) AvgCol[M any, V Number](ctx context.Context, c Column[M, V]) (float64, error) {
	return rowAggregate[float64](ctx, r, AggregateAvg, c)
}

func (r RowQuery) MinCol[M, V any](ctx context.Context, c Column[M, V]) (V, error) {
	return rowAggregate[V](ctx, r, AggregateMin, c)
}

func (r RowQuery) MaxCol[M, V any](ctx context.Context, c Column[M, V]) (V, error) {
	return rowAggregate[V](ctx, r, AggregateMax, c)
}

func rowAggregate[V any](ctx context.Context, r RowQuery, agg Aggregate, c QualifiedColumn) (V, error) {
	var zero V
	method := strings.ToLower(agg.String())
	r.orders, r.limit, r.offset, r.distinct = nil, 0, 0, false
	if ds, ok, err := r.docConn(ctx); err != nil {
		return zero, err
	} else if ok {
		p, err := r.plan(method)
		if err != nil {
			return zero, err
		}
		p.Columns = nil
		return aggregateDocs[V](ctx, ds, r.connName(), p, agg, c.Name())
	}
	if len(r.groups) > 0 {
		return zero, fmt.Errorf("orm: %s on %s: a grouped query has one value per group; select it with SelectCols(orm.%s(...).As(name)) and Get",
			method, r.name, agg.String()[:1]+method[1:])
	}
	if len(r.unions) > 0 {
		return zero, fmt.Errorf("orm: %s on %s: aggregate the union in SQL (SelectRaw over a subquery) or Get its rows", method, r.name)
	}
	return rowScalar[V](ctx, r, func(b *SQL) { fn(b, agg.String(), c) })
}

func rowScalar[V any](ctx context.Context, r RowQuery, list frag) (V, error) {
	return scalarOn[V](ctx, r.connName(), func(b *SQL) { r.render(b, list) })
}

// Exists reports whether any row matches; DoesntExist is its inverse.
func (r RowQuery) Exists(ctx context.Context) (bool, error) {
	if _, ok, err := r.docConn(ctx); err != nil {
		return false, err
	} else if ok {
		rows, err := r.Limit(1).Get(ctx)
		return len(rows) > 0, err
	}
	rows, err := readRows(ctx, r.connName(), func(b *SQL) { r.Limit(1).render(b, selectOne) })
	if err != nil {
		return false, err
	}
	defer closeRows(rows)
	return rows.Next(), rows.Err()
}

func (r RowQuery) DoesntExist(ctx context.Context) (bool, error) {
	ok, err := r.Exists(ctx)
	return !ok, err
}

// Insert inserts the given rows as-is.
func (r RowQuery) Insert(ctx context.Context, ms ...map[string]any) error {
	return r.insertRows(ctx, "insert", mapRows(ms), conflict{})
}

// InsertCols inserts one row from typed assignments:
// InsertCols(ctx, Users.Name.Set("Ada"), Users.Email.Set("ada@example.com")).
func (r RowQuery) InsertCols[M any](ctx context.Context, sets ...Assignment[M]) error {
	return r.insertRows(ctx, "insert", [][]colSet{colSets(sets)}, conflict{})
}

// InsertOrIgnore inserts rows, skipping any that violate a unique
// constraint (on a document store: whose key, see WithKey, exists).
func (r RowQuery) InsertOrIgnore(ctx context.Context, ms ...map[string]any) error {
	return r.insertRows(ctx, "insertOrIgnore", mapRows(ms), conflict{kind: conflictIgnore})
}

// Upsert inserts rows, or updates the update columns (default: every
// inserted column outside uniqueBy) of the row a uniqueBy combination
// already names: Upsert(ctx, rows, []string{"email"}, "name").
func (r RowQuery) Upsert(ctx context.Context, ms []map[string]any, uniqueBy []string, update ...string) error {
	rows := mapRows(ms)
	if len(update) == 0 {
		update = slices.DeleteFunc(rowColumns(rows), func(c string) bool { return slices.Contains(uniqueBy, c) })
	}
	return r.insertRows(ctx, "upsert", rows, conflict{kind: conflictUpdate, uniqueBy: uniqueBy, update: update})
}

func (r RowQuery) insertRows(ctx context.Context, method string, rows [][]colSet, c conflict) error {
	if len(rows) == 0 {
		return nil
	}
	if err := r.writable(method); err != nil {
		return err
	}
	if ds, ok, err := r.docConn(ctx); err != nil {
		return err
	} else if ok {
		docs := make([]map[string]any, len(rows))
		for i, row := range rows {
			if docs[i], err = docFields(row); err != nil {
				return fmt.Errorf("orm: %s on %s: %w", method, r.name, err)
			}
		}
		return c.storeDocs(ctx, ds, r.connName(), r.name, docs, r.key)
	}
	cols := rowColumns(rows)
	return insertSQL(ctx, r.connName(), r.name, cols, alignRows(cols, rows), c, r.key)
}

// InsertGetID inserts one row and returns the database-generated key,
// from the key column ("id" unless changed with WithKey). Document stores
// generate no keys, so it has no document-store form.
func (r RowQuery) InsertGetID(ctx context.Context, m map[string]any) (int64, error) {
	if err := r.writable("insert"); err != nil {
		return 0, err
	}
	row := [][]colSet{mapSets(m)}
	cols := rowColumns(row)
	return insertGetID(ctx, r.connName(), r.name, r.key, func(b *SQL) { writeInsert(b, "INSERT", r.name, cols, row) })
}

func mapRows(ms []map[string]any) [][]colSet {
	rows := make([][]colSet, len(ms))
	for i, m := range ms {
		rows[i] = mapSets(m)
	}
	return rows
}

// mapSets is m as column sets in column order, so generated SQL is stable.
func mapSets(m map[string]any) []colSet {
	sets := make([]colSet, 0, len(m))
	for _, c := range sortedKeys(m) {
		sets = append(sets, colSet{col: c, raw: m[c]})
	}
	return sets
}

// rowColumns is the sorted union of the columns rows set.
func rowColumns(rows [][]colSet) []string {
	cols := map[string]bool{}
	for _, row := range rows {
		for _, s := range row {
			cols[s.col] = true
		}
	}
	return sortedKeys(cols)
}

// alignRows orders each row's sets by cols; a column a row does not set is
// NULL in that row.
func alignRows(cols []string, rows [][]colSet) [][]colSet {
	aligned := make([][]colSet, len(rows))
	for i, row := range rows {
		out := make([]colSet, len(cols))
		for j, c := range cols {
			if k := slices.IndexFunc(row, func(s colSet) bool { return s.col == c }); k >= 0 {
				out[j] = row[k]
			} else {
				out[j] = colSet{col: c}
			}
		}
		aligned[i] = out
	}
	return aligned
}

// sortedKeys returns the map's keys in order, so generated SQL is stable.
func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

// Update sets the given values on every matching row and reports how many
// changed.
func (r RowQuery) Update(ctx context.Context, m map[string]any) (int64, error) {
	return r.update(ctx, mapSets(m))
}

// UpdateCols is Update with typed assignments:
// UpdateCols(ctx, Users.Name.Set("Ada"), Users.Karma.Set(5)). Unlike the
// model query's Update it leaves updated-at alone, as Update does.
func (r RowQuery) UpdateCols[M any](ctx context.Context, sets ...Assignment[M]) (int64, error) {
	return r.update(ctx, colSets(sets))
}

// Increment adds by to a column on every matching row; Decrement
// subtracts. Document stores apply them natively where they can ($inc on
// MongoDB).
func (r RowQuery) Increment[V Number](ctx context.Context, col string, by V) (int64, error) {
	return r.update(ctx, []colSet{increment(identExpr(col), col, by)})
}

func (r RowQuery) Decrement[V Number](ctx context.Context, col string, by V) (int64, error) {
	return r.update(ctx, []colSet{decrement(identExpr(col), col, by)})
}

func (r RowQuery) update(ctx context.Context, sets []colSet) (int64, error) {
	if len(sets) == 0 {
		return 0, fmt.Errorf("orm: update on %s: no columns to set", r.name)
	}
	if err := r.writable("update"); err != nil {
		return 0, err
	}
	if ds, ok, err := r.docConn(ctx); err != nil {
		return 0, err
	} else if ok {
		p, err := r.plan("update")
		if err != nil {
			return 0, err
		}
		return updateDocs(ctx, ds, r.connName(), p, sets)
	}
	where := r.mutationWhere()
	return mutate(ctx, r.connName(), r.name, where, func(b *SQL) { writeUpdate(b, r.name, sets, where) })
}

// UpdateOrInsert updates the rows matching the query and attrs with
// values, or inserts attrs and values when none matches. Existence is
// checked first, so engines that count only changed rows (MySQL) or none
// (ClickHouse) do not insert duplicates.
func (r RowQuery) UpdateOrInsert(ctx context.Context, attrs, values map[string]any) error {
	match := r
	for _, c := range sortedKeys(attrs) {
		match = match.Where(c, Eq, attrs[c])
	}
	exists, err := match.Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		merged := maps.Clone(attrs)
		maps.Copy(merged, values)
		return r.Insert(ctx, merged)
	}
	if len(values) == 0 {
		return nil
	}
	_, err = match.Update(ctx, values)
	return err
}

// Delete removes every matching row and reports how many went.
func (r RowQuery) Delete(ctx context.Context) (int64, error) {
	if err := r.writable("delete"); err != nil {
		return 0, err
	}
	if ds, ok, err := r.docConn(ctx); err != nil {
		return 0, err
	} else if ok {
		p, err := r.plan("delete")
		if err != nil {
			return 0, err
		}
		return deleteDocs(ctx, ds, r.connName(), p)
	}
	where := r.mutationWhere()
	return mutate(ctx, r.connName(), r.name, where, func(b *SQL) { writeDelete(b, r.name, where) })
}

func (r RowQuery) mutationWhere() frag {
	if r.limit == 0 && r.offset == 0 {
		return r.renderWhere
	}
	return keyedWhere(r.key, func(b *SQL) { r.render(b, identExpr(r.keyColumn()).build) })
}

// scanMaps reads every remaining row into maps.
func scanMaps(rows *sql.Rows) ([]map[string]any, error) {
	var out []map[string]any
	err := eachMap(rows, func(m map[string]any) bool { out = append(out, m); return true })
	return out, err
}

// eachMap hands fn each remaining row as a fresh map, with []byte values
// converted to strings, until fn returns false.
func eachMap(rows *sql.Rows, fn func(map[string]any) bool) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				row[c] = string(b)
			} else {
				row[c] = vals[i]
			}
		}
		if !fn(row) {
			return nil
		}
	}
	return rows.Err()
}
