package orm

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

// AnyExpr is any SQL expression, whatever its model or type.
type AnyExpr interface{ build(*SQL) }

// ValueExpr is an expression producing V, from any model. Subqueries and
// columns of joined models are ValueExprs, so they can be compared with or
// selected into columns of matching type.
type ValueExpr[V any] interface {
	AnyExpr
	valueOf(V)
}

// Expr is an expression over model M producing V. The phantom method makes
// Column[Post, string] unusable where an Expr[User, string] is expected,
// while still letting Go infer V from it.
type Expr[M, V any] interface {
	ValueExpr[V]
	modelOf(M)
}

// Scalar is a concrete SQL expression over model M producing V. Columns,
// aggregates and functions are all Scalars and share the comparison methods.
// tab/col are set for plain columns, so conditions built on them also carry
// a document-store-translatable form.
type Scalar[M, V any] struct {
	f   frag
	tab string
	col string
}

func (s Scalar[M, V]) build(b *SQL)      { s.f(b) }
func (s Scalar[M, V]) docColumn() string { return s.col }
func (Scalar[M, V]) valueOf(V)           {}
func (Scalar[M, V]) modelOf(M)           {}

func (s Scalar[M, V]) Eq(v V) Cond[M]  { return s.cmpArg("=", v) }
func (s Scalar[M, V]) Ne(v V) Cond[M]  { return s.cmpArg("<>", v) }
func (s Scalar[M, V]) Gt(v V) Cond[M]  { return s.cmpArg(">", v) }
func (s Scalar[M, V]) Gte(v V) Cond[M] { return s.cmpArg(">=", v) }
func (s Scalar[M, V]) Lt(v V) Cond[M]  { return s.cmpArg("<", v) }
func (s Scalar[M, V]) Lte(v V) Cond[M] { return s.cmpArg("<=", v) }

// Op is a comparison operator for the orm.From builder. The constants are
// the valid values; untyped string literals ("=") convert to Op too, so
// ad-hoc queries stay terse while the constants give completion something
// to list. Anything else fails the existing runtime check.
type Op string

const (
	Eq       Op = "="
	Neq      Op = "<>"
	Gt       Op = ">"
	Gte      Op = ">="
	Lt       Op = "<"
	Lte      Op = "<="
	Like     Op = "LIKE"
	NotLike  Op = "NOT LIKE"
	ILike    Op = "ILIKE"
	NotILike Op = "NOT ILIKE"
)

// Selectable is a SelectCols item: a typed column of any model, or any
// typed expression named with As.
type Selectable interface{ term() selectTerm }

// selectTerm is one rendered item of a select list. col names the column
// for document stores; alias, when set, renames it in the result.
type selectTerm struct {
	f     frag
	col   string
	alias string
}

func (t selectTerm) build(b *SQL) {
	t.f(b)
	if t.alias != "" {
		b.Write(" AS ")
		b.Ident(t.alias)
	}
}

// Aliased is an expression selected under a name (Scalar.As).
type Aliased struct{ t selectTerm }

func (a Aliased) term() selectTerm { return a.t }

// Cmp compares against another expression of the same type — a column of
// this or another model (whereColumn), or a scalar subquery.
func (s Scalar[M, V]) Cmp(op Op, other ValueExpr[V]) Cond[M] {
	return Cond[M]{f: func(b *SQL) {
		if knownOp(b, op) {
			s.f(b)
			b.Write(" ", string(op), " ")
			other.build(b)
		}
	}}
}

func (s Scalar[M, V]) against(op string, other AnyExpr) Cond[M] {
	return Cond[M]{f: func(b *SQL) { s.f(b); b.Write(" ", op, " "); other.build(b) }}
}

func (s Scalar[M, V]) EqCol(o ValueExpr[V]) Cond[M]  { return s.Cmp(Eq, o) }
func (s Scalar[M, V]) NeCol(o ValueExpr[V]) Cond[M]  { return s.Cmp(Neq, o) }
func (s Scalar[M, V]) GtCol(o ValueExpr[V]) Cond[M]  { return s.Cmp(Gt, o) }
func (s Scalar[M, V]) GteCol(o ValueExpr[V]) Cond[M] { return s.Cmp(Gte, o) }
func (s Scalar[M, V]) LtCol(o ValueExpr[V]) Cond[M]  { return s.Cmp(Lt, o) }
func (s Scalar[M, V]) LteCol(o ValueExpr[V]) Cond[M] { return s.Cmp(Lte, o) }

// Like matches a SQL LIKE pattern: % any run, _ one character, \ escaping
// either. It is case-sensitive on every engine; ILike ignores case.
func (s Scalar[M, V]) Like(pattern string) Cond[M]     { return s.cmpArg(Like, pattern) }
func (s Scalar[M, V]) NotLike(pattern string) Cond[M]  { return s.cmpArg(NotLike, pattern) }
func (s Scalar[M, V]) ILike(pattern string) Cond[M]    { return s.cmpArg(ILike, pattern) }
func (s Scalar[M, V]) NotILike(pattern string) Cond[M] { return s.cmpArg(NotILike, pattern) }

// Contains, StartsWith and EndsWith match text literally, wildcards
// included, case-sensitively.
func (s Scalar[M, V]) Contains(text string) Cond[M]   { return s.Like("%" + EscapeLike(text) + "%") }
func (s Scalar[M, V]) StartsWith(text string) Cond[M] { return s.Like(EscapeLike(text) + "%") }
func (s Scalar[M, V]) EndsWith(text string) Cond[M]   { return s.Like("%" + EscapeLike(text)) }

// EscapeLike makes text match itself inside a LIKE pattern.
func EscapeLike(text string) string { return likeEscaper.Replace(text) }

var likeEscaper = strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`)

func (s Scalar[M, V]) In(vs ...V) Cond[M]    { return s.in("IN", vs) }
func (s Scalar[M, V]) NotIn(vs ...V) Cond[M] { return s.in("NOT IN", vs) }

// InSub is whereIn with a subquery selecting values of the same type.
func (s Scalar[M, V]) InSub(sub Subquery[V]) Cond[M]    { return s.against("IN", sub) }
func (s Scalar[M, V]) NotInSub(sub Subquery[V]) Cond[M] { return s.against("NOT IN", sub) }

func (s Scalar[M, V]) Between(lo, hi V) Cond[M]    { return s.between("BETWEEN", lo, hi) }
func (s Scalar[M, V]) NotBetween(lo, hi V) Cond[M] { return s.between("NOT BETWEEN", lo, hi) }

func (s Scalar[M, V]) IsNull() Cond[M]    { return s.suffix(" IS NULL") }
func (s Scalar[M, V]) IsNotNull() Cond[M] { return s.suffix(" IS NOT NULL") }

func (s Scalar[M, V]) Asc() Order[M] { return Order[M]{f: s.order(" ASC"), e: s} }

// As selects the expression under a name in a row query: a column whose
// joined tables share its name, Users.Name.As("user"), or an aggregate,
// orm.Count[User]().As("users").
func (s Scalar[M, V]) As(alias string) Aliased {
	return Aliased{selectTerm{f: s.f, col: s.col, alias: alias}}
}
func (s Scalar[M, V]) Desc() Order[M] { return Order[M]{f: s.order(" DESC"), e: s, desc: true} }

// JSON extracts a value from a JSON column: Users.Settings.JSON[string]("theme").
// The result type is chosen explicitly at the call site.
func (s Scalar[M, V]) JSON[T any](path ...string) Scalar[M, T] {
	kind := jsonKindOf[T]()
	return Scalar[M, T]{f: func(b *SQL) { b.Dialect.JSONExtract(b, s, path, kind) }}
}

// JSONContains is whereJsonContains for a JSON array column.
func (s Scalar[M, V]) JSONContains(v any) Cond[M] {
	return Cond[M]{f: func(b *SQL) { b.Dialect.JSONContains(b, s, v) }}
}

// JSONDoesntContain is whereJsonDoesntContain.
func (s Scalar[M, V]) JSONDoesntContain(v any) Cond[M] { return Not(s.JSONContains(v)) }

// JSONOverlaps is whereJsonOverlaps: the JSON array column and vs share at
// least one element.
func (s Scalar[M, V]) JSONOverlaps(vs ...any) Cond[M] {
	return Cond[M]{f: func(b *SQL) { b.Dialect.JSONOverlaps(b, s, vs) }}
}

// JSONHasKey is whereJsonContainsKey: Users.Settings.JSONHasKey("theme").
func (s Scalar[M, V]) JSONHasKey(path ...string) Cond[M] {
	return Cond[M]{f: func(b *SQL) { b.Dialect.JSONHasKey(b, s, path) }}
}

// JSONLength is the length of a JSON array column (whereJsonLength).
func (s Scalar[M, V]) JSONLength() Scalar[M, int64] {
	return Scalar[M, int64]{f: func(b *SQL) { b.Dialect.JSONLength(b, s) }}
}

func (s Scalar[M, V]) cmpArg(op Op, v any) Cond[M] {
	return Cond[M]{
		f:  func(b *SQL) { compare(b, s, op, v) },
		ir: s.field(string(op), v),
	}
}

// compare writes e op v; a LIKE operator goes through the dialect, which
// gives it the same meaning everywhere.
func compare(b *SQL, e AnyExpr, op Op, v any) {
	if !knownOp(b, op) {
		return
	}
	if like, ok := likeOps[op]; ok {
		pattern, isText := v.(string)
		if !isText {
			b.fail(fmt.Errorf("orm: %s needs a string pattern, got %T", op, v))
			return
		}
		b.Dialect.Like(b, e, pattern, like.caseless, like.not)
		return
	}
	e.build(b)
	b.Write(" ", string(op), " ")
	b.Arg(v)
}

var likeOps = map[Op]struct{ caseless, not bool }{
	Like: {false, false}, NotLike: {false, true}, ILike: {true, false}, NotILike: {true, true},
}

// knownOp fails the statement on an operator outside the Op constants.
func knownOp(b *SQL, op Op) bool {
	if _, ok := negations[op]; !ok {
		b.fail(fmt.Errorf("orm: unknown comparison operator %q", op))
		return false
	}
	return true
}

func (s Scalar[M, V]) field(op string, v any) Node {
	if s.col == "" {
		return nil
	}
	return Field{Table: s.tab, Column: s.col, Op: op, Value: v}
}

func (s Scalar[M, V]) in(op string, vs []V) Cond[M] {
	vals := make([]any, len(vs))
	for i, v := range vs {
		vals[i] = v
	}
	var ir Node
	if s.col != "" {
		ir = List{Table: s.tab, Column: s.col, Not: op == "NOT IN", Values: vals}
	}
	if len(vs) == 0 {
		return Cond[M]{f: func(b *SQL) { b.Write(map[string]string{"IN": "1 = 0", "NOT IN": "1 = 1"}[op]) }, ir: ir}
	}
	return Cond[M]{
		f: func(b *SQL) {
			s.f(b)
			b.Write(" ", op, " (")
			for i, v := range vs {
				if i > 0 {
					b.Write(", ")
				}
				b.Arg(v)
			}
			b.Write(")")
		},
		ir: ir,
	}
}

func (s Scalar[M, V]) between(op string, lo, hi V) Cond[M] {
	return Cond[M]{
		f: func(b *SQL) { s.f(b); b.Write(" ", op, " "); b.Arg(lo); b.Write(" AND "); b.Arg(hi) },
		ir: func() Node {
			if s.col == "" {
				return nil
			}
			return Range{Table: s.tab, Column: s.col, Lo: lo, Hi: hi, Not: op == "NOT BETWEEN"}
		}(),
	}
}

func (s Scalar[M, V]) suffix(sql string) Cond[M] {
	return Cond[M]{
		f: func(b *SQL) { s.f(b); b.Write(sql) },
		ir: func() Node {
			if s.col == "" {
				return nil
			}
			return NullTest{Table: s.tab, Column: s.col, Not: strings.Contains(sql, "NOT")}
		}(),
	}
}

func (s Scalar[M, V]) order(dir string) frag { return func(b *SQL) { s.f(b); b.Write(dir) } }

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

// Column is a typed column of model M holding V. Columns are generated by ormgen.
type Column[M any, K comparable, V any] struct {
	Scalar[M, V]
	table *Table[M, K]
	name  string
	ptr   func(*M) *V
}

// NewColumn declares a persisted column.
func NewColumn[M any, K comparable, V any](t *Table[M, K], name string, ptr func(*M) *V) Column[M, K, V] {
	return Column[M, K, V]{
		f: func(b *SQL) { b.Col(t.Name, name) }, tab: t.Name, col: name,
		table: t, name: name, ptr: ptr,
	}
}

// NewVirtualColumn declares a column that is only ever selected, never
// persisted — the target of withCount, addSelect subqueries or aggregates
// (db:"posts_count,virtual" in a model).
func NewVirtualColumn[M any, K comparable, V any](t *Table[M, K], name string, ptr func(*M) *V) Column[M, K, V] {
	return Column[M, K, V]{
		f: func(b *SQL) { b.Ident(name) }, tab: t.Name, col: name,
		table: t, name: name, ptr: ptr,
	}
}

func (c Column[M, K, V]) Name() string  { return c.name }
func (c Column[M, K, V]) Get(m *M) V    { return *c.ptr(m) }
func (c Column[M, K, V]) Ptr(m *M) *V   { return c.ptr(m) }
func (c Column[M, K, V]) Asc() Order[M] { return Order[M]{f: c.order(" ASC"), e: c, col: c} }
func (c Column[M, K, V]) Desc() Order[M] {
	return Order[M]{f: c.order(" DESC"), e: c, col: c, desc: true}
}
func (c Column[M, K, V]) anyPtr(m *M) any   { return c.ptr(m) }
func (c Column[M, K, V]) anyValue(m *M) any { return *c.ptr(m) }

func (c Column[M, K, V]) decode(raw json.RawMessage) (any, error) {
	var v V
	err := json.Unmarshal(raw, &v)
	return v, err
}

// Set is an assignment for updates and attribute matching: Users.Karma.Set(5).
func (c Column[M, K, V]) Set(v V) Assignment[M] {
	return Assignment[M]{
		col:   c.name,
		raw:   c.table.writeValue(c.name, v),
		apply: func(m *M) { *c.ptr(m) = v },
		match: c.Eq(v),
	}
}

// SetExpr assigns an expression: Posts.Views.SetExpr(orm.Plus(Posts.Views, 1)).
func (c Column[M, K, V]) SetExpr(e Expr[M, V]) Assignment[M] {
	return Assignment[M]{col: c.name, expr: e.build}
}

func (c Column[M, K, V]) term() selectTerm { return selectTerm{f: c.build, col: c.name} }

// Original is the value as last loaded from or saved to the database.
func (c Column[M, K, V]) Original(m *M) V {
	if v, ok := c.table.state(m).original[c.name]; ok {
		if typed, ok := v.(V); ok {
			return typed
		}
	}
	var zero V
	return zero
}

// IsDirty reports whether the field changed since it was loaded or saved.
func (c Column[M, K, V]) IsDirty(m *M) bool {
	st := c.table.state(m)
	orig, ok := st.original[c.name]
	return !ok || !equal(orig, c.Get(m))
}

// WasChanged reports whether the last save changed the column.
func (c Column[M, K, V]) WasChanged(m *M) bool {
	return slices.Contains(c.table.state(m).changes, c.name)
}

// outer references the column without table aliasing, for correlating a
// subquery on the same table to its outer query.
func (c Column[M, K, V]) outer() Scalar[M, V] {
	return Scalar[M, V]{f: func(b *SQL) { b.Ident(c.table.Name); b.Write("."); b.Ident(c.name) }}
}

// AnyColumn is a column of M regardless of its value type.
type AnyColumn[M any] interface {
	AnyExpr
	Name() string
	anyPtr(*M) any
	anyValue(*M) any
	decode(json.RawMessage) (any, error)
}

// colSet is one column = value pair of an UPDATE or INSERT: a literal
// bound as an argument (and stored as-is by document stores), or an
// expression rendered in place, which only SQL can evaluate.
type colSet struct {
	col  string
	raw  any
	expr frag
	inc  any // for an increment: the amount, which document stores add natively
}

func (s colSet) renderValue(b *SQL) {
	if s.expr != nil {
		s.expr(b)
		return
	}
	b.Arg(s.raw)
}

// Assignment is a column = value pair for updates and attribute matching.
type Assignment[M any] struct {
	colSet
	apply func(*M) // sets the field on a model; nil for expressions
	match Cond[M]
}

// setLiteral is an Assignment of a literal value with no model-side effect,
// for columns the ORM maintains itself (timestamps, soft deletes).
func setLiteral[M any](col string, v any) Assignment[M] {
	return Assignment[M]{col: col, raw: v}
}

// colSets drops the model-specific parts of assignments, leaving what a
// statement or document store needs.
func colSets[M any](as []Assignment[M]) []colSet {
	sets := make([]colSet, len(as))
	for i, a := range as {
		sets[i] = a.colSet
	}
	return sets
}

// Order is an ORDER BY term for model M.
type Order[M any] struct {
	f     frag
	e     AnyExpr      // the ordered expression, when the order has one
	col   AnyColumn[M] // set when ordering by a plain column (needed by CursorPaginate)
	vec   *VectorOrder // set when ordering by distance from a vector
	desc  bool
	nulls Nulls
}

// NullsLast sorts NULLs after every value, in either direction and on
// every engine; a plain order puts them where the engine does (first on
// SQLite, MySQL and MariaDB ascending, last on Postgres, DuckDB and
// ClickHouse).
func (o Order[M]) NullsLast() Order[M] {
	e, desc := o.e, o.desc
	if e == nil {
		o.f = func(b *SQL) { b.fail(errors.New("orm: NullsLast needs a column or expression order")) }
		return o
	}
	o.f, o.nulls = func(b *SQL) { b.Dialect.OrderNulls(b, e, desc, NullsLast) }, NullsLast
	return o
}

// RawOrder is orderByRaw.
func RawOrder[M any](sql string, args ...any) Order[M] {
	return Order[M]{f: func(b *SQL) { b.Raw(sql, args...) }}
}

// ---------------------------------------------------------------------------
// Conditions
// ---------------------------------------------------------------------------

// Cond is a WHERE/HAVING condition over model M. A Cond[Post] cannot be
// passed to a Query[User] (use WhereOf after a Join for that). ir is the
// engine-independent form of the same condition, when there is one, for
// document stores; nil marks SQL-only conditions.
type Cond[M any] struct {
	f  frag
	ir Node
}

// Raw builds a condition from SQL with ? placeholders (whereRaw).
func Raw[M any](sql string, args ...any) Cond[M] {
	return Cond[M]{f: func(b *SQL) { b.Raw(sql, args...) }}
}

func And[M any](conds ...Cond[M]) Cond[M] { return join(" AND ", conds) }
func Or[M any](conds ...Cond[M]) Cond[M]  { return join(" OR ", conds) }

func Not[M any](c Cond[M]) Cond[M] {
	return Cond[M]{
		f: func(b *SQL) { b.Write("NOT ("); c.f(b); b.Write(")") },
		ir: func() Node {
			if c.ir == nil {
				return nil
			}
			if nt, ok := c.ir.(notTranslatable); ok {
				return nt
			}
			return Negate{Part: c.ir}
		}(),
	}
}

// Exists is whereExists with an arbitrary subquery.
func Exists[M, R any, RK comparable](sub Query[R, RK]) Cond[M] {
	return Cond[M]{f: func(b *SQL) { b.Write("EXISTS ("); sub.renderSelect(b, selectOne); b.Write(")") }}
}

func NotExists[M, R any, RK comparable](sub Query[R, RK]) Cond[M] { return Not(Exists[M](sub)) }

// FullTextMatch is whereFullText: a natural language match of term against
// cols (MATCH ... AGAINST on MySQL, to_tsvector @@ plainto_tsquery on
// Postgres, LIKE on SQLite). Pair it with a FullText index in the schema.
func FullTextMatch[M any](term string, cols ...AnyColumn[M]) Cond[M] {
	exprs := make([]AnyExpr, len(cols))
	for i, c := range cols {
		exprs[i] = c
	}
	return Cond[M]{f: func(b *SQL) { b.Dialect.FullText(b, exprs, term) }}
}

// AnyOf is whereAny: the value matches at least one of the expressions.
func AnyOf[M, V any](op Op, v V, exprs ...Expr[M, V]) Cond[M] {
	conds := make([]Cond[M], len(exprs))
	for i, e := range exprs {
		conds[i] = Cond[M]{f: func(b *SQL) { compare(b, e, op, v) }}
	}
	return Or(conds...)
}

// AllOf is whereAll: the value matches every expression.
func AllOf[M, V any](op Op, v V, exprs ...Expr[M, V]) Cond[M] {
	negated, ok := negations[op]
	if !ok {
		return Cond[M]{f: func(b *SQL) { knownOp(b, op) }}
	}
	return Not(AnyOf(negated, v, exprs...))
}

// negations pairs every comparison operator with its opposite.
var negations = map[Op]Op{
	Eq: Neq, Neq: Eq, "!=": Eq, Gt: Lte, Lte: Gt, Lt: Gte, Gte: Lt,
	Like: NotLike, NotLike: Like, ILike: NotILike, NotILike: ILike,
}

func join[M any](sep string, conds []Cond[M]) Cond[M] {
	var ir Node
	switch len(conds) {
	case 0:
		return Raw[M]("1 = 1")
	case 1:
		return conds[0]
	}
	irs := make([]Node, len(conds))
	allOK := true
	for i, c := range conds {
		irs[i] = c.ir
		if c.ir == nil {
			allOK = false
		}
	}
	if allOK {
		ir = Composite{Or: sep == " OR ", Parts: irs}
	}
	return Cond[M]{
		f: func(b *SQL) {
			for i, c := range conds {
				if i > 0 {
					b.Write(sep)
				}
				b.Write("(")
				c.f(b)
				b.Write(")")
			}
		},
		ir: ir,
	}
}

// ---------------------------------------------------------------------------
// Functions and aggregates
// ---------------------------------------------------------------------------

// Number is the constraint for numeric aggregates and arithmetic.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Val is a bound literal value.
func Val[M, V any](v V) Scalar[M, V] { return Scalar[M, V]{f: func(b *SQL) { b.Arg(v) }} }

// RawExpr is a raw SQL expression of a declared type (selectRaw, DB::raw).
func RawExpr[M, V any](sql string, args ...any) Scalar[M, V] {
	return Scalar[M, V]{f: func(b *SQL) { b.Raw(sql, args...) }}
}

func Count[M any]() Scalar[M, int64] { return RawExpr[M, int64]("COUNT(*)") }

func CountDistinct[M, V any](e Expr[M, V]) Scalar[M, int64] {
	return Scalar[M, int64]{f: func(b *SQL) { b.Write("COUNT(DISTINCT "); e.build(b); b.Write(")") }}
}

func Sum[M any, V Number](e Expr[M, V]) Scalar[M, V] { return wrap[M, V](AggregateSum.String(), e) }
func Avg[M any, V Number](e Expr[M, V]) Scalar[M, float64] {
	return wrap[M, float64](AggregateAvg.String(), e)
}

func Min[M, V any](e Expr[M, V]) Scalar[M, V] { return wrap[M, V](AggregateMin.String(), e) }

func Max[M, V any](e Expr[M, V]) Scalar[M, V]          { return wrap[M, V](AggregateMax.String(), e) }
func Lower[M any](e Expr[M, string]) Scalar[M, string] { return wrap[M, string]("LOWER", e) }
func Upper[M any](e Expr[M, string]) Scalar[M, string] { return wrap[M, string]("UPPER", e) }

func Coalesce[M, V any](e Expr[M, V], fallback V) Scalar[M, V] {
	return Scalar[M, V]{f: func(b *SQL) { b.Write("COALESCE("); e.build(b); b.Write(", "); b.Arg(fallback); b.Write(")") }}
}

func Plus[M any, V Number](e Expr[M, V], by V) Scalar[M, V] {
	return Scalar[M, V]{f: func(b *SQL) { e.build(b); b.Write(" + "); b.Arg(by) }}
}

func Minus[M any, V Number](e Expr[M, V], by V) Scalar[M, V] {
	return Scalar[M, V]{f: func(b *SQL) { e.build(b); b.Write(" - "); b.Arg(by) }}
}

// Date, Time, Year, Month and Day render per dialect (whereDate, whereYear, ...).
func Date[M, V any](e Expr[M, V]) Scalar[M, string] { return datePart[M, string]("date", e) }
func Time[M, V any](e Expr[M, V]) Scalar[M, string] { return datePart[M, string]("time", e) }
func Year[M, V any](e Expr[M, V]) Scalar[M, int]    { return datePart[M, int]("year", e) }
func Month[M, V any](e Expr[M, V]) Scalar[M, int]   { return datePart[M, int]("month", e) }
func Day[M, V any](e Expr[M, V]) Scalar[M, int]     { return datePart[M, int]("day", e) }

func Now[M any]() Scalar[M, time.Time] { return RawExpr[M, time.Time]("CURRENT_TIMESTAMP") }

func wrap[M, V any](name string, e AnyExpr) Scalar[M, V] {
	return Scalar[M, V]{f: func(b *SQL) { fn(b, name, e) }}
}

func datePart[M, V any](part string, e AnyExpr) Scalar[M, V] {
	return Scalar[M, V]{f: func(b *SQL) { b.Dialect.DatePart(b, part, e) }}
}

// Subquery is a query selecting a single column of type V, usable in
// InSub, comparisons (Cmp/EqCol) and SelectAs.
type Subquery[V any] struct{ f frag }

func (s Subquery[V]) build(b *SQL) { b.Write("("); s.f(b); b.Write(")") }
func (Subquery[V]) valueOf(V)      {}

// selectOne is the select list of an existence check.
func selectOne(b *SQL) { b.Write("1") }

// equal compares column values for dirty tracking.
func equal(a, b any) bool {
	if ta, ok := a.(time.Time); ok {
		tb, ok := b.(time.Time)
		return ok && ta.Equal(tb)
	}
	return reflect.DeepEqual(a, b)
}

// IfNull replaces NULL with fallback — e.g. for a correlated subquery that
// may match no rows: AddSelectAs(Users.LatestTitle, orm.IfNull(sub, "")).
func IfNull[V any](e ValueExpr[V], fallback V) ValueExpr[V] {
	return valueExpr[V]{func(b *SQL) { b.Write("COALESCE("); e.build(b); b.Write(", "); b.Arg(fallback); b.Write(")") }}
}

type valueExpr[V any] struct{ f frag }

func (v valueExpr[V]) build(b *SQL) { v.f(b) }
func (valueExpr[V]) valueOf(V)      {}
