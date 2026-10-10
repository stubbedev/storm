package orm

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// DuckDB: embedded analytical engine, Postgres-flavoured SQL with ?
// placeholders. Reads CSV/Parquet/JSON files straight from the builder, so
// it doubles as the flat-file backend (orm.From("read_csv('data.csv')")).
// ---------------------------------------------------------------------------

type duckdbDialect struct{ sqliteDialect }

var DuckDB Dialect = duckdbDialect{}

func (duckdbDialect) Name() string { return "duckdb" }

func (duckdbDialect) Bool(v bool) string {
	return map[bool]string{true: "true", false: "false"}[v]
}

func (duckdbDialect) DatePart(b *SQL, part string, e AnyExpr) {
	switch part {
	case "date", "time":
		b.Write("strftime(")
		e.build(b)
		b.Write(map[string]string{"date": ", '%Y-%m-%d')", "time": ", '%H:%M:%S')"}[part])
	default:
		b.Write("CAST(date_part('", part, "', ")
		e.build(b)
		b.Write(") AS INTEGER)")
	}
}

func (duckdbDialect) JSONExtract(b *SQL, e AnyExpr, path []string) {
	b.Write("(")
	e.build(b)
	b.Write(" ->> ")
	b.Arg(jsonPath(path))
	b.Write(")")
}

func (duckdbDialect) JSONContains(b *SQL, e AnyExpr, v any) {
	b.Write("json_contains(")
	e.build(b)
	b.Write(", CAST(")
	b.Arg(jsonOf(v))
	b.Write(" AS JSON))")
}

func (duckdbDialect) JSONOverlaps(b *SQL, e AnyExpr, v any) {
	b.Write("(")
	for i, x := range sliceOf(v) {
		if i > 0 {
			b.Write(" OR ")
		}
		duckdbDialect{}.JSONContains(b, e, x)
	}
	b.Write(")")
}

func (duckdbDialect) JSONLength(b *SQL, e AnyExpr) {
	b.Write("json_array_length(")
	e.build(b)
	b.Write(")")
}

func (duckdbDialect) JSONHasKey(b *SQL, e AnyExpr, path []string) {
	b.Write("(")
	e.build(b)
	b.Write(" -> ")
	b.Arg(jsonPath(path))
	b.Write(") IS NOT NULL")
}

// FullText matches term anywhere in any of exprs, ignoring case like the
// other engines' natural-language search.
func (duckdbDialect) FullText(b *SQL, exprs []AnyExpr, term string) {
	b.Write("(")
	for i, e := range exprs {
		if i > 0 {
			b.Write(" OR ")
		}
		e.build(b)
		b.Write(" ILIKE ")
		b.Arg("%" + term + "%")
	}
	b.Write(")")
}

func (duckdbDialect) Truncate(table string) []string {
	return []string{"DELETE FROM " + doubleQuote(table)}
}

func (duckdbDialect) VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric) {
	fs := floatsOf(arg)
	if fs == nil {
		b.Write("array_distance(")
		e.build(b)
		b.Write(", ")
		b.Arg(arg)
		b.Write(")")
		return
	}
	// array_distance needs both sides as the same fixed-size FLOAT[N]; a
	// bound parameter cannot match, so the query vector's components are
	// inlined as a literal array (our own floats, not user text).
	n := strconv.Itoa(len(fs))
	switch m {
	case Cosine:
		b.Write("1 - array_cosine_similarity(CAST(")
	case InnerProduct:
		b.Write("-array_inner_product(CAST(")
	default:
		b.Write("array_distance(CAST(")
	}
	e.build(b)
	b.Write(" AS FLOAT[" + n + "]), CAST([")
	for i, f := range fs {
		if i > 0 {
			b.Write(", ")
		}
		b.Write(strconv.FormatFloat(f, 'g', -1, 64))
	}
	b.Write("] AS FLOAT[" + n + "]))")
}

func floatsOf(arg any) []float64 {
	if vv, ok := arg.(interface{ Floats() []float64 }); ok {
		return vv.Floats()
	}
	return nil
}

// ---------------------------------------------------------------------------
// ClickHouse: OLAP with a real SQL surface but different physics — no
// transactions, mutations instead of UPDATE, MergeTree engines. Queries,
// inserts and deletes translate; updates run as ALTER TABLE ... UPDATE and
// auto-increment keys are not available (use explicit or UUID keys).
// ---------------------------------------------------------------------------

type clickhouseDialect struct{ sqliteDialect }

var ClickHouse Dialect = clickhouseDialect{}

func (clickhouseDialect) Name() string { return "clickhouse" }

func (clickhouseDialect) Quote(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

func (clickhouseDialect) Returning() bool { return false }

func (clickhouseDialect) AutoKeys() bool { return false }

func (clickhouseDialect) Lock(LockMode) string { return "" }

func (clickhouseDialect) Random() string { return "rand()" }

func (clickhouseDialect) Limit(b *SQL, l, o int) {
	if l > 0 {
		b.Write(" LIMIT ")
		b.Write(intText(l))
	} else if o > 0 {
		b.Write(" LIMIT 18446744073709551615")
	}
	if o > 0 {
		b.Write(" OFFSET ")
		b.Write(intText(o))
	}
}

func (clickhouseDialect) DatePart(b *SQL, part string, e AnyExpr) {
	switch part {
	case "date":
		b.Write("CAST(toDate(")
		e.build(b)
		b.Write(") AS String)")
	case "time":
		b.Write("formatDateTime(")
		e.build(b)
		b.Write(", '%H:%i:%S')")
	case "year":
		b.Write("toYear(")
		e.build(b)
		b.Write(")")
	case "month":
		b.Write("toMonth(")
		e.build(b)
		b.Write(")")
	case "day":
		b.Write("toDayOfMonth(")
		e.build(b)
		b.Write(")")
	}
}

func (clickhouseDialect) JSONExtract(b *SQL, e AnyExpr, path []string) {
	b.Write("JSON_VALUE(")
	e.build(b)
	b.Write(", ")
	b.Arg(jsonPath(path))
	b.Write(")")
}

// JSONContains and JSONOverlaps compare array elements in their raw JSON
// form, so strings and numbers both match exactly.
func (clickhouseDialect) JSONContains(b *SQL, e AnyExpr, v any) {
	clickhouseDialect{}.JSONOverlaps(b, e, []any{v})
}

func (clickhouseDialect) JSONOverlaps(b *SQL, e AnyExpr, v any) {
	b.Write("hasAny(JSONExtractArrayRaw(")
	e.build(b)
	b.Write("), [")
	for i, x := range sliceOf(v) {
		if i > 0 {
			b.Write(", ")
		}
		b.Write("toJSONString(")
		b.Arg(x)
		b.Write(")")
	}
	b.Write("])")
}

func (clickhouseDialect) JSONLength(b *SQL, e AnyExpr) {
	b.Write("JSONLength(")
	e.build(b)
	b.Write(")")
}

func (clickhouseDialect) JSONHasKey(b *SQL, e AnyExpr, path []string) {
	b.Write("JSONHas(")
	e.build(b)
	for _, p := range path {
		b.Write(", ")
		b.Arg(p)
	}
	b.Write(")")
}

func (clickhouseDialect) FullText(b *SQL, exprs []AnyExpr, term string) {
	b.Write("(")
	for i, e := range exprs {
		if i > 0 {
			b.Write(" OR ")
		}
		b.Write("positionCaseInsensitiveUTF8(")
		e.build(b)
		b.Write(", ")
		b.Arg(term)
		b.Write(") > 0")
	}
	b.Write(")")
}

func (clickhouseDialect) Truncate(table string) []string {
	return []string{"TRUNCATE TABLE " + clickhouseDialect{}.Quote(table)}
}

func (clickhouseDialect) UpdatePrefix(b *SQL, table string) {
	b.plain = true // mutations reject qualified columns in WHERE
	b.Write("ALTER TABLE ")
	b.Ident(table)
	b.Write(" UPDATE")
}

func (clickhouseDialect) DeletePrefix(b *SQL, table string) {
	b.plain = true
	b.Write("DELETE FROM ")
	b.Ident(table)
}

// ClickHouse has no unique constraints, so upserts and insertOrIgnore
// resolve collisions by reading keys first, and its driver reports no
// affected rows for mutations.
func (clickhouseDialect) emulatesConflicts()  {}
func (clickhouseDialect) uncountedMutations() {}

// mutationSuffix makes ClickHouse mutations synchronous.
func (clickhouseDialect) mutationSuffix(b *SQL) {
	b.Write(" SETTINGS mutations_sync = 2")
}

// VectorDistance binds the query vector as an array, which the driver
// sends as an Array(Float64) literal; inner product is negated so smaller
// still means nearer, as pgvector's <#> does.
func (clickhouseDialect) VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric) {
	b.Write(map[VectorMetric]string{L2: "L2Distance(", Cosine: "cosineDistance(", InnerProduct: "-dotProduct("}[m])
	e.build(b)
	b.Write(", ")
	if fs := floatsOf(arg); fs != nil {
		b.Arg(fs)
	} else {
		b.Arg(arg)
	}
	b.Write(")")
}

func intText(n int) string { return strconv.Itoa(n) }

func jsonOf(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("orm: %T in a JSON condition cannot be encoded: %v", v, err))
	}
	return string(b)
}

func sliceOf(v any) []any {
	if vs, ok := v.([]any); ok {
		return vs
	}
	return []any{v}
}
