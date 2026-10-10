package orm

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// DuckDB: embedded analytical engine, Postgres-flavoured SQL with ?
// placeholders. Reads CSV/Parquet/JSON files straight from the builder, so
// it doubles as the flat-file backend (orm.FromRaw("read_csv(?)", "data.csv")).
// ---------------------------------------------------------------------------

type duckdbDialect struct{ sqliteDialect }

func (duckdbDialect) noSavepoints() {}

// renderArg writes a uint64 above the int64 range as a UBIGINT literal:
// database/sql cannot bind it, and the text comes from the number itself.
func (duckdbDialect) renderArg(b *SQL, v any) bool {
	u, ok := v.(uint64)
	if !ok || u <= math.MaxInt64 {
		return false
	}
	b.Write(strconv.FormatUint(u, 10), "::UBIGINT")
	return true
}

var DuckDB Dialect = duckdbDialect{}

func (duckdbDialect) Name() string { return "duckdb" }

func (duckdbDialect) Limit(b *SQL, l, o int) { limit(b, l, o, "") }

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

func (duckdbDialect) JSONExtract(b *SQL, e AnyExpr, path []string, kind JSONKind) {
	castText(b, func(b *SQL) {
		b.Write("(")
		e.build(b)
		b.Write(" ->> ")
		b.Arg(jsonPath(path))
		b.Write(")")
	}, kind, "BIGINT", "DOUBLE")
}

func (duckdbDialect) JSONContains(b *SQL, e AnyExpr, v any) {
	b.Write("json_contains(")
	e.build(b)
	b.Write(", CAST(")
	jsonArg(b, v)
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
	nullForNull(b, e, func(b *SQL) {
		b.Write("(")
		e.build(b)
		b.Write(" -> ")
		b.Arg(jsonPath(path))
		b.Write(") IS NOT NULL")
	})
}

// FullText matches term anywhere in any of exprs, ignoring case like the
// other engines' natural-language search.
func (duckdbDialect) FullText(b *SQL, exprs []AnyExpr, term string) { likeAny(b, exprs, term) }

func (duckdbDialect) Like(b *SQL, e AnyExpr, pattern string, caseless, not bool) {
	standardLike(b, e, pattern, caseless, not)
}

func (duckdbDialect) Truncate(table string) []string {
	return []string{"DELETE FROM " + doubleQuote(table)}
}

func (duckdbDialect) VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric) {
	fs, ok := vectorOf(arg)
	if !ok {
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

// Like on ClickHouse, whose LIKE is case-sensitive, ILIKE caseless, and
// backslash the fixed escape.
func (clickhouseDialect) Like(b *SQL, e AnyExpr, pattern string, caseless, not bool) {
	e.build(b)
	b.Write(map[bool]string{true: " NOT", false: ""}[not])
	b.Write(map[bool]string{true: " ILIKE ", false: " LIKE "}[caseless])
	b.Arg(pattern)
}

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

// JSONExtract reads through JSON_VALUE, which yields text; ClickHouse's
// *OrNull conversions turn it into the kind, NULL where it does not parse.
func (clickhouseDialect) JSONExtract(b *SQL, e AnyExpr, path []string, kind JSONKind) {
	text := func(b *SQL) {
		b.Write("JSON_VALUE(")
		clickhouseDocument(b, e)
		b.Write(", ")
		b.Arg(jsonPath(path))
		b.Write(")")
	}
	switch kind {
	case JSONInt:
		b.Write("toInt64OrNull(")
		text(b)
		b.Write(")")
	case JSONFloat:
		b.Write("toFloat64OrNull(")
		text(b)
		b.Write(")")
	case JSONBool:
		b.Write("multiIf(")
		text(b)
		b.Write(" = 'true', true, ")
		text(b)
		b.Write(" = 'false', false, NULL)")
	default:
		text(b)
	}
}

// clickhouseDocument is e as a non-Nullable string: ClickHouse's JSON
// functions refuse Nullable arguments, so NULL reads as an empty document
// here and callers propagate the NULL themselves.
func clickhouseDocument(b *SQL, e AnyExpr) {
	b.Write("ifNull(")
	e.build(b)
	b.Write(", '')")
}

// JSONContains and JSONOverlaps compare array elements in their raw JSON
// form, so strings and numbers both match exactly.
func (clickhouseDialect) JSONContains(b *SQL, e AnyExpr, v any) {
	clickhouseDialect{}.JSONOverlaps(b, e, []any{v})
}

func (clickhouseDialect) JSONOverlaps(b *SQL, e AnyExpr, v any) {
	clickhouseNullForNull(b, e, func(b *SQL) {
		b.Write("hasAny(JSONExtractArrayRaw(")
		clickhouseDocument(b, e)
		b.Write("), [")
		for i, x := range sliceOf(v) {
			if i > 0 {
				b.Write(", ")
			}
			jsonArg(b, x)
		}
		b.Write("])")
	})
}

func (clickhouseDialect) JSONLength(b *SQL, e AnyExpr) {
	clickhouseNullForNull(b, e, func(b *SQL) {
		b.Write("JSONLength(")
		clickhouseDocument(b, e)
		b.Write(")")
	})
}

func (clickhouseDialect) JSONHasKey(b *SQL, e AnyExpr, path []string) {
	clickhouseNullForNull(b, e, func(b *SQL) {
		b.Write("JSONHas(")
		clickhouseDocument(b, e)
		for _, p := range path {
			b.Write(", ")
			b.Arg(p)
		}
		b.Write(")")
	})
}

// clickhouseNullForNull is nullForNull in ClickHouse's if(), whose result
// type is Nullable where CASE could not mix NULL with the test's type.
func clickhouseNullForNull(b *SQL, e AnyExpr, f frag) {
	b.Write("if(isNull(")
	e.build(b)
	b.Write("), NULL, ")
	f(b)
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

// renderArg writes times with their full fraction: the driver binds a
// time.Time at whole seconds, dropping what a DateTime64 column stores.
// In an INSERT's VALUES the time is a string the insert settings parse
// into either column type; elsewhere it is a DateTime64 expression, which
// compares and assigns to both. Either way the text comes from the time
// value, never from input.
func (clickhouseDialect) renderArg(b *SQL, v any) bool {
	t, ok := v.(time.Time)
	if p, isPtr := v.(*time.Time); isPtr && p != nil {
		t, ok = *p, true
	}
	if !ok {
		return false
	}
	text := t.UTC().Format("2006-01-02 15:04:05.000000000")
	if b.values {
		b.Write("'", text, "'")
	} else {
		b.Write("toDateTime64('", text, "', 9, 'UTC')")
	}
	return true
}

// insertSettings lets the VALUES time strings carry fractional seconds
// into DateTime columns too (which drop them).
func (clickhouseDialect) insertSettings() string {
	return "SETTINGS date_time_input_format = 'best_effort'"
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
func (clickhouseDialect) noTransactions()     {}

// mutationSuffix makes ClickHouse mutations synchronous.
func (clickhouseDialect) mutationNeedsWhere() {}

// explicitUnionDistinct: ClickHouse rejects a bare UNION unless the
// server sets union_default_mode.
func (clickhouseDialect) explicitUnionDistinct() {}

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
	if fs, ok := vectorOf(arg); ok {
		b.Arg(fs)
	} else {
		b.Arg(arg)
	}
	b.Write(")")
}

func intText(n int) string { return strconv.Itoa(n) }

func sliceOf(v any) []any {
	if vs, ok := v.([]any); ok {
		return vs
	}
	return []any{v}
}
