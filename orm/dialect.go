package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Dialect is everything that differs between database engines. Pick one per
// connection; the same query then renders for that engine.
type Dialect interface {
	Name() string
	Placeholder(n int) string
	Quote(ident string) string
	Bool(v bool) string
	// Returning reports whether INSERT ... RETURNING is supported; if not,
	// LastInsertId is used for auto-incrementing keys.
	Returning() bool
	// AutoKeys reports whether the engine generates integer keys; when not,
	// models on this engine need uuid, ulid or manual keys.
	AutoKeys() bool
	// InsertVerb is "INSERT" or e.g. MySQL's "INSERT IGNORE" when ignore is set.
	InsertVerb(ignore bool) string
	// OnConflict writes the upsert / insert-or-ignore clause.
	OnConflict(b *SQL, ignore bool, uniqueBy, update []string)
	Lock(mode LockMode) string
	// Limit writes LIMIT/OFFSET; zero means absent.
	Limit(b *SQL, limit, offset int)
	// NullsLast writes an ORDER BY term for e that sorts NULLs after every
	// value in either direction.
	NullsLast(b *SQL, e AnyExpr, desc bool)
	Random() string
	// DatePart extracts "date", "time", "year", "month" or "day" from e.
	DatePart(b *SQL, part string, e AnyExpr)
	// JSONExtract reads path from the JSON in e as kind.
	JSONExtract(b *SQL, e AnyExpr, path []string, kind JSONKind)
	JSONContains(b *SQL, e AnyExpr, v any)
	// JSONOverlaps writes a test that the JSON array in e and v share an
	// element (whereJsonOverlaps).
	JSONOverlaps(b *SQL, e AnyExpr, v any)
	JSONLength(b *SQL, e AnyExpr)
	JSONHasKey(b *SQL, e AnyExpr, path []string)
	// Like writes a LIKE match of e against pattern (% and _ wildcards, \
	// escaping them), case-sensitive unless caseless, negated when not.
	Like(b *SQL, e AnyExpr, pattern string, caseless, not bool)
	// FullText writes a full text match of term against exprs (whereFullText).
	FullText(b *SQL, exprs []AnyExpr, term string)
	// VectorDistance orders by the distance between the vector expression e
	// and arg under the metric (nearest-neighbour search).
	VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric)
	// UpdatePrefix starts an UPDATE statement for table including the SET
	// keyword; ClickHouse's is ALTER TABLE ... UPDATE (no SET).
	UpdatePrefix(b *SQL, table string)
	// DeletePrefix starts a DELETE statement for table.
	DeletePrefix(b *SQL, table string)
	// Truncate empties a table and resets its auto-increment counter.
	Truncate(table string) []string
}

// LockMode is a pessimistic row lock.
type LockMode int

const (
	NoLock LockMode = iota
	ForUpdate
	ForShare
)

var (
	SQLite   Dialect = sqliteDialect{}
	Postgres Dialect = postgresDialect{}
	MySQL    Dialect = mysqlDialect{}
	MariaDB  Dialect = mariadbDialect{}
)

// DialectFor maps a database/sql driver name to a dialect.
func DialectFor(driver string) (Dialect, bool) {
	switch driver {
	case "sqlite", "sqlite3", "libsql":
		return SQLite, true
	case "postgres", "pgx", "pq":
		return Postgres, true
	case "mysql":
		return MySQL, true
	case "mariadb":
		return MariaDB, true
	case "duckdb", "duckdb-go":
		return DuckDB, true
	case "clickhouse":
		return ClickHouse, true
	}
	return nil, false
}

// ---------------------------------------------------------------------------

type sqliteDialect struct{}

func (sqliteDialect) Name() string           { return "sqlite" }
func (sqliteDialect) Placeholder(int) string { return "?" }
func (sqliteDialect) Quote(id string) string { return doubleQuote(id) }
func (sqliteDialect) Bool(v bool) string     { return map[bool]string{true: "1", false: "0"}[v] }
func (sqliteDialect) Returning() bool        { return true }
func (sqliteDialect) AutoKeys() bool         { return true }
func (sqliteDialect) InsertVerb(bool) string { return "INSERT" }
func (sqliteDialect) Lock(LockMode) string   { return "" } // SQLite locks the whole database
func (sqliteDialect) Random() string         { return "RANDOM()" }
func (sqliteDialect) Limit(b *SQL, l, o int) { limit(b, l, o, "-1") }

func (sqliteDialect) NullsLast(b *SQL, e AnyExpr, desc bool) { nullsLast(b, e, desc) }
func (sqliteDialect) JSONLength(b *SQL, e AnyExpr)           { fn(b, "json_array_length", e) }

func (sqliteDialect) OnConflict(b *SQL, ignore bool, uniqueBy, update []string) {
	onConflict(b, ignore, uniqueBy, update)
}

func (sqliteDialect) DatePart(b *SQL, part string, e AnyExpr) {
	switch part {
	case "date", "time":
		fn(b, part, e)
	default:
		f := map[string]string{"year": "%Y", "month": "%m", "day": "%d"}[part]
		b.Write("CAST(strftime('", f, "', ")
		e.build(b)
		b.Write(") AS INTEGER)")
	}
}

func (sqliteDialect) JSONExtract(b *SQL, e AnyExpr, path []string, _ JSONKind) {
	b.Write("json_extract(")
	e.build(b)
	b.Write(", ")
	b.Arg(jsonPath(path))
	b.Write(")")
}

func (sqliteDialect) JSONHasKey(b *SQL, e AnyExpr, path []string) {
	nullForNull(b, e, func(b *SQL) {
		b.Write("json_type(")
		e.build(b)
		b.Write(", ")
		b.Arg(jsonPath(path))
		b.Write(") IS NOT NULL")
	})
}

// FullText on SQLite falls back to case-insensitive LIKE on each column;
// real full text search there needs an FTS5 virtual table.
func (sqliteDialect) FullText(b *SQL, exprs []AnyExpr, term string) { likeAny(b, exprs, term) }

// Like on SQLite: LIKE ignores ASCII case there, so a case-sensitive match
// is GLOB over the pattern translated to GLOB's wildcards.
func (sqliteDialect) Like(b *SQL, e AnyExpr, pattern string, caseless, not bool) {
	e.build(b)
	b.Write(map[bool]string{true: " NOT", false: ""}[not])
	if caseless {
		b.Write(" LIKE ")
		b.Arg(pattern)
		b.Write(` ESCAPE '\'`)
		return
	}
	b.Write(" GLOB ")
	b.Arg(globPattern(pattern))
}

// globPattern is a LIKE pattern as a GLOB pattern: % and _ become * and ?,
// and GLOB's own metacharacters match themselves.
func globPattern(pattern string) string {
	var g strings.Builder
	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			escaped = false
			g.WriteString(globLiteral(r))
		case r == '\\':
			escaped = true
		case r == '%':
			g.WriteByte('*')
		case r == '_':
			g.WriteByte('?')
		default:
			g.WriteString(globLiteral(r))
		}
	}
	return g.String()
}

func globLiteral(r rune) string {
	if r == '*' || r == '?' || r == '[' {
		return "[" + string(r) + "]"
	}
	return string(r)
}

// likeAny matches term anywhere in any of exprs, ignoring case.
func likeAny(b *SQL, exprs []AnyExpr, term string) {
	b.Write("(")
	for i, e := range exprs {
		if i > 0 {
			b.Write(" OR ")
		}
		b.Dialect.Like(b, e, "%"+EscapeLike(term)+"%", true, false)
	}
	b.Write(")")
}

// standardLike is LIKE / ILIKE with an explicit backslash escape
// (Postgres, DuckDB).
func standardLike(b *SQL, e AnyExpr, pattern string, caseless, not bool) {
	e.build(b)
	b.Write(map[bool]string{true: " NOT", false: ""}[not])
	b.Write(map[bool]string{true: " ILIKE ", false: " LIKE "}[caseless])
	b.Arg(pattern)
	b.Write(` ESCAPE '\'`)
}

func (d sqliteDialect) Truncate(table string) []string {
	return []string{"DELETE FROM " + d.Quote(table), "DELETE FROM sqlite_sequence WHERE name = " + stringLiteral(d, table)}
}

func (sqliteDialect) UpdatePrefix(b *SQL, table string) {
	b.Write("UPDATE ")
	b.Ident(table)
	b.Write(" SET")
}

func (sqliteDialect) DeletePrefix(b *SQL, table string) {
	b.Write("DELETE FROM ")
	b.Ident(table)
}

func (sqliteDialect) VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric) {
	if m != L2 {
		b.Write("orm_vector_metric_unsupported(")
		e.build(b)
		b.Write(")")
		return
	}
	component := func() {
		b.Write("(je.value - json_extract(")
		b.Arg(arg)
		b.Write(", '$[' || je.key || ']'))")
	}
	b.Write("sqrt((SELECT sum(")
	component()
	b.Write(" * ")
	component()
	b.Write(") FROM json_each(")
	e.build(b)
	b.Write(") je))")
}

// JSONContains propagates NULL for a NULL document, like Postgres and MySQL,
// so negating it doesn't match rows without JSON.
func (sqliteDialect) JSONContains(b *SQL, e AnyExpr, v any) {
	sqliteDialect{}.JSONOverlaps(b, e, []any{v})
}

func (sqliteDialect) JSONOverlaps(b *SQL, e AnyExpr, v any) {
	nullForNull(b, e, func(b *SQL) {
		b.Write("EXISTS (SELECT 1 FROM json_each(")
		e.build(b)
		b.Write(") WHERE value IN (")
		writeList(b, v)
		b.Write("))")
	})
}

// ---------------------------------------------------------------------------

type postgresDialect struct{}

func (postgresDialect) Name() string             { return "postgres" }
func (postgresDialect) Placeholder(n int) string { return "$" + strconv.Itoa(n) }
func (postgresDialect) Quote(id string) string   { return doubleQuote(id) }
func (postgresDialect) Bool(v bool) string       { return strings.ToUpper(strconv.FormatBool(v)) }
func (postgresDialect) Returning() bool          { return true }
func (postgresDialect) AutoKeys() bool           { return true }
func (postgresDialect) InsertVerb(bool) string   { return "INSERT" }
func (postgresDialect) Random() string           { return "RANDOM()" }
func (postgresDialect) Limit(b *SQL, l, o int)   { limit(b, l, o, "") }

func (postgresDialect) NullsLast(b *SQL, e AnyExpr, desc bool) { nullsLast(b, e, desc) }

func (postgresDialect) identifierLimit() (int, bool) { return 63, false }

func (postgresDialect) Like(b *SQL, e AnyExpr, pattern string, caseless, not bool) {
	standardLike(b, e, pattern, caseless, not)
}

func (postgresDialect) Lock(m LockMode) string {
	return map[LockMode]string{ForUpdate: " FOR UPDATE", ForShare: " FOR SHARE"}[m]
}

func (postgresDialect) OnConflict(b *SQL, ignore bool, uniqueBy, update []string) {
	onConflict(b, ignore, uniqueBy, update)
}

func (postgresDialect) DatePart(b *SQL, part string, e AnyExpr) {
	switch part {
	case "date", "time":
		b.Write("CAST(")
		e.build(b)
		b.Write(" AS ", strings.ToUpper(part), ")")
	default:
		b.Write("CAST(EXTRACT(", strings.ToUpper(part), " FROM ")
		e.build(b)
		b.Write(") AS INTEGER)")
	}
}

func (postgresDialect) JSONExtract(b *SQL, e AnyExpr, path []string, kind JSONKind) {
	castText(b, func(b *SQL) {
		b.Write("((")
		e.build(b)
		b.Write(")::jsonb #>> ")
		b.Arg("{" + strings.Join(path, ",") + "}")
		b.Write(")")
	}, kind, "bigint", "double precision")
}

func (postgresDialect) JSONContains(b *SQL, e AnyExpr, v any) {
	postgresDialect{}.JSONOverlaps(b, e, []any{v})
}

// JSONOverlaps tests containment of each value as a one-element array, so
// strings, numbers and booleans all match by JSON equality.
func (postgresDialect) JSONOverlaps(b *SQL, e AnyExpr, v any) {
	b.Write("(")
	e.build(b)
	b.Write(")::jsonb @> ANY (ARRAY[")
	for i, x := range sliceOf(v) {
		if i > 0 {
			b.Write(", ")
		}
		jsonArg(b, []any{x})
		b.Write("::jsonb")
	}
	b.Write("])")
}

func (postgresDialect) JSONHasKey(b *SQL, e AnyExpr, path []string) {
	nullForNull(b, e, func(b *SQL) {
		b.Write("(")
		e.build(b)
		b.Write(")::jsonb #> ")
		b.Arg("{" + strings.Join(path, ",") + "}")
		b.Write(" IS NOT NULL")
	})
}

// FullText matches the expression index the schema grammar creates for
// FullText(...) columns, so Postgres can use it.
func (postgresDialect) FullText(b *SQL, exprs []AnyExpr, term string) {
	b.Write("(")
	for i, e := range exprs {
		if i > 0 {
			b.Write(" || ")
		}
		b.Write("to_tsvector('english', ")
		e.build(b)
		b.Write(")")
	}
	b.Write(") @@ plainto_tsquery('english', ")
	b.Arg(term)
	b.Write(")")
}

func (d postgresDialect) Truncate(table string) []string {
	return []string{"TRUNCATE TABLE " + d.Quote(table) + " RESTART IDENTITY CASCADE"}
}

func (postgresDialect) UpdatePrefix(b *SQL, table string) {
	b.Write("UPDATE ")
	b.Ident(table)
	b.Write(" SET")
}

func (postgresDialect) DeletePrefix(b *SQL, table string) {
	b.Write("DELETE FROM ")
	b.Ident(table)
}

func (postgresDialect) VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric) {
	e.build(b)
	b.Write(" ", map[VectorMetric]string{L2: "<->", Cosine: "<=>", InnerProduct: "<#>"}[m], " ")
	b.Arg(arg)
	b.Write("::vector")
}

func (postgresDialect) JSONLength(b *SQL, e AnyExpr) {
	b.Write("jsonb_array_length((")
	e.build(b)
	b.Write(")::jsonb)")
}

// ---------------------------------------------------------------------------

type mysqlDialect struct{}

func (mysqlDialect) Name() string           { return "mysql" }
func (mysqlDialect) Placeholder(int) string { return "?" }
func (mysqlDialect) Quote(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}
func (mysqlDialect) Bool(v bool) string     { return map[bool]string{true: "1", false: "0"}[v] }
func (mysqlDialect) Returning() bool        { return false }
func (mysqlDialect) AutoKeys() bool         { return true }
func (mysqlDialect) Random() string         { return "RAND()" }
func (mysqlDialect) Limit(b *SQL, l, o int) { limit(b, l, o, "18446744073709551615") }

func (mysqlDialect) identifierLimit() (int, bool) { return 64, true }

// NullsLast on MySQL and MariaDB, which have no NULLS LAST: sorting on
// "e IS NULL" first puts the NULLs after every value.
func (mysqlDialect) NullsLast(b *SQL, e AnyExpr, desc bool) {
	e.build(b)
	b.Write(" IS NULL, ")
	e.build(b)
	b.Write(map[bool]string{false: " ASC", true: " DESC"}[desc])
}

// Like on MySQL and MariaDB, where LIKE follows the column's collation
// (case-insensitive by default): a binary pattern makes the match
// case-sensitive, lowering both sides makes it caseless on any collation.
// Backslash is LIKE's default escape there.
func (mysqlDialect) Like(b *SQL, e AnyExpr, pattern string, caseless, not bool) {
	if caseless {
		fn(b, "LOWER", e)
	} else {
		e.build(b)
	}
	b.Write(map[bool]string{true: " NOT", false: ""}[not], " LIKE ")
	if caseless {
		b.Write("LOWER(")
		b.Arg(pattern)
		b.Write(")")
		return
	}
	b.Write("CAST(")
	b.Arg(pattern)
	b.Write(" AS BINARY)")
}

func (mysqlDialect) InsertVerb(ignore bool) string {
	if ignore {
		return "INSERT IGNORE"
	}
	return "INSERT"
}

func (mysqlDialect) Lock(m LockMode) string {
	return map[LockMode]string{ForUpdate: " FOR UPDATE", ForShare: " FOR SHARE"}[m]
}

func (mysqlDialect) OnConflict(b *SQL, ignore bool, _, update []string) {
	if ignore || len(update) == 0 {
		return // handled by INSERT IGNORE
	}
	b.Write(" ON DUPLICATE KEY UPDATE ")
	for i, c := range update {
		if i > 0 {
			b.Write(", ")
		}
		b.Ident(c)
		b.Write(" = VALUES(")
		b.Ident(c)
		b.Write(")")
	}
}

func (mysqlDialect) DatePart(b *SQL, part string, e AnyExpr) { fn(b, strings.ToUpper(part), e) }

func (mysqlDialect) JSONExtract(b *SQL, e AnyExpr, path []string, kind JSONKind) {
	castText(b, func(b *SQL) {
		b.Write("JSON_UNQUOTE(JSON_EXTRACT(")
		e.build(b)
		b.Write(", ")
		b.Arg(jsonPath(path))
		b.Write("))")
	}, kind, "SIGNED", "DOUBLE")
}

func (mysqlDialect) JSONContains(b *SQL, e AnyExpr, v any) {
	b.Write("JSON_CONTAINS(")
	e.build(b)
	b.Write(", ")
	jsonArg(b, v)
	b.Write(")")
}

func (mysqlDialect) JSONOverlaps(b *SQL, e AnyExpr, v any) {
	b.Write("JSON_OVERLAPS(")
	e.build(b)
	b.Write(", ")
	jsonArg(b, sliceOf(v))
	b.Write(")")
}

func (mysqlDialect) JSONLength(b *SQL, e AnyExpr) { fn(b, "JSON_LENGTH", e) }

func (mysqlDialect) JSONHasKey(b *SQL, e AnyExpr, path []string) {
	b.Write("JSON_CONTAINS_PATH(")
	e.build(b)
	b.Write(", 'one', ")
	b.Arg(jsonPath(path))
	b.Write(")")
}

func (mysqlDialect) FullText(b *SQL, exprs []AnyExpr, term string) {
	b.Write("MATCH (")
	for i, e := range exprs {
		if i > 0 {
			b.Write(", ")
		}
		e.build(b)
	}
	b.Write(") AGAINST (")
	b.Arg(term)
	b.Write(" IN NATURAL LANGUAGE MODE)")
}

func (d mysqlDialect) Truncate(table string) []string {
	return []string{"TRUNCATE TABLE " + d.Quote(table)}
}

func (mysqlDialect) UpdatePrefix(b *SQL, table string) {
	b.Write("UPDATE ")
	b.Ident(table)
	b.Write(" SET")
}

func (mysqlDialect) DeletePrefix(b *SQL, table string) {
	b.Write("DELETE FROM ")
	b.Ident(table)
}

func (mysqlDialect) renderArg(b *SQL, v any) bool { return wrapVector(b, v, "STRING_TO_VECTOR") }

func (mysqlDialect) VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric) {
	b.Write("DISTANCE(")
	e.build(b)
	b.Write(", ")
	b.Arg(arg)
	b.Write(", ", map[VectorMetric]string{L2: "'EUCLIDEAN'", Cosine: "'COSINE'", InnerProduct: "'DOT'"}[m], ")")
}

// ---------------------------------------------------------------------------

// mariadbDialect is MySQL's wire protocol and most of its SQL, diverging
// where MariaDB does: shared locks, INSERT ... RETURNING and vector
// functions (MariaDB 11.7+).
type mariadbDialect struct{ mysqlDialect }

func (mariadbDialect) Name() string    { return "mariadb" }
func (mariadbDialect) Returning() bool { return true }

func (mariadbDialect) Lock(m LockMode) string {
	return map[LockMode]string{ForUpdate: " FOR UPDATE", ForShare: " LOCK IN SHARE MODE"}[m]
}

// VectorDistance uses MariaDB's VEC_DISTANCE functions, which have no
// inner-product form; that metric fails with an unknown-function error.
func (mariadbDialect) renderArg(b *SQL, v any) bool { return wrapVector(b, v, "VEC_FromText") }

func (mariadbDialect) VectorDistance(b *SQL, e AnyExpr, arg any, m VectorMetric) {
	b.Write(map[VectorMetric]string{L2: "VEC_DISTANCE_EUCLIDEAN(", Cosine: "VEC_DISTANCE_COSINE(", InnerProduct: "orm_vector_metric_unsupported("}[m])
	e.build(b)
	b.Write(", ")
	b.Arg(arg)
	b.Write(")")
}

// ---------------------------------------------------------------------------

func onConflict(b *SQL, ignore bool, uniqueBy, update []string) {
	b.Write(" ON CONFLICT")
	if len(uniqueBy) > 0 {
		b.Write(" (")
		for i, c := range uniqueBy {
			if i > 0 {
				b.Write(", ")
			}
			b.Ident(c)
		}
		b.Write(")")
	}
	if ignore || len(update) == 0 {
		b.Write(" DO NOTHING")
		return
	}
	b.Write(" DO UPDATE SET ")
	for i, c := range update {
		if i > 0 {
			b.Write(", ")
		}
		b.Ident(c)
		b.Write(" = excluded.")
		b.Ident(c)
	}
}

// limit writes LIMIT/OFFSET; unbounded is the LIMIT required before a bare
// OFFSET ("" if the dialect allows OFFSET alone).
func nullsLast(b *SQL, e AnyExpr, desc bool) {
	e.build(b)
	b.Write(map[bool]string{false: " ASC", true: " DESC"}[desc], " NULLS LAST")
}

func limit(b *SQL, l, o int, unbounded string) {
	switch {
	case l > 0:
		b.Write(" LIMIT ", strconv.Itoa(l))
	case o > 0 && unbounded != "":
		b.Write(" LIMIT ", unbounded)
	}
	if o > 0 {
		b.Write(" OFFSET ", strconv.Itoa(o))
	}
}

func fn(b *SQL, name string, e AnyExpr) {
	b.Write(name, "(")
	e.build(b)
	b.Write(")")
}

func doubleQuote(id string) string { return `"` + strings.ReplaceAll(id, `"`, `""`) + `"` }

func jsonPath(path []string) string { return "$." + strings.Join(path, ".") }

// writeList writes v (a slice or a single value) as a comma-separated list
// of bound arguments.
// JSONKind is the SQL type a JSON value is read as.
type JSONKind int

const (
	JSONText JSONKind = iota
	JSONInt
	JSONFloat
	JSONBool
)

// jsonKindOf is the JSONKind a Go type reads as.
func jsonKindOf[T any]() JSONKind {
	switch reflect.TypeFor[T]().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return JSONInt
	case reflect.Float32, reflect.Float64:
		return JSONFloat
	case reflect.Bool:
		return JSONBool
	}
	return JSONText
}

// jsonArg binds v encoded as JSON, failing the statement when it cannot
// be encoded.
func jsonArg(b *SQL, v any) {
	enc, err := json.Marshal(v)
	if err != nil {
		b.fail(fmt.Errorf("orm: %T in a JSON condition cannot be encoded: %w", v, err))
		return
	}
	b.Arg(string(enc))
}

// nullForNull writes f, or NULL when the document e is NULL, so a negated
// JSON test excludes rows without JSON as SQL's NULL logic does.
func nullForNull(b *SQL, e AnyExpr, f frag) {
	b.Write("CASE WHEN ")
	e.build(b)
	b.Write(" IS NULL THEN NULL ELSE ")
	f(b)
	b.Write(" END")
}

// castText writes text as kind with an SQL CAST; bools read their JSON
// spelling.
func castText(b *SQL, text frag, kind JSONKind, intType, floatType string) {
	switch kind {
	case JSONInt:
		b.Write("CAST(")
		text(b)
		b.Write(" AS ", intType, ")")
	case JSONFloat:
		b.Write("CAST(")
		text(b)
		b.Write(" AS ", floatType, ")")
	case JSONBool:
		b.Write("CASE ")
		text(b)
		b.Write(" WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END")
	default:
		text(b)
	}
}

func writeList(b *SQL, v any) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := range rv.Len() {
			if i > 0 {
				b.Write(", ")
			}
			b.Arg(rv.Index(i).Interface())
		}
		return
	}
	b.Arg(v)
}
