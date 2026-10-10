package orm

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// SQL accumulates a statement and its arguments for one dialect. Every query
// part is a fragment that writes itself into an SQL, so the same Query renders
// differently for SQLite, Postgres or MySQL depending on its connection.
type SQL struct {
	Dialect Dialect

	sb     strings.Builder
	args   []any
	inline bool              // ToRawSQL: write literals instead of placeholders
	values bool              // rendering an INSERT's VALUES rows
	plain  bool              // unqualified columns (ClickHouse mutations)
	alias  map[string]string // table -> alias, for self-referencing subqueries
	err    error             // the first fragment that could not render
}

func (b *SQL) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}

// frag is a renderable piece of SQL.
type frag func(*SQL)

// Write appends raw SQL text.
func (b *SQL) Write(parts ...string) {
	for _, p := range parts {
		b.sb.WriteString(p)
	}
}

// Ident appends a quoted identifier; a dotted name is quoted per part
// ("users.id" is table users, column id) and a * part stays bare
// ("users.*").
func (b *SQL) Ident(name string) {
	i := 0
	for part := range strings.SplitSeq(name, ".") {
		if i > 0 {
			b.sb.WriteByte('.')
		}
		i++
		if part == "*" {
			b.sb.WriteByte('*')
			continue
		}
		b.sb.WriteString(b.Dialect.Quote(part))
	}
}

// argRenderer marks dialects that render some argument values themselves
// rather than binding them as they are: vectors built from their text form
// on MySQL and MariaDB, exact-precision times on ClickHouse.
type argRenderer interface{ renderArg(b *SQL, v any) bool }

// Arg appends a bound argument using the dialect's placeholder syntax.
func (b *SQL) Arg(v any) {
	v = boundValue(v)
	if r, ok := b.Dialect.(argRenderer); ok && r.renderArg(b, v) {
		return
	}
	b.bind(v)
}

// boundValue is v as it is bound: a nil pointer is NULL (some drivers,
// ClickHouse's, would call a value method on it), and times move to UTC:
// engines that store wall-clock timestamps (Postgres timestamp, SQLite
// text) would otherwise keep the caller's offset or drop it, shifting or
// misordering instants.
func boundValue(v any) any {
	switch t := v.(type) {
	case time.Time:
		return t.UTC()
	case *time.Time:
		if t != nil {
			return t.UTC()
		}
		return nil
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && rv.IsNil() {
		return nil
	}
	return v
}

// wrapVector binds v inside fn(...) when it is a vector value.
func wrapVector(b *SQL, v any, fn string) bool {
	if _, ok := vectorOf(v); !ok {
		return false
	}
	b.sb.WriteString(fn + "(")
	b.bind(v)
	b.sb.WriteString(")")
	return true
}

func (b *SQL) bind(v any) {
	if b.inline {
		text, err := literal(b.Dialect, v)
		if err != nil {
			b.fail(err)
			return
		}
		b.sb.WriteString(text)
		return
	}
	b.args = append(b.args, v)
	b.sb.WriteString(b.Dialect.Placeholder(len(b.args)))
}

// Raw appends a SQL fragment, binding each ? to the next argument. A ?
// inside a quoted string or identifier is text, ?? is a literal ?, and a
// placeholder count that differs from len(args) fails the statement.
func (b *SQL) Raw(sql string, args ...any) {
	used := 0
	var quote byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '?' && i+1 < len(sql) && sql[i+1] == '?':
			b.sb.WriteByte('?')
			i++
			continue
		case c == '?':
			if used == len(args) {
				b.fail(fmt.Errorf("orm: raw SQL %q has more placeholders than its %d arguments", sql, len(args)))
				return
			}
			b.Arg(args[used])
			used++
			continue
		}
		b.sb.WriteByte(c)
	}
	if used < len(args) {
		b.fail(fmt.Errorf("orm: raw SQL %q has %d placeholders for %d arguments", sql, used, len(args)))
	}
}

// Col appends a qualified column, honouring any active table alias.
func (b *SQL) Col(table, column string) {
	if a, ok := b.alias[table]; ok {
		table = a
	}
	if b.plain {
		// ClickHouse mutations reject qualified columns in WHERE.
		b.Ident(column)
		return
	}
	b.Ident(table)
	b.sb.WriteString(".")
	b.Ident(column)
}

// withAlias renders f with table aliased, restoring the previous mapping after.
func (b *SQL) withAlias(table, alias string, f frag) {
	prev, had := b.alias[table]
	if b.alias == nil {
		b.alias = map[string]string{}
	}
	b.alias[table] = alias
	f(b)
	if had {
		b.alias[table] = prev
	} else {
		delete(b.alias, table)
	}
}

func (b *SQL) String() string { return b.sb.String() }

// render produces the final statement for dialect d.
func render(d Dialect, f frag) (string, []any, error) {
	b := &SQL{Dialect: d}
	f(b)
	return b.String(), b.args, b.err
}

// literal is v as an SQL literal of dialect d, for ToRawSQL: driver
// values are converted first, and strings are escaped for the dialect,
// backslashes included where MySQL, MariaDB and ClickHouse treat them as
// escapes.
func literal(d Dialect, v any) (string, error) {
	if val, ok := v.(driver.Valuer); ok {
		raw, err := val.Value()
		if err != nil {
			return "", fmt.Errorf("orm: converting %T: %w", v, err)
		}
		return literal(d, raw)
	}
	switch v := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return stringLiteral(d, v), nil
	case []byte:
		return stringLiteral(d, string(v)), nil
	case bool:
		return d.Bool(v), nil
	case time.Time:
		return "'" + v.UTC().Format("2006-01-02 15:04:05.999999999") + "'", nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	}
	return stringLiteral(d, fmt.Sprint(v)), nil
}

func stringLiteral(d Dialect, s string) string {
	switch d.Name() {
	case "mysql", "mariadb", "clickhouse":
		s = strings.ReplaceAll(s, `\`, `\\`)
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
