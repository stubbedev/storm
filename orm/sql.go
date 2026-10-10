package orm

import (
	"fmt"
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
	v = inUTC(v)
	if r, ok := b.Dialect.(argRenderer); ok && r.renderArg(b, v) {
		return
	}
	b.bind(v)
}

// inUTC moves times to UTC before they are bound: engines that store
// wall-clock timestamps (Postgres timestamp, SQLite text) would otherwise
// keep the caller's offset or drop it, shifting or misordering instants.
func inUTC(v any) any {
	switch t := v.(type) {
	case time.Time:
		return t.UTC()
	case *time.Time:
		if t != nil {
			return t.UTC()
		}
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
		b.sb.WriteString(literal(b.Dialect, v))
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

func literal(d Dialect, v any) string {
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
	case bool:
		return d.Bool(v)
	case time.Time:
		return "'" + v.UTC().Format("2006-01-02 15:04:05.999999999") + "'"
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	return literal(d, fmt.Sprint(v))
}
