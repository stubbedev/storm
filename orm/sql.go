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
	plain  bool              // unqualified columns (ClickHouse mutations)
	alias  map[string]string // table -> alias, for self-referencing subqueries
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

// vectorArgs marks dialects whose vector columns take a vector built from
// its text form (MariaDB's VEC_FromText, MySQL's STRING_TO_VECTOR).
type vectorArgs interface{ vectorFromText() string }

// Arg appends a bound argument using the dialect's placeholder syntax.
func (b *SQL) Arg(v any) {
	va, wraps := b.Dialect.(vectorArgs)
	if _, isVector := vectorOf(v); wraps && isVector {
		b.sb.WriteString(va.vectorFromText() + "(")
		b.bind(v)
		b.sb.WriteString(")")
		return
	}
	b.bind(v)
}

func (b *SQL) bind(v any) {
	if b.inline {
		b.sb.WriteString(literal(b.Dialect, v))
		return
	}
	b.args = append(b.args, v)
	b.sb.WriteString(b.Dialect.Placeholder(len(b.args)))
}

// Raw appends a SQL fragment, binding each ? to the next argument.
func (b *SQL) Raw(sql string, args ...any) {
	i := 0
	for {
		j := strings.IndexByte(sql, '?')
		if j < 0 || i >= len(args) {
			b.sb.WriteString(sql)
			return
		}
		b.sb.WriteString(sql[:j])
		b.Arg(args[i])
		i++
		sql = sql[j+1:]
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
func render(d Dialect, f frag) (string, []any) {
	b := &SQL{Dialect: d}
	f(b)
	return b.String(), b.args
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
		return "'" + v.Format("2006-01-02 15:04:05") + "'"
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
	return literal(d, fmt.Sprint(v))
}
