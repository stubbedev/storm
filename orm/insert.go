package orm

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// insertSettings marks dialects whose INSERT statements carry settings
// (ClickHouse parses the VALUES times with them).
type insertSettings interface{ insertSettings() string }

// writeInsert renders rows, each aligned to cols, as one INSERT into table.
func writeInsert(b *SQL, verb, table string, cols []string, rows [][]colSet) {
	b.Write(verb, " INTO ")
	b.Ident(table)
	b.Write(" (")
	for i, c := range cols {
		if i > 0 {
			b.Write(", ")
		}
		b.Ident(c)
	}
	b.Write(") ")
	if s, ok := b.Dialect.(insertSettings); ok {
		b.Write(s.insertSettings(), " ")
	}
	b.Write("VALUES ")
	b.values = true
	defer func() { b.values = false }()
	for r, row := range rows {
		if r > 0 {
			b.Write(", ")
		}
		b.Write("(")
		for i, s := range row {
			if i > 0 {
				b.Write(", ")
			}
			s.renderValue(b)
		}
		b.Write(")")
	}
}

// conflictKind is what an insert does with a row that collides with an
// existing one.
type conflictKind int

const (
	conflictFail   conflictKind = iota // the statement fails (plain insert)
	conflictIgnore                     // the row is skipped (insertOrIgnore)
	conflictUpdate                     // the existing row is updated (upsert)
)

// conflict is an insert's collision handling: uniqueBy names the
// colliding columns, update the columns an upsert overwrites.
type conflict struct {
	kind     conflictKind
	uniqueBy []string
	update   []string
}

func (c conflict) verb(d Dialect) string { return d.InsertVerb(c.kind == conflictIgnore) }

func (c conflict) clause(b *SQL) {
	if c.kind != conflictFail {
		b.Dialect.OnConflict(b, c.kind == conflictIgnore, c.uniqueBy, c.update)
	}
}

// keys are the columns collisions are matched on where the engine has no
// constraint to match them: uniqueBy, or the table's key.
func (c conflict) keys(key string) []string {
	if len(c.uniqueBy) > 0 {
		return c.uniqueBy
	}
	return []string{key}
}

// storeDocs inserts docs on a document store according to c.
func (c conflict) storeDocs(ctx context.Context, ds DocStore, conn, table string, docs []map[string]any, key string) error {
	start := time.Now()
	var err error
	switch c.kind {
	case conflictIgnore:
		err = insertOrIgnoreDocs(ctx, ds, table, key, docs, c.keys(key))
	case conflictUpdate:
		err = ds.UpsertDocs(ctx, table, key, docs, c.keys(key), c.update)
	default:
		err = ds.InsertDocs(ctx, table, key, docs)
	}
	verb := map[conflictKind]string{conflictFail: "insert", conflictIgnore: "insert or ignore", conflictUpdate: "upsert"}[c.kind]
	emitDocRaw(conn, fmt.Sprintf("%s %s (%d documents)", verb, table, len(docs)), time.Since(start), err)
	return err
}

// conflictEmulator marks dialects with no unique constraints and so no
// conflict clause (ClickHouse): insertSQL resolves collisions itself.
type conflictEmulator interface{ emulatesConflicts() }

// insertSQL inserts rows, aligned to cols, into table on an SQL connection.
func insertSQL(ctx context.Context, name, table string, cols []string, rows [][]colSet, c conflict, key string) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := sqlConn(ctx, name)
	if err != nil {
		return err
	}
	if _, ok := conn.Dialect.(conflictEmulator); ok && c.kind != conflictFail {
		return emulateConflicts(ctx, name, table, cols, rows, c, key)
	}
	_, err = execOn(ctx, name, func(b *SQL) {
		writeInsert(b, c.verb(b.Dialect), table, cols, rows)
		c.clause(b)
	})
	return err
}

// emulateConflicts is insertOrIgnore and upsert for engines without unique
// constraints: it reads which key combinations exist, inserts the rows
// whose keys are new and, for an upsert, updates the rest one statement
// per row. A concurrent writer can race the read and the writes.
func emulateConflicts(ctx context.Context, name, table string, cols []string, rows [][]colSet, c conflict, key string) error {
	keys := c.keys(key)
	pos := make([]int, len(keys))
	for i, k := range keys {
		if pos[i] = slices.Index(cols, k); pos[i] < 0 {
			return fmt.Errorf("orm: insert into %s: rows must set the conflict column %q", table, k)
		}
	}
	keyOf := func(row []colSet) (string, bool) {
		vals := make([]any, len(pos))
		for i, p := range pos {
			if row[p].expr != nil || row[p].raw == nil {
				return "", false
			}
			vals[i] = row[p].raw
		}
		return keyString(vals), true
	}
	taken, err := takenKeys(ctx, name, table, keys, pos, rows, keyOf)
	if err != nil {
		return err
	}
	fresh, clash := splitByKey(rows, keyOf, taken)
	if len(fresh) > 0 {
		if _, err := execOn(ctx, name, func(b *SQL) { writeInsert(b, "INSERT", table, cols, fresh) }); err != nil {
			return err
		}
	}
	if c.kind != conflictUpdate {
		return nil
	}
	for _, row := range clash {
		sets := make([]colSet, 0, len(c.update))
		for _, u := range c.update {
			if p := slices.Index(cols, u); p >= 0 {
				sets = append(sets, row[p])
			}
		}
		if len(sets) == 0 {
			continue
		}
		where := func(b *SQL) {
			b.Write(" WHERE ")
			for i, p := range pos {
				if i > 0 {
					b.Write(" AND ")
				}
				b.Ident(keys[i])
				b.Write(" = ")
				b.Arg(row[p].raw)
			}
		}
		if _, err := execOn(ctx, name, func(b *SQL) { writeUpdate(b, table, sets, where) }); err != nil {
			return err
		}
	}
	return nil
}

// takenKeys reads which of the rows' key combinations table already holds.
func takenKeys(ctx context.Context, name, table string, keys []string, pos []int, rows [][]colSet, keyOf func([]colSet) (string, bool)) (map[string]bool, error) {
	taken := map[string]bool{}
	var keyed [][]colSet
	for _, row := range rows {
		if _, ok := keyOf(row); ok {
			keyed = append(keyed, row)
		}
	}
	if len(keyed) == 0 {
		return taken, nil
	}
	found, err := readRows(ctx, name, func(b *SQL) {
		b.Write("SELECT ")
		for i, k := range keys {
			if i > 0 {
				b.Write(", ")
			}
			b.Ident(k)
		}
		b.Write(" FROM ")
		b.Ident(table)
		b.Write(" WHERE ")
		for i, k := range keys {
			if i > 0 {
				b.Write(" AND ")
			}
			b.Ident(k)
			b.Write(" IN (")
			for j, row := range keyed {
				if j > 0 {
					b.Write(", ")
				}
				b.Arg(row[pos[i]].raw)
			}
			b.Write(")")
		}
	})
	if err != nil {
		return nil, err
	}
	defer closeRows(found)
	existing, err := scanMaps(found)
	if err != nil {
		return nil, err
	}
	for _, d := range existing {
		taken[docKey(keys, d)] = true
	}
	return taken, nil
}

// splitByKey separates items whose key is new from those whose key is
// taken already or repeats an earlier item; an item without a key (ok
// false) never collides.
func splitByKey[T any](items []T, key func(T) (string, bool), taken map[string]bool) (fresh, clash []T) {
	seen := make(map[string]bool, len(items))
	for _, it := range items {
		k, ok := key(it)
		switch {
		case !ok:
			fresh = append(fresh, it)
		case taken[k] || seen[k]:
			clash = append(clash, it)
		default:
			seen[k] = true
			fresh = append(fresh, it)
		}
	}
	return fresh, clash
}

// keyString identifies a key combination across value types: an int64 read
// back matches the int that was written.
// keyString is a comparable form of key values, the same for a value as
// written and as read back: times as UTC instants, bytes as text.
func keyString(vals []any) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		switch x := boundValue(v).(type) {
		case time.Time:
			parts[i] = x.Format(time.RFC3339Nano)
		case []byte:
			parts[i] = string(x)
		default:
			parts[i] = fmt.Sprint(x)
		}
	}
	return strings.Join(parts, "\x00")
}

// insertGetID runs insert and returns the key column the database
// generated for its row: through RETURNING where the dialect has it,
// LastInsertId otherwise.
func insertGetID(ctx context.Context, name, table, key string, insert frag) (int64, error) {
	c, err := sqlConn(ctx, name)
	if err != nil {
		return 0, err
	}
	if err := needAutoKeys(c.Dialect, table); err != nil {
		return 0, err
	}
	if !c.Dialect.Returning() {
		res, err := execOn(ctx, name, insert)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	}
	rows, err := writeRows(ctx, name, func(b *SQL) {
		insert(b)
		b.Write(" RETURNING ")
		b.Ident(key)
	})
	if err != nil {
		return 0, err
	}
	defer closeRows(rows)
	if !rows.Next() {
		return 0, cmp.Or(rows.Err(), fmt.Errorf("orm: insert returned no %s", key))
	}
	var id int64
	if err := rows.Scan(&id); err != nil {
		return 0, fmt.Errorf("orm: reading the generated %s: %w", key, err)
	}
	return id, rows.Err()
}

// needAutoKeys refuses an insert that relies on the database generating an
// integer key on a dialect that generates none.
func needAutoKeys(d Dialect, table string) error {
	if d.AutoKeys() {
		return nil
	}
	return fmt.Errorf("orm: %s does not generate integer keys; give %s a uuid, ulid or manual key", d.Name(), table)
}

// uncountedMutator marks dialects whose driver reports no affected rows
// for UPDATE and DELETE (ClickHouse mutations).
type uncountedMutator interface{ uncountedMutations() }

// mutate runs an UPDATE or DELETE of table and reports how many rows it
// matched; where an engine reports none, the rows where selects are
// counted first.
func mutate(ctx context.Context, name, table string, where, stmt frag) (int64, error) {
	c, err := sqlConn(ctx, name)
	if err != nil {
		return 0, err
	}
	if _, ok := c.Dialect.(uncountedMutator); !ok {
		res, err := execOn(ctx, name, stmt)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}
	n, err := scalarOn[int64](ctx, name, func(b *SQL) {
		b.Write("SELECT COUNT(*) FROM ")
		b.Ident(table)
		where(b)
	})
	if err != nil {
		return 0, err
	}
	if _, err := execOn(ctx, name, stmt); err != nil {
		return 0, err
	}
	return n, nil
}
