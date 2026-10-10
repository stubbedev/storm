package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// rebuildSQLite applies alterations SQLite's ALTER TABLE cannot express —
// changing columns, adding or dropping primary and foreign keys, renaming
// indexes — by recreating the table, as Laravel does:
//
//	ALTER TABLE t RENAME COLUMN ...   (SQLite rewrites CHECKs, generated columns, indexes)
//	CREATE TABLE __temp__t (...new definition...)
//	INSERT INTO __temp__t (...) SELECT ... FROM t
//	DROP TABLE t
//	ALTER TABLE __temp__t RENAME TO t
//	recreate indexes
//
// It runs atomically in a savepoint on one connection, with foreign key
// enforcement off so dropping the old table fires no ON DELETE actions, and
// checks the table's foreign keys before committing. SQLite cannot turn
// enforcement off inside a transaction, so a rebuild inside one with
// enforcement on is refused.
func (s *Builder) rebuildSQLite(ctx context.Context, x session, bp *Blueprint) error {
	renames, rest := splitRenames(bp.commands)
	if x.pretend != nil {
		if len(renames) > 0 {
			return fmt.Errorf("schema: sqlite cannot pretend to rebuild table %s after renaming its columns: the rebuild reads the renamed table from the database; rename in a separate Table call", bp.table)
		}
		stmts, err := x.planRebuild(ctx, bp, rest)
		if err != nil {
			return err
		}
		return x.exec(ctx, stmts...)
	}
	return s.pinned(ctx, func(ctx context.Context, x session) (bool, error) {
		on, err := x.foreignKeysOn(ctx)
		if err != nil {
			return true, err
		}
		if _, inTx := x.db.(*sql.Tx); inTx && on {
			return true, fmt.Errorf("schema: sqlite cannot rebuild table %s inside a transaction while foreign keys are enforced: PRAGMA foreign_keys is ignored there, and dropping the old table would fire ON DELETE actions on rows referencing it; run the migration with NoTransaction", bp.table)
		}
		if on {
			if err := x.exec(ctx, x.g.foreignKeyChecks(false)); err != nil {
				return false, err
			}
		}
		err = x.savepoint(ctx, func() error {
			for _, cmd := range renames {
				stmts, err := x.g.alterCommand(bp.table, cmd)
				if err != nil {
					return err
				}
				if err := x.exec(ctx, stmts...); err != nil {
					return err
				}
			}
			stmts, err := x.planRebuild(ctx, bp, rest)
			if err != nil {
				return err
			}
			if err := x.exec(ctx, stmts...); err != nil {
				return err
			}
			if on {
				return x.sqliteForeignKeyViolations(ctx, bp.table)
			}
			return nil
		})
		if on {
			if rerr := x.exec(ctx, x.g.foreignKeyChecks(true)); rerr != nil {
				return false, errors.Join(err, fmt.Errorf("schema: re-enabling foreign keys on connection %q: %w", x.name, rerr))
			}
		}
		return true, err
	})
}

func splitRenames(cmds []*command) (renames, rest []*command) {
	for _, cmd := range cmds {
		if cmd.kind == cmdRenameColumn {
			renames = append(renames, cmd)
		} else {
			rest = append(rest, cmd)
		}
	}
	return renames, rest
}

// savepoint runs fn atomically; outside a transaction the savepoint is the
// transaction.
func (x session) savepoint(ctx context.Context, fn func() error) error {
	const name = "eloquent_rebuild"
	if err := x.exec(ctx, "SAVEPOINT "+name); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return errors.Join(err, x.exec(ctx, "ROLLBACK TO SAVEPOINT "+name, "RELEASE SAVEPOINT "+name))
	}
	return x.exec(ctx, "RELEASE SAVEPOINT "+name)
}

// sqliteForeignKeyViolations fails when rows of table, or rows referencing
// it, break a foreign key.
func (x session) sqliteForeignKeyViolations(ctx context.Context, table string) error {
	var found []string
	err := x.query(ctx, "PRAGMA foreign_key_check", nil, func(r *sql.Rows) error {
		var child, parent string
		var rowid sql.NullInt64
		var fkid int
		if err := r.Scan(&child, &rowid, &parent, &fkid); err != nil {
			return err
		}
		if child == table || parent == table {
			found = append(found, fmt.Sprintf("%s rowid %d references a missing %s row", child, rowid.Int64, parent))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(found) > 0 {
		return fmt.Errorf("schema: sqlite: rebuilding table %s would break %d foreign key(s): %s", table, len(found), strings.Join(found, "; "))
	}
	return nil
}

// sqliteTable is the part of an existing table a rebuild carries over.
type sqliteTable struct {
	columns     []sqliteColumn
	pk          []string
	indexes     []sqliteIndex
	fks         []*Foreign
	constraints []string
	autoinc     bool
}

type sqliteColumn struct {
	name, def string
	existing  bool
	generated bool
}

type sqliteIndex struct {
	name, sql string
	cols      []string
}

// planRebuild reads table from the database and compiles the rebuild
// statements for cmds.
func (x session) planRebuild(ctx context.Context, bp *Blueprint, cmds []*command) ([]string, error) {
	t, err := x.sqliteTable(ctx, bp.table)
	if err != nil {
		return nil, err
	}
	return x.g.planSQLiteRebuild(bp, cmds, t)
}

func (x session) sqliteTable(ctx context.Context, table string) (sqliteTable, error) {
	create, found, err := x.sqliteCreateSQL(ctx, table)
	if err != nil {
		return sqliteTable{}, err
	}
	if !found {
		reason := ""
		if x.pretend != nil {
			reason = " (pretend mode reads the live database, where an earlier pending statement has not created it)"
		}
		return sqliteTable{}, fmt.Errorf("schema: sqlite cannot rebuild table %s: it does not exist%s", table, reason)
	}
	defs, constraints := columnDefs(create)
	t := sqliteTable{constraints: constraints}
	err = x.query(ctx, "SELECT name, hidden IN (2, 3) FROM pragma_table_xinfo(?) ORDER BY cid", []any{table}, func(r *sql.Rows) error {
		var c sqliteColumn
		if err := r.Scan(&c.name, &c.generated); err != nil {
			return err
		}
		def, ok := defs[c.name]
		if !ok {
			return fmt.Errorf("schema: sqlite: no definition for column %s.%s in %q", table, c.name, create)
		}
		c.def, c.existing = def, true
		t.columns = append(t.columns, c)
		return nil
	})
	if err != nil {
		return sqliteTable{}, err
	}
	if t.pk, err = x.sqlitePrimaryKey(ctx, table); err != nil {
		return sqliteTable{}, err
	}
	infos, err := x.indexes(ctx, table)
	if err != nil {
		return sqliteTable{}, err
	}
	err = x.query(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name",
		[]any{table}, func(r *sql.Rows) error {
			var ix sqliteIndex
			if err := r.Scan(&ix.name, &ix.sql); err != nil {
				return err
			}
			if i := slices.IndexFunc(infos, func(info IndexInfo) bool { return info.Name == ix.name }); i >= 0 {
				ix.cols = infos[i].Columns
			}
			t.indexes = append(t.indexes, ix)
			return nil
		})
	if err != nil {
		return sqliteTable{}, err
	}
	fkInfos, err := x.foreignKeys(ctx, table)
	if err != nil {
		return sqliteTable{}, err
	}
	for _, f := range fkInfos {
		fk := &Foreign{Name: f.Name, Columns: f.Columns, RefTable: f.ForeignTable, RefColumns: f.ForeignColumns}
		if f.OnDelete != "no action" {
			fk.OnDel = f.OnDelete
		}
		if f.OnUpdate != "no action" {
			fk.OnUpd = f.OnUpdate
		}
		t.fks = append(t.fks, fk)
	}
	t.autoinc = slices.ContainsFunc(t.columns, func(c sqliteColumn) bool { return autoincrementDef(c.def) })
	return t, nil
}

func autoincrementDef(def string) bool {
	return strings.Contains(strings.ToUpper(def), "AUTOINCREMENT")
}

func inlinePrimaryKey(def string) bool {
	_, rest := splitName(def)
	return strings.Contains(strings.ToUpper(rest), "PRIMARY KEY")
}

// planSQLiteRebuild applies cmds and bp's column definitions to the
// existing table t and compiles the rebuild. Every name the blueprint
// targets must exist.
func (g grammar) planSQLiteRebuild(bp *Blueprint, cmds []*command, t sqliteTable) ([]string, error) {
	table := bp.table
	cols, pk, indexes, fks := t.columns, t.pk, t.indexes, t.fks
	find := func(name string) int {
		return slices.IndexFunc(cols, func(c sqliteColumn) bool { return c.name == name })
	}
	missing := func(what, name string) error {
		return fmt.Errorf("schema: sqlite: table %s has no %s %s", table, what, name)
	}
	for _, c := range bp.columns {
		def, err := g.column(c)
		if err != nil {
			return nil, err
		}
		i := find(c.Name)
		switch {
		case c.Changed && i < 0:
			return nil, missing("column", c.Name+" to change")
		case c.Changed:
			cols[i].def, cols[i].generated = def, c.StoredExpr != "" || c.VirtualExpr != ""
		case i >= 0:
			return nil, fmt.Errorf("schema: sqlite: table %s already has a column %s", table, c.Name)
		default:
			cols = append(cols, sqliteColumn{name: c.Name, def: def, generated: c.StoredExpr != "" || c.VirtualExpr != ""})
		}
		if c.IsAutoIncrement {
			pk = []string{c.Name}
		}
	}
	var newIndexes []string
	for _, cmd := range cmds {
		switch cmd.kind {
		case cmdDropColumn:
			for _, name := range cmd.columns {
				i := find(name)
				if i < 0 {
					return nil, missing("column", name+" to drop")
				}
				cols = slices.Delete(cols, i, i+1)
				indexes = slices.DeleteFunc(indexes, func(ix sqliteIndex) bool { return slices.Contains(ix.cols, name) })
				fks = slices.DeleteFunc(fks, func(f *Foreign) bool { return slices.Contains(f.Columns, name) })
				pk = slices.DeleteFunc(pk, func(c string) bool { return c == name })
			}
		case cmdPrimary:
			pk = cmd.columns
		case cmdDropPrimary:
			if len(pk) == 0 {
				return nil, missing("primary key", "to drop")
			}
			pk = nil
			for i := range cols {
				cols[i].def = strings.Replace(cols[i].def, " PRIMARY KEY AUTOINCREMENT", "", 1)
			}
		case cmdForeign:
			fks = append(fks, cmd.foreign)
		case cmdDropForeign:
			n := len(fks)
			fks = slices.DeleteFunc(fks, func(f *Foreign) bool { return f.Name == cmd.index.IndexName })
			if len(fks) == n {
				return nil, missing("foreign key", cmd.index.IndexName)
			}
		case cmdUnique, cmdIndex, cmdFullText, cmdSpatial, cmdHnsw:
			stmt, err := g.createIndex(table, cmd, false)
			if err != nil {
				return nil, err
			}
			newIndexes = append(newIndexes, stmt)
		case cmdDropIndex:
			n := len(indexes)
			indexes = slices.DeleteFunc(indexes, func(ix sqliteIndex) bool { return ix.name == cmd.index.IndexName })
			if len(indexes) == n {
				return nil, missing("index", cmd.index.IndexName)
			}
		case cmdRenameIndex:
			i := slices.IndexFunc(indexes, func(ix sqliteIndex) bool { return ix.name == cmd.from })
			if i < 0 {
				return nil, missing("index", cmd.from+" to rename")
			}
			text := strings.Replace(indexes[i].sql, g.quote(cmd.from), g.quote(cmd.to), 1)
			if text == indexes[i].sql {
				text = strings.Replace(text, " "+cmd.from+" ", " "+g.quote(cmd.to)+" ", 1)
			}
			if text == indexes[i].sql {
				return nil, fmt.Errorf("schema: sqlite: cannot find the name of index %s in %q", cmd.from, text)
			}
			indexes[i].sql, indexes[i].name = text, cmd.to
		default:
			return nil, fmt.Errorf("schema: sqlite: cannot %s while rebuilding table %s", cmd.kind, table)
		}
	}

	tmp := "__temp__" + table
	defs := make([]string, 0, len(cols)+len(fks)+len(t.constraints)+1)
	autoinc, inlinePK := false, false
	for _, c := range cols {
		defs = append(defs, c.def)
		autoinc = autoinc || autoincrementDef(c.def)
		inlinePK = inlinePK || inlinePrimaryKey(c.def)
	}
	if len(pk) > 0 && !inlinePK {
		defs = append(defs, "PRIMARY KEY ("+g.list(pk)+")")
	}
	for _, f := range fks {
		def, err := g.foreign(table, f)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	defs = append(defs, t.constraints...)
	var from, to []string
	for _, c := range cols {
		if c.existing && !c.generated {
			from, to = append(from, g.quote(c.name)), append(to, g.quote(c.name))
		}
	}
	stmts := []string{
		"CREATE TABLE " + g.quote(tmp) + " (" + strings.Join(defs, ", ") + ")",
		"INSERT INTO " + g.quote(tmp) + " (" + strings.Join(to, ", ") + ") SELECT " + strings.Join(from, ", ") + " FROM " + g.quote(table),
	}
	if autoinc && t.autoinc {
		stmts = append(stmts,
			"DELETE FROM sqlite_sequence WHERE name = "+g.literal(tmp),
			"INSERT INTO sqlite_sequence (name, seq) SELECT "+g.literal(tmp)+", seq FROM sqlite_sequence WHERE name = "+g.literal(table))
	}
	stmts = append(stmts,
		"DROP TABLE "+g.quote(table),
		"ALTER TABLE "+g.quote(tmp)+" RENAME TO "+g.quote(table),
	)
	for _, ix := range indexes {
		stmts = append(stmts, ix.sql)
	}
	return append(stmts, newIndexes...), nil
}

// columnDefs splits a CREATE TABLE statement into its column definitions,
// keyed by column name, preserving CHECK, GENERATED, COLLATE and other
// clauses that SQLite's pragmas don't report, and its CHECK and UNIQUE
// table constraints verbatim. Primary and foreign keys are recomputed from
// pragmas.
func columnDefs(create string) (map[string]string, []string) {
	out := map[string]string{}
	var constraints []string
	open, end := strings.IndexByte(create, '('), strings.LastIndexByte(create, ')')
	if open < 0 || end <= open {
		return out, nil
	}
	for _, part := range splitTopLevel(create[open+1 : end]) {
		part = strings.TrimSpace(part)
		name, rest := splitName(part)
		kind := strings.ToUpper(name)
		if kind == "CONSTRAINT" {
			_, after := splitName(rest)
			kind, _ = splitName(strings.ToUpper(after))
		}
		switch kind {
		case "PRIMARY", "FOREIGN":
		case "CHECK", "UNIQUE":
			constraints = append(constraints, part)
		default:
			out[name] = part
		}
	}
	return out, constraints
}

// splitTopLevel splits on commas outside parentheses and quotes.
func splitTopLevel(s string) []string {
	var parts []string
	depth, start := 0, 0
	var quote byte
	for i := range len(s) {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'' || c == '`':
			quote = c
		case c == '[':
			quote = ']'
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// splitName returns the (unquoted) leading identifier of a definition and the rest.
func splitName(def string) (name, rest string) {
	def = strings.TrimSpace(def)
	if def == "" {
		return "", ""
	}
	closer := map[byte]byte{'"': '"', '`': '`', '[': ']'}[def[0]]
	if closer == 0 {
		i := strings.IndexAny(def, " \t\n(")
		if i < 0 {
			return def, ""
		}
		return def[:i], def[i:]
	}
	for i := 1; i < len(def); i++ {
		if def[i] == closer {
			if closer == '"' && i+1 < len(def) && def[i+1] == '"' {
				i++
				continue
			}
			return strings.ReplaceAll(def[1:i], `""`, `"`), def[i+1:]
		}
	}
	return def, ""
}
