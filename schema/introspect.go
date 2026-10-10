package schema

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
)

// ColumnInfo describes an existing column (Schema::getColumns).
type ColumnInfo struct {
	Name          string
	Type          string
	Nullable      bool
	Default       *string
	AutoIncrement bool
}

// IndexInfo describes an existing index (Schema::getIndexes).
type IndexInfo struct {
	Name    string
	Columns []string
	Unique  bool
	Primary bool
}

// ForeignKeyInfo describes an existing foreign key (Schema::getForeignKeys).
type ForeignKeyInfo struct {
	Name           string
	Columns        []string
	ForeignTable   string
	ForeignColumns []string
	OnUpdate       string
	OnDelete       string
}

// Tables lists the user tables on the connection (Schema::getTableListing).
func (s *Builder) Tables(ctx context.Context) ([]string, error) {
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		if err != nil {
			return nil, err
		}
		return docCollections(ctx, ds)
	}
	x, err := s.session(ctx)
	if err != nil {
		return nil, err
	}
	return x.tables(ctx)
}

// Columns lists column names (Schema::getColumnListing).
func (s *Builder) Columns(ctx context.Context, table string) ([]string, error) {
	cols, err := s.GetColumns(ctx, table)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names, err
}

// GetColumns describes the columns of table. MariaDB defaults are reported
// as MySQL reports them: unquoted literals, nil for no default.
func (s *Builder) GetColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		if err != nil {
			return nil, err
		}
		return s.docColumns(ctx, ds, table)
	}
	x, err := s.session(ctx)
	if err != nil {
		return nil, err
	}
	return x.columns(ctx, table)
}

// GetIndexes describes the indexes of table, including the primary key.
func (s *Builder) GetIndexes(ctx context.Context, table string) ([]IndexInfo, error) {
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		if err != nil {
			return nil, err
		}
		return s.docIndexes(ctx, ds, table)
	}
	x, err := s.session(ctx)
	if err != nil {
		return nil, err
	}
	return x.indexes(ctx, table)
}

// GetForeignKeys describes the foreign keys of table. SQLite does not name
// foreign keys; they are reported under Laravel's conventional name.
func (s *Builder) GetForeignKeys(ctx context.Context, table string) ([]ForeignKeyInfo, error) {
	if _, ok, err := s.docStore(ctx); err != nil || ok {
		return nil, err
	}
	x, err := s.session(ctx)
	if err != nil {
		return nil, err
	}
	return x.foreignKeys(ctx, table)
}

// HasIndex reports whether table has an index with the given name or on
// exactly the given columns; kind optionally filters by "unique" or "primary".
func (s *Builder) HasIndex(ctx context.Context, table string, nameOrCols []string, kind ...string) (bool, error) {
	idxs, err := s.GetIndexes(ctx, table)
	if err != nil {
		return false, err
	}
	for _, i := range idxs {
		match := len(nameOrCols) == 1 && i.Name == nameOrCols[0] || slices.Equal(i.Columns, nameOrCols)
		switch first(kind, "") {
		case "unique":
			match = match && i.Unique
		case "primary":
			match = match && i.Primary
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// GetColumnType returns the database type of a column.
func (s *Builder) GetColumnType(ctx context.Context, table, column string) (string, error) {
	cols, err := s.GetColumns(ctx, table)
	if err != nil {
		return "", err
	}
	for _, c := range cols {
		if c.Name == column {
			return c.Type, nil
		}
	}
	return "", fmt.Errorf("schema: column %s.%s not found", table, column)
}

// introspect runs the query for the session's engine and calls each for
// every row.
func (x session) introspect(ctx context.Context, what string, queries map[engine]string, args []any, each func(*sql.Rows) error) error {
	q, ok := queries[x.g.dialect()]
	if !ok {
		return x.g.unsupported("", "list "+what, "introspection is not implemented for this engine")
	}
	return x.query(ctx, q, args, each)
}

// pgUserRelation keeps relations an extension owns (PostGIS's
// spatial_ref_sys and geometry_columns) out of listings, since dropping
// them fails and they are not the user's schema.
const pgUserRelation = "NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')"

func (x session) tables(ctx context.Context) ([]string, error) {
	var out []string
	err := x.introspect(ctx, "tables", map[engine]string{
		engineSQLite: "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
		enginePostgres: `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind IN ('r', 'p') AND n.nspname = current_schema() AND ` + pgUserRelation + ` ORDER BY c.relname`,
		engineMySQL:      "SELECT table_name FROM information_schema.tables WHERE table_schema = database() AND table_type = 'BASE TABLE' ORDER BY table_name",
		engineDuckDB:     "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' ORDER BY table_name",
		engineClickHouse: "SELECT name FROM system.tables WHERE database = currentDatabase() AND engine != 'View' ORDER BY name",
	}, nil, func(r *sql.Rows) error {
		var n string
		if err := r.Scan(&n); err != nil {
			return err
		}
		out = append(out, n)
		return nil
	})
	return out, err
}

func (x session) views(ctx context.Context) ([]string, error) {
	var out []string
	err := x.introspect(ctx, "views", map[engine]string{
		engineSQLite: "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name",
		enginePostgres: `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind = 'v' AND n.nspname = current_schema() AND ` + pgUserRelation + ` ORDER BY c.relname`,
		engineMySQL:      "SELECT table_name FROM information_schema.views WHERE table_schema = database() ORDER BY table_name",
		engineDuckDB:     "SELECT view_name FROM duckdb_views() WHERE NOT internal AND schema_name = current_schema() ORDER BY view_name",
		engineClickHouse: "SELECT name FROM system.tables WHERE database = currentDatabase() AND engine = 'View' ORDER BY name",
	}, nil, func(r *sql.Rows) error {
		var n string
		if err := r.Scan(&n); err != nil {
			return err
		}
		out = append(out, n)
		return nil
	})
	return out, err
}

func (x session) columns(ctx context.Context, table string) ([]ColumnInfo, error) {
	autoinc := false
	if x.g.engine == engineSQLite {
		create, _, err := x.sqliteCreateSQL(ctx, table)
		if err != nil {
			return nil, err
		}
		autoinc = strings.Contains(strings.ToLower(create), "autoincrement")
	}
	var out []ColumnInfo
	err := x.introspect(ctx, "columns", map[engine]string{
		engineSQLite:     `SELECT name, type, "notnull" = 0, dflt_value, pk = 1 AND lower(type) = 'integer' FROM pragma_table_xinfo(?) WHERE hidden != 1 ORDER BY cid`,
		enginePostgres:   `SELECT column_name, data_type, is_nullable = 'YES', column_default, coalesce(column_default LIKE 'nextval(%', false) OR is_identity = 'YES' FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 ORDER BY ordinal_position`,
		engineMySQL:      `SELECT column_name, column_type, is_nullable = 'YES', column_default, extra LIKE '%auto_increment%' FROM information_schema.columns WHERE table_schema = database() AND table_name = ? ORDER BY ordinal_position`,
		engineDuckDB:     `SELECT column_name, data_type, is_nullable = 'YES', column_default, false FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? ORDER BY ordinal_position`,
		engineClickHouse: `SELECT name, type, type LIKE 'Nullable%', if(default_kind = 'DEFAULT', default_expression, NULL), false FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position`,
	}, []any{table}, func(r *sql.Rows) error {
		var c ColumnInfo
		var def sql.NullString
		var auto bool
		if err := r.Scan(&c.Name, &c.Type, &c.Nullable, &def, &auto); err != nil {
			return err
		}
		if def.Valid {
			c.Default = &def.String
		}
		if x.g.engine == engineMariaDB && c.Default != nil {
			c.Default = mariadbDefault(*c.Default)
		}
		c.AutoIncrement = auto && (autoinc || x.g.engine != engineSQLite)
		out = append(out, c)
		return nil
	})
	return out, err
}

var mariadbUnescape = strings.NewReplacer("''", "'", `\\`, `\`, `\'`, "'", `\n`, "\n", `\r`, "\r", `\t`, "\t", `\0`, "\x00")

// mariadbDefault turns MariaDB's column_default, which is an SQL
// expression ('abc', NULL, current_timestamp()), into MySQL's form.
func mariadbDefault(def string) *string {
	switch {
	case def == "NULL":
		return nil
	case len(def) >= 2 && def[0] == '\'' && def[len(def)-1] == '\'':
		v := mariadbUnescape.Replace(def[1 : len(def)-1])
		return &v
	case strings.HasPrefix(def, "current_timestamp("):
		v := "CURRENT_TIMESTAMP" + strings.TrimSuffix(strings.TrimPrefix(def, "current_timestamp"), "()")
		return &v
	}
	return &def
}

func (x session) indexes(ctx context.Context, table string) ([]IndexInfo, error) {
	var out []IndexInfo
	err := x.introspect(ctx, "indexes", map[engine]string{
		engineSQLite: `SELECT il.name, group_concat(ii.name, ',' ORDER BY ii.seqno), il."unique", il.origin = 'pk'
			FROM pragma_index_list(?) AS il JOIN pragma_index_info(il.name) AS ii
			GROUP BY il.name, il."unique", il.origin`,
		enginePostgres: `SELECT ic.relname, coalesce(string_agg(a.attname, ',' ORDER BY array_position(i.indkey::int2[], a.attnum)), ''), i.indisunique, i.indisprimary
			FROM pg_index i
			JOIN pg_class tc ON tc.oid = i.indrelid
			JOIN pg_class ic ON ic.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = tc.relnamespace
			LEFT JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = ANY(i.indkey) AND a.attnum > 0
			WHERE tc.relname = $1 AND n.nspname = current_schema()
			GROUP BY ic.relname, i.indisunique, i.indisprimary`,
		engineMySQL: `SELECT index_name, group_concat(column_name ORDER BY seq_in_index), non_unique = 0, index_name = 'PRIMARY'
			FROM information_schema.statistics WHERE table_schema = database() AND table_name = ?
			GROUP BY index_name, non_unique`,
		engineDuckDB: `SELECT constraint_name, list_transform(constraint_column_names, c -> '"' || replace(c, '"', '""') || '"')::VARCHAR,
				true, constraint_type = 'PRIMARY KEY'
			FROM duckdb_constraints() WHERE table_name = ? AND schema_name = current_schema() AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
			UNION ALL
			SELECT index_name, expressions, is_unique, is_primary FROM duckdb_indexes() WHERE table_name = ? AND schema_name = current_schema()`,
		engineClickHouse: `SELECT name, expr, false, false FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = ?
			UNION ALL
			SELECT 'primary', primary_key, false, true FROM system.tables WHERE database = currentDatabase() AND name = ? AND primary_key != ''`,
	}, x.tableArgs(table), func(r *sql.Rows) error {
		var i IndexInfo
		var cols sql.NullString
		if err := r.Scan(&i.Name, &cols, &i.Unique, &i.Primary); err != nil {
			return err
		}
		i.Columns = x.indexColumns(cols.String)
		out = append(out, i)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if x.g.engine == engineSQLite && !slices.ContainsFunc(out, func(i IndexInfo) bool { return i.Primary }) {
		pk, err := x.sqlitePrimaryKey(ctx, table)
		if err != nil {
			return nil, err
		}
		if len(pk) > 0 {
			out = append(out, IndexInfo{Name: "primary", Columns: pk, Unique: true, Primary: true})
		}
	}
	slices.SortFunc(out, func(a, b IndexInfo) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// tableArgs binds the table name once per placeholder of the indexes
// query, which the engines whose query is a union take twice.
func (x session) tableArgs(table string) []any {
	if x.g.engine == engineDuckDB || x.g.engine == engineClickHouse {
		return []any{table, table}
	}
	return []any{table}
}

// indexColumns splits an index's column list: comma separated names, or
// the expression lists DuckDB (['"a b"', c]) and ClickHouse ((a, b))
// report, with identifier quoting removed.
func (x session) indexColumns(cols string) []string {
	cols = strings.TrimSpace(cols)
	if cols == "" {
		return nil
	}
	if x.g.engine == engineDuckDB || x.g.engine == engineClickHouse {
		cols = strings.Trim(cols, "[]()")
	}
	parts := strings.Split(cols, ",")
	for i, p := range parts {
		p = strings.Trim(strings.TrimSpace(p), "'`")
		if len(p) >= 2 && p[0] == '"' && p[len(p)-1] == '"' {
			p = strings.ReplaceAll(p[1:len(p)-1], `""`, `"`)
		}
		parts[i] = p
	}
	return parts
}

// sqlitePrimaryKey reads the primary key columns in key order; rowid
// primary keys have no index entry, so Laravel reports them this way.
func (x session) sqlitePrimaryKey(ctx context.Context, table string) ([]string, error) {
	var pk []string
	err := x.query(ctx, "SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk", []any{table}, func(r *sql.Rows) error {
		var n string
		if err := r.Scan(&n); err != nil {
			return err
		}
		pk = append(pk, n)
		return nil
	})
	return pk, err
}

// sqliteCreateSQL returns the CREATE TABLE statement SQLite stored for table.
func (x session) sqliteCreateSQL(ctx context.Context, table string) (create string, found bool, err error) {
	err = x.query(ctx, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", []any{table}, func(r *sql.Rows) error {
		found = true
		return r.Scan(&create)
	})
	return create, found, err
}

func (x session) foreignKeys(ctx context.Context, table string) ([]ForeignKeyInfo, error) {
	if x.g.engine == engineClickHouse {
		return nil, nil
	}
	var out []ForeignKeyInfo
	actions := map[string]string{"a": "no action", "r": "restrict", "c": "cascade", "n": "set null", "d": "set default"}
	err := x.introspect(ctx, "foreign keys", map[engine]string{
		engineSQLite: `SELECT id, group_concat("from", ','), "table", group_concat("to", ','), on_update, on_delete
			FROM (SELECT * FROM pragma_foreign_key_list(?) ORDER BY id, seq) GROUP BY id, "table", on_update, on_delete`,
		enginePostgres: `SELECT c.conname, string_agg(la.attname, ',' ORDER BY k.ord), ft.relname, string_agg(fa.attname, ',' ORDER BY k.ord), c.confupdtype, c.confdeltype
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			JOIN pg_class ft ON ft.oid = c.confrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			CROSS JOIN LATERAL unnest(c.conkey, c.confkey) WITH ORDINALITY AS k(l, f, ord)
			JOIN pg_attribute la ON la.attrelid = c.conrelid AND la.attnum = k.l
			JOIN pg_attribute fa ON fa.attrelid = c.confrelid AND fa.attnum = k.f
			WHERE c.contype = 'f' AND t.relname = $1 AND n.nspname = current_schema()
			GROUP BY c.conname, ft.relname, c.confupdtype, c.confdeltype`,
		engineMySQL: `SELECT kc.constraint_name, group_concat(kc.column_name ORDER BY kc.ordinal_position), kc.referenced_table_name,
				group_concat(kc.referenced_column_name ORDER BY kc.ordinal_position), rc.update_rule, rc.delete_rule
			FROM information_schema.key_column_usage kc
			JOIN information_schema.referential_constraints rc
				ON rc.constraint_schema = kc.table_schema AND rc.constraint_name = kc.constraint_name
			WHERE kc.table_schema = database() AND kc.table_name = ? AND kc.referenced_table_name IS NOT NULL
			GROUP BY kc.constraint_name, kc.referenced_table_name, rc.update_rule, rc.delete_rule`,
		engineDuckDB: `SELECT constraint_name, array_to_string(constraint_column_names, ','), referenced_table,
				array_to_string(referenced_column_names, ','), 'no action', 'no action'
			FROM duckdb_constraints() WHERE table_name = ? AND schema_name = current_schema() AND constraint_type = 'FOREIGN KEY'`,
	}, []any{table}, func(r *sql.Rows) error {
		var f ForeignKeyInfo
		var cols, fcols string
		if err := r.Scan(&f.Name, &cols, &f.ForeignTable, &fcols, &f.OnUpdate, &f.OnDelete); err != nil {
			return err
		}
		f.Columns, f.ForeignColumns = strings.Split(cols, ","), strings.Split(fcols, ",")
		if a, ok := actions[f.OnUpdate]; ok {
			f.OnUpdate, f.OnDelete = a, actions[f.OnDelete]
		}
		f.OnUpdate, f.OnDelete = strings.ToLower(f.OnUpdate), strings.ToLower(f.OnDelete)
		if x.g.engine == engineSQLite {
			f.Name = indexName(table, "foreign", f.Columns)
		}
		out = append(out, f)
		return nil
	})
	slices.SortFunc(out, func(a, b ForeignKeyInfo) int { return strings.Compare(a.Name, b.Name) })
	return out, err
}

// duckdbSequenceDrops returns DROP SEQUENCE statements for the sequences
// the tables' column defaults draw from (nextval('"t_id_seq"')).
func (x session) duckdbSequenceDrops(ctx context.Context, tables []string) ([]string, error) {
	var stmts []string
	err := x.query(ctx, `SELECT table_name, column_default FROM duckdb_columns()
		WHERE schema_name = current_schema() AND column_default LIKE 'nextval(''%'')' ORDER BY table_name, column_index`,
		nil, func(r *sql.Rows) error {
			var table, def string
			if err := r.Scan(&table, &def); err != nil {
				return err
			}
			if !slices.Contains(tables, table) {
				return nil
			}
			seq := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(def, "nextval('"), "')"), "''", "'")
			stmts = append(stmts, "DROP SEQUENCE IF EXISTS "+seq)
			return nil
		})
	return stmts, err
}

// duckdbDropOrder orders tables so that every table is dropped before the
// tables its foreign keys reference; DuckDB cannot disable foreign keys.
func (x session) duckdbDropOrder(ctx context.Context, tables []string) ([]string, error) {
	children := map[string][]string{}
	err := x.query(ctx, `SELECT table_name, referenced_table FROM duckdb_constraints()
		WHERE schema_name = current_schema() AND constraint_type = 'FOREIGN KEY' ORDER BY table_name, referenced_table`,
		nil, func(r *sql.Rows) error {
			var child, parent string
			if err := r.Scan(&child, &parent); err != nil {
				return err
			}
			if child != parent {
				children[parent] = append(children[parent], child)
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	remaining := slices.Clone(tables)
	ordered := make([]string, 0, len(tables))
	for len(remaining) > 0 {
		i := slices.IndexFunc(remaining, func(t string) bool {
			return !slices.ContainsFunc(children[t], func(c string) bool { return slices.Contains(remaining, c) })
		})
		if i < 0 {
			return nil, fmt.Errorf("schema: duckdb: tables %s reference each other in a cycle; drop them by hand", strings.Join(remaining, ", "))
		}
		ordered = append(ordered, remaining[i])
		remaining = slices.Delete(remaining, i, i+1)
	}
	return ordered, nil
}

// postgresOwnedRenames renames the primary key, enum checks and sequences
// named after a Postgres table that is being renamed, so the grammar's
// conventional names (to_pkey, to_col_seq, to_col_check) keep matching.
func (x session) postgresOwnedRenames(ctx context.Context, from, to string) ([]string, error) {
	var stmts []string
	err := x.query(ctx, `SELECT 'constraint', c.conname, c.contype::text FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE t.relname = $1 AND n.nspname = current_schema() AND c.contype IN ('p', 'c')
		UNION ALL
		SELECT 'sequence', s.relname, 'S' FROM pg_class s
			JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
			JOIN pg_class t ON t.oid = d.refobjid JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE s.relkind = 'S' AND t.relname = $1 AND n.nspname = current_schema()
		ORDER BY 1, 2`, []any{from}, func(r *sql.Rows) error {
		var kind, name, contype string
		if err := r.Scan(&kind, &name, &contype); err != nil {
			return err
		}
		rest, ok := strings.CutPrefix(name, from+"_")
		switch {
		case !ok:
		case kind == "sequence" && strings.HasSuffix(rest, "_seq"):
			stmts = append(stmts, "ALTER SEQUENCE "+x.g.quote(name)+" RENAME TO "+x.g.quote(to+"_"+rest))
		case contype == "p" && rest == "pkey", contype == "c" && strings.HasSuffix(rest, "_check"):
			stmts = append(stmts, "ALTER TABLE "+x.g.quote(to)+" RENAME CONSTRAINT "+x.g.quote(name)+" TO "+x.g.quote(to+"_"+rest))
		}
		return nil
	})
	return stmts, err
}
