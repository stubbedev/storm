package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/stubbedev/storm/orm"
)

// execOn runs a raw statement (with ? placeholders) on conn.
func execOn(t *testing.T, ctx context.Context, conn, q string, args ...any) {
	t.Helper()
	check(t, tryExec(ctx, conn, q, args...))
}

func tryExec(ctx context.Context, conn, q string, args ...any) error {
	x, err := On(conn).session(ctx)
	if err != nil {
		return err
	}
	stmt, _ := render(x.dialect, q)
	return x.execArgs(ctx, stmt, args...)
}

// column reads the first column of every row of a raw query on conn.
func column[T any](t *testing.T, ctx context.Context, conn, q string, args ...any) []T {
	t.Helper()
	x, err := On(conn).session(ctx)
	check(t, err)
	stmt, _ := render(x.dialect, q)
	var out []T
	check(t, x.query(ctx, stmt, args, func(r *sql.Rows) error {
		var v T
		if err := r.Scan(&v); err != nil {
			return err
		}
		out = append(out, v)
		return nil
	}))
	return out
}

// TestLiveFixes runs on the engine the suite targets (SQLite by default).
func TestLiveFixes(t *testing.T) {
	ctx, s := setup(t)
	x, err := s.session(ctx)
	check(t, err)
	conn := orm.DefaultConnection

	orders := func(t *Blueprint) {
		t.ID().From(100)
		t.String("ref").Unique()
		t.String("note").Default(`C:\temp\it's`)
		t.Index("note")
	}
	check(t, s.CreateIfNotExists(ctx, "orders", orders))
	execOn(t, ctx, conn, "INSERT INTO orders (ref) VALUES (?)", "a")
	check(t, s.CreateIfNotExists(ctx, "orders", orders))
	execOn(t, ctx, conn, "INSERT INTO orders (ref) VALUES (?)", "b")
	want(t, "From(n) survives a second CreateIfNotExists", column[int64](t, ctx, conn, "SELECT id FROM orders ORDER BY id"), []int64{100, 101})
	want(t, "backslashes and quotes in defaults", column[string](t, ctx, conn, "SELECT note FROM orders WHERE ref = ?", "a"), []string{`C:\temp\it's`})

	check(t, s.Table(ctx, "orders", func(t *Blueprint) { t.ID().Change() }))
	execOn(t, ctx, conn, "INSERT INTO orders (ref) VALUES (?)", "c")
	want(t, "auto increment survives Change", column[int64](t, ctx, conn, "SELECT id FROM orders WHERE ref = ?", "c"), []int64{102})

	check(t, s.Create(ctx, "keyed", func(t *Blueprint) {
		t.ID().Primary()
		t.String("code")
	}))
	execOn(t, ctx, conn, "INSERT INTO keyed (code) VALUES (?), (?)", "a", "b")
	want(t, "ID().Primary() declares one auto-incrementing key", column[int64](t, ctx, conn, "SELECT id FROM keyed ORDER BY id"), []int64{1, 2})
	indexes, err := s.GetIndexes(ctx, "keyed")
	check(t, err)
	var keys [][]string
	for _, ix := range indexes {
		if ix.Primary {
			keys = append(keys, ix.Columns)
		}
	}
	want(t, "ID().Primary() keys the table by id", keys, [][]string{{"id"}})
	if x.g.engine == engineSQLite {
		failed(t, "a second key beside the inline one", s.Table(ctx, "keyed", func(t *Blueprint) { t.Primary("code") }), "DropPrimary first")
	} else {
		failed(t, "a second key beside the inline one", s.Table(ctx, "keyed", func(t *Blueprint) { t.Primary("code") }))
	}

	planes := func(t *Blueprint) {
		t.ID()
		t.String("tail").Unique()
		t.String("model")
		t.Index("model")
		t.ForeignID("keyed_id").Nullable().Constrained("keyed")
	}
	check(t, s.Create(ctx, "planes", planes))
	check(t, s.Rename(ctx, "planes", "old_planes"))
	check(t, s.Create(ctx, "planes", planes))
	execOn(t, ctx, conn, "INSERT INTO planes (tail, model) VALUES (?, ?)", "N1", "A320")
	if x.g.engine != engineMySQL && x.g.engine != engineMariaDB {
		indexes, err := s.GetIndexes(ctx, "old_planes")
		check(t, err)
		var names []string
		for _, ix := range indexes {
			if !ix.Primary {
				names = append(names, ix.Name)
			}
		}
		slices.Sort(names)
		want(t, "a renamed table's indexes follow it", names, []string{"old_planes_model_index", "old_planes_tail_unique"})
	}
	if x.g.engine != engineSQLite {
		fks, err := s.GetForeignKeys(ctx, "old_planes")
		check(t, err)
		want(t, "a renamed table's foreign keys follow it", fks[0].Name, "old_planes_keyed_id_foreign")
	}

	if x.g.mysqlFamily() {
		check(t, s.Create(ctx, "defaults", func(t *Blueprint) {
			t.String("b").Default("abc")
			t.String("c").Nullable()
			t.String("d").Nullable().Default("NULL")
			t.Timestamp("e").UseCurrent()
			t.Geometry("spot", "point", 4326)
		}))
		cols, err := s.GetColumns(ctx, "defaults")
		check(t, err)
		got := map[string]string{}
		for _, c := range cols {
			got[c.Name] = "<nil>"
			if c.Default != nil {
				got[c.Name] = *c.Default
			}
		}
		want(t, string(x.g.engine)+" defaults reported like MySQL", got, map[string]string{
			"b": "abc", "c": "<nil>", "d": "NULL", "e": "CURRENT_TIMESTAMP", "spot": "<nil>",
		})
	}

	if x.g.engine == enginePostgres {
		check(t, s.Create(ctx, "parents", func(t *Blueprint) {
			t.ID()
			t.Enum("role", "a", "b")
		}))
		check(t, s.Create(ctx, "children", func(t *Blueprint) {
			t.ID()
			t.ForeignID("parent_id").Constrained().Deferrable()
		}))
		failed(t, "drop without cascade", s.Drop(ctx, "parents"), "depend")
		fks, err := s.GetForeignKeys(ctx, "children")
		check(t, err)
		want(t, "child foreign key survives", len(fks), 1)

		check(t, s.Table(ctx, "parents", func(t *Blueprint) { t.Enum("role", "a", "b", "c").Change() }))
		execOn(t, ctx, conn, "INSERT INTO parents (role) VALUES (?)", "c")
		failed(t, "old check replaced", tryExec(ctx, conn, "INSERT INTO parents (role) VALUES (?)", "z"), "parents_role_check")

		check(t, s.WithoutForeignKeyConstraints(ctx, func(ctx context.Context) error {
			if err := tryExec(ctx, conn, "INSERT INTO children (parent_id) VALUES (?)", 77); err != nil {
				return err
			}
			return tryExec(ctx, conn, "INSERT INTO parents (id, role) VALUES (?, ?)", 77, "a")
		}))

		check(t, s.Create(ctx, "tickets", func(t *Blueprint) {
			t.ID()
			t.Enum("status", "open", "closed").Default("open")
			t.String("n").Default("0")
		}))
		check(t, s.Table(ctx, "tickets", func(t *Blueprint) {
			t.String("status").Change()
			t.Integer("n").Default(0).Change()
		}))
		execOn(t, ctx, conn, "INSERT INTO tickets (status) VALUES (?)", "zzz")
		want(t, "an enum changed to a string loses its check; a default survives a type change", column[int64](t, ctx, conn, "SELECT n FROM tickets WHERE status = ?", "zzz"), []int64{0})
		check(t, s.Table(ctx, "tickets", func(t *Blueprint) { t.Enum("status", "open", "closed", "zzz").Change() }))
		check(t, s.Table(ctx, "tickets", func(t *Blueprint) { t.RenameColumn("status", "state") }))
		check(t, s.Table(ctx, "tickets", func(t *Blueprint) { t.Enum("state", "open", "closed", "zzz", "held").Change() }))
		execOn(t, ctx, conn, "INSERT INTO tickets (state) VALUES (?)", "held")
		failed(t, "the renamed column's check is replaced", tryExec(ctx, conn, "INSERT INTO tickets (state) VALUES (?)", "nope"), "tickets_state_check")
		check(t, s.Table(ctx, "tickets", func(t *Blueprint) { t.RenameColumn("n", "m") }))

		check(t, s.Rename(ctx, "parents", "elders"))
		check(t, s.Table(ctx, "elders", func(t *Blueprint) { t.Enum("role", "a", "b", "c", "d").Change() }))
		execOn(t, ctx, conn, "INSERT INTO elders (role) VALUES (?)", "d")
		want(t, "sequence renamed", column[string](t, ctx, conn, "SELECT pg_get_serial_sequence('elders', 'id')"), []string{"public.elders_id_seq"})
		check(t, s.Table(ctx, "children", func(t *Blueprint) { t.DropForeignOn("parent_id") }))
		check(t, s.Table(ctx, "elders", func(t *Blueprint) { t.DropPrimary() }))
	}

	if x.g.engine != enginePostgres && x.g.engine != engineClickHouse && x.g.engine != engineDuckDB {
		check(t, s.Create(ctx, "owners", func(t *Blueprint) { t.ID() }))
		check(t, s.Create(ctx, "pets", func(t *Blueprint) {
			t.ID()
			t.ForeignID("owner_id").Constrained()
		}))
		orphan := map[string]any{"owner_id": 404}
		failed(t, "foreign keys enforced outside", orm.From("pets").Insert(orphan))
		check(t, s.WithoutForeignKeyConstraints(ctx, func(ctx context.Context) error {
			return orm.From("pets").WithContext(ctx).Insert(orphan)
		}))
		pets, err := orm.From("pets").Count()
		check(t, err)
		want(t, "orm writes inside WithoutForeignKeyConstraints run on the held connection", pets, int64(1))
	}

	if x.g.engine != enginePostgres {
		check(t, s.WithoutForeignKeyConstraints(ctx, func(ctx context.Context) error {
			x, err := s.session(ctx)
			if err != nil {
				return err
			}
			on, err := x.foreignKeysOn(ctx)
			if err == nil && on {
				err = errors.New("foreign keys still enforced inside WithoutForeignKeyConstraints")
			}
			return err
		}))
		pooled := 4
		if x.g.engine == engineSQLite {
			pooled = 1
		}
		db, ok := x.db.(*sql.DB)
		want(t, "pool handle", ok, true)
		var conns []*sql.Conn
		for range pooled {
			c, err := db.Conn(ctx)
			check(t, err)
			conns = append(conns, c)
			cx := x
			cx.db = c
			on, err := cx.foreignKeysOn(ctx)
			check(t, err)
			want(t, "pooled connection enforces foreign keys again", on, true)
		}
		for _, c := range conns {
			check(t, c.Close())
		}
	}
}

// TestSQLiteRebuild covers rebuilds on a file database with foreign key
// enforcement on and a pool of several connections.
func TestSQLiteRebuild(t *testing.T) {
	if driver != "" && driver != "sqlite" {
		t.Skip("SQLite only")
	}
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "rebuild.db")+"?_pragma=foreign_keys(1)&_time_format=sqlite")
	check(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(3)
	orm.AddConnection("rebuild", db, orm.SQLite)
	s := On("rebuild")
	conn := "rebuild"

	check(t, s.Create(ctx, "authors", func(t *Blueprint) {
		t.ID()
		t.String("name")
		t.Enum("status", "draft", "live").Default("draft")
		t.Integer("name_length").VirtualAs("length(name)")
		t.Index("status", "name")
	}))
	check(t, s.Create(ctx, "books", func(t *Blueprint) {
		t.ID()
		t.ForeignID("author_id").Constrained().CascadeOnDelete()
	}))
	for i := range 3 {
		execOn(t, ctx, conn, "INSERT INTO authors (name) VALUES (?)", fmt.Sprintf("author %d", i))
		execOn(t, ctx, conn, "INSERT INTO books (author_id) VALUES (?)", i+1)
	}
	execOn(t, ctx, conn, "DELETE FROM books WHERE author_id = 3")
	execOn(t, ctx, conn, "DELETE FROM authors WHERE id = 3")

	idx, err := s.GetIndexes(ctx, "authors")
	check(t, err)
	i := slices.IndexFunc(idx, func(i IndexInfo) bool { return i.Name == "authors_status_name_index" })
	want(t, "index columns in key order", idx[i].Columns, []string{"status", "name"})

	check(t, s.Table(ctx, "authors", func(t *Blueprint) {
		t.RenameColumn("name", "full_name")
		t.RenameColumn("status", "state")
		t.String("full_name", 100).Nullable().Change()
	}))
	want(t, "children kept across the rebuild", column[int64](t, ctx, conn, "SELECT count(*) FROM books"), []int64{2})
	want(t, "generated column follows the rename", column[int64](t, ctx, conn, "SELECT name_length FROM authors WHERE id = 1"), []int64{8})
	failed(t, "check follows the rename", tryExec(ctx, conn, "INSERT INTO authors (full_name, state) VALUES ('x', 'gone')"), "CHECK")
	execOn(t, ctx, conn, "INSERT INTO authors (full_name) VALUES ('y')")
	want(t, "autoincrement sequence kept", column[int64](t, ctx, conn, "SELECT max(id) FROM authors"), []int64{4})
	failed(t, "enforcement restored", tryExec(ctx, conn, "INSERT INTO books (author_id) VALUES (99)"), "FOREIGN KEY")

	tables := func() []string {
		ts, err := s.Tables(ctx)
		check(t, err)
		return ts
	}
	execOn(t, ctx, conn, "INSERT INTO authors (full_name) VALUES (NULL)")
	before := tables()
	for _, c := range []struct {
		what string
		fn   func(*Blueprint)
	}{
		{"change a missing column", func(t *Blueprint) { t.String("nope").Change() }},
		{"drop a missing column", func(t *Blueprint) { t.DropColumn("nope"); t.String("state").Change() }},
		{"drop a missing foreign key", func(t *Blueprint) { t.DropForeign("authors_nope_foreign") }},
		{"drop a missing index", func(t *Blueprint) { t.DropIndex("nope"); t.String("state").Change() }},
		{"rename a missing index", func(t *Blueprint) { t.RenameIndex("nope", "still_nope") }},
		{"hnsw index", func(t *Blueprint) { t.HnswIndex("full_name", orm.L2); t.String("state").Change() }},
		{"not null over null values", func(t *Blueprint) { t.String("full_name", 100).Change() }},
	} {
		failed(t, c.what, s.Table(ctx, "authors", c.fn))
		want(t, c.what+" leaves no temporary table", tables(), before)
	}
	failed(t, "rename index error is precise", s.Table(ctx, "authors", func(t *Blueprint) { t.RenameIndex("nope", "x") }), "has no index nope")
	failed(t, "change error is precise", s.Table(ctx, "authors", func(t *Blueprint) { t.String("nope").Change() }), "has no column nope")

	m := &Migrator{Connection: conn, Migrations: []Migration{{
		Name: "2026_01_01_000001_change_books",
		Up: func(ctx context.Context, s *Builder) error {
			return s.Table(ctx, "books", func(t *Blueprint) { t.ForeignID("author_id").Nullable().Change() })
		},
		Down: func(context.Context, *Builder) error { return nil },
	}}}
	_, err = m.Migrate(ctx, false)
	failed(t, "rebuild in a migration transaction with enforcement on", err, "NoTransaction", "inside a transaction")
	m.Migrations[0].NoTransaction = true
	_, err = m.Migrate(ctx, false)
	check(t, err)

	p, _ := s.Pretend()
	failed(t, "pretend rebuild of a missing table", p.Table(ctx, "later", func(t *Blueprint) { t.String("x").Change() }), "does not exist", "pretend")

	check(t, s.Create(ctx, "keywords", func(t *Blueprint) {
		t.ID()
		t.String("primary")
		t.String("check").Nullable()
		t.String("unique").Nullable()
		t.ForeignID("foreign").Nullable().Constrained("authors")
		t.UnsignedBigInteger("writer").Nullable()
		t.Foreign("writer").References("id").On("authors").Named("fk_custom")
	}))
	fks, err := s.GetForeignKeys(ctx, "keywords")
	check(t, err)
	want(t, "a named foreign key keeps its name", []string{fks[0].Name, fks[1].Name}, []string{"fk_custom", "keywords_foreign_foreign"})
	check(t, s.Create(ctx, "keyword_log", func(t *Blueprint) { t.String("msg") }))
	execOn(t, ctx, conn, `CREATE TRIGGER "keywords_logged" AFTER INSERT ON "keywords" BEGIN INSERT INTO "keyword_log" VALUES (new."primary"); END`)
	execOn(t, ctx, conn, `CREATE VIEW "keyword_names" AS SELECT "primary" FROM "keywords"`)
	check(t, s.Table(ctx, "keywords", func(t *Blueprint) { t.String("primary", 50).Change() }))
	execOn(t, ctx, conn, `INSERT INTO keywords ("primary") VALUES ('p')`)
	want(t, "a trigger survives the rebuild", column[string](t, ctx, conn, "SELECT msg FROM keyword_log"), []string{"p"})
	want(t, "a view over the table survives the rebuild", column[string](t, ctx, conn, "SELECT \"primary\" FROM keyword_names"), []string{"p"})
	want(t, "legacy_alter_table is restored", column[int64](t, ctx, conn, "PRAGMA legacy_alter_table"), []int64{0})
	fks, err = s.GetForeignKeys(ctx, "keywords")
	check(t, err)
	want(t, "a rebuild keeps a named foreign key's name", fks[0].Name, "fk_custom")
	check(t, s.Table(ctx, "keywords", func(t *Blueprint) { t.DropForeign("fk_custom") }))
	fks, err = s.GetForeignKeys(ctx, "keywords")
	check(t, err)
	want(t, "a named foreign key drops by its name", len(fks), 1)
}

func TestDuckDBSchema(t *testing.T) {
	ctx := context.Background()
	db, err := orm.Open("duck-schema", "duckdb", filepath.Join(t.TempDir(), "schema.duckdb"))
	check(t, err)
	t.Cleanup(func() { db.Close() })
	s := On("duck-schema")
	conn := "duck-schema"
	seqs := func() int64 { return column[int64](t, ctx, conn, "SELECT count(*) FROM duckdb_sequences()")[0] }

	check(t, s.Create(ctx, "it's", func(t *Blueprint) {
		t.ID().From(10)
		t.TimestampTz("at").Nullable()
		t.Enum("level", "info", "warn").Default("info")
		t.String("name").Comment("display name")
	}))
	check(t, s.Create(ctx, "children", func(t *Blueprint) {
		t.ID()
		t.ForeignID("parent_id").Constrained("it's")
	}))
	execOn(t, ctx, conn, `INSERT INTO "it's" (name, "at") VALUES ('a', TIMESTAMPTZ '2026-01-01 00:00:00+02')`)
	want(t, "From(n)", column[int64](t, ctx, conn, `SELECT id FROM "it's"`), []int64{10})
	want(t, "timestamptz", column[string](t, ctx, conn, "SELECT data_type FROM information_schema.columns WHERE table_name = 'it''s' AND column_name = 'at'"), []string{"TIMESTAMP WITH TIME ZONE"})
	want(t, "column comment", column[string](t, ctx, conn, "SELECT comment FROM duckdb_columns() WHERE table_name = 'it''s' AND column_name = 'name'"), []string{"display name"})

	check(t, s.Create(ctx, "keyed", func(t *Blueprint) { t.ID().Primary(); t.String("code") }))
	execOn(t, ctx, conn, "INSERT INTO keyed (code) VALUES ('a'), ('b')")
	want(t, "ID().Primary() declares one auto-incrementing key", column[int64](t, ctx, conn, "SELECT id FROM keyed ORDER BY id"), []int64{1, 2})
	failed(t, "duckdb refuses renaming a table fed by a sequence", s.Rename(ctx, "keyed", "old_keyed"), "draws from a sequence")
	check(t, s.Drop(ctx, "keyed"))
	check(t, s.Create(ctx, "tags", func(t *Blueprint) { t.String("name").Index() }))
	failed(t, "duckdb refuses renaming an indexed table", s.Rename(ctx, "tags", "labels"), "has index tags_name_index")
	check(t, s.Table(ctx, "tags", func(t *Blueprint) { t.DropIndexOn("name") }))
	check(t, s.Rename(ctx, "tags", "labels"))
	check(t, s.Drop(ctx, "labels"))
	failed(t, "duckdb cannot alter a referenced table", s.Table(ctx, "it's", func(t *Blueprint) { t.Integer("votes").Default(0) }), "Dependency")
	ok, err := s.HasColumn(ctx, "it's", "votes")
	check(t, err)
	want(t, "the failed alteration left nothing behind", ok, false)

	check(t, s.Create(ctx, "notes", func(t *Blueprint) {
		t.ID()
		t.String("name")
		t.Enum("level", "info", "warn")
	}))
	execOn(t, ctx, conn, "INSERT INTO notes (name, level) VALUES ('a', 'info')")
	check(t, s.Table(ctx, "notes", func(t *Blueprint) {
		t.Integer("votes").Default(0)
		t.String("name", 50).Nullable().Change()
		t.Enum("level", "info", "warn", "error").Change()
	}))
	execOn(t, ctx, conn, "INSERT INTO notes (level) VALUES ('error')")
	want(t, "added not null column filled from its default", column[int64](t, ctx, conn, "SELECT votes FROM notes ORDER BY id"), []int64{0, 0})
	failed(t, "added column is not null", tryExec(ctx, conn, "INSERT INTO notes (level, votes) VALUES ('info', NULL)"), "NOT NULL")
	refused(t, "rename index", s.Table(ctx, "it's", func(t *Blueprint) { t.RenameIndex("a", "b") }), "duckdb", "rename index")
	refused(t, "disable foreign keys", s.WithoutForeignKeyConstraints(ctx, func(context.Context) error { return nil }), "duckdb")

	check(t, s.CreateIfNotExists(ctx, "tags", func(t *Blueprint) { t.ID(); t.String("name").Unique() }))
	check(t, s.CreateIfNotExists(ctx, "tags", func(t *Blueprint) { t.ID(); t.String("name").Unique() }))
	want(t, "sequences", seqs(), int64(4))
	check(t, s.Drop(ctx, "tags"))
	want(t, "Drop drops the table's sequence", seqs(), int64(3))

	check(t, s.DropAllTables(ctx))
	tables, err := s.Tables(ctx)
	check(t, err)
	want(t, "DropAllTables drops children first", len(tables), 0)
	want(t, "DropAllTables drops sequences", seqs(), int64(0))

	m := &Migrator{Connection: conn, Migrations: []Migration{
		migration("2026_01_01_000001_create_a_table", "a"),
		migration("2026_01_01_000002_create_b_table", "b"),
	}}
	ran, err := m.Migrate(ctx, false)
	check(t, err)
	want(t, "migrate", len(ran), 2)
	back, err := m.Reset(ctx)
	check(t, err)
	want(t, "reset", len(back), 2)
}

func TestClickHouseSchema(t *testing.T) {
	dsn := os.Getenv("STORM_TEST_CLICKHOUSE")
	if dsn == "" {
		t.Skip("set STORM_TEST_CLICKHOUSE to a disposable database to run")
	}
	ctx := context.Background()
	db, err := orm.Open("ch-schema", "clickhouse", dsn)
	check(t, err)
	t.Cleanup(func() { db.Close() })
	s := On("ch-schema")
	conn := "ch-schema"
	check(t, s.DropAllTables(ctx))

	events := func(t *Blueprint) {
		t.UUIDPrimary()
		t.Morphs("subject")
		t.IPAddress().Nullable()
		t.Enum("level", "info", "warn")
		t.UnsignedInteger("hits").Default(5)
		t.Integer("double_hits").StoredAs("hits * 2")
		t.Timestamp("at").UseCurrent()
		t.String("note").Nullable().Comment("free text")
		t.PartitionBy("toYYYYMM(at)")
		t.Comment("events")
	}
	check(t, s.Create(ctx, "events", events))
	check(t, s.CreateIfNotExists(ctx, "events", events))
	execOn(t, ctx, conn, "INSERT INTO events (id, subject_id, subject_type, ip_address, level) VALUES (?, ?, ?, ?, ?)",
		"0190d0f5-0000-7000-8000-000000000000", 1, "posts", "10.0.0.1", "warn")
	want(t, "materialized", column[int64](t, ctx, conn, "SELECT double_hits FROM events"), []int64{10})
	want(t, "sorting key", column[string](t, ctx, conn, "SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'events'"), []string{"id"})

	cols, err := s.GetColumns(ctx, "events")
	check(t, err)
	i := slices.IndexFunc(cols, func(c ColumnInfo) bool { return c.Name == "hits" })
	want(t, "default read from default_expression", cols[i].Default != nil && *cols[i].Default == "5", true)
	i = slices.IndexFunc(cols, func(c ColumnInfo) bool { return c.Name == "double_hits" })
	want(t, "materialized is not a default", cols[i].Default == nil, true)
	i = slices.IndexFunc(cols, func(c ColumnInfo) bool { return c.Name == "ip_address" })
	want(t, "ip addresses hold IPv6", cols[i].Type, "Nullable(IPv6)")

	check(t, s.Table(ctx, "events", func(t *Blueprint) {
		t.String("note").Nullable().Comment("longer").Change()
		t.Index("note")
		t.DropIndex("events_subject_type_subject_id_index")
		t.RenameColumn("level", "severity")
	}))
	check(t, s.Rename(ctx, "events", "happenings"))
	ok, err := s.HasColumns(ctx, "happenings", "severity", "note")
	check(t, err)
	want(t, "altered and renamed", ok, true)
	refused(t, "unique", s.Table(ctx, "happenings", func(t *Blueprint) { t.Unique("note") }), "clickhouse", "unique")

	check(t, s.Create(ctx, "scratch", func(t *Blueprint) { t.Engine("Memory"); t.String("v") }))
	check(t, s.Drop(ctx, "scratch"))

	m := &Migrator{Connection: conn, Migrations: []Migration{{
		Name: "2026_01_01_000001_create_pings_table",
		Up: func(ctx context.Context, s *Builder) error {
			return s.Create(ctx, "pings", func(t *Blueprint) { t.UUIDPrimary(); t.Timestamps() })
		},
		Down: func(ctx context.Context, s *Builder) error { return s.DropIfExists(ctx, "pings") },
	}}}
	ran, err := m.Migrate(ctx, false)
	check(t, err)
	want(t, "migrate on clickhouse", ran, []string{"2026_01_01_000001_create_pings_table"})
	st, err := m.Status(ctx)
	check(t, err)
	want(t, "status", st[0].Ran, true)
	back, err := m.Rollback(ctx, 0)
	check(t, err)
	want(t, "rollback on clickhouse", back, []string{"2026_01_01_000001_create_pings_table"})
	check(t, s.DropAllTables(ctx))
}
