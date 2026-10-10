package schema

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stubbedev/storm/orm"
)

var (
	sqliteG     = grammar{engine: engineSQLite}
	postgresG   = grammar{engine: enginePostgres}
	mysqlG      = grammar{engine: engineMySQL}
	mariadbG    = grammar{engine: engineMariaDB}
	duckdbG     = grammar{engine: engineDuckDB}
	clickhouseG = grammar{engine: engineClickHouse}
)

// create compiles Schema::create(table) for g.
func create(g grammar, table string, fn func(*Blueprint)) ([]string, error) {
	bp := &Blueprint{table: table}
	fn(bp)
	return g.compileCreate(bp)
}

// alter compiles Schema::table(table) for g.
func alter(g grammar, table string, fn func(*Blueprint)) ([]string, error) {
	bp := &Blueprint{table: table}
	fn(bp)
	return g.compileAlter(bp)
}

func mustCreate(t *testing.T, g grammar, table string, fn func(*Blueprint)) []string {
	t.Helper()
	stmts, err := create(g, table, fn)
	check(t, err)
	return stmts
}

func mustAlter(t *testing.T, g grammar, table string, fn func(*Blueprint)) []string {
	t.Helper()
	stmts, err := alter(g, table, fn)
	check(t, err)
	return stmts
}

func mustCompile(t *testing.T, g grammar, bp *Blueprint) []string {
	t.Helper()
	stmts, err := g.compileCreate(bp)
	check(t, err)
	return stmts
}

// refused asserts err is an *UnsupportedError for engine whose message
// mentions every fragment.
func refused(t *testing.T, what string, err error, engine string, fragments ...string) {
	t.Helper()
	var u *UnsupportedError
	if !errors.As(err, &u) || u.Engine != engine {
		t.Errorf("%s: want an UnsupportedError from %s, got %v", what, engine, err)
		return
	}
	for _, f := range fragments {
		if !strings.Contains(err.Error(), f) {
			t.Errorf("%s: %q does not mention %q", what, err, f)
		}
	}
}

func failed(t *testing.T, what string, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: want an error", what)
		return
	}
	for _, f := range fragments {
		if !strings.Contains(err.Error(), f) {
			t.Errorf("%s: %q does not mention %q", what, err, f)
		}
	}
}

func TestClickHouseGrammar(t *testing.T) {
	stmts := mustCreate(t, clickhouseG, "events", func(t *Blueprint) {
		t.UUIDPrimary()
		t.Morphs("subject")
		t.IPAddress()
		t.Float("ratio", 24)
		t.Float("score")
		t.UnsignedInteger("hits")
		t.Enum("level", "info", "warn")
		t.Timestamp("at").UseCurrent()
		t.DateTime("seen").UseCurrent()
		t.Integer("double_hits").StoredAs("hits * 2")
		t.Integer("next_hits").VirtualAs("hits + 1")
		t.String("note").Nullable().Comment("free text")
		t.PartitionBy("toYYYYMM(at)")
		t.TTL("toDateTime(at) + INTERVAL 1 YEAR")
		t.Setting("index_granularity", 8192)
		t.Comment("audit events")
	})
	want(t, "clickhouse create", stmts, []string{"CREATE TABLE `events` (" +
		"`id` String, `subject_id` UInt64, `subject_type` String, `ip_address` IPv6, `ratio` Float32, `score` Float64, " +
		"`hits` UInt32, `level` Enum8('info' = 1, 'warn' = 2), `at` DateTime64(3) DEFAULT now64(3), `seen` DateTime DEFAULT now(), " +
		"`double_hits` Int32 MATERIALIZED hits * 2, `next_hits` Int32 ALIAS hits + 1, `note` Nullable(String) COMMENT 'free text', " +
		"INDEX `events_subject_type_subject_id_index` (`subject_type`, `subject_id`) TYPE bloom_filter GRANULARITY 1) " +
		"ENGINE = MergeTree ORDER BY (`id`) PARTITION BY toYYYYMM(at) TTL toDateTime(at) + INTERVAL 1 YEAR " +
		"SETTINGS index_granularity = 8192 COMMENT 'audit events'"})

	stmts = mustCreate(t, clickhouseG, "hits", func(t *Blueprint) {
		t.BigInteger("site")
		t.Date("day")
		t.Primary("site")
		t.OrderBy("site", "day")
		t.Index("day").Algorithm("minmax")
	})
	want(t, "primary key prefix of sorting key", stmts, []string{"CREATE TABLE `hits` (`site` Int64, `day` Date, " +
		"INDEX `hits_day_index` `day` TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY (`site`, `day`) PRIMARY KEY (`site`)"})

	stmts = mustCreate(t, clickhouseG, "scratch", func(t *Blueprint) { t.Engine("Memory"); t.String("v") })
	want(t, "memory engine takes no sorting key", stmts, []string{"CREATE TABLE `scratch` (`v` String) ENGINE = Memory"})
	stmts = mustCreate(t, clickhouseG, "raw", func(t *Blueprint) { t.String("v"); t.Unsorted() })
	want(t, "unsorted", stmts, []string{"CREATE TABLE `raw` (`v` String) ENGINE = MergeTree ORDER BY tuple()"})

	_, err := create(clickhouseG, "raw", func(t *Blueprint) { t.String("v") })
	refused(t, "mergetree without sort key", err, "clickhouse", "without a sorting key", "OrderBy")
	_, err = create(clickhouseG, "scratch", func(t *Blueprint) { t.Engine("Memory"); t.String("v").Primary() })
	refused(t, "memory with primary key", err, "clickhouse", "not a MergeTree engine")
	_, err = create(clickhouseG, "hits", func(t *Blueprint) { t.Integer("a"); t.Integer("b"); t.Primary("b"); t.OrderBy("a", "b") })
	refused(t, "primary key not a prefix", err, "clickhouse", "prefix of the sorting key")
	_, err = create(clickhouseG, "widgets", func(t *Blueprint) { t.ID() })
	refused(t, "auto increment", err, "clickhouse", "auto-incrementing column id", "does not generate integer keys")
	_, err = create(clickhouseG, "widgets", func(t *Blueprint) { t.Increments("n"); t.Primary("n") })
	refused(t, "increments", err, "clickhouse", "auto-incrementing column n")
	_, err = create(clickhouseG, "users", func(t *Blueprint) { t.UUIDPrimary(); t.String("email").Unique() })
	refused(t, "unique", err, "clickhouse", "unique index users_email_unique", "no unique constraints")
	_, err = create(clickhouseG, "posts", func(t *Blueprint) { t.UUIDPrimary(); t.ForeignID("user_id").Constrained() })
	refused(t, "foreign key", err, "clickhouse", "foreign key posts_user_id_foreign", "no foreign keys")
	_, err = create(clickhouseG, "posts", func(t *Blueprint) { t.UUIDPrimary(); t.Set("tags", "a") })
	refused(t, "set", err, "clickhouse", "set column tags")

	stmts = mustAlter(t, clickhouseG, "events", func(t *Blueprint) {
		t.String("note", 500).Comment("longer").Change()
		t.Index("note")
		t.DropIndex("events_subject_type_subject_id_index")
		t.RenameColumn("note", "remark")
		t.Comment("renamed")
	})
	want(t, "clickhouse alter", stmts, []string{
		"ALTER TABLE `events` DROP INDEX `events_subject_type_subject_id_index`",
		"ALTER TABLE `events` MODIFY COLUMN `note` String COMMENT 'longer'",
		"ALTER TABLE `events` ADD INDEX `events_note_index` `note` TYPE bloom_filter GRANULARITY 1",
		"ALTER TABLE `events` RENAME COLUMN `note` TO `remark`",
		"ALTER TABLE `events` MODIFY COMMENT 'renamed'",
	})
	_, err = alter(clickhouseG, "events", func(t *Blueprint) { t.RenameIndex("a", "b") })
	refused(t, "rename index", err, "clickhouse", "rename index a")
	_, err = alter(clickhouseG, "events", func(t *Blueprint) { t.DropForeign("events_x_foreign") })
	refused(t, "drop foreign", err, "clickhouse", "no foreign keys")
	_, err = alter(clickhouseG, "events", func(t *Blueprint) { t.OrderBy("x") })
	refused(t, "sorting key on alter", err, "clickhouse", "only when creating")
	want(t, "rename table", clickhouseG.rename("a", "b"), "RENAME TABLE `a` TO `b`")
}

func TestDuckDBGrammar(t *testing.T) {
	stmts := mustCreate(t, duckdbG, "it's", func(t *Blueprint) {
		t.ID().From(100)
		t.TimestampTz("at")
		t.DateTimeTz("seen")
		t.TimeTz("clock")
		t.Enum("level", "info", "warn")
		t.Integer("twice").VirtualAs("id * 2")
		t.String("name").Comment("display name")
		t.Comment("quoted table")
	})
	want(t, "duckdb create", stmts, []string{
		`CREATE SEQUENCE IF NOT EXISTS "it's_id_seq" START WITH 100`,
		`CREATE TABLE "it's" ("id" ubigint NOT NULL PRIMARY KEY DEFAULT nextval('"it''s_id_seq"'), "at" timestamptz NOT NULL, ` +
			`"seen" timestamptz NOT NULL, "clock" timetz NOT NULL, "level" enum('info', 'warn') NOT NULL, ` +
			`"twice" integer GENERATED ALWAYS AS (id * 2) VIRTUAL, "name" varchar(255) NOT NULL)`,
		`COMMENT ON TABLE "it's" IS 'quoted table'`,
		`COMMENT ON COLUMN "it's"."name" IS 'display name'`,
	})

	_, err := create(duckdbG, "posts", func(t *Blueprint) { t.ID(); t.ForeignID("user_id").Constrained().CascadeOnDelete() })
	refused(t, "cascade", err, "duckdb", "ON DELETE cascade", "only NO ACTION and RESTRICT")
	_, err = create(duckdbG, "posts", func(t *Blueprint) { t.ID(); t.ForeignID("user_id").Constrained().NullOnDelete() })
	refused(t, "set null", err, "duckdb", "ON DELETE set null")
	_, err = create(duckdbG, "posts", func(t *Blueprint) { t.ID(); t.Set("tags", "a") })
	refused(t, "set", err, "duckdb", "set column tags")
	_, err = create(duckdbG, "posts", func(t *Blueprint) { t.Integer("n"); t.Integer("m").StoredAs("n") })
	refused(t, "stored generated", err, "duckdb", "stored generated column m")

	stmts = mustAlter(t, duckdbG, "posts", func(t *Blueprint) {
		t.Integer("votes").Default(0)
		t.String("title", 100).Nullable().Default("untitled").Change()
		t.Enum("state", "a", "b", "c").Change()
	})
	want(t, "duckdb alter", stmts, []string{
		`ALTER TABLE "posts" ADD COLUMN "votes" integer DEFAULT 0`,
		`ALTER TABLE "posts" ALTER COLUMN "votes" SET NOT NULL`,
		`ALTER TABLE "posts" ALTER COLUMN "title" TYPE varchar(100)`,
		`ALTER TABLE "posts" ALTER COLUMN "title" DROP NOT NULL`,
		`ALTER TABLE "posts" ALTER COLUMN "title" SET DEFAULT 'untitled'`,
		`ALTER TABLE "posts" ALTER COLUMN "state" TYPE enum('a', 'b', 'c')`,
		`ALTER TABLE "posts" ALTER COLUMN "state" SET NOT NULL`,
		`ALTER TABLE "posts" ALTER COLUMN "state" DROP DEFAULT`,
	})
	_, err = alter(duckdbG, "posts", func(t *Blueprint) { t.RenameIndex("a", "b") })
	refused(t, "rename index", err, "duckdb", "rename index a")
	_, err = alter(duckdbG, "posts", func(t *Blueprint) { t.ID("n") })
	refused(t, "add auto increment", err, "duckdb", "add auto-incrementing column n")
	_, err = alter(duckdbG, "posts", func(t *Blueprint) { t.ForeignID("user_id").Constrained() })
	refused(t, "add foreign key", err, "duckdb", "only in CREATE TABLE")
	_, err = alter(duckdbG, "posts", func(t *Blueprint) { t.DropForeign("posts_user_id_foreign") })
	refused(t, "drop foreign key", err, "duckdb", "cannot drop constraints")
}

func TestPostgresGrammar(t *testing.T) {
	want(t, "drop", postgresG.drop("users", false), `DROP TABLE "users"`)
	want(t, "drop if exists", postgresG.drop("users", true), `DROP TABLE IF EXISTS "users"`)

	stmts := mustAlter(t, postgresG, "users", func(t *Blueprint) {
		t.ID().Change()
		t.Enum("role", "user", "admin", "owner").Default("user").Change()
		t.String("status").Change()
	})
	want(t, "change auto increment and enum", stmts, []string{
		`ALTER TABLE "users" ALTER COLUMN "id" TYPE bigint USING "id"::bigint`,
		`ALTER TABLE "users" ALTER COLUMN "id" SET NOT NULL`,
		`ALTER TABLE "users" DROP CONSTRAINT IF EXISTS "users_role_check"`,
		`ALTER TABLE "users" ALTER COLUMN "role" TYPE varchar(255) USING "role"::varchar(255)`,
		`ALTER TABLE "users" ALTER COLUMN "role" SET NOT NULL`,
		`ALTER TABLE "users" ALTER COLUMN "role" SET DEFAULT 'user'`,
		`ALTER TABLE "users" ADD CONSTRAINT "users_role_check" CHECK ("role" IN ('user', 'admin', 'owner'))`,
		`ALTER TABLE "users" ALTER COLUMN "status" TYPE varchar(255) USING "status"::varchar(255)`,
		`ALTER TABLE "users" ALTER COLUMN "status" SET NOT NULL`,
		`ALTER TABLE "users" ALTER COLUMN "status" DROP DEFAULT`,
	})

	stmts = mustCreate(t, postgresG, "orders", func(t *Blueprint) {
		t.ID().From(1000)
		t.String("ref").Unique()
	})
	want(t, "From is part of the column", stmts, []string{
		`CREATE TABLE "orders" ("id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1000) PRIMARY KEY, "ref" varchar(255) NOT NULL)`,
		`CREATE UNIQUE INDEX "orders_ref_unique" ON "orders" ("ref")`,
	})
	bp := &Blueprint{table: "orders", ifNot: true}
	bp.ID().From(1000)
	bp.String("ref").Unique()
	want(t, "create if not exists", mustCompile(t, postgresG, bp), []string{
		`CREATE TABLE IF NOT EXISTS "orders" ("id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1000) PRIMARY KEY, "ref" varchar(255) NOT NULL)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS "orders_ref_unique" ON "orders" ("ref")`,
	})

	_, err := create(postgresG, "posts", func(t *Blueprint) { t.Set("tags", "a", "b") })
	refused(t, "set", err, "postgres", "set column tags", "Enum")
	_, err = create(postgresG, "posts", func(t *Blueprint) { t.Engine("InnoDB"); t.ID() })
	refused(t, "engine", err, "postgres", "set table engine InnoDB")
	_, err = create(postgresG, "posts", func(t *Blueprint) { t.Charset("utf8mb4"); t.ID() })
	refused(t, "charset", err, "postgres", "character set")
	_, err = create(postgresG, "posts", func(t *Blueprint) { t.ID(); t.Timestamp("at").UseCurrentOnUpdate() })
	refused(t, "on update", err, "postgres", "ON UPDATE CURRENT_TIMESTAMP")
}

func TestMySQLGrammar(t *testing.T) {
	stmts := mustCreate(t, mysqlG, "paths", func(t *Blueprint) {
		t.ID().From(50)
		t.String("dir").Default(`C:\temp\'x'`).Comment(`back\slash`)
		t.Enum("sep", `\`, `/`)
		t.DateTime("at").Default(time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("CET", 3600)))
		t.Geometry("spot", "point", 0)
	})
	want(t, "escaping, starting value and UTC defaults", stmts, []string{"CREATE TABLE `paths` (" +
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
		"`dir` varchar(255) NOT NULL DEFAULT 'C:\\\\temp\\\\''x''' COMMENT 'back\\\\slash', " +
		"`sep` enum('\\\\', '/') NOT NULL, `at` datetime NOT NULL DEFAULT '2026-01-02 02:04:05', `spot` point NOT NULL) AUTO_INCREMENT = 50"})

	stmts = mustAlter(t, mysqlG, "users", func(t *Blueprint) { t.ID().Change() })
	want(t, "change auto increment", stmts, []string{"ALTER TABLE `users` MODIFY `id` bigint unsigned NOT NULL AUTO_INCREMENT"})

	stmts = mustCreate(t, mysqlG, "places", func(t *Blueprint) { t.Geometry("spot", "point", 4326) })
	want(t, "mysql srid", stmts, []string{"CREATE TABLE `places` (`spot` point srid 4326 NOT NULL)"})
	stmts = mustCreate(t, mariadbG, "places", func(t *Blueprint) { t.Geometry("spot", "point", 4326) })
	want(t, "mariadb srid", stmts, []string{"CREATE TABLE `places` (`spot` point ref_system_id=4326 NOT NULL)"})

	_, err := create(mysqlG, "posts", func(t *Blueprint) { t.ID(); t.HnswIndex("id", orm.L2) })
	refused(t, "hnsw", err, "mysql", "pgvector")
	_, err = create(mysqlG, "posts", func(t *Blueprint) { t.ID(); t.ForeignID("user_id").Constrained().Deferrable() })
	refused(t, "deferrable", err, "mysql", "deferrable")
}

func TestSQLiteGrammar(t *testing.T) {
	bp := &Blueprint{table: "orders", ifNot: true}
	bp.ID().From(10)
	bp.String("ref").Index()
	want(t, "create if not exists is idempotent", mustCompile(t, sqliteG, bp), []string{
		`CREATE TABLE IF NOT EXISTS "orders" ("id" integer PRIMARY KEY AUTOINCREMENT NOT NULL, "ref" varchar NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS "orders_ref_index" ON "orders" ("ref")`,
		`INSERT INTO sqlite_sequence (name, seq) SELECT 'orders', 9 WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'orders')`,
	})
	_, err := create(sqliteG, "posts", func(t *Blueprint) { t.Set("tags", "a") })
	refused(t, "set", err, "sqlite", "set column tags")
	_, err = create(sqliteG, "posts", func(t *Blueprint) { t.String("a"); t.Index("a").Algorithm("hash") })
	refused(t, "algorithm", err, "sqlite", "using hash", "B-trees")
	_, err = create(sqliteG, "posts", func(t *Blueprint) { t.ID().GeneratedAs() })
	refused(t, "identity", err, "sqlite", "identity column")
}

func TestBlueprintNames(t *testing.T) {
	stmts := mustAlter(t, postgresG, "posts", func(t *Blueprint) {
		t.DropUnique("posts_ref_uq")
		t.DropUniqueOn("ref")
		t.DropIndexOn("user_id", "created_at")
		t.DropForeign("posts_author_fk")
		t.DropForeignOn("user_id")
	})
	want(t, "drop by name and by columns", stmts, []string{
		`DROP INDEX "posts_ref_uq"`,
		`DROP INDEX "posts_ref_unique"`,
		`DROP INDEX "posts_user_id_created_at_index"`,
		`ALTER TABLE "posts" DROP CONSTRAINT "posts_author_fk"`,
		`ALTER TABLE "posts" DROP CONSTRAINT "posts_user_id_foreign"`,
	})
	_, err := alter(postgresG, "posts", func(t *Blueprint) { t.DropIndex("") })
	failed(t, "drop without a name", err, "drop an index on table posts", "no name")

	stmts = mustCreate(t, postgresG, "products", func(t *Blueprint) {
		t.ForeignID("category_id").Constrained()
		t.ForeignID("status_id").Constrained()
		t.ForeignID("person_id").Constrained()
		t.ForeignID("box_id").Constrained()
	})
	for _, ref := range []string{`REFERENCES "categories"`, `REFERENCES "statuses"`, `REFERENCES "people"`, `REFERENCES "boxes"`} {
		want(t, "guessed "+ref, strings.Contains(stmts[0], ref), true)
	}
	_, err = create(postgresG, "products", func(t *Blueprint) { t.UUID("owner_uuid").Constrained() })
	failed(t, "unguessable table", err, "owner_uuid", "pass the table to Constrained")

	type Tenant struct{}
	tenants := &orm.Table[Tenant]{Name: "tenants", PrimaryKey: "ulid", KeyType: orm.KeyULID}
	stmts = mustCreate(t, postgresG, "sites", func(t *Blueprint) { t.ForeignIDFor(tenants).Constrained() })
	want(t, "ForeignIDFor a ULID model", stmts, []string{`CREATE TABLE "sites" ("tenant_ulid" char(26) NOT NULL, ` +
		`CONSTRAINT "sites_tenant_ulid_foreign" FOREIGN KEY ("tenant_ulid") REFERENCES "tenants" ("ulid"))`})
	manual := &orm.Table[Tenant]{Name: "tenants", PrimaryKey: "code", KeyType: orm.KeyManual}
	_, err = create(postgresG, "sites", func(t *Blueprint) { t.ForeignIDFor(manual).Constrained() })
	failed(t, "ForeignIDFor a manual key", err, "set manually", "tenant_code")

	stmts = mustAlter(t, postgresG, "sites", func(t *Blueprint) { t.DropConstrainedForeignIDFor(tenants) })
	want(t, "DropConstrainedForeignIDFor", stmts, []string{
		`ALTER TABLE "sites" DROP CONSTRAINT "sites_tenant_ulid_foreign"`,
		`ALTER TABLE "sites" DROP COLUMN "tenant_ulid"`,
	})
	stmts = mustAlter(t, postgresG, "sites", func(t *Blueprint) { t.DropForeignIDFor(tenants) })
	want(t, "DropForeignIDFor drops the column", stmts, []string{`ALTER TABLE "sites" DROP COLUMN "tenant_ulid"`})
}

func TestInflection(t *testing.T) {
	for singularForm, plural := range map[string]string{
		"user": "users", "category": "categories", "status": "statuses", "address": "addresses",
		"box": "boxes", "match": "matches", "wish": "wishes", "person": "people", "child": "children",
		"house": "houses", "movie": "movies", "key": "keys", "news": "news", "line_item": "line_items",
		"order_status": "order_statuses", "response": "responses", "quiz": "quizzes",
	} {
		want(t, "plural of "+singularForm, pluralize(singularForm), plural)
		want(t, "singular of "+plural, singular(plural), singularForm)
	}
}

// schemalessStore is a document store with no schema operations.
type schemalessStore struct{ orm.DocStore }

func TestDocumentStoreConnection(t *testing.T) {
	ctx := context.Background()
	orm.AddDocStore("schema-schemaless", schemalessStore{})
	err := On("schema-schemaless").Create(ctx, "points", func(t *Blueprint) { t.UUIDPrimary() })
	failed(t, "create on a store without schema operations", err, `connection "schema-schemaless"`, "orm.DocSchema")
	_, err = (&Migrator{Connection: "schema-schemaless"}).Migrate(ctx, false)
	failed(t, "migrate on a store without schema operations", err, "orm.DocSchema")

	_, err = orm.OpenQdrant("schema-unreachable", "http://127.0.0.1:1")
	check(t, err)
	_, err = On("schema-unreachable").Tables(ctx)
	failed(t, "an unreachable store", err, "listing qdrant collections", "connection refused")
}

func TestPretendOrder(t *testing.T) {
	ctx, _ := setup(t)
	var ms []Migration
	for _, name := range []string{"2026_01_01_000003_create_c_table", "2026_01_01_000001_create_a_table", "2026_01_01_000002_create_b_table"} {
		ms = append(ms, migration(name, name[len("2026_01_01_000001_create_"):len(name)-len("_table")]))
	}
	m := &Migrator{Migrations: ms, Table: "pretend_migrations"}
	for range 3 {
		var out bytes.Buffer
		check(t, Run(ctx, m, []string{"migrate", "-pretend"}, &out))
		a, b, c := strings.Index(out.String(), "create_a_table"), strings.Index(out.String(), "create_b_table"), strings.Index(out.String(), "create_c_table")
		want(t, "pretend output in migration order", a < b && b < c && a >= 0, true)
	}
	ok, err := Default().HasTable(ctx, "pretend_migrations")
	check(t, err)
	want(t, "pretend creates no migrations table", ok, false)
}

func TestResetUnregistered(t *testing.T) {
	ctx, _ := setup(t)
	m := &Migrator{Migrations: []Migration{
		migration("2026_01_01_000001_create_a_table", "a"),
		migration("2026_01_01_000002_create_b_table", "b"),
	}}
	_, err := m.Migrate(ctx, false)
	check(t, err)
	m.Migrations = m.Migrations[:1]
	_, err = m.Reset(ctx)
	failed(t, "reset with an unregistered migration", err, "2026_01_01_000002_create_b_table", "not registered")
	_, err = m.Rollback(ctx, 0)
	failed(t, "rollback with an unregistered migration", err, "2026_01_01_000002_create_b_table", "not registered")
}

func TestMakeMigrationUTC(t *testing.T) {
	path, err := MakeMigration(t.TempDir(), "migrations", "create_x_table", time.Date(2026, 1, 2, 1, 0, 0, 0, time.FixedZone("UTC+2", 7200)))
	check(t, err)
	want(t, "timestamp in UTC", strings.HasSuffix(path, "2026_01_01_230000_create_x_table.go"), true)
}
