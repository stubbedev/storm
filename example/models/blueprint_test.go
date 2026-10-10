package models_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

// probe is one blueprint feature: def declares it on a fresh table, col is
// the column it adds, keyed marks definitions that bring their own key.
type probe struct {
	name      string
	col       string
	keyed     bool
	transient bool
	def       func(*schema.Blueprint)
}

// extensions lists features that need an extension or a newer server than
// the test containers run (DuckDB's spatial extension, MySQL 9 vectors);
// the engine's own error must name what is missing.
var extensions = map[string]map[string]string{
	"geometry":              {"duckdb": "spatial extension"},
	"geography":             {"duckdb": "spatial extension"},
	"spatialIndex":          {"duckdb": "spatial extension"},
	"blueprintSpatialIndex": {"duckdb": "spatial extension"},
	"vector":                {"mysql": "vector(3)"},
	"hnswIndex":             {"mysql": "vector(3)"},
}

var collations = map[string]string{
	"sqlite": "NOCASE", "postgres": "C", "mysql": "utf8mb4_bin", "mariadb": "utf8mb4_bin", "duckdb": "nocase", "clickhouse": "binary",
}

func blueprintProbes(engine string) []probe {
	col := func(name string, def func(*schema.Blueprint) *schema.Column) probe {
		return probe{name: name, col: "c", def: func(b *schema.Blueprint) { def(b) }}
	}
	return []probe{
		{name: "tinyIncrements", col: "n", keyed: true, def: func(b *schema.Blueprint) { b.TinyIncrements("n") }},
		{name: "smallIncrements", col: "n", keyed: true, def: func(b *schema.Blueprint) { b.SmallIncrements("n") }},
		{name: "mediumIncrements", col: "n", keyed: true, def: func(b *schema.Blueprint) { b.MediumIncrements("n") }},
		{name: "increments", col: "n", keyed: true, def: func(b *schema.Blueprint) { b.Increments("n") }},
		{name: "ulidPrimary", col: "id", keyed: true, def: func(b *schema.Blueprint) { b.ULIDPrimary() }},
		{name: "uuidPrimary", col: "id", keyed: true, def: func(b *schema.Blueprint) { b.UUIDPrimary() }},
		col("string", func(b *schema.Blueprint) *schema.Column { return b.String("c", 40) }),
		col("char", func(b *schema.Blueprint) *schema.Column { return b.Char("c", 3) }),
		col("tinyText", func(b *schema.Blueprint) *schema.Column { return b.TinyText("c") }),
		col("text", func(b *schema.Blueprint) *schema.Column { return b.Text("c") }),
		col("mediumText", func(b *schema.Blueprint) *schema.Column { return b.MediumText("c") }),
		col("longText", func(b *schema.Blueprint) *schema.Column { return b.LongText("c") }),
		col("tinyInteger", func(b *schema.Blueprint) *schema.Column { return b.TinyInteger("c") }),
		col("smallInteger", func(b *schema.Blueprint) *schema.Column { return b.SmallInteger("c") }),
		col("mediumInteger", func(b *schema.Blueprint) *schema.Column { return b.MediumInteger("c") }),
		col("integer", func(b *schema.Blueprint) *schema.Column { return b.Integer("c") }),
		col("bigInteger", func(b *schema.Blueprint) *schema.Column { return b.BigInteger("c") }),
		col("unsignedTinyInteger", func(b *schema.Blueprint) *schema.Column { return b.UnsignedTinyInteger("c") }),
		col("unsignedSmallInteger", func(b *schema.Blueprint) *schema.Column { return b.UnsignedSmallInteger("c") }),
		col("unsignedMediumInteger", func(b *schema.Blueprint) *schema.Column { return b.UnsignedMediumInteger("c") }),
		col("unsignedInteger", func(b *schema.Blueprint) *schema.Column { return b.UnsignedInteger("c") }),
		col("unsignedBigInteger", func(b *schema.Blueprint) *schema.Column { return b.UnsignedBigInteger("c") }),
		col("boolean", func(b *schema.Blueprint) *schema.Column { return b.Boolean("c") }),
		col("decimal", func(b *schema.Blueprint) *schema.Column { return b.Decimal("c", 8, 2) }),
		col("float", func(b *schema.Blueprint) *schema.Column { return b.Float("c") }),
		col("double", func(b *schema.Blueprint) *schema.Column { return b.Double("c") }),
		col("date", func(b *schema.Blueprint) *schema.Column { return b.Date("c") }),
		col("time", func(b *schema.Blueprint) *schema.Column { return b.Time("c") }),
		col("timeTz", func(b *schema.Blueprint) *schema.Column { return b.TimeTz("c") }),
		col("dateTime", func(b *schema.Blueprint) *schema.Column { return b.DateTime("c") }),
		col("dateTimeTz", func(b *schema.Blueprint) *schema.Column { return b.DateTimeTz("c") }),
		col("timestamp", func(b *schema.Blueprint) *schema.Column { return b.Timestamp("c") }),
		col("timestampTz", func(b *schema.Blueprint) *schema.Column { return b.TimestampTz("c") }),
		col("year", func(b *schema.Blueprint) *schema.Column { return b.Year("c") }),
		col("json", func(b *schema.Blueprint) *schema.Column { return b.JSON("c") }),
		col("jsonb", func(b *schema.Blueprint) *schema.Column { return b.JSONB("c") }),
		col("binary", func(b *schema.Blueprint) *schema.Column { return b.Binary("c") }),
		col("uuid", func(b *schema.Blueprint) *schema.Column { return b.UUID("c") }),
		col("ulid", func(b *schema.Blueprint) *schema.Column { return b.ULID("c") }),
		col("ipAddress", func(b *schema.Blueprint) *schema.Column { return b.IPAddress("c") }),
		col("macAddress", func(b *schema.Blueprint) *schema.Column { return b.MACAddress("c") }),
		col("enum", func(b *schema.Blueprint) *schema.Column { return b.Enum("c", "a", "b") }),
		col("set", func(b *schema.Blueprint) *schema.Column { return b.Set("c", "a", "b") }),
		col("geometry", func(b *schema.Blueprint) *schema.Column { return b.Geometry("c", "point", 4326) }),
		col("geography", func(b *schema.Blueprint) *schema.Column { return b.Geography("c", "point", 4326) }),
		col("vector", func(b *schema.Blueprint) *schema.Column { return b.Vector("c", 3) }),
		col("nullable", func(b *schema.Blueprint) *schema.Column { return b.String("c").Nullable() }),
		col("default", func(b *schema.Blueprint) *schema.Column { return b.Integer("c").Default(7) }),
		col("comment", func(b *schema.Blueprint) *schema.Column { return b.String("c").Comment("it's a note") }),
		col("charset", func(b *schema.Blueprint) *schema.Column { return b.String("c").Charset("utf8mb4") }),
		col("collation", func(b *schema.Blueprint) *schema.Column { return b.String("c").Collation(collations[engine]) }),
		col("first", func(b *schema.Blueprint) *schema.Column { return b.String("c").First() }),
		col("after", func(b *schema.Blueprint) *schema.Column { return b.String("c").After("pk") }),
		col("invisible", func(b *schema.Blueprint) *schema.Column { return b.String("c").Invisible() }),
		col("autoIncrement", func(b *schema.Blueprint) *schema.Column { return b.Integer("c").AutoIncrement() }),
		col("storedAs", func(b *schema.Blueprint) *schema.Column { return b.Integer("c").StoredAs("1 + 1") }),
		col("virtualAs", func(b *schema.Blueprint) *schema.Column { return b.Integer("c").VirtualAs("1 + 1") }),
		col("generatedAs", func(b *schema.Blueprint) *schema.Column { return b.BigInteger("c").GeneratedAs() }),
		col("generatedAlways", func(b *schema.Blueprint) *schema.Column { return b.BigInteger("c").GeneratedAs().Always() }),
		col("useCurrent", func(b *schema.Blueprint) *schema.Column { return b.Timestamp("c").UseCurrent() }),
		col("useCurrentOnUpdate", func(b *schema.Blueprint) *schema.Column { return b.Timestamp("c").UseCurrent().UseCurrentOnUpdate() }),
		col("unique", func(b *schema.Blueprint) *schema.Column { return b.String("c").Unique() }),
		col("index", func(b *schema.Blueprint) *schema.Column { return b.String("c").Index() }),
		col("fullText", func(b *schema.Blueprint) *schema.Column { return b.Text("c").FullText() }),
		col("spatialIndex", func(b *schema.Blueprint) *schema.Column { return b.Geometry("c", "point", 0).SpatialIndex() }),
		col("hnswIndex", func(b *schema.Blueprint) *schema.Column { return b.Vector("c", 3).HnswIndex(orm.Cosine) }),
		{name: "fullTextLanguage", col: "c", def: func(b *schema.Blueprint) { b.Text("c"); b.FullText("c").Language("english").Name("c_search") }},
		{name: "indexAlgorithm", col: "c", def: func(b *schema.Blueprint) {
			b.String("c")
			b.Index("c").Algorithm(map[bool]string{true: "minmax", false: "hash"}[engine == "clickhouse"])
		}},
		{name: "blueprintSpatialIndex", col: "c", def: func(b *schema.Blueprint) { b.Geometry("c", "point", 0); b.SpatialIndex("c") }},
		{name: "tableComment", col: "c", def: func(b *schema.Blueprint) { b.String("c"); b.Comment("probe table") }},
		{name: "tableCharset", col: "c", def: func(b *schema.Blueprint) { b.String("c"); b.Charset("utf8mb4") }},
		{name: "tableCollation", col: "c", def: func(b *schema.Blueprint) { b.String("c"); b.Collation(collations[engine]) }},
		{name: "temporary", col: "c", transient: true, def: func(b *schema.Blueprint) { b.String("c"); b.Temporary() }},
		{name: "timestampsTz", col: "created_at", def: func(b *schema.Blueprint) { b.TimestampsTz() }},
		{name: "nullableTimestamps", col: "updated_at", def: func(b *schema.Blueprint) { b.NullableTimestamps() }},
		{name: "datetimes", col: "created_at", def: func(b *schema.Blueprint) { b.Datetimes() }},
		{name: "softDeletesTz", col: "deleted_at", def: func(b *schema.Blueprint) { b.SoftDeletesTz() }},
		{name: "uuidMorphs", col: "owner_id", def: func(b *schema.Blueprint) { b.UUIDMorphs("owner") }},
		{name: "ulidMorphs", col: "owner_id", def: func(b *schema.Blueprint) { b.ULIDMorphs("owner") }},
		{name: "nullableMorphs", col: "owner_type", def: func(b *schema.Blueprint) { b.NullableMorphs("owner") }},
		{name: "nullableUuidMorphs", col: "owner_id", def: func(b *schema.Blueprint) { b.NullableUUIDMorphs("owner") }},
		{name: "nullableUlidMorphs", col: "owner_id", def: func(b *schema.Blueprint) { b.NullableULIDMorphs("owner") }},
		{name: "rememberToken", col: "remember_token", def: func(b *schema.Blueprint) { b.RememberToken() }},
	}
}

// assertBlueprintSurface creates a table for every column type, modifier
// and table option: each must produce DDL the engine runs, or a refusal
// naming the engine — never an engine error from wrong DDL.
func assertBlueprintSurface(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	engine := must(s.DriverName(ctx))
	if engine == "postgres" {
		db := must(orm.Connection(ctx, conn)).DB
		for _, ext := range []string{"postgis", "vector"} {
			must(db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS "+ext))
		}
		eq(t, "extension tables are not the user's", slices.Contains(must(s.Tables(ctx)), "spatial_ref_sys"), false)
		ok(s.DropAllTables(ctx))
	}
	created, refused := 0, 0
	for i, p := range blueprintProbes(engine) {
		table := fmt.Sprintf("probe_%d", i)
		err := s.Create(ctx, table, func(b *schema.Blueprint) {
			if !p.keyed {
				b.UUID("pk").Primary()
			}
			p.def(b)
		})
		if u, ok := errors.AsType[*schema.UnsupportedError](err); ok {
			eq(t, fmt.Sprintf("%s refusal on %s names the engine", p.name, engine), u.Engine, engine)
			refused++
			continue
		}
		if err != nil {
			if needs, ok := extensions[p.name][engine]; !ok || !strings.Contains(err.Error(), needs) {
				t.Errorf("%s on %s: %v", p.name, engine, err)
			}
			continue
		}
		created++
		if !p.transient {
			eq(t, fmt.Sprintf("%s on %s adds %s", p.name, engine, p.col), must(s.HasColumn(ctx, table, p.col)), true)
		}
		ok(s.DropIfExists(ctx, table))
	}
	eq(t, engine+" creates most features", created > refused, true)
}

// assertForeignKeys checks every referential action either holds on the
// engine or is refused.
func assertForeignKeys(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	engine := must(s.DriverName(ctx))
	ok(s.DropIfExists(ctx, "fk_parent"))
	ok(s.Create(ctx, "fk_parent", func(b *schema.Blueprint) { b.UUIDPrimary() }))
	for i, action := range []func(*schema.Foreign){
		func(f *schema.Foreign) { f.CascadeOnDelete() },
		func(f *schema.Foreign) { f.RestrictOnDelete() },
		func(f *schema.Foreign) { f.NoActionOnDelete() },
		func(f *schema.Foreign) { f.CascadeOnUpdate() },
		func(f *schema.Foreign) { f.RestrictOnUpdate() },
		func(f *schema.Foreign) { f.NullOnUpdate() },
		func(f *schema.Foreign) { f.NoActionOnUpdate() },
		func(f *schema.Foreign) { f.OnUpdate("cascade") },
		func(f *schema.Foreign) { f.Named(fmt.Sprintf("custom_fk_%d", 99)) },
		func(f *schema.Foreign) { f.Deferrable().InitiallyDeferred() },
	} {
		table := fmt.Sprintf("fk_child_%d", i)
		err := s.Create(ctx, table, func(b *schema.Blueprint) {
			b.UUIDPrimary()
			action(b.ForeignUUID("parent_id").Nullable().Constrained("fk_parent"))
		})
		if u, isRefusal := errors.AsType[*schema.UnsupportedError](err); isRefusal {
			eq(t, fmt.Sprintf("foreign action %d refusal names %s", i, engine), u.Engine, engine)
			continue
		}
		ok(err)
		eq(t, fmt.Sprintf("foreign action %d on %s", i, engine), len(must(s.GetForeignKeys(ctx, table))), 1)
		ok(s.Drop(ctx, table))
	}
	ok(s.Drop(ctx, "fk_parent"))
}

// assertDropHelpers removes the columns the convenience helpers added.
func assertDropHelpers(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	ok(s.DropIfExists(ctx, "helpers"))
	ok(s.Create(ctx, "helpers", func(b *schema.Blueprint) {
		b.UUIDPrimary()
		b.String("body")
		b.Timestamps()
		b.SoftDeletes()
		b.RememberToken()
		b.Morphs("owner")
	}))
	err := s.Table(ctx, "helpers", func(b *schema.Blueprint) { b.FullText("body") })
	if _, refused := errors.AsType[*schema.UnsupportedError](err); !refused {
		ok(err)
		ok(s.Table(ctx, "helpers", func(b *schema.Blueprint) { b.DropFullTextOn("body") }))
		ok(s.Table(ctx, "helpers", func(b *schema.Blueprint) { b.FullText("body").Name("helpers_search") }))
		ok(s.Table(ctx, "helpers", func(b *schema.Blueprint) { b.DropFullText("helpers_search") }))
		eq(t, "full text dropped on "+conn, must(s.HasIndex(ctx, "helpers", []string{"helpers_search"})), false)
	}
	ok(s.Table(ctx, "helpers", func(b *schema.Blueprint) {
		b.DropTimestamps()
		b.DropSoftDeletes()
		b.DropRememberToken()
		b.DropMorphs("owner")
	}))
	cols := must(s.Columns(ctx, "helpers"))
	slices.Sort(cols)
	eq(t, "drop helpers on "+conn, cols, []string{"body", "id"})
	ok(s.Table(ctx, "helpers", func(b *schema.Blueprint) { b.TimestampsTz(); b.SoftDeletesTz() }))
	ok(s.Table(ctx, "helpers", func(b *schema.Blueprint) { b.DropTimestampsTz(); b.DropSoftDeletesTz() }))
	eq(t, "tz drop helpers on "+conn, len(must(s.Columns(ctx, "helpers"))), 2)
	ok(s.Drop(ctx, "helpers"))

	err = s.Create(ctx, "places", func(b *schema.Blueprint) {
		b.UUIDPrimary()
		b.Geometry("spot", "point", 0)
		b.SpatialIndex("spot")
	})
	if _, refused := errors.AsType[*schema.UnsupportedError](err); refused || err != nil && strings.Contains(err.Error(), "geometry") {
		return
	}
	ok(err)
	ok(s.Table(ctx, "places", func(b *schema.Blueprint) { b.DropSpatialIndexOn("spot") }))
	eq(t, "spatial index dropped by columns on "+conn, must(s.HasIndex(ctx, "places", []string{"spot"})), false)
	ok(s.Table(ctx, "places", func(b *schema.Blueprint) { b.SpatialIndex("spot").Name("places_spot_spatial") }))
	ok(s.Table(ctx, "places", func(b *schema.Blueprint) { b.DropSpatialIndex("places_spot_spatial") }))
	eq(t, "spatial index dropped by name on "+conn, must(s.HasIndex(ctx, "places", []string{"places_spot_spatial"})), false)
	ok(s.Drop(ctx, "places"))
}

func TestBlueprintSurface(t *testing.T) {
	ctx := setup(t)
	assertBlueprintSurface(t, ctx, orm.DefaultConnection)
	assertForeignKeys(t, ctx, orm.DefaultConnection)
	assertDropHelpers(t, ctx, orm.DefaultConnection)
	assertTimePrecision(t, ctx, orm.DefaultConnection)
	assertIndexForms(t, ctx, orm.DefaultConnection)
	assertIdentifierLimits(t, ctx, orm.DefaultConnection, map[string]int{"pgx": 63, "mysql": 64}[driver])
	assertViews(t, ctx, orm.DefaultConnection)
	assertDatabases(t, ctx, orm.DefaultConnection)

	ok(schema.Create(ctx, "shortcuts", func(b *schema.Blueprint) { b.ID(); b.String("a") }))
	ok(schema.Table(ctx, "shortcuts", func(b *schema.Blueprint) { b.String("b").Nullable() }))
	eq(t, "hasColumn shortcut", must(schema.HasColumn(ctx, "shortcuts", "b")), true)
	ok(schema.Rename(ctx, "shortcuts", "shortcuts_renamed"))
	eq(t, "hasTable shortcut", must(schema.HasTable(ctx, "shortcuts_renamed")), true)
	ok(schema.Drop(ctx, "shortcuts_renamed"))
	ok(schema.DropIfExists(ctx, "shortcuts_renamed"))
}

// Stamp holds a fine-grained time, for the precision round trip.
type Stamp struct {
	orm.Model
	ID string    `db:"id"`
	At time.Time `db:"at"`
}

var stampTable = &orm.Table[Stamp]{
	Name:       "stamps",
	PrimaryKey: "id",
	KeyType:    orm.KeyUUID,
	Columns:    []string{"id", "at"},
	Ptr: func(m *Stamp, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "at":
			return &m.At
		}
		return nil
	},
	State: func(m *Stamp) *orm.Model { return &m.Model },
}

var stampAt = orm.NewColumn(stampTable, "at", func(m *Stamp) *time.Time { return &m.At })

// assertTimePrecision stores a microsecond time in a precision-6 column and
// reads it back unchanged, with a precision-6 current-time default
// alongside; precision past microseconds is refused.
func assertTimePrecision(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	ok(s.DropIfExists(ctx, "stamps"))
	ok(s.Create(ctx, "stamps", func(b *schema.Blueprint) {
		b.UUIDPrimary()
		b.Timestamp("at", 6)
		b.DateTime("noted", 6).UseCurrent()
	}))
	want := time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC)
	stamp := Stamp{At: want}
	ok(stampTable.Query().On(conn).Create(ctx, &stamp))
	got := must(stampTable.Query().On(conn).Value(ctx, stampAt))
	eq(t, conn+" microseconds survive", got.UTC().Equal(want), true)
	eq(t, conn+" microseconds filter", must(stampTable.Query().On(conn).Where(stampAt.Eq(want)).Count(ctx)), int64(1))
	later := want.Add(time.Hour + 654321*time.Microsecond)
	must(stampTable.Query().On(conn).Where(stampAt.Eq(want)).Update(ctx, stampAt.Set(later)))
	eq(t, conn+" microseconds update", must(stampTable.Query().On(conn).Value(ctx, stampAt)).UTC().Equal(later), true)
	ok(s.Drop(ctx, "stamps"))
	err := s.Create(ctx, "stamps", func(b *schema.Blueprint) { b.UUIDPrimary(); b.Timestamp("at", 7) })
	eq(t, conn+" precision past microseconds refused", err != nil && strings.Contains(err.Error(), "0 to 6"), true)
}

// assertIdentifierLimits checks names longer than the engine keeps fail
// loudly — in DDL, in generated index names and in queries — rather than
// being truncated (Postgres) into a name another one may share. limit is
// zero where the engine has none.
func assertIdentifierLimits(t *testing.T, ctx context.Context, conn string, limit int) {
	t.Helper()
	long := strings.Repeat("n", max(limit, 64)+1)
	err := schema.On(conn).Create(ctx, "long_names", func(t *schema.Blueprint) {
		t.ID()
		t.String(long)
	})
	defer func() { tCheck(t, schema.On(conn).DropIfExists(ctx, "long_names")) }()
	if limit == 0 {
		ok(err)
		eq(t, conn+" long names kept", must(orm.From("long_names").On(conn).Where(long, orm.Eq, "x").Count(ctx)), int64(0))
		return
	}
	_, refused := errors.AsType[*schema.UnsupportedError](err)
	eq(t, conn+" long column refused", refused && strings.Contains(err.Error(), "names are at most"), true)
	exact := strings.Repeat("c", limit)
	err = schema.On(conn).Create(ctx, "long_names", func(t *schema.Blueprint) {
		t.ID()
		t.String(exact).Index()
	})
	_, refused = errors.AsType[*schema.UnsupportedError](err)
	eq(t, conn+" long generated index name refused", refused && strings.Contains(err.Error(), "long_names_"+exact+"_index"), true)
	tCheck(t, schema.On(conn).Create(ctx, "long_names", func(t *schema.Blueprint) {
		t.ID()
		t.String(exact)
		t.Index(exact).Name("long_names_short")
	}))
	eq(t, conn+" a name at the limit works", must(orm.From("long_names").On(conn).Where(exact, orm.Eq, "x").Count(ctx)), int64(0))
	_, err = orm.From("long_names").On(conn).Where(long, orm.Eq, "x").Count(ctx)
	eq(t, conn+" long names refused in queries", err != nil && strings.Contains(err.Error(), "keeps names of at most"), true)
}

// assertIndexForms creates a partial unique index and an expression index:
// where the engine has them they must behave, elsewhere they are refused.
func assertIndexForms(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	engine := must(s.DriverName(ctx))
	refusedOr := func(what string, err error) bool {
		t.Helper()
		if u, ok := errors.AsType[*schema.UnsupportedError](err); ok {
			eq(t, what+" refusal names "+engine, u.Engine, engine)
			return true
		}
		ok(err)
		return false
	}
	ok(s.DropIfExists(ctx, "accounts"))
	err := s.Create(ctx, "accounts", func(b *schema.Blueprint) {
		b.UUIDPrimary()
		b.String("email")
		b.Timestamp("deleted_at").Nullable()
		b.Unique("email").Where("deleted_at IS NULL")
	})
	if !refusedOr("partial index", err) {
		accounts := orm.From("accounts").On(conn)
		ok(accounts.Insert(ctx, map[string]any{"id": "0198c0de-0000-7000-8000-0000000000c1", "email": "x@y.z", "deleted_at": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}))
		ok(accounts.Insert(ctx, map[string]any{"id": "0198c0de-0000-7000-8000-0000000000c2", "email": "x@y.z"}))
		eq(t, engine+" partial unique ignores deleted rows", must(accounts.Count(ctx)), int64(2))
		eq(t, engine+" partial unique holds for live rows", accounts.Insert(ctx, map[string]any{"id": "0198c0de-0000-7000-8000-0000000000c3", "email": "x@y.z"}) != nil, true)
		ok(s.Drop(ctx, "accounts"))
	}
	ok(s.DropIfExists(ctx, "accounts"))
	err = s.Create(ctx, "accounts", func(b *schema.Blueprint) {
		b.UUIDPrimary()
		b.String("email")
		b.RawIndex("lower(email)", "accounts_email_lower")
	})
	if !refusedOr("expression index", err) {
		eq(t, engine+" expression index exists", must(s.HasIndex(ctx, "accounts", []string{"accounts_email_lower"})), true)
		ok(s.Table(ctx, "accounts", func(b *schema.Blueprint) { b.DropIndex("accounts_email_lower") }))
		eq(t, engine+" expression index dropped", must(s.HasIndex(ctx, "accounts", []string{"accounts_email_lower"})), false)
	}
	ok(s.DropIfExists(ctx, "accounts"))
}

// assertViews creates, lists, queries and drops views, and checks the raw
// statement and select helpers on the same connection.
func assertViews(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	ok(s.DropViewIfExists(ctx, "loud_notes"))
	ok(s.DropIfExists(ctx, "notes"))
	ok(s.Create(ctx, "notes", func(b *schema.Blueprint) {
		b.UUIDPrimary()
		b.String("body")
		b.Integer("volume")
	}))
	inserted := must(orm.StatementOn(ctx, conn, "INSERT INTO notes (id, body, volume) VALUES (?, ?, ?), (?, ?, ?)",
		"0198c0de-0000-7000-8000-0000000000d1", "whisper", 1, "0198c0de-0000-7000-8000-0000000000d2", "shout", 9))
	if engine := must(s.DriverName(ctx)); engine != "clickhouse" {
		eq(t, conn+" statement reports affected rows", inserted, int64(2))
	}
	ok(s.CreateView(ctx, "loud_notes", "SELECT body FROM notes WHERE volume > 5"))
	eq(t, conn+" views", must(s.Views(ctx)), []string{"loud_notes"})
	eq(t, conn+" hasView", must(s.HasView(ctx, "loud_notes")), true)
	eq(t, conn+" views are not tables", slices.Contains(must(s.Tables(ctx)), "loud_notes"), false)
	rows := must(orm.SelectOn(ctx, conn, "SELECT body FROM loud_notes WHERE body <> ?", "nothing"))
	eq(t, conn+" select through a view", fmt.Sprint(rows[0]["body"]), "shout")
	ok(s.DropView(ctx, "loud_notes"))
	eq(t, conn+" dropView", must(s.HasView(ctx, "loud_notes")), false)
	eq(t, conn+" dropView missing fails", s.DropView(ctx, "loud_notes") != nil, true)
	ok(s.DropViewIfExists(ctx, "loud_notes"))
	ok(s.CreateView(ctx, "loud_notes", "SELECT body FROM notes WHERE volume > 5"))
	ok(s.DropAllViews(ctx))
	eq(t, conn+" dropAllViews", len(must(s.Views(ctx))), 0)
	ok(s.Drop(ctx, "notes"))
}

// assertDatabases creates and drops a database on servers, and checks the
// file-based engines refuse.
func assertDatabases(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	engine := must(s.DriverName(ctx))
	err := s.CreateDatabase(ctx, "storm_scratch")
	if engine == "sqlite" || engine == "duckdb" {
		_, refused := errors.AsType[*schema.UnsupportedError](err)
		eq(t, engine+" refuses databases", refused, true)
		return
	}
	ok(err)
	eq(t, engine+" duplicate database fails", s.CreateDatabase(ctx, "storm_scratch") != nil, true)
	ok(s.DropDatabaseIfExists(ctx, "storm_scratch"))
	ok(s.DropDatabaseIfExists(ctx, "storm_scratch"))
}
