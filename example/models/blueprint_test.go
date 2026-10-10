package models_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

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
// the test containers run (PostGIS, pgvector before CREATE EXTENSION,
// DuckDB spatial, MySQL 9 / MariaDB 11.7 vectors); the engine's own error
// must name what is missing.
var extensions = map[string]map[string]string{
	"geometry":              {"postgres": `type "geometry" does not exist`, "duckdb": "spatial extension"},
	"geography":             {"postgres": `type "geography" does not exist`, "duckdb": "spatial extension"},
	"spatialIndex":          {"postgres": `type "geometry" does not exist`, "duckdb": "spatial extension"},
	"blueprintSpatialIndex": {"postgres": `type "geometry" does not exist`, "duckdb": "spatial extension"},
	"vector":                {"postgres": `type "vector" does not exist`, "mysql": "vector(3)", "mariadb": "Unknown data type: 'vector'"},
	"hnswIndex":             {"postgres": `type "vector" does not exist`, "mysql": "vector(3)", "mariadb": "Unknown data type: 'vector'"},
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

	ok(schema.Create(ctx, "shortcuts", func(b *schema.Blueprint) { b.ID(); b.String("a") }))
	ok(schema.Table(ctx, "shortcuts", func(b *schema.Blueprint) { b.String("b").Nullable() }))
	eq(t, "hasColumn shortcut", must(schema.HasColumn(ctx, "shortcuts", "b")), true)
	ok(schema.Rename(ctx, "shortcuts", "shortcuts_renamed"))
	eq(t, "hasTable shortcut", must(schema.HasTable(ctx, "shortcuts_renamed")), true)
	ok(schema.Drop(ctx, "shortcuts_renamed"))
	ok(schema.DropIfExists(ctx, "shortcuts_renamed"))
}
