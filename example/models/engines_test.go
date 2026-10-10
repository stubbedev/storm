package models_test

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/marcboeker/go-duckdb/v2"
	_ "modernc.org/sqlite"

	. "github.com/stubbedev/storm/example/models"
	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

// ---------------------------------------------------------------------------
// Test models: widgets for SQL engines, documents with vectors for stores.
// ---------------------------------------------------------------------------

type Widget struct {
	orm.Model
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Kind      string    `db:"kind"`
	Karma     int64     `db:"karma"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

var widgetTable = &orm.Table[Widget]{
	Name:       "widgets",
	PrimaryKey: "id",
	KeyType:    orm.KeyAutoIncrement,
	CreatedAt:  "created_at",
	UpdatedAt:  "updated_at",
	Columns:    []string{"id", "name", "kind", "karma", "created_at", "updated_at"},
	Ptr: func(m *Widget, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "name":
			return &m.Name
		case "kind":
			return &m.Kind
		case "karma":
			return &m.Karma
		case "created_at":
			return &m.CreatedAt
		case "updated_at":
			return &m.UpdatedAt
		}
		return nil
	},
	State: func(m *Widget) *orm.Model { return &m.Model },
}

type WidgetSchema struct {
	*orm.Table[Widget]
	ID        orm.Column[Widget, int64]
	Name      orm.Column[Widget, string]
	Kind      orm.Column[Widget, string]
	Karma     orm.Column[Widget, int64]
	CreatedAt orm.Column[Widget, time.Time]
}

var Widgets = WidgetSchema{
	Table:     widgetTable,
	ID:        orm.NewColumn(widgetTable, "id", func(m *Widget) *int64 { return &m.ID }),
	Name:      orm.NewColumn(widgetTable, "name", func(m *Widget) *string { return &m.Name }),
	Kind:      orm.NewColumn(widgetTable, "kind", func(m *Widget) *string { return &m.Kind }),
	Karma:     orm.NewColumn(widgetTable, "karma", func(m *Widget) *int64 { return &m.Karma }),
	CreatedAt: orm.NewColumn(widgetTable, "created_at", func(m *Widget) *time.Time { return &m.CreatedAt }),
}

type Doc struct {
	orm.Model
	ID    string             `db:"id"`
	Title string             `db:"title"`
	Views int64              `db:"views"`
	Vec   orm.Vector[orm.D3] `db:"vec"`
}

var docTable = &orm.Table[Doc]{
	Name:       "documents",
	PrimaryKey: "id",
	KeyType:    orm.KeyUUID,
	UpdatedAt:  "",
	Columns:    []string{"id", "title", "views", "vec"},
	Ptr: func(m *Doc, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "title":
			return &m.Title
		case "views":
			return &m.Views
		case "vec":
			return &m.Vec
		}
		return nil
	},
	State: func(m *Doc) *orm.Model { return &m.Model },
}

type DocSchema struct {
	*orm.Table[Doc]
	ID    orm.Column[Doc, string]
	Title orm.Column[Doc, string]
	Views orm.Column[Doc, int64]
	Vec   orm.Column[Doc, orm.Vector[orm.D3]]
}

var Docs = DocSchema{
	Table: docTable,
	ID:    orm.NewColumn(docTable, "id", func(m *Doc) *string { return &m.ID }),
	Title: orm.NewColumn(docTable, "title", func(m *Doc) *string { return &m.Title }),
	Views: orm.NewColumn(docTable, "views", func(m *Doc) *int64 { return &m.Views }),
	Vec:   orm.NewColumn(docTable, "vec", func(m *Doc) *orm.Vector[orm.D3] { return &m.Vec }),
}

// createWidgetTable creates widgets keyed by key, which adds the id column:
// auto-incrementing where the engine generates keys, manual on ClickHouse.
func createWidgetTable(ctx context.Context, t *testing.T, conn string, key func(*schema.Blueprint)) {
	t.Helper()
	tCheck(t, schema.On(conn).DropAllTables(ctx))
	tCheck(t, schema.On(conn).Create(ctx, "widgets", func(t *schema.Blueprint) {
		key(t)
		t.String("name")
		t.String("kind")
		t.BigInteger("karma").Default(0)
		t.Timestamps()
	}))
}

func seedWidgets(t *testing.T) []Widget {
	t.Helper()
	return []Widget{
		{Name: "alpha", Kind: "gear", Karma: 5},
		{Name: "beta", Kind: "spring", Karma: 15},
		{Name: "gamma", Kind: "gear", Karma: 25},
	}
}

func assertWidgets(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	ws := must(Widgets.Query().On(conn).Where(Widgets.Kind.Eq("gear")).OrderBy(Widgets.Karma.Desc()).Get(ctx))
	eq(t, "where + order", len(ws), 2)
	eq(t, "order desc", ws[0].Name, "gamma")
	n := must(Widgets.Query().On(conn).Count(ctx))
	eq(t, "count", n, int64(3))
	first := must(Widgets.Query().On(conn).Where(Widgets.Name.Like("alp")).First(ctx))
	eq(t, "like", first.Name, "alpha")

	w := first
	w.Karma = 42
	ok(Widgets.Query().On(conn).Save(ctx, &w))
	eq(t, "save reloads", must(Widgets.Query().On(conn).Find(ctx, w.ID)).Karma, int64(42))

	rows := must(orm.From("widgets").On(conn).Where("kind", "=", "gear").OrderBy("name").Get(ctx))
	eq(t, "table query", fmt.Sprint(rows[0]["name"]), "alpha")

	n = must(Widgets.Query().On(conn).Where(Widgets.ID.Eq(w.ID)).Delete(ctx))
	eq(t, "delete", n, int64(1))
	eq(t, "gone", must(Widgets.Query().On(conn).Count(ctx)), int64(2))

	beta := orm.FromTable(Widgets).On(conn).WhereCond(Widgets.Name.Eq("beta"))
	eq(t, "table update", must(beta.UpdateCols(ctx, Widgets.Karma.SetExpr(orm.Plus(Widgets.Karma, 1)))), int64(1))
	eq(t, "table update applied", must(beta.PluckCol(ctx, Widgets.Karma)), []int64{16})
	eq(t, "table delete", must(beta.Delete(ctx)), int64(1))
	eq(t, "table deleted", must(orm.From("widgets").On(conn).Count(ctx)), int64(1))
	gamma := must(orm.FromTable(Widgets).On(conn).PluckCol(ctx, Widgets.ID))[0]
	ok(orm.From("widgets").On(conn).Upsert(ctx, []map[string]any{{"id": gamma, "name": "gamma2", "kind": "gear", "karma": 1}}, []string{"id"}, "name"))
	eq(t, "table upsert", must(orm.FromTable(Widgets).On(conn).PluckCol(ctx, Widgets.Name)), []string{"gamma2"})
}

func TestDuckDB(t *testing.T) {
	current = t
	ctx := context.Background()
	path := t.TempDir() + "/test.duckdb"
	db, err := orm.Open("duck", "duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	createWidgetTable(ctx, t, "duck", func(t *schema.Blueprint) { t.ID() })
	for i := range seedWidgets(t) {
		w := seedWidgets(t)[i]
		ok(Widgets.Query().On("duck").Create(ctx, &w))
	}
	assertWidgets(t, ctx, "duck")
	assertEvents(t, ctx, "duck")
	assertBlueprintSurface(t, ctx, "duck")
	assertForeignKeys(t, ctx, "duck")
	assertDropHelpers(t, ctx, "duck")

	// Flat files: DuckDB reads CSV straight from the builder.
	csv := t.TempDir() + "/flat.csv"
	ok(os.WriteFile(csv, []byte("id,name,kind,karma\n1,alpha,gear,5\n2,beta,spring,15\n3,gamma,gear,25\n"), 0o644))
	rows := must(orm.From(fmt.Sprintf("read_csv('%s')", csv)).On("duck").Where("kind", "=", "gear").Count(ctx))
	eq(t, "csv query", rows, int64(2))

	// Vectors: FLOAT[3] columns and array_distance ordering.
	tCheck(t, schema.On("duck").Create(ctx, "documents", func(t *schema.Blueprint) {
		t.UUID("id")
		t.String("title")
		t.BigInteger("views").Default(0)
		t.Vector("vec", 3)
	}))
	for _, d := range []Doc{
		{Title: "near", Vec: orm.NewVector[orm.D3](1, 0, 0)},
		{Title: "far", Vec: orm.NewVector[orm.D3](10, 10, 10)},
	} {
		ok(Docs.Query().On("duck").Create(ctx, &d))
	}
	found := must(Docs.Query().On("duck").
		OrderBy(Docs.Vec.Nearest(orm.NewVector[orm.D3](1, 0, 0), orm.L2)).
		Limit(1).Get(ctx))
	eq(t, "nearest", found[0].Title, "near")

	refusesExampleSchema(ctx, t, "duck", "duckdb", "create_users_table")
}

// refusesExampleSchema checks that the example migrations, written for the
// transactional engines, fail on an analytical one with a precise refusal
// rather than a half-right schema.
func refusesExampleSchema(ctx context.Context, t *testing.T, conn, engine, migration string) {
	t.Helper()
	_, err := (&schema.Migrator{Connection: conn, Migrations: schema.Registered()}).Migrate(ctx, false)
	var refusal *schema.UnsupportedError
	eq(t, "example schema refused on "+engine, errors.As(err, &refusal) && refusal.Engine == engine && strings.Contains(err.Error(), migration), true)
	tCheck(t, schema.On(conn).DropAllTables(ctx))
}

func TestClickHouse(t *testing.T) {
	current = t
	if os.Getenv("STORM_TEST_CLICKHOUSE") == "" {
		t.Skip("set STORM_TEST_CLICKHOUSE=clickhouse://default:storm@127.0.0.1:19000/storm?dial_timeout=5s to run")
	}
	ctx := context.Background()
	db, err := orm.Open("ch", "clickhouse", os.Getenv("STORM_TEST_CLICKHOUSE"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	createWidgetTable(ctx, t, "ch", func(t *schema.Blueprint) { t.BigInteger("id").Primary() })
	for i, w := range seedWidgets(t) {
		w.ID = int64(i + 1) // ClickHouse does not generate integer keys
		ok(Widgets.Query().On("ch").Create(ctx, &w))
	}
	ws := must(Widgets.Query().On("ch").Where(Widgets.Kind.Eq("gear")).OrderBy(Widgets.Karma.Desc()).Get(ctx))
	eq(t, "where + order", len(ws), 2)
	eq(t, "order desc", ws[0].Name, "gamma")
	eq(t, "count", must(Widgets.Query().On("ch").Count(ctx)), int64(3))
	eq(t, "update count", must(Widgets.Query().On("ch").Where(Widgets.ID.Eq(ws[0].ID)).Update(ctx, Widgets.Karma.Set(99))), int64(1))
	eq(t, "updated", must(Widgets.Query().On("ch").Find(ctx, ws[0].ID)).Karma, int64(99))
	eq(t, "delete count", must(Widgets.Query().On("ch").Where(Widgets.ID.Eq(ws[0].ID)).Delete(ctx)), int64(1))
	eq(t, "gone", must(Widgets.Query().On("ch").Count(ctx)), int64(2))

	beta := orm.FromTable(Widgets).On("ch").WhereCond(Widgets.Name.Eq("beta"))
	eq(t, "table update count", must(beta.UpdateCols(ctx, Widgets.Karma.SetExpr(orm.Plus(Widgets.Karma, 1)))), int64(1))
	eq(t, "table update", must(beta.PluckCol(ctx, Widgets.Karma)), []int64{16})
	eq(t, "table increment", must(beta.Increment(ctx, "karma", 4)), int64(1))
	eq(t, "table incremented", must(beta.PluckCol(ctx, Widgets.Karma)), []int64{20})
	eq(t, "table delete count", must(beta.Delete(ctx)), int64(1))
	eq(t, "table delete", must(orm.From("widgets").On("ch").Count(ctx)), int64(1))

	widgets := orm.FromTable(Widgets).On("ch")
	ok(widgets.InsertOrIgnore(ctx,
		map[string]any{"id": 1, "name": "clash", "kind": "gear", "karma": 0},
		map[string]any{"id": 10, "name": "ten", "kind": "gear", "karma": 0},
		map[string]any{"id": 10, "name": "ten again", "kind": "gear", "karma": 0}))
	eq(t, "insertOrIgnore", must(widgets.OrderByCol(Widgets.ID).PluckCol(ctx, Widgets.Name)), []string{"alpha", "ten"})
	ok(widgets.Upsert(ctx, []map[string]any{
		{"id": 10, "name": "ten upserted", "kind": "spring", "karma": 5},
		{"id": 11, "name": "eleven", "kind": "gear", "karma": 0},
	}, []string{"id"}, "name"))
	eq(t, "upsert", must(widgets.OrderByCol(Widgets.ID).PluckCol(ctx, Widgets.Name)), []string{"alpha", "ten upserted", "eleven"})
	eq(t, "upsert leaves other columns", must(widgets.WhereCond(Widgets.ID.Eq(10)).PluckCol(ctx, Widgets.Kind)), []string{"gear"})
	ws = must(Widgets.Query().On("ch").Where(Widgets.ID.Eq(11)).Get(ctx))
	ws[0].Karma = 7
	ok(Widgets.Query().On("ch").Upsert(ctx, ws, []orm.AnyColumn[Widget]{Widgets.ID}, Widgets.Karma))
	eq(t, "model upsert", must(Widgets.Query().On("ch").Find(ctx, int64(11))).Karma, int64(7))

	_, err = Widgets.Query().On("ch").InsertGetID(ctx, Widget{Name: "keyless"})
	eq(t, "insertGetID needs generated keys", err != nil && strings.Contains(err.Error(), "does not generate integer keys"), true)
	err = Widgets.Query().On("ch").Create(ctx, &Widget{Name: "keyless"})
	eq(t, "create needs a key", err != nil && strings.Contains(err.Error(), "does not generate integer keys"), true)
	assertEvents(t, ctx, "ch")
	assertBlueprintSurface(t, ctx, "ch")
	assertForeignKeys(t, ctx, "ch")
	assertDropHelpers(t, ctx, "ch")
	refusesExampleSchema(ctx, t, "ch", "clickhouse", "create_countries_table")
}

func TestMongo(t *testing.T) {
	current = t
	if os.Getenv("STORM_TEST_MONGO") == "" {
		t.Skip("set STORM_TEST_MONGO=mongodb://127.0.0.1:17017 to run")
	}
	ctx := context.Background()
	store, err := orm.OpenMongo("mongo", os.Getenv("STORM_TEST_MONGO"), "storm"+strings.ToLower(strconv.FormatInt(time.Now().UnixNano()%100000, 10)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close(ctx) })

	for _, d := range []Doc{
		{Title: "near", Views: 1, Vec: orm.NewVector[orm.D3](1, 0, 0)},
		{Title: "far", Views: 2, Vec: orm.NewVector[orm.D3](10, 10, 10)},
		{Title: "middle", Views: 3, Vec: orm.NewVector[orm.D3](5, 5, 5)},
	} {
		ok(Docs.Query().On("mongo").Create(ctx, &d))
	}

	eq(t, "count", must(Docs.Query().On("mongo").Count(ctx)), int64(3))
	docs := must(Docs.Query().On("mongo").Where(Docs.Views.Gt(1)).OrderBy(Docs.Views.Asc()).Get(ctx))
	eq(t, "where + order", len(docs), 2)
	eq(t, "order", docs[0].Title, "far")
	eq(t, "vectors hydrate", docs[0].Vec.String(), "[10,10,10]")

	one := must(Docs.Query().On("mongo").Where(Docs.Title.Eq("near")).First(ctx))
	eq(t, "first", one.Views, int64(1))
	eq(t, "exists", must(Docs.Query().On("mongo").Where(Docs.Title.Eq("nope")).DoesntExist(ctx)), true)
	eq(t, "find", must(Docs.Query().On("mongo").Find(ctx, one.ID)).Title, "near")
	eq(t, "in", must(Docs.Query().On("mongo").Where(Docs.Views.In(1, 2)).Count(ctx)), int64(2))
	eq(t, "between", must(Docs.Query().On("mongo").Where(Docs.Views.Between(2, 3)).Count(ctx)), int64(2))
	eq(t, "or", must(Docs.Query().On("mongo").Where(
		orm.Or(Docs.Title.Eq("near"), Docs.Title.Eq("far"))).Count(ctx)), int64(2))
	eq(t, "not", must(Docs.Query().On("mongo").WhereNot(Docs.Title.Eq("near")).Count(ctx)), int64(2))

	one.Views = 10
	ok(Docs.Query().On("mongo").Save(ctx, &one))
	eq(t, "save", must(Docs.Query().On("mongo").Find(ctx, one.ID)).Views, int64(10))
	n := must(Docs.Query().On("mongo").Where(Docs.Views.Eq(3)).Update(ctx, Docs.Title.Set("renamed")))
	eq(t, "update", n, int64(1))
	eq(t, "update applied", must(Docs.Query().On("mongo").Where(Docs.Title.Eq("renamed")).Count(ctx)), int64(1))

	// orm.From works on document stores too.
	rows := must(orm.From("documents").On("mongo").Where("views", ">=", 2).OrderBy("views").Get(ctx))
	eq(t, "table query", len(rows), 3)
	eq(t, "table query values", fmt.Sprint(rows[0]["views"]), "2")
	eq(t, "table count", must(orm.From("documents").On("mongo").Where("title", "=", "renamed").Count(ctx)), int64(1))
	eq(t, "table orWhere", must(orm.From("documents").On("mongo").
		Where("views", ">=", 10).OrWhere("title", "=", "renamed").Count(ctx)), int64(2))
	eq(t, "typed table where", must(orm.From("documents").On("mongo").
		WhereCond(Docs.Views.Gt(1)).Count(ctx)), int64(3))
	_, err = orm.From("documents").On("mongo").WhereCond(orm.Raw[Doc]("views > 1")).Count(ctx)
	eq(t, "typed raw refused", orm.IsNotTranslatable(err), true)
	eq(t, "table exists", must(orm.From("documents").On("mongo").WhereCond(Docs.Views.Gt(1)).Exists(ctx)), true)
	eq(t, "table doesn't exist", must(orm.From("documents").On("mongo").WhereCond(Docs.Views.Gt(100)).Exists(ctx)), false)
	_, err = orm.From("documents").On("mongo").InsertGetID(ctx, map[string]any{"title": "x"})
	eq(t, "table insertGetID refused", orm.IsNotTranslatable(err), true)
	_, err = Docs.Query().On("mongo").Pluck(ctx, Docs.Title)
	eq(t, "SQL-only method refused", orm.IsNotTranslatable(err), true)
	ok(orm.From("documents").On("mongo").InsertCols(ctx, Docs.ID.Set("typed-row"), Docs.Title.Set("typed"), Docs.Views.Set(4)))
	typedRow := orm.From("documents").On("mongo").WhereCond(Docs.ID.Eq("typed-row"))
	eq(t, "typed table update", must(typedRow.UpdateCols(ctx, Docs.Views.Set(6))), int64(1))
	eq(t, "typed table pluck", must(typedRow.PluckCol(ctx, Docs.Views)), []int64{6})
	_, err = typedRow.UpdateCols(ctx, Docs.Views.SetExpr(orm.Plus(Docs.Views, 1)))
	eq(t, "typed table expr refused", orm.IsNotTranslatable(err), true)
	eq(t, "increment", must(typedRow.Increment(ctx, "views", 3)), int64(1))
	eq(t, "decrement", must(typedRow.Decrement(ctx, "views", 2)), int64(1))
	eq(t, "model increment", must(Docs.Query().On("mongo").Where(Docs.ID.Eq("typed-row")).Increment(ctx, Docs.Views, 10)), int64(1))
	eq(t, "increments applied", must(typedRow.PluckCol(ctx, Docs.Views)), []int64{17})
	_, err = typedRow.UpdateCols(ctx, Docs.Views.SetExpr(orm.Plus(Docs.Views, 1)))
	eq(t, "other expressions refused", orm.IsNotTranslatable(err), true)
	_, err = orm.From("documents").On("mongo").Sum[int64](ctx, "views")
	eq(t, "aggregate refused", orm.IsNotTranslatable(err), true)
	_, err = orm.From("documents").On("mongo").WhereCond(Users.Name.Eq("x")).Count(ctx)
	eq(t, "foreign column refused", orm.IsNotTranslatable(err), true)
	ok(orm.From("documents").On("mongo").InsertOrIgnore(ctx,
		map[string]any{"id": "typed-row", "title": "dropped", "views": 5},
		map[string]any{"id": "fresh-row", "title": "fresh", "views": 1}))
	eq(t, "row insertOrIgnore kept", must(typedRow.PluckCol(ctx, Docs.Title)), []string{"typed"})
	freshRow := orm.From("documents").On("mongo").WhereCond(Docs.ID.Eq("fresh-row"))
	eq(t, "row insertOrIgnore inserted", must(freshRow.PluckCol(ctx, Docs.Title)), []string{"fresh"})
	ok(orm.From("documents").On("mongo").Upsert(ctx, []map[string]any{{"id": "fresh-row", "title": "upserted", "views": 1}}, []string{"id"}))
	eq(t, "row upsert", must(freshRow.PluckCol(ctx, Docs.Title)), []string{"upserted"})
	eq(t, "row cleanup", must(freshRow.Delete(ctx)), int64(1))
	_, err = typedRow.SelectCols(Docs.Title.As("t")).Get(ctx)
	eq(t, "alias refused", orm.IsNotTranslatable(err), true)
	eq(t, "typed table delete", must(typedRow.Delete(ctx)), int64(1))
	eq(t, "restore", must(Users.Query().On("mongo").Restore(ctx)), int64(0))

	eq(t, "delete", must(Docs.Query().On("mongo").Where(Docs.Title.Eq("renamed")).Delete(ctx)), int64(1))
	eq(t, "deleted", must(Docs.Query().On("mongo").Count(ctx)), int64(2))

	// Joins and raw conditions refuse with a precise error.
	_, err = Users.Query().Join(Posts, Posts.UserID.EqCol(Users.ID)).On("mongo").Get(ctx)
	eq(t, "join refused", orm.IsNotTranslatable(err), true)
	_, err = Docs.Query().On("mongo").Where(orm.Raw[Doc]("views > 1")).Get(ctx)
	eq(t, "raw refused", orm.IsNotTranslatable(err), true)

	// Upsert replaces documents matched on the key columns.
	kept := must(Docs.Query().On("mongo").Where(Docs.Title.Eq("near")).First(ctx))
	kept.Views = 77
	fresh := Doc{Title: "brand new", Views: 1, Vec: orm.NewVector3(2, 0, 0)}
	ok(Docs.Query().On("mongo").Upsert(ctx, []Doc{kept, fresh}, []orm.AnyColumn[Doc]{Docs.ID}))
	eq(t, "upsert count", must(Docs.Query().On("mongo").Count(ctx)), int64(3))
	eq(t, "upsert replaced", must(Docs.Query().On("mongo").Find(ctx, kept.ID)).Views, int64(77))
	eq(t, "upsert inserted", must(Docs.Query().On("mongo").Where(Docs.Title.Eq("brand new")).Count(ctx)), int64(1))

	// InsertOrIgnore skips documents whose key is already taken.
	dup := must(Docs.Query().On("mongo").Where(Docs.Title.Eq("near")).First(ctx))
	dup.Views = 999 // must not land: the key exists
	another := Doc{Title: "ignored newcomer", Views: 5, Vec: orm.NewVector3(0, 2, 0)}
	ok(Docs.Query().On("mongo").InsertOrIgnore(ctx, dup, another))
	eq(t, "orIgnore count", must(Docs.Query().On("mongo").Count(ctx)), int64(4))
	eq(t, "orIgnore skipped existing", must(Docs.Query().On("mongo").Find(ctx, dup.ID)).Views, int64(77))
	eq(t, "orIgnore inserted missing", must(Docs.Query().On("mongo").Where(Docs.Title.Eq("ignored newcomer")).Count(ctx)), int64(1))
	assertDocPagination(t, ctx, "mongo", 4)

	eq(t, "like", must(Docs.Query().On("mongo").Where(Docs.Title.Like("ea")).Count(ctx)), int64(1))
	eq(t, "like escapes regex syntax", must(Docs.Query().On("mongo").Where(Docs.Title.Like("(n")).Count(ctx)), int64(0))
	eq(t, "like with wildcards", must(orm.From("documents").On("mongo").Where("title", orm.Like, "n_ar").Count(ctx)), int64(1))

	assertValuerErrors(t, ctx, "mongo")
	mongoDocs := orm.From("documents").On("mongo")
	eq(t, "row find", must(mongoDocs.Find(ctx, one.ID))["title"], any("near"))
	eq(t, "row sole", must(mongoDocs.Where("title", orm.Eq, "near").Sole(ctx))["id"], any(one.ID))
	eq(t, "row value", must(mongoDocs.Where("title", orm.Eq, "near").Value(ctx, "views")) != nil, true)
	eq(t, "row latest", len(must(mongoDocs.Latest("views").Get(ctx))) > 0, true)
	for what, q := range map[string]orm.RowQuery{
		"union":        mongoDocs.Union(mongoDocs),
		"lock":         mongoDocs.LockForUpdate(),
		"random order": mongoDocs.InRandomOrder(),
		"whereExists":  mongoDocs.WhereExists(mongoDocs),
		"whereColumn":  mongoDocs.WhereColumn("views", orm.Gt, "title"),
		"orWhereRaw":   mongoDocs.Where("views", orm.Gt, 1).OrWhereRaw("1 = 1"),
	} {
		_, err := q.Get(ctx)
		eq(t, "row "+what+" refused", orm.IsNotTranslatable(err), true)
	}
	_, err = mongoDocs.Explain(ctx)
	eq(t, "row explain refused", orm.IsNotTranslatable(err), true)
	eq(t, "not equal", must(orm.From("documents").On("mongo").Where("title", "!=", "near").Count(ctx)), int64(3))
	eq(t, "ilike", must(orm.From("documents").On("mongo").Where("title", orm.Ilike, "NEAR").Count(ctx)), int64(1))
	eq(t, "not ilike", must(orm.From("documents").On("mongo").Where("title", orm.NotIlike, "%E%").Count(ctx)), int64(1))
	eq(t, "unsigned decrement", must(orm.From("documents").On("mongo").Where("title", orm.Eq, "near").Decrement(ctx, "views", uint8(1))), int64(1))

	assertDocMigrations(t, ctx, "mongo")

	txDocs := orm.From("tx_docs").On("mongo")
	ok(txDocs.Insert(ctx, map[string]any{"id": "seed"}))
	committed := false
	ok(orm.TransactionOn(ctx, "mongo", func(ctx context.Context) error {
		eq(t, "inTransaction on mongo", must(txDocs.Count(ctx)), int64(1))
		orm.AfterCommitOn(ctx, "mongo", func(context.Context) { committed = true })
		return txDocs.Insert(ctx, map[string]any{"id": "kept"})
	}))
	eq(t, "mongo transaction commits", fmt.Sprint(must(txDocs.Count(ctx)), committed), "2 true")
	err = orm.TransactionOn(ctx, "mongo", func(ctx context.Context) error {
		ok(txDocs.Insert(ctx, map[string]any{"id": "dropped"}))
		return errors.New("roll back")
	})
	eq(t, "mongo transaction rolls back", err != nil && must(txDocs.Count(ctx)) == 2, true)
	err = orm.TransactionOn(ctx, "mongo", func(ctx context.Context) error {
		ok(txDocs.Insert(ctx, map[string]any{"id": "outer"}))
		_ = orm.TransactionOn(ctx, "mongo", func(ctx context.Context) error { return errors.New("inner fails") })
		return nil
	})
	eq(t, "failed nested transaction rolls back the whole", errors.Is(err, orm.ErrNestedFailed) && must(txDocs.Count(ctx)) == 2, true)
	eq(t, "mongo supports transactions", must(orm.SupportsTransactions(ctx, "mongo")), true)
	ok(schema.On("mongo").Drop(ctx, "tx_docs"))
}

// failingValuer fails to convert, as Encrypted does without a key.
type failingValuer struct{}

func (failingValuer) Value() (sqldriver.Value, error) { return nil, errors.New("no key") }

// assertValuerErrors checks a value that fails to convert is never stored
// in some other form: writes and filters with it fail.
func assertValuerErrors(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	docs := orm.From("documents").On(conn)
	before := must(docs.Count(ctx))
	err := docs.Insert(ctx, map[string]any{"id": "0198c0de-0000-7000-8000-000000000001", "title": failingValuer{}})
	eq(t, conn+" insert refuses a failing value", err != nil && strings.Contains(err.Error(), "no key"), true)
	eq(t, conn+" nothing stored", must(docs.Count(ctx)), before)
	_, err = docs.Where("title", orm.Eq, "near").Update(ctx, map[string]any{"title": failingValuer{}})
	eq(t, conn+" update refuses a failing value", err != nil && strings.Contains(err.Error(), "no key"), true)
	_, err = docs.Where("title", orm.Eq, failingValuer{}).Count(ctx)
	eq(t, conn+" filter refuses a failing value", err != nil && strings.Contains(err.Error(), "no key"), true)
}

// assertDocPagination pages through a document store's documents one at a
// time and in chunks, checking every document turns up exactly once.
func assertDocPagination(t *testing.T, ctx context.Context, conn string, total int64) {
	t.Helper()
	docs := orm.From("documents").On(conn)
	seen := map[string]bool{}
	for page := 1; page <= int(total); page++ {
		p := must(docs.Paginate(ctx, 1, page))
		eq(t, "page meta", fmt.Sprint(p.Total, p.LastPage, p.From, p.To, p.HasMore),
			fmt.Sprint(total, total, page, page, int64(page) < total))
		seen[fmt.Sprint(p.Items[0]["id"])] = true
	}
	eq(t, "pages cover every document once", len(seen), int(total))
	model := must(Docs.Query().On(conn).Paginate(ctx, 2, 1))
	eq(t, "model page", fmt.Sprint(len(model.Items), model.Total, model.LastPage), fmt.Sprint(2, total, (total+1)/2))
	var chunked int
	ok(docs.Chunk(ctx, 3, func(rows []map[string]any) error { chunked += len(rows); return nil }))
	eq(t, "chunk covers every document", chunked, int(total))
	var byID int
	ok(docs.ChunkByID(ctx, 2, func(rows []map[string]any) error { byID += len(rows); return nil }))
	eq(t, "chunkByID covers every document", byID, int(total))
	var streamed int
	for _, err := range docs.Cursor(ctx) {
		ok(err)
		streamed++
	}
	eq(t, "cursor covers every document", streamed, int(total))
	seen = map[string]bool{}
	for cursor := ""; ; {
		page := must(docs.CursorPaginate(ctx, 2, cursor))
		for _, d := range page.Items {
			seen[fmt.Sprint(d["id"])] = true
		}
		if cursor = page.NextCursor; cursor == "" {
			break
		}
	}
	eq(t, "cursor pages cover every document once", len(seen), int(total))
}

func TestQdrant(t *testing.T) {
	current = t
	if os.Getenv("STORM_TEST_QDRANT") == "" {
		t.Skip("set STORM_TEST_QDRANT=http://127.0.0.1:16333 to run")
	}
	ctx := context.Background()
	store, err := orm.OpenQdrant("qdrant", os.Getenv("STORM_TEST_QDRANT"))
	if err != nil {
		t.Fatal(err)
	}
	ok(store.EnsureCollection(ctx, "documents", 3, orm.Cosine))
	ok(store.DropCollection(ctx, "documents"))
	ok(store.EnsureCollection(ctx, "documents", 3, orm.Cosine))

	for _, d := range []Doc{
		{Title: "near", Views: 1, Vec: orm.NewVector[orm.D3](1, 0, 0)},
		{Title: "far", Views: 2, Vec: orm.NewVector[orm.D3](0, 1, 0)},
		{Title: "middle", Views: 3, Vec: orm.NewVector[orm.D3](0.7, 0.7, 0)},
	} {
		ok(Docs.Query().On("qdrant").Create(ctx, &d))
	}

	eq(t, "count", must(Docs.Query().On("qdrant").Count(ctx)), int64(3))
	eq(t, "filter", must(Docs.Query().On("qdrant").Where(Docs.Views.Gte(2)).Count(ctx)), int64(2))
	eq(t, "in", must(Docs.Query().On("qdrant").Where(Docs.Views.In(1, 3)).Count(ctx)), int64(2))
	eq(t, "range", must(Docs.Query().On("qdrant").Where(Docs.Views.Between(2, 3)).Count(ctx)), int64(2))
	eq(t, "not", must(Docs.Query().On("qdrant").WhereNot(Docs.Title.Eq("near")).Count(ctx)), int64(2))
	eq(t, "or", must(Docs.Query().On("qdrant").Where(
		orm.Or(Docs.Title.Eq("near"), Docs.Title.Eq("far"))).Count(ctx)), int64(2))

	// Nearest-neighbour search: same builder, vector order.
	found := must(Docs.Query().On("qdrant").
		OrderBy(Docs.Vec.Nearest(orm.NewVector[orm.D3](1, 0, 0), orm.Cosine)).
		Limit(2).Get(ctx))
	eq(t, "knn count", len(found), 2)
	eq(t, "nearest first", found[0].Title, "near")

	n := must(Docs.Query().On("qdrant").Where(Docs.Views.Eq(3)).Update(ctx, Docs.Title.Set("renamed")))
	eq(t, "update", n, int64(1))
	eq(t, "updated", must(Docs.Query().On("qdrant").Where(Docs.Title.Eq("renamed")).Count(ctx)), int64(1))

	rows := must(orm.From("documents").On("qdrant").Where("views", "=", 2).Get(ctx))
	eq(t, "table query", len(rows), 1)
	_, err = orm.From("documents").On("qdrant").Where("views", "=", 2).Increment(ctx, "views", 1)
	eq(t, "increment refused", orm.IsNotTranslatable(err), true)

	// Upsert overwrites the matching point; the PUT is idempotent.
	kept := must(Docs.Query().On("qdrant").Where(Docs.Title.Eq("near")).First(ctx))
	kept.Views = 77
	fresh := Doc{Title: "brand new", Views: 9, Vec: orm.NewVector3(2, 0, 0)}
	ok(Docs.Query().On("qdrant").Upsert(ctx, []Doc{kept, fresh}, []orm.AnyColumn[Doc]{Docs.ID}))
	eq(t, "upsert count", must(Docs.Query().On("qdrant").Count(ctx)), int64(4))
	eq(t, "upsert replaced", must(Docs.Query().On("qdrant").Find(ctx, kept.ID)).Views, int64(77))

	// InsertOrIgnore skips points whose key is already taken.
	dup := kept
	dup.Views = 999 // must not land: the key exists
	another := Doc{Title: "ignored newcomer", Views: 5, Vec: orm.NewVector3(0, 2, 0)}
	ok(Docs.Query().On("qdrant").InsertOrIgnore(ctx, dup, another))
	eq(t, "orIgnore count", must(Docs.Query().On("qdrant").Count(ctx)), int64(5))
	assertDocPagination(t, ctx, "qdrant", 5)

	assertValuerErrors(t, ctx, "qdrant")
	must(orm.From("documents").On("qdrant").Where("title", orm.Eq, "brand new").Update(ctx, map[string]any{"title": nil}))
	cleared := must(orm.From("documents").On("qdrant").WhereNull("title").Count(ctx))
	eq(t, "nil clears a payload field", cleared, int64(1))

	_, err = orm.From("missing_collection").On("qdrant").Get(ctx)
	eq(t, "http errors name the request", err != nil && strings.Contains(err.Error(), "/collections/missing_collection/points/scroll: 404"), true)
	keyed, err := orm.OpenQdrant("qdrant_keyed", os.Getenv("STORM_TEST_QDRANT"))
	ok(err)
	keyed.WithKey("code")
	ok(keyed.EnsureCollection(ctx, "keyed", 3, orm.L2))
	ok(orm.From("keyed").On("qdrant_keyed").Insert(ctx, map[string]any{"code": 7, "name": "seven", "vec": orm.NewVector3(1, 2, 3)}))
	point := must(orm.From("keyed").On("qdrant_keyed").Where("code", orm.Eq, 7).First(ctx))
	eq(t, "withKey names the point id column", fmt.Sprintf("%v %v", point["code"], point["name"]), "7 seven")
	ok(keyed.DropCollection(ctx, "keyed"))
	assertDocMigrations(t, ctx, "qdrant")
	err = orm.TransactionOn(ctx, "qdrant", func(context.Context) error { return nil })
	eq(t, "qdrant transactions refused", orm.IsNotTranslatable(err), true)
	eq(t, "qdrant has no transactions", must(orm.SupportsTransactions(ctx, "qdrant")), false)
	ok(store.EnsureCollection(ctx, "documents", 3, orm.Cosine))
	err = store.EnsureCollection(ctx, "documents", 4, orm.Cosine)
	eq(t, "ensureCollection mismatch", err != nil && strings.Contains(err.Error(), "exists with 3-dimension Cosine"), true)
	eq(t, "orIgnore skipped existing", must(Docs.Query().On("qdrant").Find(ctx, dup.ID)).Views, int64(77))

	eq(t, "delete", must(Docs.Query().On("qdrant").Where(Docs.Title.Eq("renamed")).Delete(ctx)), int64(1))
	eq(t, "deleted", must(Docs.Query().On("qdrant").Count(ctx)), int64(4))
}

func TestPgVector(t *testing.T) {
	if driver != "pgx" {
		t.Skip("pgvector runs against the postgres container")
	}
	current = t
	ctx := setup(t)
	c, err := orm.Connection(ctx, orm.DefaultConnection)
	tCheck(t, err)
	_, err = c.DB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS vector")
	tCheck(t, err)
	tCheck(t, schema.On(orm.DefaultConnection).DropIfExists(ctx, "documents"))
	tCheck(t, schema.On(orm.DefaultConnection).Create(ctx, "documents", func(t *schema.Blueprint) {
		t.UUID("id")
		t.String("title")
		t.BigInteger("views").Default(0)
		t.Vector("vec", 3)
		t.HnswIndex("vec", orm.Cosine)
	}))
	likeHnsw := must(orm.From("pg_indexes").Where("tablename", "=", "documents").Where("indexdef", "LIKE", "%hnsw%vector_cosine_ops%").Count(ctx))
	eq(t, "hnsw index", likeHnsw, int64(1))

	for _, d := range []Doc{
		{Title: "near", Vec: orm.NewVector[orm.D3](1, 0, 0)},
		{Title: "far", Vec: orm.NewVector[orm.D3](10, 10, 10)},
	} {
		ok(Docs.Query().Create(ctx, &d))
	}
	found := must(Docs.Query().
		OrderBy(Docs.Vec.Nearest(orm.NewVector[orm.D3](1, 0, 0), orm.L2)).
		Limit(1).Get(ctx))
	eq(t, "knn", found[0].Title, "near")
	eq(t, "vectors round-trip", must(Docs.Query().Where(Docs.Title.Eq("far")).First(ctx)).Vec.String(), "[10,10,10]")
}

func tCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
