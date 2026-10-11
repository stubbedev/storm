package models_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

// Event exercises the dialect-specific functions of the analytical
// engines: JSON paths, date parts, full text and vectors.
type Event struct {
	orm.Model
	ID     int64                       `db:"id"`
	Body   string                      `db:"body"`
	Active bool                        `db:"active"`
	Tags   orm.JSON[[]string]          `db:"tags"`
	Meta   orm.JSON[map[string]string] `db:"meta"`
	At     time.Time                   `db:"at"`
	Vec    orm.Vector[orm.D3]          `db:"vec"`
}

var eventTable = &orm.Table[Event, int64]{
	Name:       "events",
	PrimaryKey: "id",
	KeyType:    orm.KeyManual,
	Columns:    []string{"id", "body", "active", "tags", "meta", "at", "vec"},
	Ptr: func(m *Event, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "body":
			return &m.Body
		case "active":
			return &m.Active
		case "tags":
			return &m.Tags
		case "meta":
			return &m.Meta
		case "at":
			return &m.At
		case "vec":
			return &m.Vec
		}
		return nil
	},
	State: func(m *Event) *orm.Model { return &m.Model },
}

var Events = struct {
	*orm.Table[Event, int64]
	ID     orm.Column[Event, int64, int64]
	Body   orm.Column[Event, int64, string]
	Active orm.Column[Event, int64, bool]
	Tags   orm.Column[Event, int64, orm.JSON[[]string]]
	Meta   orm.Column[Event, int64, orm.JSON[map[string]string]]
	At     orm.Column[Event, int64, time.Time]
	Vec    orm.Column[Event, int64, orm.Vector[orm.D3]]
}{
	Table:  eventTable,
	ID:     orm.NewColumn(eventTable, "id", func(m *Event) *int64 { return &m.ID }),
	Body:   orm.NewColumn(eventTable, "body", func(m *Event) *string { return &m.Body }),
	Active: orm.NewColumn(eventTable, "active", func(m *Event) *bool { return &m.Active }),
	Tags:   orm.NewColumn(eventTable, "tags", func(m *Event) *orm.JSON[[]string] { return &m.Tags }),
	Meta:   orm.NewColumn(eventTable, "meta", func(m *Event) *orm.JSON[map[string]string] { return &m.Meta }),
	At:     orm.NewColumn(eventTable, "at", func(m *Event) *time.Time { return &m.At }),
	Vec:    orm.NewColumn(eventTable, "vec", func(m *Event) *orm.Vector[orm.D3] { return &m.Vec }),
}

func TestSQLiteVectors(t *testing.T) {
	if driver != "" {
		t.Skip("SQLite only")
	}
	ctx := setup(t)
	tCheck(t, schema.Create(ctx, "events", func(t *schema.Blueprint) {
		t.BigInteger("id")
		t.Text("body")
		t.Boolean("active")
		t.JSON("tags")
		t.JSON("meta")
		t.DateTime("at")
		t.Vector("vec", 3)
	}))
	for i, v := range []orm.Vector[orm.D3]{orm.NewVector3(0, 5, 0), orm.NewVector3(1, 0, 0), orm.NewVector3(3, 4, 0)} {
		ok(Events.Create(ctx, &Event{ID: int64(i + 1), At: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Vec: v}))
	}
	near := must(Events.Query().OrderBy(orm.Nearest(Events.Vec, orm.NewVector3(1, 1, 0), orm.L2)).Pluck(ctx, Events.ID))
	eq(t, "sqlite l2 order", near, []int64{2, 3, 1})
	_, err := Events.Query().OrderBy(orm.Nearest(Events.Vec, orm.NewVector3(1, 1, 0), orm.Cosine)).Get(ctx)
	eq(t, "sqlite refuses other metrics", err != nil, true)

	stmt, _ := rendered(Events.Query().OrderBy(orm.Nearest(Events.Vec, orm.NewVector3(1, 1, 0), orm.Cosine)).ToSQLFor(orm.MySQL))
	eq(t, "mysql (HeatWave) distance converts the bound vector", strings.Contains(stmt, "DISTANCE(`events`.`vec`, STRING_TO_VECTOR(?), 'COSINE')"), true)
}

// JDoc holds nullable JSON documents for checking the JSON helpers read
// typed values and propagate NULL the same on every engine.
type JDoc struct {
	orm.Model
	ID   int64                     `db:"id"`
	Doc  *orm.JSON[map[string]any] `db:"doc"`
	Tags *orm.JSON[[]any]          `db:"tags"`
}

var jdocTable = &orm.Table[JDoc, int64]{
	Name:       "jdocs",
	PrimaryKey: "id",
	KeyType:    orm.KeyManual,
	Columns:    []string{"id", "doc", "tags"},
	Ptr: func(m *JDoc, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "doc":
			return &m.Doc
		case "tags":
			return &m.Tags
		}
		return nil
	},
	State: func(m *JDoc) *orm.Model { return &m.Model },
}

var JDocs = struct {
	*orm.Table[JDoc, int64]
	ID   orm.Column[JDoc, int64, int64]
	Doc  orm.Column[JDoc, int64, *orm.JSON[map[string]any]]
	Tags orm.Column[JDoc, int64, *orm.JSON[[]any]]
}{
	Table: jdocTable,
	ID:    orm.NewColumn(jdocTable, "id", func(m *JDoc) *int64 { return &m.ID }),
	Doc:   orm.NewColumn(jdocTable, "doc", func(m *JDoc) **orm.JSON[map[string]any] { return &m.Doc }),
	Tags:  orm.NewColumn(jdocTable, "tags", func(m *JDoc) **orm.JSON[[]any] { return &m.Tags }),
}

// assertJSON checks typed JSON paths compare as their Go type, arrays
// overlap by JSON equality, and a NULL document makes every JSON test
// NULL, so negations exclude it.
func assertJSON(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	tCheck(t, schema.On(conn).DropIfExists(ctx, "jdocs"))
	tCheck(t, schema.On(conn).Create(ctx, "jdocs", func(t *schema.Blueprint) {
		t.BigInteger("id").Primary()
		t.JSON("doc").Nullable()
		t.JSON("tags").Nullable()
	}))
	t.Cleanup(func() { tCheck(t, schema.On(conn).DropIfExists(context.Background(), "jdocs")) })
	ok(JDocs.Query().On(conn).Insert(ctx,
		JDoc{ID: 1, Doc: &orm.JSON[map[string]any]{V: map[string]any{"n": 7, "f": 1.5, "on": true}}, Tags: &orm.JSON[[]any]{V: []any{"a", 1}}},
		JDoc{ID: 2, Doc: &orm.JSON[map[string]any]{V: map[string]any{"n": 2, "on": false}}, Tags: &orm.JSON[[]any]{V: []any{2}}},
		JDoc{ID: 3}))
	ids := func(c orm.Cond[JDoc]) []int64 {
		return must(JDocs.Query().On(conn).Where(c).OrderBy(JDocs.ID.Asc()).Pluck(ctx, JDocs.ID))
	}
	eq(t, conn+" json int", ids(JDocs.Doc.JSON[int64]("n").Gt(5)), []int64{1})
	eq(t, conn+" json float", ids(JDocs.Doc.JSON[float64]("f").Gt(1.0)), []int64{1})
	eq(t, conn+" json bool", ids(JDocs.Doc.JSON[bool]("on").Eq(true)), []int64{1})
	eq(t, conn+" json overlaps a number", ids(JDocs.Tags.JSONOverlaps(1)), []int64{1})
	eq(t, conn+" json overlaps mixed", ids(JDocs.Tags.JSONOverlaps("a", 2)), []int64{1, 2})
	eq(t, conn+" json contains a number", ids(JDocs.Tags.JSONContains(2)), []int64{2})
	eq(t, conn+" not overlaps skips NULL", ids(orm.Not(JDocs.Tags.JSONOverlaps("a"))), []int64{2})
	eq(t, conn+" not contains skips NULL", ids(JDocs.Tags.JSONDoesntContain(2)), []int64{1})
	eq(t, conn+" not has key skips NULL", ids(orm.Not(JDocs.Doc.JSONHasKey("f"))), []int64{2})
	_, err := JDocs.Query().On(conn).Where(JDocs.Tags.JSONContains(func() {})).Get(ctx)
	eq(t, conn+" unencodable JSON refused", err != nil, true)
}

// assertEvents runs the dialect-specific expressions against a real
// engine: every helper must select exactly the expected events.
func assertEvents(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	tCheck(t, schema.On(conn).Create(ctx, "events", func(t *schema.Blueprint) {
		t.BigInteger("id").Primary()
		t.Text("body")
		t.Boolean("active")
		t.JSON("tags")
		t.JSON("meta")
		t.DateTime("at")
		t.Vector("vec", 3)
	}))
	for _, e := range []Event{
		{
			ID: 1, Body: "Storm warning", Active: true, Tags: orm.JSON[[]string]{V: []string{"go", "orm"}},
			Meta: orm.JSON[map[string]string]{V: map[string]string{"color": "red"}},
			At:   time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC), Vec: orm.NewVector3(1, 0, 0),
		},
		{
			ID: 2, Body: "calm day", Tags: orm.JSON[[]string]{V: []string{}},
			Meta: orm.JSON[map[string]string]{V: map[string]string{}},
			At:   time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC), Vec: orm.NewVector3(0, 1, 0),
		},
	} {
		ok(Events.Query().On(conn).Create(ctx, &e))
	}
	ids := func(conds ...orm.Cond[Event]) []int64 {
		return must(Events.Query().On(conn).Where(conds...).OrderBy(Events.ID.Asc()).Pluck(ctx, Events.ID))
	}

	eq(t, conn+" year", ids(orm.Year(Events.At).Eq(2026)), []int64{1})
	eq(t, conn+" month", ids(orm.Month(Events.At).Eq(12)), []int64{2})
	eq(t, conn+" day", ids(orm.Day(Events.At).Eq(4)), []int64{1})
	eq(t, conn+" date", ids(orm.Date(Events.At).Eq("2026-03-04")), []int64{1})
	eq(t, conn+" time", ids(orm.Time(Events.At).Eq("05:06:07")), []int64{1})

	eq(t, conn+" json path", ids(Events.Meta.JSON[string]("color").Eq("red")), []int64{1})
	eq(t, conn+" json contains", ids(Events.Tags.JSONContains("go")), []int64{1})
	eq(t, conn+" json doesn't contain", ids(Events.Tags.JSONDoesntContain("go")), []int64{2})
	eq(t, conn+" json overlaps", ids(Events.Tags.JSONOverlaps("x", "orm")), []int64{1})
	eq(t, conn+" json length", ids(Events.Tags.JSONLength().Eq(2)), []int64{1})
	eq(t, conn+" json has key", ids(Events.Meta.JSONHasKey("color")), []int64{1})
	eq(t, conn+" full text", ids(orm.FullTextMatch("STORM", Events.Body)), []int64{1})

	for _, m := range []orm.VectorMetric{orm.L2, orm.Cosine, orm.InnerProduct} {
		nearest := must(Events.Query().On(conn).OrderBy(orm.Nearest(Events.Vec, orm.NewVector3(0.9, 0.1, 0), m)).Pluck(ctx, Events.ID))
		eq(t, fmt.Sprintf("%s nearest by metric %d", conn, m), nearest, []int64{1, 2})
	}
	eq(t, conn+" random order", len(must(Events.Query().On(conn).InRandomOrder().Get(ctx))), 2)

	raw := must(Events.Query().On(conn).Where(Events.Active.Eq(true), Events.Body.Contains("warn")).ToRawSQL())
	c := must(orm.Connection(ctx, conn))
	rows := must(c.DB.QueryContext(ctx, raw))
	matched := 0
	for rows.Next() {
		matched++
	}
	ok(rows.Close())
	eq(t, conn+" raw sql runs as rendered", matched, 1)
	eq(t, conn+" raw sql inlines the values", strings.Contains(raw, "'%warn%'"), true)

	ok(Events.Query().On(conn).Truncate(ctx))
	eq(t, conn+" truncate", must(Events.Query().On(conn).Count(ctx)), int64(0))
}

func TestMySQLFlavour(t *testing.T) {
	if driver != "mysql" {
		t.Skip("MySQL and MariaDB only")
	}
	ctx := setup(t)
	c := must(orm.Connection(ctx, orm.DefaultConnection))
	var version string
	rows := must(c.DB.QueryContext(ctx, "SELECT VERSION()"))
	for rows.Next() {
		ok(rows.Scan(&version))
	}
	ok(rows.Close())
	want := map[bool]string{true: "mariadb", false: "mysql"}[strings.Contains(strings.ToLower(version), "mariadb")]
	eq(t, "open detects the flavour", c.Dialect.Name(), want)
	explicit, known := orm.DialectFor("mariadb")
	eq(t, "mariadb driver name", known && explicit == orm.MariaDB, true)

	stmt, _ := rendered(Events.Query().OrderBy(orm.Nearest(Events.Vec, orm.NewVector3(1, 1, 0), orm.Cosine)).ToSQLFor(orm.MariaDB))
	eq(t, "mariadb vector distance", strings.Contains(stmt, "VEC_DISTANCE_COSINE(`events`.`vec`, VEC_FromText(?))"), true)
	if want != "mariadb" {
		return
	}
	tCheck(t, schema.Create(ctx, "documents", func(t *schema.Blueprint) {
		t.UUIDPrimary()
		t.String("title")
		t.BigInteger("views")
		t.Vector("vec", 3)
		t.HnswIndex("vec", orm.L2)
	}))
	for _, d := range []Doc{
		{Title: "near", Vec: orm.NewVector3(1, 0, 0)},
		{Title: "far", Vec: orm.NewVector3(10, 10, 10)},
		{Title: "middle", Vec: orm.NewVector3(5, 5, 5)},
	} {
		ok(Docs.Query().Create(ctx, &d))
	}
	near := must(Docs.Query().Where(Docs.Title.Eq("near")).First(ctx))
	eq(t, "mariadb vectors read back", near.Vec.String(), "[1,0,0]")
	for _, m := range []orm.VectorMetric{orm.L2, orm.Cosine} {
		titles := must(Docs.Query().OrderBy(orm.Nearest(Docs.Vec, orm.NewVector3(0.9, 0.1, 0), m)).Pluck(ctx, Docs.Title))
		eq(t, fmt.Sprintf("mariadb nearest by metric %d", m), titles[0], "near")
	}
	_, err := Docs.Query().OrderBy(orm.Nearest(Docs.Vec, orm.NewVector3(1, 0, 0), orm.InnerProduct)).Get(ctx)
	eq(t, "mariadb has no inner product", err != nil, true)
	err = schema.Table(ctx, "documents", func(t *schema.Blueprint) { t.HnswIndex("vec", orm.InnerProduct) })
	_, refused := errors.AsType[*schema.UnsupportedError](err)
	eq(t, "inner product index refused", refused, true)
}

func TestDialectCapabilities(t *testing.T) {
	for _, d := range []struct {
		dialect             orm.Dialect
		name                string
		returning, autoKeys bool
	}{
		{orm.SQLite, "sqlite", true, true},
		{orm.Postgres, "postgres", true, true},
		{orm.MySQL, "mysql", false, true},
		{orm.MariaDB, "mariadb", true, true},
		{orm.DuckDB, "duckdb", true, true},
		{orm.ClickHouse, "clickhouse", false, false},
	} {
		eq(t, d.name+" capabilities", fmt.Sprint(d.dialect.Name(), d.dialect.Returning(), d.dialect.AutoKeys()), fmt.Sprint(d.name, d.returning, d.autoKeys))
	}
}
