package models_test

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
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

// Note is a soft-deleting document model with a global scope hiding
// drafts, so document stores are checked against the same implicit
// conditions SQL queries carry.
type Note struct {
	orm.Model
	ID        string     `db:"id"`
	Title     string     `db:"title"`
	Views     int64      `db:"views"`
	Seen      *time.Time `db:"seen"`
	DeletedAt *time.Time `db:"deleted_at"`
}

var noteTable = &orm.Table[Note]{
	Name:       "notes",
	PrimaryKey: "id",
	KeyType:    orm.KeyUUID,
	DeletedAt:  "deleted_at",
	Columns:    []string{"id", "title", "views", "seen", "deleted_at"},
	Ptr: func(m *Note, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "title":
			return &m.Title
		case "views":
			return &m.Views
		case "seen":
			return &m.Seen
		case "deleted_at":
			return &m.DeletedAt
		}
		return nil
	},
	State: func(m *Note) *orm.Model { return &m.Model },
}

type NoteSchema struct {
	*orm.Table[Note]
	ID    orm.Column[Note, string]
	Title orm.Column[Note, string]
	Views orm.Column[Note, int64]
	Seen  orm.Column[Note, *time.Time]
}

var Notes = func() NoteSchema {
	s := NoteSchema{
		Table: noteTable,
		ID:    orm.NewColumn(noteTable, "id", func(m *Note) *string { return &m.ID }),
		Title: orm.NewColumn(noteTable, "title", func(m *Note) *string { return &m.Title }),
		Views: orm.NewColumn(noteTable, "views", func(m *Note) *int64 { return &m.Views }),
		Seen:  orm.NewColumn(noteTable, "seen", func(m *Note) **time.Time { return &m.Seen }),
	}
	noteTable.AddGlobalScope("published", func(q orm.Query[Note]) orm.Query[Note] { return q.Where(s.Title.Ne("draft")) })
	return s
}()

// assertDocLogic checks a document store evaluates nested AND / OR / NOT
// and null (missing) fields as SQL's three-valued logic does: a negative
// condition never matches a document lacking the field.
func assertDocLogic(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	rows := orm.From("notes").On(conn)
	ok(rows.Insert(ctx,
		map[string]any{"id": "00000000-0000-4000-8000-000000000001", "title": "x", "views": 1},
		map[string]any{"id": "00000000-0000-4000-8000-000000000002", "views": 2},
		map[string]any{"id": "00000000-0000-4000-8000-000000000003", "title": "y", "views": 3},
		map[string]any{"id": "00000000-0000-4000-8000-000000000004", "title": "x", "views": 4}))
	notes := Notes.Query().On(conn).WithoutGlobalScopes()
	for what, c := range map[string]struct {
		cond orm.Cond[Note]
		want int64
	}{
		"ne skips missing":           {Notes.Title.Ne("x"), 1},
		"not in skips missing":       {Notes.Title.NotIn("x"), 1},
		"not eq skips missing":       {orm.Not(Notes.Title.Eq("x")), 1},
		"not between":                {orm.Not(Notes.Views.Between(2, 3)), 2},
		"or of a negation":           {orm.Or(Notes.Title.Ne("x"), Notes.Views.Eq(2)), 2},
		"or of an and":               {orm.Or(orm.And(Notes.Views.Gt(1), Notes.Title.Eq("x")), Notes.Views.Eq(1)), 2},
		"and of ors":                 {orm.And(orm.Or(Notes.Views.Eq(1), Notes.Views.Eq(2)), orm.Or(Notes.Views.Eq(2), Notes.Views.Eq(3))), 1},
		"not of an or with a null":   {orm.Not(orm.Or(Notes.Views.Eq(1), Notes.Title.Eq("x"))), 1},
		"not of a not":               {orm.Not(orm.Not(Notes.Title.Eq("x"))), 2},
		"not of an and":              {orm.Not(orm.And(Notes.Views.Gt(1), Notes.Title.Eq("x"))), 2},
		"empty in matches nothing":   {Notes.Title.In(), 0},
		"empty not in matches every": {Notes.Title.NotIn(), 4},
	} {
		eq(t, conn+" "+what, must(notes.Where(c.cond).Count(ctx)), c.want)
	}
	eq(t, conn+" row and then or", must(rows.Where("views", orm.Gt, 1).Where("title", orm.Eq, "x").OrWhere("views", orm.Eq, 1).Count(ctx)), int64(2))
	eq(t, conn+" row compare with nil matches nothing", must(rows.Where("title", orm.Eq, nil).Count(ctx)), int64(0))
	eq(t, conn+" row not in skips missing", must(rows.WhereNotIn("title", "y").Count(ctx)), int64(2))
	eq(t, conn+" row null", must(rows.WhereNull("title").Count(ctx)), int64(1))
	eq(t, conn+" qualified where", must(rows.Where("notes.views", orm.Eq, 1).Count(ctx)), int64(1))
	eq(t, conn+" qualified order and pluck", fmt.Sprint(must(rows.OrderBy("notes.views").Limit(1).Pluck(ctx, "notes.title"))), "[x]")
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*3600))
	eq(t, conn+" set an offset time", must(notes.Where(Notes.Views.Eq(1)).Update(ctx, Notes.Seen.Set(&at))), int64(1))
	utc := at.UTC()
	eq(t, conn+" offset time equals its UTC form", must(notes.Where(Notes.Seen.Eq(&utc)).Count(ctx)), int64(1))
	eq(t, conn+" offset time reads back as the same instant", must(notes.Where(Notes.Views.Eq(1)).First(ctx)).Seen.Equal(at), true)
	const big = int64(1)<<53 + 1
	eq(t, conn+" set a big integer", must(notes.Where(Notes.Views.Eq(2)).Update(ctx, Notes.Views.Set(big))), int64(1))
	eq(t, conn+" big integers read back exactly", must(notes.Where(Notes.Views.Gt(big-1)).Pluck(ctx, Notes.Views)), []int64{big})
	eq(t, conn+" big integers in row maps", fmt.Sprint(must(rows.Where("views", orm.Gt, big-1).Pluck(ctx, "views"))), fmt.Sprint([]int64{big}))
	_, err := notes.Limit(0).Get(ctx)
	eq(t, conn+" a zero limit is refused", err != nil && strings.Contains(err.Error(), "Limit(0)"), true)
	eq(t, conn+" logic cleanup", must(rows.Delete(ctx)), int64(4))
}

// assertDocKeys checks a document store keeps keys unique, as a primary
// key does in SQL, and that Upsert overwrites only its update columns.
func assertDocKeys(t *testing.T, ctx context.Context, conn string, upsertByOtherColumns bool) {
	t.Helper()
	notes := Notes.Query().On(conn).WithoutGlobalScopes()
	first := Note{ID: "00000000-0000-4000-8000-0000000000a1", Title: "first", Views: 1}
	ok(notes.Create(ctx, &first))
	again := Note{ID: first.ID, Title: "second", Views: 2}
	err := notes.Create(ctx, &again)
	eq(t, conn+" a taken key is a unique violation", orm.IsUniqueViolation(err), true)
	eq(t, conn+" a taken key keeps the first", must(notes.Find(ctx, first.ID)).Title, "first")
	found := must(notes.CreateOrFirst(ctx, orm.Attrs(Notes.ID.Set(first.ID)), Notes.Title.Set("third")))
	eq(t, conn+" createOrFirst finds the taken key", found.Title, "first")

	fresh := Note{ID: "00000000-0000-4000-8000-0000000000a2", Title: "fresh", Views: 5}
	changed := Note{ID: first.ID, Title: "upserted", Views: 99}
	ok(notes.Upsert(ctx, []Note{changed, fresh}, []orm.AnyColumn[Note]{Notes.ID}, Notes.Title))
	kept := must(notes.Find(ctx, first.ID))
	eq(t, conn+" upsert sets only the update columns", fmt.Sprintf("%s %d", kept.Title, kept.Views), "upserted 1")
	eq(t, conn+" upsert inserts the new key", must(notes.Find(ctx, fresh.ID)).Views, int64(5))

	rows := orm.From("notes").On(conn)
	err = rows.Upsert(ctx, []map[string]any{{"id": "00000000-0000-4000-8000-0000000000a3", "title": "fresh", "views": 7}}, []string{"title"}, "views")
	if upsertByOtherColumns {
		ok(err)
		eq(t, conn+" upsert by another column keeps the key", must(notes.Find(ctx, fresh.ID)).Views, int64(7))
		eq(t, conn+" upsert by another column inserts nothing", must(notes.Count(ctx)), int64(2))
	} else {
		eq(t, conn+" upsert by another column refused", orm.IsNotTranslatable(err), true)
	}
	row := must(rows.Find(ctx, first.ID))
	_, hasID := row["_id"]
	eq(t, conn+" rows carry no store id", hasID, false)
	eq(t, conn+" keys cleanup", must(rows.Delete(ctx)), int64(2))
}

// assertDocModelQueries runs the model builder's implicit conditions (soft
// deletes, global scopes, key lookups) and its key walks against a
// document store.
func assertDocModelQueries(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	notes := Notes.Query().On(conn)
	for _, n := range []Note{{Title: "a", Views: 1}, {Title: "b", Views: 2}, {Title: "c", Views: 3}, {Title: "draft", Views: 4}} {
		ok(notes.Create(ctx, &n))
	}
	eq(t, conn+" global scope", must(notes.Count(ctx)), int64(3))
	eq(t, conn+" without global scope", must(notes.WithoutGlobalScope("published").Count(ctx)), int64(4))
	eq(t, conn+" scoped exists", must(notes.Where(Notes.Views.Eq(4)).Exists(ctx)), false)
	eq(t, conn+" soft delete", must(notes.Where(Notes.Title.Eq("a")).Delete(ctx)), int64(1))
	eq(t, conn+" soft deleted hidden", must(notes.OrderBy(Notes.Views.Asc()).Pluck(ctx, Notes.Title)), []string{"b", "c"})
	eq(t, conn+" with trashed", must(notes.WithTrashed().Count(ctx)), int64(3))
	eq(t, conn+" only trashed", must(notes.OnlyTrashed().Pluck(ctx, Notes.Title)), []string{"a"})
	eq(t, conn+" pluck map", must(notes.OrderBy(Notes.Views.Asc()).PluckMap(ctx, Notes.Title, Notes.Views)), map[string]int64{"b": 2, "c": 3})
	eq(t, conn+" value", must(notes.Where(Notes.Title.Eq("c")).Value(ctx, Notes.Views)), int64(3))
	_, err := notes.Pluck(ctx, orm.Upper(Notes.Title))
	eq(t, conn+" pluck of an expression refused", orm.IsNotTranslatable(err), true)
	eq(t, conn+" restore", must(notes.OnlyTrashed().Restore(ctx)), int64(1))
	eq(t, conn+" restored", must(notes.Count(ctx)), int64(3))

	ids := must(notes.Pluck(ctx, Notes.ID))
	eq(t, conn+" where key", must(notes.WhereKey(ids[0], ids[1]).Count(ctx)), int64(2))
	eq(t, conn+" where no keys", must(notes.WhereKey().Count(ctx)), int64(0))
	eq(t, conn+" where key not none", must(notes.WhereKeyNot().Count(ctx)), int64(3))
	var walked []string
	ok(notes.ChunkByID(ctx, 2, func(ns []Note) error {
		for _, n := range ns {
			walked = append(walked, n.ID)
		}
		return nil
	}))
	sort.Strings(ids)
	eq(t, conn+" chunkByID walks keys in order", walked, ids)
	var lazy int
	for _, err := range notes.Lazy(ctx, 2) {
		ok(err)
		lazy++
	}
	eq(t, conn+" lazy", lazy, 3)
	eq(t, conn+" limited update", must(notes.OrderBy(Notes.Views.Asc()).Offset(2).Limit(1).Update(ctx, Notes.Title.Set("top"))), int64(1))
	eq(t, conn+" limited update hit the top", must(notes.OrderBy(Notes.Views.Asc()).Pluck(ctx, Notes.Title)), []string{"a", "b", "top"})
	eq(t, conn+" offset delete", must(notes.OrderBy(Notes.Views.Asc()).Offset(1).Limit(1).ForceDelete(ctx)), int64(1))
	eq(t, conn+" offset delete hit the second", must(notes.OrderBy(Notes.Views.Asc()).Pluck(ctx, Notes.Title)), []string{"a", "top"})
	eq(t, conn+" unconditional update", must(notes.WithoutGlobalScopes().Update(ctx, Notes.Views.Set(0))), int64(3))
	eq(t, conn+" force delete", must(notes.WithoutGlobalScopes().WithTrashed().ForceDelete(ctx)), int64(3))
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

// assertBigNumbers checks integers past 2^53 (and, where the engine has
// them, unsigned ones past 2^63) survive scanning and aggregates exactly.
func assertBigNumbers(t *testing.T, ctx context.Context, conn string, unsigned bool) {
	t.Helper()
	tCheck(t, schema.On(conn).DropIfExists(ctx, "big_numbers"))
	tCheck(t, schema.On(conn).Create(ctx, "big_numbers", func(t *schema.Blueprint) {
		t.BigInteger("id").Primary()
		t.BigInteger("n")
		if unsigned {
			t.UnsignedBigInteger("u").Default(0)
		}
	}))
	defer func() { tCheck(t, schema.On(conn).DropIfExists(ctx, "big_numbers")) }()
	const big = int64(1)<<53 + 1
	rows := orm.From("big_numbers").On(conn)
	ok(rows.Insert(ctx, map[string]any{"id": 1, "n": big}, map[string]any{"id": 2, "n": int64(2)}))
	eq(t, conn+" big max", must(rows.Max[int64](ctx, "n")), big)
	eq(t, conn+" big sum", must(rows.Sum[int64](ctx, "n")), big+2)
	_, err := rows.Sum[int8](ctx, "n")
	eq(t, conn+" overflow is an error", err != nil && strings.Contains(err.Error(), "overflows"), true)
	if unsigned {
		must(rows.Where("id", orm.Eq, 1).Update(ctx, map[string]any{"u": uint64(math.MaxUint64 - 1)}))
		eq(t, conn+" big unsigned", must(rows.Max[uint64](ctx, "u")), uint64(math.MaxUint64-1))
	}
}

// assertTransactionModes checks DuckDB's transactions nest by joining the
// outer one (it has no savepoints) and ClickHouse refuses transactions
// instead of pretending to roll back.
func assertTransactionModes(t *testing.T, ctx context.Context, conn string, transactional bool) {
	t.Helper()
	tCheck(t, schema.On(conn).DropIfExists(ctx, "tx_rows"))
	tCheck(t, schema.On(conn).Create(ctx, "tx_rows", func(t *schema.Blueprint) { t.BigInteger("id").Primary() }))
	defer func() { tCheck(t, schema.On(conn).DropIfExists(ctx, "tx_rows")) }()
	rows := orm.From("tx_rows").On(conn)
	eq(t, conn+" supports transactions", must(orm.SupportsTransactions(ctx, conn)), transactional)
	err := orm.TransactionOn(ctx, conn, func(ctx context.Context) error {
		ok(rows.Insert(ctx, map[string]any{"id": 1}))
		return errors.New("roll back")
	})
	if !transactional {
		eq(t, conn+" transaction refused", orm.IsNotTranslatable(err), true)
		eq(t, conn+" refused transaction never ran", must(rows.Count(ctx)), int64(0))
		return
	}
	eq(t, conn+" rolled back", err != nil && must(rows.Count(ctx)) == 0, true)
	ok(orm.TransactionOn(ctx, conn, func(ctx context.Context) error {
		ok(rows.Insert(ctx, map[string]any{"id": 1}))
		return orm.TransactionOn(ctx, conn, func(ctx context.Context) error { return rows.Insert(ctx, map[string]any{"id": 2}) })
	}))
	eq(t, conn+" nested commit", must(rows.Count(ctx)), int64(2))
	err = orm.TransactionOn(ctx, conn, func(ctx context.Context) error {
		ok(rows.Insert(ctx, map[string]any{"id": 3}))
		_ = orm.TransactionOn(ctx, conn, func(context.Context) error { return errors.New("inner fails") })
		return nil
	})
	eq(t, conn+" failed nested transaction rolls back the whole", errors.Is(err, orm.ErrNestedFailed) && must(rows.Count(ctx)) == 2, true)
}

// scratchWidgets creates a fresh widgets table holding names (ids 1.., karma
// equal to the id, kind "plain"), dropped when the test ends.
func scratchWidgets(t *testing.T, ctx context.Context, conn string, names ...string) orm.RowQuery {
	t.Helper()
	tCheck(t, schema.On(conn).DropIfExists(ctx, "widgets"))
	tCheck(t, schema.On(conn).Create(ctx, "widgets", func(t *schema.Blueprint) {
		t.BigInteger("id").Primary()
		t.String("name")
		t.String("kind")
		t.BigInteger("karma").Default(0)
		t.Timestamps()
	}))
	t.Cleanup(func() { tCheck(t, schema.On(conn).DropIfExists(context.Background(), "widgets")) })
	rows := orm.From("widgets").On(conn)
	for i, name := range names {
		ok(rows.Insert(ctx, map[string]any{"id": i + 1, "name": name, "kind": "plain", "karma": i + 1}))
	}
	return rows
}

// assertLikes checks LIKE means the same on every engine: the pattern's
// wildcards and escapes, case sensitivity, and matches across newlines.
func assertLikes(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	scratchWidgets(t, ctx, conn, "100%", "1000", "a_b", "axb", "Alpha", "alpha", "line\nbreak", "star*")
	widgets := Widgets.Query().On(conn)
	count := func(c orm.Cond[Widget]) int64 { return must(widgets.Where(c).Count(ctx)) }
	eq(t, conn+" contains escapes %", count(Widgets.Name.Contains("100%")), int64(1))
	eq(t, conn+" like wildcards", count(Widgets.Name.Like("a_b")), int64(2))
	eq(t, conn+" like escapes", count(Widgets.Name.Like(`a\_b`)), int64(1))
	eq(t, conn+" like is case-sensitive", count(Widgets.Name.StartsWith("Al")), int64(1))
	eq(t, conn+" ilike ignores case", count(Widgets.Name.ILike("al%")), int64(2))
	eq(t, conn+" not ilike", count(Widgets.Name.NotILike("AL%")), int64(6))
	eq(t, conn+" like crosses newlines", count(Widgets.Name.Contains("break")), int64(1))
	eq(t, conn+" not like", count(Widgets.Name.NotLike("%a%")), int64(2))
	eq(t, conn+" glob metacharacters are text", count(Widgets.Name.EndsWith("*")), int64(1))
	eq(t, conn+" row like", must(orm.From("widgets").On(conn).Where("name", orm.Like, "1%").Count(ctx)), int64(2))
	eq(t, conn+" row ilike", must(orm.From("widgets").On(conn).Where("name", orm.ILike, "ALPHA").Count(ctx)), int64(2))
}

// assertInjectionSafety feeds hostile names and values through every
// builder entry point: identifiers are quoted, values bound, inlined
// literals escaped, and schema names refused, so none can change the
// statement.
func assertInjectionSafety(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	rows := scratchWidgets(t, ctx, conn, "a", "b")
	hostile := []string{`' OR '1'='1`, `x\' OR 1=1 -- `, `"; DROP TABLE widgets; --`, "` OR 1=1 -- ", `a') OR ('1'='1`}
	for _, h := range hostile {
		eq(t, conn+" hostile value bound: "+h, must(rows.Where("name", orm.Eq, h).Count(ctx)), int64(0))
		eq(t, conn+" hostile typed value bound: "+h, must(Widgets.Query().On(conn).Where(Widgets.Name.Eq(h)).Count(ctx)), int64(0))
		eq(t, conn+" hostile pattern bound: "+h, must(Widgets.Query().On(conn).Where(Widgets.Name.Contains(h)).Count(ctx)), int64(0))
		matched, err := Widgets.Query().On(conn).Where(Widgets.Name.JSON[string](h).Eq("x")).Get(ctx)
		eq(t, conn+" hostile JSON path bound: "+h, err != nil || len(matched) == 0, true)
		n, err := rows.Where(h, orm.Eq, "a").Count(ctx)
		eq(t, conn+" hostile column quoted: "+h, err != nil || n == 0, true)
		ordered, err := rows.OrderBy(h).Get(ctx)
		eq(t, conn+" hostile order quoted: "+h, err != nil || len(ordered) == 2, true)
		_, err = orm.From(h).On(conn).Count(ctx)
		eq(t, conn+" hostile table quoted: "+h, err != nil, true)
		selected, err := rows.Select(h).Get(ctx)
		eq(t, conn+" hostile select quoted: "+h, err != nil || len(selected) == 2, true)
		raw := must(Widgets.Query().On(conn).Where(Widgets.Name.Eq(h)).ToRawSQL())
		inlined := must(orm.SelectOn(ctx, conn, raw))
		eq(t, conn+" hostile literal escaped: "+h, len(inlined), 0)
		err = schema.On(conn).Create(ctx, "hostile_names", func(t *schema.Blueprint) {
			t.ID()
			t.String("name").Collation(h)
		})
		_, refused := errors.AsType[*schema.UnsupportedError](err)
		eq(t, conn+" hostile collation refused: "+h, refused, true)
	}
	eq(t, conn+" widgets survive", must(rows.Count(ctx)), int64(2))
}

// assertLimitedMutations checks Limit, Offset and OrderBy narrow updates
// and deletes to the rows a SELECT would pick, through both builders.
func assertLimitedMutations(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	rows := scratchWidgets(t, ctx, conn, "a", "b", "c", "d", "e")
	eq(t, conn+" offset without limit", len(must(rows.OrderBy("name").Offset(3).Get(ctx))), 2)
	eq(t, conn+" qualified pluck", fmt.Sprint(must(rows.OrderBy("widgets.name").Pluck(ctx, "widgets.name"))), "[a b c d e]")
	widgetQuery := Widgets.Query().On(conn)
	taken := orm.Attrs(Widgets.ID.Set(int64(1)))
	eq(t, conn+" createOrFirst returns the taken key", must(widgetQuery.CreateOrFirst(ctx, taken, Widgets.Name.Set("dup"))).Name, "a")
	if must(orm.SupportsTransactions(ctx, conn)) {
		ok(orm.TransactionOn(ctx, conn, func(ctx context.Context) error {
			found, err := widgetQuery.CreateOrFirst(ctx, taken, Widgets.Name.Set("dup"))
			eq(t, conn+" createOrFirst in a transaction", found.Name, "a")
			return err
		}))
	}
	eq(t, conn+" createOrFirst inserted nothing", must(rows.Count(ctx)), int64(5))
	byName := func() string {
		return fmt.Sprint(must(rows.OrderBy("name").Pluck(ctx, "kind")))
	}
	widgets := Widgets.Query().On(conn)
	eq(t, conn+" limited update", must(widgets.OrderBy(Widgets.Karma.Desc()).Limit(2).Update(ctx, Widgets.Kind.Set("top"))), int64(2))
	eq(t, conn+" limited update hit the top two", byName(), "[plain plain plain top top]")
	eq(t, conn+" offset update", must(widgets.OrderBy(Widgets.Karma.Asc()).Limit(1).Offset(1).Update(ctx, Widgets.Kind.Set("second"))), int64(1))
	eq(t, conn+" offset update hit the second", byName(), "[plain second plain top top]")
	eq(t, conn+" row limited update", must(rows.OrderBy("karma").Limit(1).Update(ctx, map[string]any{"kind": "first"})), int64(1))
	eq(t, conn+" row limited update hit the first", byName(), "[first second plain top top]")
	eq(t, conn+" an update that changes nothing still matched", must(rows.Where("id", orm.Eq, 3).Update(ctx, map[string]any{"kind": "plain"})), int64(1))
	eq(t, conn+" limited delete", must(widgets.OrderBy(Widgets.Karma.Desc()).Limit(1).Delete(ctx)), int64(1))
	eq(t, conn+" row limited delete", must(rows.OrderBy("karma").Limit(2).Delete(ctx)), int64(2))
	eq(t, conn+" limited deletes left the middle", fmt.Sprint(must(rows.OrderBy("name").Pluck(ctx, "name"))), "[c d]")
	eq(t, conn+" unconditional update", must(rows.Update(ctx, map[string]any{"kind": "all"})), int64(2))
	eq(t, conn+" unconditional delete", must(widgets.Delete(ctx)), int64(2))
}

func assertWidgets(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	ws := must(Widgets.Query().On(conn).Where(Widgets.Kind.Eq("gear")).OrderBy(Widgets.Karma.Desc()).Get(ctx))
	eq(t, "where + order", len(ws), 2)
	eq(t, "order desc", ws[0].Name, "gamma")
	n := must(Widgets.Query().On(conn).Count(ctx))
	eq(t, "count", n, int64(3))
	first := must(Widgets.Query().On(conn).Where(Widgets.Name.StartsWith("alp")).First(ctx))
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

	assertLimitedMutations(t, ctx, "duck")
	assertLikes(t, ctx, "duck")
	assertInjectionSafety(t, ctx, "duck")
	assertTransactionModes(t, ctx, "duck", true)
	assertBigNumbers(t, ctx, "duck", true)
	createWidgetTable(ctx, t, "duck", func(t *schema.Blueprint) { t.ID() })
	for i := range seedWidgets(t) {
		w := seedWidgets(t)[i]
		ok(Widgets.Query().On("duck").Create(ctx, &w))
	}
	assertWidgets(t, ctx, "duck")
	assertEvents(t, ctx, "duck")
	assertJSON(t, ctx, "duck")
	assertBlueprintSurface(t, ctx, "duck")
	assertForeignKeys(t, ctx, "duck")
	assertDropHelpers(t, ctx, "duck")
	assertTimePrecision(t, ctx, "duck")
	assertIndexForms(t, ctx, "duck")
	assertViews(t, ctx, "duck")
	assertDatabases(t, ctx, "duck")

	// Flat files: DuckDB reads CSV straight from the builder.
	csv := t.TempDir() + "/flat.csv"
	ok(os.WriteFile(csv, []byte("id,name,kind,karma\n1,alpha,gear,5\n2,beta,spring,15\n3,gamma,gear,25\n"), 0o644))
	rows := must(orm.FromRaw("read_csv(?)", csv).On("duck").Where("kind", "=", "gear").Count(ctx))
	_, err = orm.FromRaw("read_csv(?)", csv).On("duck").Delete(ctx)
	eq(t, "raw table sources are read-only", err != nil && strings.Contains(err.Error(), "read-only"), true)
	_, err = orm.From("x'); DROP TABLE widgets; --").On("duck").Count(ctx)
	eq(t, "from quotes its name", err != nil && must(Widgets.Query().On("duck").Count(ctx)) > 0, true)
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
		OrderBy(orm.Nearest(Docs.Vec, orm.NewVector[orm.D3](1, 0, 0), orm.L2)).
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

	assertLimitedMutations(t, ctx, "ch")
	assertLikes(t, ctx, "ch")
	assertInjectionSafety(t, ctx, "ch")
	assertTransactionModes(t, ctx, "ch", false)
	assertBigNumbers(t, ctx, "ch", true)
	createWidgetTable(ctx, t, "ch", func(t *schema.Blueprint) { t.BigInteger("id").Primary() })
	for i, w := range seedWidgets(t) {
		w.ID = int64(i + 1) // ClickHouse does not generate integer keys
		ok(Widgets.Query().On("ch").Create(ctx, &w))
	}
	chWidgets := orm.From("widgets").On("ch")
	eq(t, "union", len(must(chWidgets.Where("kind", orm.Eq, "gear").Union(chWidgets.Where("kind", orm.Eq, "gear")).Get(ctx))), 2)
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
	assertJSON(t, ctx, "ch")
	assertBlueprintSurface(t, ctx, "ch")
	assertForeignKeys(t, ctx, "ch")
	assertDropHelpers(t, ctx, "ch")
	assertTimePrecision(t, ctx, "ch")
	assertIndexForms(t, ctx, "ch")
	assertViews(t, ctx, "ch")
	assertDatabases(t, ctx, "ch")
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
	eq(t, "model pluck", must(Docs.Query().On("mongo").Where(Docs.Views.Between(2, 3)).OrderBy(Docs.Views.Asc()).Pluck(ctx, Docs.Title)), []string{"far", "renamed"})
	_, err = Docs.Query().On("mongo").Explain(ctx)
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
	assertDocAggregates(t, ctx, "mongo")
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

	eq(t, "like", must(Docs.Query().On("mongo").Where(Docs.Title.Contains("ea")).Count(ctx)), int64(1))
	eq(t, "like escapes regex syntax", must(Docs.Query().On("mongo").Where(Docs.Title.Contains("(n")).Count(ctx)), int64(0))
	eq(t, "like with wildcards", must(orm.From("documents").On("mongo").Where("title", orm.Like, "n_ar").Count(ctx)), int64(1))
	eq(t, "like escapes wildcards", must(orm.From("documents").On("mongo").Where("title", orm.Like, `n\_ar`).Count(ctx)), int64(0))

	assertValuerErrors(t, ctx, "mongo")
	_, err = orm.SelectOn(ctx, "mongo", "SELECT 1")
	eq(t, "raw select refused", orm.IsNotTranslatable(err), true)
	_, err = orm.StatementOn(ctx, "mongo", "DELETE FROM documents")
	eq(t, "raw statement refused", orm.IsNotTranslatable(err), true)
	_, refusal := errors.AsType[*schema.UnsupportedError](schema.On("mongo").CreateView(ctx, "v", "SELECT 1"))
	eq(t, "views refused", refusal, true)
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
	eq(t, "ilike", must(orm.From("documents").On("mongo").Where("title", orm.ILike, "NEAR").Count(ctx)), int64(1))
	eq(t, "not ilike", must(orm.From("documents").On("mongo").Where("title", orm.NotILike, "%E%").Count(ctx)), int64(1))
	eq(t, "unsigned decrement", must(orm.From("documents").On("mongo").Where("title", orm.Eq, "near").Decrement(ctx, "views", uint8(1))), int64(1))

	assertDocMigrations(t, ctx, "mongo")
	assertDocLogic(t, ctx, "mongo")
	assertDocKeys(t, ctx, "mongo", true)
	blobs := orm.From("blobs").On("mongo")
	ok(blobs.Insert(ctx, map[string]any{"id": "b1", "blob": []byte{0, 255, 'a'}, "meta": map[string]any{"a": map[string]any{"b": 1}}, "tags": []any{"x", "y"}}))
	blob := must(blobs.Find(ctx, "b1"))
	eq(t, "binary round-trips", fmt.Sprintf("%x", blob["blob"]), "00ff61")
	meta, isMap := blob["meta"].(map[string]any)
	_, nestedMap := meta["a"].(map[string]any)
	_, isSlice := blob["tags"].([]any)
	eq(t, "nested documents and arrays read as maps and slices", isMap && nestedMap && isSlice, true)
	ok(schema.On("mongo").Drop(ctx, "blobs"))
	hostileDocs := orm.From("documents").On("mongo")
	total := must(hostileDocs.Count(ctx))
	_, err = hostileDocs.Where("$where", orm.Eq, "sleep(100) || true").Count(ctx)
	eq(t, "mongo operator as a column refused", err != nil && strings.Contains(err.Error(), "operator"), true)
	_, err = hostileDocs.Where("meta.$ne", orm.Eq, 1).Count(ctx)
	eq(t, "mongo operator inside a path refused", err != nil, true)
	eq(t, "mongo operator as a value is data", must(hostileDocs.Where("title", orm.Eq, map[string]any{"$ne": ""}).Count(ctx)), int64(0))
	_, err = hostileDocs.OrderBy("$natural").Get(ctx)
	eq(t, "mongo operator as an order refused", err != nil, true)
	err = hostileDocs.Insert(ctx, map[string]any{"id": "hostile", "$set": map[string]any{"title": "x"}})
	eq(t, "mongo operator as a field refused", err != nil, true)
	eq(t, "mongo documents untouched", must(hostileDocs.Count(ctx)), total)
	fine := time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC)
	_, err = Notes.Query().On("mongo").Update(ctx, Notes.Seen.Set(&fine))
	eq(t, "sub-millisecond times refused", err != nil && strings.Contains(err.Error(), "finer than the millisecond"), true)
	assertDocModelQueries(t, ctx, "mongo")

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

// assertDocAggregates checks a document store aggregates the documents a
// query matches through both builders, scopes and filters applied.
func assertDocAggregates(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	docs := Docs.Query().On(conn)
	views := must(docs.Pluck(ctx, Docs.Views))
	var sum, high int64
	low := views[0]
	for _, v := range views {
		sum += v
		high, low = max(high, v), min(low, v)
	}
	eq(t, conn+" sum", must(docs.Sum(ctx, Docs.Views)), sum)
	eq(t, conn+" avg", must(docs.Avg(ctx, Docs.Views)), float64(sum)/float64(len(views)))
	eq(t, conn+" min", must(docs.Min(ctx, Docs.Views)), low)
	eq(t, conn+" max", must(docs.Max(ctx, Docs.Views)), high)
	eq(t, conn+" min of strings", must(docs.Where(Docs.Title.In("far", "near")).Min(ctx, Docs.Title)), "far")
	eq(t, conn+" filtered sum ignores order and limit", must(docs.Where(Docs.Views.Gt(high-1)).OrderBy(Docs.Views.Desc()).Limit(1).Offset(5).Sum(ctx, Docs.Views)), high)
	eq(t, conn+" sum of nothing", must(docs.Where(Docs.Views.Gt(high)).Sum(ctx, Docs.Views)), int64(0))
	_, err := docs.Where(Docs.Views.Gt(high)).Avg(ctx, Docs.Views)
	eq(t, conn+" avg of nothing is not found", errors.Is(err, orm.ErrNotFound), true)
	_, err = docs.Sum(ctx, orm.Plus(Docs.Views, 1))
	eq(t, conn+" aggregate of an expression refused", orm.IsNotTranslatable(err), true)

	rows := orm.From("documents").On(conn)
	eq(t, conn+" row sum", must(rows.Sum[int64](ctx, "views")), sum)
	eq(t, conn+" row max col", must(rows.MaxCol(ctx, Docs.Views)), high)
	eq(t, conn+" row avg filtered", must(rows.Where("views", orm.Lte, low).Avg(ctx, "views")), float64(low))
	eq(t, conn+" row min col", must(rows.MinCol(ctx, Docs.Title)), "far")
	_, err = rows.GroupBy("title").Sum[int64](ctx, "views")
	eq(t, conn+" grouped aggregate refused", orm.IsNotTranslatable(err), true)
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
		OrderBy(orm.Nearest(Docs.Vec, orm.NewVector[orm.D3](1, 0, 0), orm.Cosine)).
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
	ok(store.DropCollection(ctx, "keyed"))
	ok(store.EnsureCollection(ctx, "keyed", 3, orm.L2))
	keyed := orm.From("keyed").On("qdrant").WithKey("code")
	ok(keyed.Insert(ctx, map[string]any{"code": 7, "name": "seven", "vec": orm.NewVector3(1, 2, 3)}))
	point := must(keyed.Where("code", orm.Eq, 7).First(ctx))
	eq(t, "withKey names the point id column", fmt.Sprintf("%v %v", point["code"], point["name"]), "7 seven")
	eq(t, "withKey finds by point id", must(keyed.Find(ctx, 7))["name"], any("seven"))
	ok(store.DropCollection(ctx, "keyed"))

	before := must(Docs.Query().On("qdrant").Count(ctx))
	extra := make([]Doc, 300)
	ids := make([]any, len(extra))
	for i := range extra {
		extra[i] = Doc{ID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), Title: "extra", Vec: orm.NewVector3(1, float64(i), 0)}
		ids[i] = extra[i].ID
	}
	ok(Docs.Query().On("qdrant").Insert(ctx, extra...))
	all := must(Docs.Query().On("qdrant").OrderBy(orm.Nearest(Docs.Vec, orm.NewVector3(1, 0, 0), orm.Cosine)).Get(ctx))
	eq(t, "unlimited nearest returns every match", int64(len(all)), before+300)
	eq(t, "extra cleanup", must(Docs.Query().On("qdrant").WhereKey(ids...).Delete(ctx)), int64(300))

	ok(store.DropCollection(ctx, "many"))
	rank := orm.DocIndex{Name: "many_rank", Fields: []orm.DocField{{Name: "rank", Kind: orm.KindInt}}}
	ok(store.CreateCollection(ctx, orm.Collection{Name: "many", Indexes: []orm.DocIndex{rank}}))
	many := orm.From("many").On("qdrant")
	points := make([]map[string]any, 300)
	for i := range points {
		points[i] = map[string]any{"id": fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)}
		if i%3 > 0 {
			points[i]["rank"] = 300 - i
		}
	}
	ok(many.Insert(ctx, points...))
	ordered := must(many.OrderBy("rank").Get(ctx))
	eq(t, "order by keeps points missing the field", len(ordered), 300)
	eq(t, "points missing the field sort last", fmt.Sprint(ordered[0]["rank"], " ", ordered[199]["rank"], " ", ordered[200]["rank"]), "1 299 <nil>")
	eq(t, "limited order by reaches points missing the field", len(must(many.OrderBy("rank").Offset(190).Limit(20).Get(ctx))), 20)
	ok(store.DropCollection(ctx, "many"))

	moved := must(Docs.Query().On("qdrant").Where(Docs.Title.Eq("far")).First(ctx))
	moved.Vec = orm.NewVector3(0, 0, 1)
	ok(Docs.Query().On("qdrant").Save(ctx, &moved))
	nearest := must(Docs.Query().On("qdrant").OrderBy(orm.Nearest(Docs.Vec, orm.NewVector3(0, 0, 1), orm.Cosine)).Limit(1).Get(ctx))
	eq(t, "saving a vector moves the point", nearest[0].ID, moved.ID)
	assertDocMigrations(t, ctx, "qdrant")
	ok(store.DropCollection(ctx, "notes"))
	ok(store.CreateCollection(ctx, orm.Collection{Name: "notes", Indexes: []orm.DocIndex{{Name: "notes_views", Fields: []orm.DocField{{Name: "views", Kind: orm.KindInt}}}}}))
	assertDocLogic(t, ctx, "qdrant")
	assertDocKeys(t, ctx, "qdrant", false)
	err = orm.From("documents").On("qdrant").Insert(ctx, map[string]any{"id": "00000000-0000-4000-8000-0000000000b1", "blob": []byte{1}})
	eq(t, "binary payloads refused", orm.IsNotTranslatable(err), true)
	assertDocModelQueries(t, ctx, "qdrant")
	ok(store.DropCollection(ctx, "notes"))
	_, err = Docs.Query().On("qdrant").Sum(ctx, Docs.Views)
	eq(t, "qdrant model aggregate refused", orm.IsNotTranslatable(err), true)
	_, err = orm.From("documents").On("qdrant").Max[int64](ctx, "views")
	eq(t, "qdrant row aggregate refused", orm.IsNotTranslatable(err), true)
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
		OrderBy(orm.Nearest(Docs.Vec, orm.NewVector[orm.D3](1, 0, 0), orm.L2)).
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
