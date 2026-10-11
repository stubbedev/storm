package models_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	. "github.com/stubbedev/storm/example/models"
	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

func TestInsertGetID(t *testing.T) {
	setup(t)

	id := must(Posts.Query().InsertGetID(Post{UserID: 2, Title: "raw insert", Published: true}))
	eq(t, "id assigned", id > 0, true)
	eq(t, "row stored", must(Posts.Find(id)).Title, "raw insert")

	_, err := Users.Query().InsertGetID(User{ID: 10, Name: "with key"})
	eq(t, "error with key set", err != nil, true)
}

func TestFindOr(t *testing.T) {
	setup(t)

	eq(t, "findOr hit", must(Users.Query().FindOr(1, func() (User, error) {
		return User{Name: "fallback"}, nil
	})).Name, "Alice")
	eq(t, "findOr miss", must(Users.Query().FindOr(99, func() (User, error) {
		return User{Name: "fallback"}, nil
	})).Name, "fallback")
}

func TestChunkWhile(t *testing.T) {
	setup(t)

	var sizes []string
	err := Posts.Query().ChunkWhile(func(prev, cur Post) bool { return prev.UserID == cur.UserID },
		func(ps []Post) error {
			sizes = append(sizes, fmt.Sprintf("%d:%d", len(ps), ps[0].UserID))
			return nil
		})
	ok(err)
	eq(t, "chunkWhile groups", sizes, []string{"3:1", "1:2", "1:3", "1:5", "1:99"})
}

func TestExplain(t *testing.T) {
	setup(t)

	rows, err := Users.Where(Users.Karma.Gt(1)).Explain()
	ok(err)
	eq(t, "explain rows", len(rows) > 0, true)
	eq(t, "explain has columns", len(rows[0]) > 0, true)
}

func TestJSONOverlapConditions(t *testing.T) {
	setup(t)

	labels := Users.Settings.JSON[[]string]("labels")
	eq(t, "jsonOverlaps", must(Users.Where(labels.JSONOverlaps("admin", "nope")).Count()), int64(1))
	eq(t, "jsonDoesntContain", must(Users.Where(Users.Settings.JSONDoesntContain("admin")).Count()), int64(2))
}

func TestQueryLog(t *testing.T) {
	setup(t)

	orm.EnableQueryLog()
	defer orm.DisableQueryLog()
	must(Users.Query().Count())
	log := orm.QueryLog()
	eq(t, "query log records", len(log) > 0 && strings.HasPrefix(log[0].SQL, "SELECT"), true)

	orm.FlushQueryLog()
	eq(t, "flush", len(orm.QueryLog()), 0)
}

func TestRetryingTransaction(t *testing.T) {
	ctx := setup(t)

	attempts := 0
	ok(orm.RetryingTransaction(ctx, 3, func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("Error 1213 (40001): Deadlock found when trying to get lock")
		}
		return nil
	}))
	eq(t, "deadlock retried", attempts, 2)

	attempts = 0
	err := orm.RetryingTransaction(ctx, 3, func(ctx context.Context) error {
		attempts++
		return errors.New("boom")
	})
	eq(t, "other errors returned", attempts == 1 && err.Error() == "boom", true)

	eq(t, "IsDeadlock", orm.IsDeadlock(errors.New("SQLSTATE 40P01: deadlock detected")), true)
	eq(t, "not deadlock", orm.IsDeadlock(errors.New("boom")), false)
}

func TestReadWriteSplit(t *testing.T) {
	if driver != "" {
		t.Skip("file-based SQLite only")
	}
	ctx := context.Background()
	dir := t.TempDir()
	readPath, writePath := dir+"/read.db", dir+"/write.db"

	read, err := orm.Open("rwread", "sqlite", "file:"+readPath+"?_time_format=sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { read.Close() })
	audit, err := orm.Open("audit", "sqlite", "file:"+dir+"/audit.db?_time_format=sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { audit.Close() })
	if _, err := (&schema.Migrator{Connection: "rwread", Migrations: schema.Registered()}).Migrate(ctx, false); err != nil {
		t.Fatal(err)
	}
	if err := orm.From("users").On("rwread").Insert(
		map[string]any{"id": 1, "name": "Alice", "email": "alice@example.com", "active": true, "karma": 42},
	); err != nil {
		t.Fatal(err)
	}

	write, err := orm.OpenReadWrite("rw", "sqlite", "file:"+readPath+"?_time_format=sqlite", "file:"+writePath+"?_time_format=sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { write.Close() })

	eq(t, "selects read from the read pool", must(Users.Query().On("rw").Count()), int64(1))
	_, err = Users.Where(Users.ID.Eq(1)).On("rw").Update(Users.Karma.Set(7))
	eq(t, "writes go to the write pool", err != nil && strings.Contains(err.Error(), "no such table"), true)
	_, err = Users.Query().On("rw").LockForUpdate().Get()
	eq(t, "locking reads go to the write pool", err != nil && strings.Contains(err.Error(), "no such table"), true)
	_, err = orm.From("users").On("rw").SharedLock().Get()
	eq(t, "locking row reads go to the write pool", err != nil && strings.Contains(err.Error(), "no such table"), true)
}

func TestRowQueryBuilder(t *testing.T) {
	ctx := setup(t)

	rows := must(orm.From("users").
		Where("active", "=", true).
		WhereNull("deleted_at").
		WhereRaw("karma > ?", 10).
		OrderBy("name").
		Get())
	eq(t, "get", len(rows), 2)
	eq(t, "row values", rows[0]["name"], "Alice")

	eq(t, "count", must(orm.From("users").WhereIn("country_id", 1, 2).Count()), int64(5))
	eq(t, "exists", must(orm.From("users").Where("email", orm.Eq, "alice@example.com").Exists()), true)

	// Typed columns work on row queries too: completion lists the model's
	// columns and the compared values are compile-checked.
	typed := must(orm.From("users").
		SelectCols(Users.Name, Users.Karma).
		WhereCond(Users.Active.Eq(true), Users.Karma.Gt(10)).
		WhereCond(Users.DeletedAt.IsNull()).
		OrderByCol(Users.Name).
		Get())
	eq(t, "typed where", len(typed), 2)
	eq(t, "typed row values", typed[0]["name"], "Alice")
	eq(t, "typed neq", must(orm.From("users").WhereCond(Users.Name.Ne("Alice")).Count()), int64(4))
	eq(t, "typed order", must(orm.From("users").OrderByColDesc(Users.Karma).Limit(1).Pluck("name"))[0], "Carol")
	joined := must(orm.From("users").
		SelectCols(Users.Email).
		Join(Countries, Countries.ID.EqCol(Users.CountryID)).
		WhereCond(Countries.Name.Eq("Norway")).
		Count())
	eq(t, "typed join", joined, int64(2))

	aliased := must(orm.From("users").
		SelectCols(Users.Name.As("user"), Countries.Name.As("country")).
		Join(Countries, Countries.ID.EqCol(Users.CountryID)).
		WhereCond(Countries.Name.Eq("Norway")).
		OrderByColDesc(Users.Name).
		Get())
	eq(t, "aliased rows", len(aliased), 2)
	eq(t, "aliased left", aliased[0]["user"], any("Dave"))
	eq(t, "aliased right", aliased[0]["country"], any("Norway"))

	eq(t, "fromTable", must(orm.FromTable(Users).WhereCond(Users.Name.Eq("Alice")).Count()), int64(1))
	eq(t, "fromTable connection", must(orm.FromTable(AuditLog).Count()), int64(0))
	_, err := orm.From("audit_log").Count()
	eq(t, "from defaults to the main connection", err != nil, true)

	eq(t, "pluckCol", must(orm.From("countries").OrderBy("id").PluckCol(Countries.Name)), []string{"Denmark", "Norway"})
	eq(t, "pluckCol converts", must(orm.FromTable(Users).OrderByCol(Users.ID).Limit(3).PluckCol(Users.Active)), []bool{true, true, false})

	posts := orm.FromTable(Posts)
	ok(posts.InsertCols(Posts.UserID.Set(2), Posts.Title.Set("typed insert"), Posts.Views.Set(3)))
	inserted := posts.WhereCond(Posts.Title.Eq("typed insert"))
	eq(t, "insertCols", must(inserted.PluckCol(Posts.Views)), []int64{3})
	eq(t, "updateCols", must(inserted.UpdateCols(Posts.Views.SetExpr(orm.Plus(Posts.Views, 2)), Posts.Published.Set(true))), int64(1))
	eq(t, "updateCols expr", must(inserted.PluckCol(Posts.Views)), []int64{5})
	eq(t, "updateCols literal", must(inserted.PluckCol(Posts.Published)), []bool{true})
	eq(t, "delete typed insert", must(inserted.Delete()), int64(1))

	eq(t, "first", must(orm.From("countries").OrderBy("id").First())["name"], "Denmark")
	eq(t, "pluck", fmt.Sprint(must(orm.From("countries").OrderBy("id").Pluck("name"))), "[Denmark Norway]")

	id := must(orm.From("posts").InsertGetID(map[string]any{"user_id": 2, "title": "row insert", "views": 0}))
	eq(t, "insertGetID", id > 0, true)
	n := must(orm.From("posts").Where("id", "=", id).Update(map[string]any{"views": 7}))
	eq(t, "update", n, int64(1))
	eq(t, "updated value", must(orm.From("posts").Where("id", "=", id).Pluck("views"))[0], any(int64(7)))

	ok(orm.From("users").UpdateOrInsert(
		map[string]any{"email": "zed@example.com"},
		map[string]any{"name": "Zed", "active": true}))
	z := must(orm.From("users").Where("email", "=", "zed@example.com").First())
	eq(t, "updateOrInsert inserted", z["name"], "Zed")
	ok(orm.From("users").UpdateOrInsert(
		map[string]any{"email": "zed@example.com"},
		map[string]any{"name": "Zed Zed"}))
	eq(t, "updateOrInsert updated", must(orm.From("users").Where("email", "=", "zed@example.com").First())["name"], "Zed Zed")

	eq(t, "delete", must(orm.From("posts").Where("id", "=", id).Delete()), int64(1))

	eq(t, "distinct count", must(orm.From("users").Select("country_id").Distinct().Count()), int64(3))
	eq(t, "dotted join", must(orm.From("users").
		LeftJoin(Countries, Countries.ID.EqCol(Users.CountryID)).
		Where("countries.name", orm.Eq, "Norway").
		Count()), int64(2))
	eq(t, "dotted star", must(orm.From("users").
		Select("users.*").
		Join(Countries, Countries.ID.EqCol(Users.CountryID)).
		Where("countries.name", orm.Eq, "Norway").
		OrderBy("users.name").
		First())["name"], any("Carol"))
	eq(t, "whereIn none", must(orm.From("users").WhereIn("country_id").Count()), int64(0))
	eq(t, "whereNotIn none", must(orm.From("users").WhereNotIn("country_id").Count()), int64(6))
	eq(t, "whereNotBetween", must(orm.From("users").WhereNotBetween("karma", 10, 60).Count()), int64(3))
	eq(t, "orWhereCond", must(orm.From("users").
		WhereCond(Users.Name.Eq("Alice")).
		OrWhereCond(Users.Karma.Gt(90)).
		Count()), int64(2))
	eq(t, "doesntExist", must(orm.From("users").Where("email", orm.Eq, "nobody@example.com").DoesntExist()), true)

	eq(t, "sum", must(orm.From("users").Sum[int64]("karma")), int64(211))
	eq(t, "avg", must(orm.From("users").WhereNotNull("country_id").Avg("karma")), 42.2)
	eq(t, "min", must(orm.From("users").Min[string]("name")), "Alice")
	eq(t, "max", must(orm.From("users").WhereCond(Users.Active.Eq(true)).Max[int64]("karma")), int64(50))
	eq(t, "sumCol", must(orm.FromTable(Users).SumCol(Users.Karma)), 211)
	eq(t, "avgCol", must(orm.FromTable(Users).WhereCond(Users.CountryID.Eq(2)).AvgCol(Users.Karma)), 51.0)
	eq(t, "minCol", must(orm.FromTable(Users).WhereCond(Users.CountryID.IsNotNull()).MinCol(Users.Karma)), 3)
	eq(t, "maxCol", must(orm.FromTable(Users).WhereCond(Users.CountryID.IsNotNull()).MaxCol(Users.Name)), "Eve")
	eq(t, "aggregate of none", must(orm.From("users").WhereIn("id").Sum[int64]("karma")), int64(0))

	grouped := must(orm.FromTable(Users).
		SelectCols(Users.CountryID, orm.Count[User]().As("users"), orm.Sum(Users.Karma).As("karma")).
		WhereCond(Users.CountryID.IsNotNull()).
		GroupByCols(Users.CountryID).
		OrderByCol(Users.CountryID).
		Get())
	eq(t, "grouped rows", len(grouped), 2)
	eq(t, "grouped count", fmt.Sprintf("%v %v", grouped[0]["users"], grouped[1]["users"]), "3 2")
	eq(t, "grouped sum", fmt.Sprintf("%v %v", grouped[0]["karma"], grouped[1]["karma"]), "109 102")
	eq(t, "havingCond", len(must(orm.FromTable(Users).
		SelectCols(Users.CountryID).
		GroupByCols(Users.CountryID).
		HavingCond(orm.Count[User]().Gt(2)).
		Get())), 1)
	eq(t, "havingRaw", len(must(orm.From("users").
		SelectRaw("country_id, COUNT(*) AS n").
		GroupBy("country_id").
		HavingRaw("COUNT(*) > ?", 2).
		Get())), 1)
	eq(t, "count groups", must(orm.From("users").GroupBy("country_id").Count()), int64(3))
	_, err = orm.From("users").GroupBy("country_id").Sum[int64]("karma")
	eq(t, "grouped aggregate refused", err != nil && strings.Contains(err.Error(), "one value per group"), true)

	dave := orm.From("users").Where("name", orm.Eq, "Dave")
	eq(t, "increment", must(dave.Increment("karma", 10)), int64(1))
	eq(t, "decrement", must(dave.Decrement("karma", 4)), int64(1))
	eq(t, "incremented", must(dave.PluckCol(Users.Karma)), []int{9})

	countries := orm.From("countries")
	ok(countries.InsertOrIgnore(map[string]any{"id": 1, "name": "Duplicate"}, map[string]any{"id": 3, "name": "Sweden"}))
	eq(t, "insertOrIgnore", fmt.Sprint(must(countries.OrderBy("id").Pluck("name"))), "[Denmark Norway Sweden]")
	ok(countries.Upsert([]map[string]any{{"id": 1, "name": "Danmark"}, {"id": 4, "name": "Finland"}}, []string{"id"}))
	eq(t, "upsert", fmt.Sprint(must(countries.OrderBy("id").Pluck("name"))), "[Danmark Norway Sweden Finland]")
	eq(t, "upsert nothing", countries.Upsert(nil, []string{"id"}), error(nil))

	ordered := countries.OrderBy("id")
	last := must(ordered.Paginate(3, 2))
	eq(t, "paginate items", fmt.Sprint(last.Items[0]["name"]), "Finland")
	eq(t, "paginate meta", fmt.Sprint(last.Total, last.LastPage, last.From, last.To, last.HasMore), "4 2 4 4 false")
	first := must(ordered.Paginate(3, 1))
	eq(t, "paginate first", fmt.Sprint(len(first.Items), first.From, first.To, first.HasMore), "3 1 3 true")
	beyond := must(ordered.Paginate(3, 5))
	eq(t, "paginate beyond", fmt.Sprint(len(beyond.Items), beyond.From, beyond.To), "0 0 0")
	eq(t, "simplePaginate more", must(ordered.SimplePaginate(3, 1)).HasMore, true)
	eq(t, "simplePaginate last", must(ordered.SimplePaginate(3, 2)).HasMore, false)
	_, err = ordered.Paginate(0, 1)
	eq(t, "paginate zero perPage refused", err != nil, true)
	_, err = Users.Query().Paginate(2, 0)
	eq(t, "model paginate zero page refused", err != nil, true)
	modelPage := must(Users.Query().OrderBy(Users.ID.Asc()).Paginate(2, 2))
	eq(t, "model paginate", fmt.Sprint(names(modelPage.Items), modelPage.From, modelPage.To), "[Carol Dave] 3 4")

	firstCursor := must(countries.CursorPaginate(3, ""))
	eq(t, "cursorPaginate first", fmt.Sprint(len(firstCursor.Items), firstCursor.NextCursor != ""), "3 true")
	lastCursor := must(countries.CursorPaginate(3, firstCursor.NextCursor))
	eq(t, "cursorPaginate next", fmt.Sprintf("%v %q", lastCursor.Items[0]["name"], lastCursor.NextCursor), `Finland ""`)
	byName := countries.OrderByDesc("name")
	descPage := must(byName.CursorPaginate(2, must(byName.CursorPaginate(2, "")).NextCursor))
	eq(t, "cursorPaginate desc", fmt.Sprintf("%v %v", descPage.Items[0]["name"], descPage.Items[1]["name"]), "Finland Danmark")
	_, err = Users.Query().CursorPaginate(0, "")
	eq(t, "model cursorPaginate zero refused", err != nil, true)
	_, err = countries.CursorPaginate(2, "not-a-cursor")
	eq(t, "bad cursor refused", err != nil && strings.Contains(err.Error(), "bad cursor"), true)

	var sizes []int
	ok(ordered.Chunk(3, func(rows []map[string]any) error { sizes = append(sizes, len(rows)); return nil }))
	eq(t, "chunk", sizes, []int{3, 1})
	sizes = nil
	ok(countries.ChunkByID(3, func(rows []map[string]any) error { sizes = append(sizes, len(rows)); return nil }))
	eq(t, "chunkByID", sizes, []int{3, 1})
	eq(t, "chunk zero size refused", ordered.Chunk(0, func([]map[string]any) error { return nil }) != nil, true)
	eq(t, "model chunk zero size refused", Users.Query().Chunk(0, func([]User) error { return nil }) != nil, true)
	var lazy, cursor []string
	for row, err := range countries.Lazy(3) {
		ok(err)
		lazy = append(lazy, fmt.Sprint(row["name"]))
	}
	eq(t, "lazy", lazy, []string{"Danmark", "Norway", "Sweden", "Finland"})
	for row, err := range ordered.Cursor() {
		ok(err)
		cursor = append(cursor, fmt.Sprint(row["name"]))
		if len(cursor) == 2 {
			break
		}
	}
	eq(t, "cursor stops early", cursor, []string{"Danmark", "Norway"})
	eq(t, "chunkByID needs the key", countries.Select("name").ChunkByID(2, func([]map[string]any) error { return nil }) != nil, true)

	for range 2 {
		ok(orm.From("users").UpdateOrInsert(
			map[string]any{"email": "same@example.com"},
			map[string]any{"name": "Same", "active": true}))
	}
	eq(t, "updateOrInsert unchanged row", must(orm.From("users").Where("email", orm.Eq, "same@example.com").Count()), int64(1))

	_, err = orm.From("users").Update(map[string]any{})
	eq(t, "empty update refused", err != nil && strings.Contains(err.Error(), "no columns to set"), true)

	eq(t, "forPage", fmt.Sprint(must(countries.OrderBy("id").ForPage(2, 3).Pluck("name"))), "[Finland]")
	eq(t, "whereBetween", must(orm.From("users").WhereBetween("karma", 10, 60).Count()), int64(3))
	eq(t, "having", len(must(orm.From("users").SelectRaw("country_id").GroupBy("country_id").Having("country_id", orm.Gt, 1).Get())), 1)
	eq(t, "rightJoin", must(orm.From("users").RightJoin(Countries, Countries.ID.EqCol(Users.CountryID)).Count()), int64(7))
	eq(t, "crossJoin", must(countries.CrossJoin(Roles).Count()), int64(12))
	var roleNames []string
	ok(orm.From("roles").WithKey("name").ChunkByID(2, func(rows []map[string]any) error {
		for _, r := range rows {
			roleNames = append(roleNames, fmt.Sprint(r["name"]))
		}
		return nil
	}))
	eq(t, "withKey pages by that column", roleNames, []string{"admin", "editor", "viewer"})

	stmt, args := rendered(orm.From("users").Where("users.karma", orm.Gt, 10).ToSQL())
	eq(t, "toSQL", strings.Contains(stmt, "karma") && len(args) == 1, true)

	quoted := must(orm.Select(ctx, "SELECT 'what?' AS q, ? AS v", "five"))
	eq(t, "a quoted ? is text", fmt.Sprintf("%v %v", quoted[0]["q"], quoted[0]["v"]), "what? five")
	_, err = orm.Select(ctx, "SELECT 1 WHERE 1 = ?", 1, 2)
	eq(t, "surplus raw arguments refused", err != nil && strings.Contains(err.Error(), "1 placeholders for 2 arguments"), true)
	_, err = orm.Select(ctx, "SELECT ?")
	eq(t, "missing raw arguments refused", err != nil && strings.Contains(err.Error(), "more placeholders"), true)
	_, _, err = orm.From("users").WhereRaw("karma > ?", 1, 2).ToSQL()
	eq(t, "builder raw mismatch refused", err != nil, true)
	escaped, escapedArgs := rendered(orm.From("users").WhereRaw("settings ?? ?", "theme").ToSQL())
	eq(t, "?? is a literal ?", strings.Contains(escaped, "settings ? ") && len(escapedArgs) == 1, true)
}

func TestRawSQLRuns(t *testing.T) {
	ctx := setup(t)
	raw := must(Users.Where(
		Users.Name.Eq("Alice"),
		Users.Name.Ne("O'Brien"),
		Users.Active.Eq(true),
		Users.Karma.Gt(1),
		Users.CreatedAt.Lt(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)),
		orm.Raw[User]("karma < ?", 100.5),
		orm.Raw[User]("karma > ?", int64(2)),
		orm.Raw[User]("karma > ?", int32(3)),
		orm.Raw[User]("email <> ?", []byte("x@example.com")),
		orm.Raw[User]("(deleted_at IS NULL OR deleted_at = ?)", nil),
	).ToRawSQL())
	c := must(orm.Connection(ctx, orm.DefaultConnection))
	rows := must(c.DB.QueryContext(ctx, raw))
	var found int
	for rows.Next() {
		found++
	}
	ok(rows.Err())
	ok(rows.Close())
	eq(t, "inlined values select the same rows as bound ones", found, 1)
	eq(t, "quotes are escaped", strings.Contains(raw, "'O''Brien'"), true)
}

func TestStickyReadsSeeWrites(t *testing.T) {
	if driver != "" {
		t.Skip("file-based SQLite only")
	}
	ctx := context.Background()
	dir := t.TempDir()
	readDSN := "file:" + dir + "/read.db?_time_format=sqlite"
	writeDSN := "file:" + dir + "/write.db?_time_format=sqlite"

	// DDL happens before the split, so both pools get the tables.
	write, err := orm.Open("sticky", "sqlite", writeDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { write.Close() })
	read, err := orm.Open("stickyseed", "sqlite", readDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { read.Close() })
	for i, name := range []string{"sticky", "stickyseed"} {
		audit, err := orm.Open("audit", "sqlite", fmt.Sprintf("file:%s/audit%d.db?_time_format=sqlite", dir, i))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { audit.Close() })
		if _, err := (&schema.Migrator{Connection: name, Migrations: schema.Registered()}).Migrate(ctx, false); err != nil {
			t.Fatal(err)
		}
	}

	// The read replica only ever sees the seeded row.
	seedRow := map[string]any{
		"name": "Replica Only", "email": "replica@example.com",
		"active": true, "karma": 1, "created_at": "2026-01-01 10:00:00", "updated_at": "2026-01-01 10:00:00",
	}
	if err := orm.From("users").On("stickyseed").Insert(seedRow); err != nil {
		t.Fatal(err)
	}

	if _, err := orm.OpenReadWrite("sticky", "sqlite", readDSN, writeDSN); err != nil {
		t.Fatal(err)
	}

	insert := func(ctx context.Context, email string) error {
		return orm.From("users").On("sticky").WithContext(ctx).Insert(map[string]any{
			"name": "W", "email": email,
			"active": true, "karma": 0, "created_at": "2026-01-02 10:00:00", "updated_at": "2026-01-02 10:00:00",
		})
	}

	// Without Sticky, reads always hit the read pool.
	eq(t, "plain ctx reads the replica", must(Users.Query().On("sticky").Count()), int64(1))
	ok(insert(ctx, "w1@example.com"))
	eq(t, "plain ctx still reads the replica after a write", must(Users.Query().On("sticky").Count()), int64(1))

	// With Sticky, reads stay on the replica until the first write,
	// then follow the writes for the rest of the ctx.
	sticky := orm.Sticky(ctx)
	eq(t, "sticky ctx reads the replica before writing", must(Users.Query().On("sticky").WithContext(sticky).Count()), int64(1))
	ok(insert(sticky, "w2@example.com"))
	eq(t, "sticky ctx reads its own writes", must(Users.Query().On("sticky").WithContext(sticky).Count()), int64(2))
	eq(t, "table queries are sticky too",
		len(must(orm.From("users").On("sticky").Where("email", "=", "w2@example.com").WithContext(sticky).Get())), 1)
	eq(t, "other ctxs keep reading the replica", must(Users.Query().On("sticky").Count()), int64(1))

	// Writes inside a transaction mark the scope too.
	ok(orm.TransactionOn(sticky, "sticky", func(ctx context.Context) error {
		return insert(ctx, "w3@example.com")
	}))
	eq(t, "transaction writes are sticky", must(Users.Query().On("sticky").WithContext(sticky).Count()), int64(3))
}

func TestEncryptedAndHashedCasts(t *testing.T) {
	ctx := setup(t)

	ok(schema.On(orm.DefaultConnection).Create(ctx, "secrets", func(t *schema.Blueprint) {
		t.String("token")
		t.String("pw")
	}))
	orm.SetEncryptionKey([]byte("test key"))

	tok := orm.Encrypted[map[string]string]{V: map[string]string{"api_key": "s3cret"}}
	var pw orm.Hashed = "hunter2"
	ok(orm.From("secrets").Insert(map[string]any{"token": tok, "pw": pw}))

	row := must(orm.From("secrets").First())
	rawTok, rawPw := row["token"].(string), row["pw"].(string)
	eq(t, "encrypted at rest", rawTok != `{"api_key":"s3cret"}`, true)
	eq(t, "hashed at rest", strings.HasPrefix(rawPw, "$2"), true)

	var gotTok orm.Encrypted[map[string]string]
	var gotPw orm.Hashed
	ok(gotTok.Scan(rawTok))
	ok(gotPw.Scan(rawPw))
	eq(t, "decrypts", gotTok.V["api_key"], "s3cret")
	eq(t, "hash verifies", gotPw.Matches("hunter2"), true)
	eq(t, "hash rejects", gotPw.Matches("wrong"), false)
}

func TestRowQueryParity(t *testing.T) {
	ctx := setup(t)
	users := orm.From("users")
	names := func(q orm.RowQuery) string { return fmt.Sprint(must(q.OrderBy("id").Pluck("name"))) }

	eq(t, "find", must(users.Find(2))["name"], any("Bob"))
	_, err := users.Find(404)
	eq(t, "find missing", errors.Is(err, orm.ErrNotFound), true)
	eq(t, "sole", must(users.Where("name", orm.Eq, "Alice").Sole())["email"], any("alice@example.com"))
	_, err = users.Sole()
	eq(t, "sole of many", errors.Is(err, orm.ErrMultipleRecords), true)
	_, err = users.Where("name", orm.Eq, "Nobody").Sole()
	eq(t, "sole of none", errors.Is(err, orm.ErrNotFound), true)
	eq(t, "value", must(users.Where("name", orm.Eq, "Bob").Value("email")), any("bob@example.com"))
	_, err = users.Where("name", orm.Eq, "Nobody").Value("email")
	eq(t, "value of none", errors.Is(err, orm.ErrNotFound), true)
	eq(t, "valueCol", must(users.Where("name", orm.Eq, "Carol").ValueCol(Users.Karma)), 99)

	eq(t, "latest", must(users.Latest().First())["name"], any("Dave"))
	eq(t, "oldest", must(users.Oldest().First())["name"], any("Alice"))
	eq(t, "latest by column", must(users.Latest("karma").First())["name"], any("Carol"))
	eq(t, "random order", len(must(users.InRandomOrder().Get())), 5)
	eq(t, "reorder", must(users.OrderBy("name").Reorder().OrderByDesc("id").First())["name"], any("Eve"))
	eq(t, "when", must(users.When(true, func(q orm.RowQuery) orm.RowQuery { return q.Where("name", orm.Eq, "Alice") }).Count()), int64(1))
	eq(t, "unless", must(users.Unless(true, func(q orm.RowQuery) orm.RowQuery { return q.Where("name", orm.Eq, "Alice") }).Count()), int64(5))

	ok(orm.Transaction(ctx, func(ctx context.Context) error {
		eq(t, "lockForUpdate", len(must(users.Where("id", orm.Eq, 1).LockForUpdate().WithContext(ctx).Get())), 1)
		eq(t, "sharedLock", len(must(users.Where("id", orm.Eq, 1).SharedLock().WithContext(ctx).Get())), 1)
		return nil
	}))

	alice, bob := users.Select("name").Where("id", orm.Eq, 1), users.Select("name").Where("id", orm.Eq, 2)
	eq(t, "union", fmt.Sprint(must(alice.Union(bob).OrderBy("name").Pluck("name"))), "[Alice Bob]")
	eq(t, "union removes duplicates", must(alice.Union(alice).Count()), int64(1))
	eq(t, "union ordered by a qualified column", fmt.Sprint(must(bob.Union(alice).OrderBy("users.name").Pluck("name"))), "[Alice Bob]")
	eq(t, "union branch keeps its order and limit", len(must(alice.Union(users.Select("name").OrderBy("id").Limit(1).Offset(2)).Get())), 2)
	eq(t, "unionAll keeps them", must(alice.UnionAll(alice).Count()), int64(2))
	_, err = alice.Union(bob).Sum[int64]("karma")
	eq(t, "aggregate of a union refused", err != nil && strings.Contains(err.Error(), "union"), true)

	alices := users.Where("name", orm.Eq, "Alice")
	eq(t, "orWhereIn", names(alices.OrWhereIn("id", 2, 3)), "[Alice Bob Carol]")
	eq(t, "orWhereNotIn", names(alices.OrWhereNotIn("id", 1, 2, 3, 4)), "[Alice Eve]")
	eq(t, "orWhereNull", must(alices.OrWhereNull("settings").Count()), int64(4))
	eq(t, "orWhereNotNull", names(alices.OrWhereNotNull("deleted_at")), "[Alice Eve]")
	eq(t, "orWhereBetween", names(alices.OrWhereBetween("karma", 90, 100)), "[Alice Carol]")
	eq(t, "orWhereNotBetween", names(alices.OrWhereNotBetween("karma", 10, 100)), "[Alice Dave]")
	eq(t, "orWhereRaw", names(alices.OrWhereRaw("karma < ?", 5)), "[Alice Dave]")
	eq(t, "whereColumn", must(users.WhereColumn("created_at", orm.Eq, "updated_at").Count()), int64(5))
	eq(t, "orWhereColumn", must(alices.OrWhereColumn("created_at", orm.Neq, "updated_at").Count()), int64(1))

	posts := orm.From("posts").WhereColumn("posts.user_id", orm.Eq, "users.id")
	eq(t, "whereExists", names(users.WhereExists(posts)), "[Alice Bob Carol Eve]")
	eq(t, "whereNotExists", names(users.WhereNotExists(posts)), "[Dave]")
	eq(t, "orWhereExists", must(users.Where("id", orm.Eq, 4).OrWhereExists(posts).Count()), int64(5))
	eq(t, "orWhereNotExists", names(users.Where("id", orm.Eq, 1).OrWhereNotExists(posts)), "[Alice Dave]")

	inserted := must(orm.From("roles").InsertUsing([]string{"name"}, orm.From("countries").Select("name")))
	eq(t, "insertUsing", fmt.Sprint(inserted, must(orm.From("roles").Count())), "2 5")
	eq(t, "explain", len(must(users.Where("id", orm.Eq, 1).Explain())) > 0, true)
}
