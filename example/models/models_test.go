package models_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	_ "github.com/stubbedev/storm/example/migrations"
	. "github.com/stubbedev/storm/example/models"
	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

const seed = `
INSERT INTO countries (id, name) VALUES (1, 'Denmark'), (2, 'Norway');
INSERT INTO users (id, country_id, name, email, active, karma, settings, created_at, updated_at, deleted_at) VALUES
  (1, 1, 'Alice', 'alice@example.com', TRUE, 42, '{"theme":"dark","labels":["admin","beta"]}', '2025-01-15 10:00:00', '2025-01-15 10:00:00', NULL),
  (2, 1, 'Bob',   'bob@example.com',   TRUE, 17, '{"theme":"light","labels":[]}',            '2025-03-02 09:30:00', '2025-03-02 09:30:00', NULL),
  (3, 2, 'Carol', 'carol@example.com', FALSE, 99, NULL,                                       '2025-06-20 18:45:00', '2025-06-20 18:45:00', NULL),
  (4, 2, 'Dave',  'dave@example.com',  TRUE,  3, NULL,                                       '2026-02-11 12:00:00', '2026-02-11 12:00:00', NULL),
  (5, 1, 'Eve',   'eve@example.com',   TRUE, 50, NULL,                                       '2025-05-05 05:05:05', '2025-05-05 05:05:05', '2026-01-01 00:00:00');
INSERT INTO posts (id, user_id, title, published, views, created_at, updated_at) VALUES
  (1, 1, 'Generics in Go 1.27', TRUE, 540, '2026-01-10 08:00:00', '2026-01-10 08:00:00'),
  (2, 1, 'Draft: ORMs',         FALSE,  12, '2026-02-01 08:00:00', '2026-02-01 08:00:00'),
  (3, 1, 'Iterators',           TRUE,  88, '2026-03-05 08:00:00', '2026-03-05 08:00:00'),
  (4, 2, 'Hello world',         TRUE, 150, '2026-01-20 08:00:00', '2026-01-20 08:00:00'),
  (5, 3, 'Carol''s post',       TRUE, 300, '2025-12-24 08:00:00', '2025-12-24 08:00:00'),
  (6, 5, 'Eve post',            TRUE,  10, '2026-01-02 08:00:00', '2026-01-02 08:00:00'),
  (7, 99, 'Orphan',             TRUE,   1, '2026-01-03 08:00:00', '2026-01-03 08:00:00');
INSERT INTO videos (id, title) VALUES (1, 'Intro video');
INSERT INTO comments (id, commentable_id, commentable_type, body) VALUES
  (1, 1, 'posts', 'Great'), (2, 1, 'posts', 'Finally'), (3, 4, 'posts', 'Welcome'),
  (4, 1, 'videos', 'Nice video'), (5, 3, 'posts', 'Nice');
INSERT INTO roles (id, name) VALUES (1, 'admin'), (2, 'editor'), (3, 'viewer');
INSERT INTO role_user (user_id, role_id, granted_by) VALUES (1, 1, 'system'), (1, 2, 'bob'), (2, 2, 'alice');
INSERT INTO tags (id, name) VALUES (1, 'go'), (2, 'orm');
INSERT INTO taggables (tag_id, taggable_id, taggable_type) VALUES
  (1, 1, 'posts'), (2, 1, 'posts'), (1, 1, 'videos'), (2, 3, 'posts');
INSERT INTO images (id, imageable_id, imageable_type, url) VALUES (1, 1, 'users', 'alice.png');
`

// The suite runs on SQLite by default. Point it at another engine with
//
//	STORM_TEST_DRIVER=pgx STORM_TEST_DSN=... STORM_TEST_AUDIT_DSN=... go test ./...
//
// (driver "pgx" or "mysql"; both DSNs must be empty, disposable databases).
var (
	driver   = os.Getenv("STORM_TEST_DRIVER")
	openOnce sync.Once
	mainDB   *sql.DB
)

func setup(t *testing.T) context.Context {
	t.Helper()
	current = t
	ctx := context.Background()
	if driver == "" || driver == "sqlite" {
		open := func() *sql.DB {
			db, err := sql.Open("sqlite", "file::memory:?_time_format=sqlite")
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { db.Close() })
			return db
		}
		mainDB = open()
		orm.SetDefault(mainDB, orm.SQLite)
		orm.AddConnection("audit", open(), orm.SQLite)
	} else {
		openOnce.Do(func() {
			var err error
			if mainDB, err = orm.Open(orm.DefaultConnection, driver, os.Getenv("STORM_TEST_DSN")); err != nil {
				t.Fatal(err)
			}
			if _, err = orm.Open("audit", driver, os.Getenv("STORM_TEST_AUDIT_DSN")); err != nil {
				t.Fatal(err)
			}
		})
		for _, conn := range []string{orm.DefaultConnection, "audit"} {
			if err := schema.On(conn).DropAllTables(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := schema.NewMigrator().Migrate(ctx, false); err != nil {
		t.Fatal(err)
	}
	for stmt := range strings.SplitSeq(strings.TrimSpace(seed), ";\n") {
		if _, err := mainDB.Exec(strings.TrimSuffix(stmt, ";")); err != nil {
			t.Fatalf("seed: %v\n%s", err, stmt)
		}
	}
	if driver == "pgx" { // explicit ids don't advance Postgres sequences
		for _, table := range []string{"countries", "users", "posts", "videos", "comments", "roles", "tags", "images"} {
			if _, err := mainDB.Exec(fmt.Sprintf(`SELECT setval(pg_get_serial_sequence('%s', 'id'), (SELECT MAX(id) FROM %s))`, table, table)); err != nil {
				t.Fatal(err)
			}
		}
	}
	return ctx
}

// current is the running test; tests are sequential (connections are global).
var current *testing.T

// ok fails the running test on error.
func ok(err error) {
	if err != nil {
		current.Helper()
		current.Fatal(err)
	}
}

// rendered fails the running test when a query does not render.
func rendered(stmt string, args []any, err error) (string, []any) {
	ok(err)
	return stmt, args
}

// must fails the running test on error.
func must[T any](v T, err error) T {
	if err != nil {
		current.Helper()
		current.Fatal(err)
	}
	return v
}

func eq[T any](t *testing.T, what string, got, want T) {
	t.Helper()
	if !equalish(got, want) {
		t.Errorf("%s:\n got  %#v\n want %#v", what, got, want)
	}
}

func equalish(a, b any) bool { return strings.TrimSpace(sprint(a)) == strings.TrimSpace(sprint(b)) }

func names(us []User) []string {
	out := make([]string, len(us))
	for i, u := range us {
		out[i] = u.Name
	}
	return out
}

func titles(ps []Post) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.Title
	}
	return out
}

// ---------------------------------------------------------------------------

func TestDialects(t *testing.T) {
	q := Users.Query().
		Select(Users.ID, Users.Name).
		Where(Users.Karma.Gt(10), orm.Year(Users.CreatedAt).Eq(2026)).
		OrderBy(Users.Name.Asc()).
		Offset(5).
		LockForUpdate()

	want := map[orm.Dialect]string{
		orm.SQLite:   `SELECT "users"."id", "users"."name" FROM "users" WHERE ("users"."karma" > ?) AND (CAST(strftime('%Y', "users"."created_at") AS INTEGER) = ?) AND ("users"."deleted_at" IS NULL) ORDER BY "users"."name" ASC LIMIT -1 OFFSET 5`,
		orm.Postgres: `SELECT "users"."id", "users"."name" FROM "users" WHERE ("users"."karma" > $1) AND (CAST(EXTRACT(YEAR FROM "users"."created_at") AS INTEGER) = $2) AND ("users"."deleted_at" IS NULL) ORDER BY "users"."name" ASC OFFSET 5 FOR UPDATE`,
		orm.MySQL:    "SELECT `users`.`id`, `users`.`name` FROM `users` WHERE (`users`.`karma` > ?) AND (YEAR(`users`.`created_at`) = ?) AND (`users`.`deleted_at` IS NULL) ORDER BY `users`.`name` ASC LIMIT 18446744073709551615 OFFSET 5 FOR UPDATE",
	}
	for d, w := range want {
		got, args := rendered(q.ToSQLFor(d))
		eq(t, d.Name(), got, w)
		eq(t, d.Name()+" args", args, []any{10, 2026})
	}

	if d, ok := orm.DialectFor("pgx"); !ok || d != orm.Postgres {
		t.Error("DialectFor(pgx) should be Postgres")
	}
}

func TestRetrieval(t *testing.T) {
	setup(t)

	eq(t, "count (soft deletes excluded)", must(Users.Query().Count()), int64(4))
	eq(t, "withTrashed", must(Users.Query().WithTrashed().Count()), int64(5))
	eq(t, "onlyTrashed", names(must(Users.Query().OnlyTrashed().Get())), []string{"Eve"})

	emails := must(Users.Query().OrderBy(Users.Name.Asc()).Pluck(Users.Email))
	eq(t, "pluck", emails, []string{"alice@example.com", "bob@example.com", "carol@example.com", "dave@example.com"})

	eq(t, "sum", must(Users.Query().Where(Users.Active.Eq(true)).Sum(Users.Karma)), 62)
	eq(t, "avg", must(Posts.Query().Scope(Published).Avg(Posts.Views)), 181.5)
	eq(t, "max", must(Users.Query().Max(Users.Name)), "Dave")
	eq(t, "value", must(Users.Query().OrderBy(Users.Karma.Desc()).Value(Users.Name)), "Carol")
	eq(t, "pluckMap", must(Posts.Query().GroupBy(Posts.UserID).PluckMap(Posts.UserID, orm.Count[Post]())),
		map[int64]int64{1: 3, 2: 1, 3: 1, 5: 1, 99: 1})

	alice := must(Users.Find(1))
	eq(t, "find", alice.Name, "Alice")
	eq(t, "json cast", alice.Settings.V.Labels, []string{"admin", "beta"})
	eq(t, "findMany", len(must(Users.Query().FindMany(1, 2, 5))), 2)

	_, err := Users.Find(5)
	eq(t, "find trashed", errors.Is(err, orm.ErrNotFound), true)
	_, err = Users.Where(Users.Karma.Gt(10)).Sole()
	eq(t, "sole many", errors.Is(err, orm.ErrMultipleRecords), true)
	eq(t, "sole", must(Users.Where(Users.Name.Eq("Bob")).Sole()).Email, "bob@example.com")
	fallback := must(Users.Where(Users.Name.Eq("Nobody")).FirstOr(func() (User, error) { return User{Name: "default"}, nil }))
	eq(t, "firstOr", fallback.Name, "default")

	eq(t, "exists", must(Users.Where(Users.Name.Eq("Alice")).Exists()), true)
	eq(t, "doesntExist", must(Users.Where(Users.Name.Eq("Zed")).DoesntExist()), true)

	byEmail := must(Users.Query().KeyBy(Users.Email))
	eq(t, "keyBy", byEmail["bob@example.com"].Name, "Bob")
	grouped := must(Posts.Query().Grouped(Posts.UserID))
	eq(t, "grouped", len(grouped[1]), 3)
	upper := must(Users.Query().OrderBy(Users.ID.Asc()).Map(func(u User) string { return strings.ToUpper(u.Name) }))
	eq(t, "map", upper, []string{"ALICE", "BOB", "CAROL", "DAVE"})
}

func TestWhereClauses(t *testing.T) {
	setup(t)
	get := func(q UserQuery) []string { return names(must(q.OrderBy(Users.ID.Asc()).Get())) }

	eq(t, "orWhere", get(Users.Where(Users.Karma.Gt(90)).OrWhere(Users.Name.Eq("Bob"))), []string{"Bob", "Carol"})
	eq(t, "whereNot", get(Users.Query().WhereNot(Users.Active.Eq(true))), []string{"Carol"})
	eq(t, "nested", get(Users.Where(Users.Active.Eq(true), orm.Or(Users.Karma.Lt(5), Users.Karma.Gt(40)))), []string{"Alice", "Dave"})
	eq(t, "in", get(Users.Where(Users.ID.In(2, 3))), []string{"Bob", "Carol"})
	eq(t, "between", get(Users.Where(Users.Karma.Between(10, 50))), []string{"Alice", "Bob"})
	eq(t, "like", get(Users.Where(Users.Name.Like("%o%"))), []string{"Bob", "Carol"})
	eq(t, "isNull", get(Users.Where(Users.Settings.IsNull())), []string{"Carol", "Dave"})
	eq(t, "whereKey", get(Users.Query().WhereKey(1, 4)), []string{"Alice", "Dave"})
	eq(t, "whereColumn", get(Users.Where(Users.CreatedAt.EqCol(Users.UpdatedAt))), []string{"Alice", "Bob", "Carol", "Dave"})
	eq(t, "anyOf", get(Users.Where(orm.AnyOf("LIKE", "%car%", Users.Name, Users.Email))), []string{"Carol"})
	eq(t, "raw", get(Users.Where(orm.Raw[User]("karma % 2 = ?", 1))), []string{"Bob", "Carol", "Dave"})
	eq(t, "when", get(Users.Query().When(true, func(q UserQuery) UserQuery { return q.Where(Users.Karma.Gt(40)) })), []string{"Alice", "Carol"})
	eq(t, "unless", len(get(Users.Query().Unless(true, func(q UserQuery) UserQuery { return q.Where(Users.Karma.Gt(40)) }))), 4)

	// Date and JSON functions, rendered per dialect.
	posts := func(q PostQuery) []string { return titles(must(q.OrderBy(Posts.ID.Asc()).Get())) }
	eq(t, "whereYear", len(posts(Posts.Where(orm.Year(Posts.CreatedAt).Eq(2026)))), 6)
	eq(t, "whereDate", posts(Posts.Where(orm.Date(Posts.CreatedAt).Eq("2026-01-10"))), []string{"Generics in Go 1.27"})
	eq(t, "whereMonth", posts(Posts.Where(orm.Month(Posts.CreatedAt).Eq(3))), []string{"Iterators"})
	eq(t, "json path", get(Users.Where(Users.Settings.JSON[string]("theme").Eq("dark"))), []string{"Alice"})
	eq(t, "jsonContains", get(Users.Where(Users.Settings.JSON[string]("labels").JSONContains("admin"))), []string{"Alice"})
	eq(t, "jsonLength", get(Users.Where(Users.Settings.JSON[string]("labels").JSONLength().Eq(0))), []string{"Bob"})

	// Subqueries.
	popularAuthors := Posts.Where(Posts.Views.Gt(200)).Subquery(Posts.UserID)
	eq(t, "whereIn subquery", get(Users.Where(Users.ID.InSub(popularAuthors))), []string{"Alice", "Carol"})
	eq(t, "whereExists", get(Users.Where(orm.Exists[User](Posts.Where(Posts.UserID.EqCol(Users.ID), Posts.Views.Gt(500))))), []string{"Alice"})

	raw := must(Users.Where(Users.Name.Eq("O'Brien")).ToRawSQL())
	if !strings.Contains(raw, `name`+quoteChar()+` = 'O''Brien'`) {
		t.Errorf("ToRawSQL: %s", raw)
	}
}

func TestSelectsJoinsUnions(t *testing.T) {
	setup(t)

	latest := Posts.Where(Posts.UserID.EqCol(Users.ID)).Latest().Limit(1).Subquery(Posts.Title)
	users := must(Users.Query().AddSelectAs(Users.LatestPostTitle, orm.IfNull(latest, "")).OrderBy(Users.ID.Asc()).Get())
	got := []string{}
	for _, u := range users {
		got = append(got, u.LatestPostTitle)
	}
	eq(t, "addSelect subquery", got, []string{"Iterators", "Hello world", "Carol's post", ""})

	partial := must(Users.Query().Select(Users.ID, Users.Name).OrderBy(Users.ID.Asc()).First())
	eq(t, "select", [2]string{partial.Name, partial.Email}, [2]string{"Alice", ""})

	prolific := must(Users.Query().
		Join(Posts, Posts.UserID.EqCol(Users.ID)).
		WhereOf(Posts.Published.Eq(true)).
		GroupBy(Users.ID).
		Having(orm.Count[User]().Gte(2)).
		Pluck(Users.Name))
	eq(t, "join/groupBy/having", prolific, []string{"Alice"})

	leftJoined := must(Users.Query().LeftJoin(Posts, Posts.UserID.EqCol(Users.ID)).Distinct().Count())
	eq(t, "leftJoin distinct count", leftJoined, int64(4))

	union := must(Users.Where(Users.Karma.Gt(90)).Union(Users.Where(Users.Name.Eq("Bob"))).Get())
	eq(t, "union", len(union), 2)
	eq(t, "union count", must(Users.Where(Users.Karma.Gt(90)).UnionAll(Users.Where(Users.Karma.Gt(90))).Count()), int64(2))
	newest := Users.Query().OrderBy(Users.ID.Desc()).Limit(1)
	eq(t, "union branch keeps its limit", len(must(Users.Where(Users.Name.Eq("Alice")).Union(newest).Get())), 2)
	eq(t, "union ordered by a column", must(Users.Where(Users.Name.Eq("Bob")).Union(Users.Where(Users.Name.Eq("Alice"))).OrderBy(Users.Name.Asc()).Pluck(Users.Name)), []string{"Alice", "Bob"})

	eq(t, "distinct", must(Posts.Query().Distinct().OrderBy(Posts.UserID.Asc()).Pluck(Posts.UserID)), []int64{1, 2, 3, 5, 99})
	eq(t, "inRandomOrder", len(must(Users.Query().InRandomOrder().Get())), 4)
	eq(t, "reorder", must(Users.Query().OrderBy(Users.Name.Desc()).Reorder(Users.Name.Asc()).Value(Users.Name)), "Alice")
}

func TestEagerLoading(t *testing.T) {
	ctx := setup(t)

	users := must(Users.Query().
		Where(Users.Active.Eq(true)).
		OrderBy(Users.ID.Asc()).
		With(UserPosts, Published, func(q PostQuery) PostQuery {
			return q.OrderBy(Posts.Views.Desc()).With(PostComments)
		}).
		With(UserLatestPost).
		With(UserCountry).
		With(UserRoles, func(q RoleQuery) RoleQuery { return q.OrderBy(Roles.ID.Asc()) }).
		With(UserAvatar).
		Get())

	alice, bob, dave := users[0], users[1], users[2]
	eq(t, "hasMany constrained", titles(alice.Posts), []string{"Generics in Go 1.27", "Iterators"})
	eq(t, "nested morphMany", len(alice.Posts[0].Comments), 2)
	eq(t, "hasMany empty", len(dave.Posts), 0)
	eq(t, "latestOfMany", alice.LatestPost.Title, "Iterators")
	eq(t, "latestOfMany nil", dave.LatestPost == nil, true)
	popular := must(Users.Query().WhereHas(UserPosts, Popular(100)).Pluck(Users.Name))
	latestPopular := must(Users.Query().WhereHas(UserLatestPost, Popular(100)).Pluck(Users.Name))
	eq(t, "whereHas on latestOfMany tests only the latest post", slices.Contains(popular, "Alice") && !slices.Contains(latestPopular, "Alice"), true)
	counted := must(Users.Query().WithCount(UserLatestPost, Users.PostsCount).OrderBy(Users.ID.Asc()).Get())
	eq(t, "withCount on latestOfMany counts the pick", []int64{counted[0].PostsCount, counted[3].PostsCount}, []int64{1, 0})
	eq(t, "latestOfMany for one parent", must(UserLatestPost.Of(&alice).Pluck(Posts.Title)), []string{"Iterators"})
	var eagerSQL []string
	stop := orm.Listen(func(e orm.QueryEvent) { eagerSQL = append(eagerSQL, e.SQL) })
	scoped := must(Users.Query().Where(Users.ID.Eq(alice.ID)).With(UserLatestPost, Popular(100)).First())
	stop()
	eq(t, "an eager scope filters the pick instead of choosing among its rows", scoped.LatestPost == nil, true)
	eq(t, "the pick is made in SQL", slices.ContainsFunc(eagerSQL, func(q string) bool { return strings.Contains(q, "laravel_reserved_2") }), true)
	oldest := orm.HasOne(Users.ID, Posts.UserID, func(u *User, p *Post) { u.LatestPost = p }).OfMany(Posts.CreatedAt, false)
	eq(t, "oldestOfMany by a time column", must(Users.Query().Where(Users.ID.Eq(alice.ID)).With(oldest).First()).LatestPost.Title, "Generics in Go 1.27")
	eq(t, "belongsTo", alice.Country.Name, "Denmark")
	eq(t, "belongsToMany", []string{alice.Roles[0].Name, alice.Roles[1].Name}, []string{"admin", "editor"})
	eq(t, "pivot", alice.Roles[1].Pivot.GrantedBy, "bob")
	eq(t, "bob roles", len(bob.Roles), 1)
	eq(t, "morphOne", alice.Avatar.URL, "alice.png")
	eq(t, "morphOne nil", bob.Avatar == nil, true)

	perParent := func(offset int) []User {
		return must(Users.Query().Where(Users.Active.Eq(true)).OrderBy(Users.ID.Asc()).
			With(UserPosts, func(q PostQuery) PostQuery { return q.OrderBy(Posts.Views.Desc()).Limit(1).Offset(offset) }).
			Get())
	}
	for i, u := range perParent(0) {
		all := must(Posts.Where(Posts.UserID.Eq(u.ID)).OrderBy(Posts.Views.Desc()).Pluck(Posts.Title))
		eq(t, u.Name+": eager limit applies per parent", titles(u.Posts), all[:min(1, len(all))])
		second := perParent(1)[i]
		eq(t, u.Name+": eager offset applies per parent", titles(second.Posts), all[min(1, len(all)):min(2, len(all))])
	}
	_, err := Users.Query().With(UserRoles, func(q RoleQuery) RoleQuery { return q.Limit(1) }).Get()
	eq(t, "many-to-many eager limit refused", err != nil && strings.Contains(err.Error(), "cannot apply per parent"), true)

	posts := must(Posts.Query().OrderBy(Posts.ID.Asc()).With(PostAuthor).With(PostTags).Get())
	eq(t, "belongsTo default (orphan)", posts[6].Author.Name, "Guest Author")
	eq(t, "belongsTo default (trashed owner)", posts[5].Author.Name, "Guest Author")
	eq(t, "morphToMany", len(posts[0].Tags), 2)

	tags := must(Tags.Query().OrderBy(Tags.ID.Asc()).With(TagPosts).Get())
	eq(t, "morphedByMany", titles(tags[0].Posts), []string{"Generics in Go 1.27"})
	eq(t, "morphedByMany 2", len(tags[1].Posts), 2)

	comments := must(Comments.Query().OrderBy(Comments.ID.Asc()).With(CommentCommentable).Get())
	eq(t, "morphTo post", comments[0].Commentable.(*Post).Title, "Generics in Go 1.27")
	eq(t, "morphTo video", comments[3].Commentable.(*Video).Title, "Intro video")

	countries := must(Countries.Query().OrderBy(Countries.ID.Asc()).With(CountryPosts).Get())
	eq(t, "hasManyThrough", len(countries[0].Posts), 4)
	eq(t, "hasManyThrough 2", titles(countries[1].Posts), []string{"Carol's post"})

	// Lazy eager loading onto already-loaded models.
	plain := must(Users.Query().OrderBy(Users.ID.Asc()).Get())
	if err := orm.Load(ctx, plain, UserPosts); err != nil {
		t.Fatal(err)
	}
	eq(t, "load", len(plain[0].Posts), 3)
}

func TestRelationQueries(t *testing.T) {
	setup(t)
	get := func(q UserQuery) []string { return names(must(q.OrderBy(Users.ID.Asc()).Get())) }

	eq(t, "whereHas", get(Users.Query().WhereHas(UserPosts, Published, Popular(100))), []string{"Alice", "Bob", "Carol"})
	eq(t, "whereDoesntHave", get(Users.Query().WhereDoesntHave(UserPosts)), []string{"Dave"})
	eq(t, "orWhereHas", get(Users.Where(Users.Name.Eq("Dave")).OrWhereHas(UserRoles)), []string{"Alice", "Bob", "Dave"})
	eq(t, "whereRelation", get(Users.Query().WhereRelation(UserRoles, Roles.Name.Eq("admin"))), []string{"Alice"})
	eq(t, "hasCount", get(Users.Query().HasCount(UserPosts, orm.Gte, 2)), []string{"Alice"})
	_, err := Users.Query().HasCount(UserPosts, "= 0 OR 1 = 1 --", 2).Get()
	eq(t, "hasCount refuses an operator outside the constants", err != nil && strings.Contains(err.Error(), "unknown comparison operator"), true)
	eq(t, "whereHas through", len(must(Countries.Query().WhereHas(CountryPosts, Popular(500)).Get())), 1)

	editor := must(Roles.Where(Roles.Name.Eq("editor")).First())
	eq(t, "whereAttachedTo", get(Users.Where(UserRoles.AttachedTo(&editor))), []string{"Alice", "Bob"})

	alice := must(Users.Find(1))
	eq(t, "whereBelongsTo", len(must(Posts.Where(PostAuthor.Is(&alice)).Get())), 3)
	eq(t, "morphToMany whereHas", titles(must(Posts.Query().WhereHas(PostTags, func(q TagQuery) TagQuery {
		return q.Where(Tags.Name.Eq("go"))
	}).Get())), []string{"Generics in Go 1.27"})

	onPopular := must(Comments.Where(CommentCommentable.HasMorph(Posts.ID, Published, Popular(100))).OrderBy(Comments.ID.Asc()).Pluck(Comments.Body))
	eq(t, "whereHasMorph", onPopular, []string{"Great", "Finally", "Welcome"})
	reply := Comment{CommentableID: 1, CommentableType: "comments", Body: "Reply"}
	ok(Comments.Create(&reply))
	replies := must(Comments.Where(CommentCommentable.HasMorph(Comments.ID)).Pluck(Comments.Body))
	eq(t, "whereHasMorph on its own table", replies, []string{"Reply"})
	must(Comments.Query().WhereKey(reply.ID).Delete())

	stats := must(Users.Query().
		WithCount(UserPosts, Users.PostsCount).
		WithSum(UserPosts, Posts.Views, Users.PostViews, Published).
		WithExists(UserAvatar, Users.HasAvatar).
		OrderBy(Users.ID.Asc()).
		Get())
	var counts, views []int64
	var avatars []bool
	for _, u := range stats {
		counts, views, avatars = append(counts, u.PostsCount), append(views, u.PostViews), append(avatars, u.HasAvatar)
	}
	eq(t, "withCount", counts, []int64{3, 1, 1, 0})
	eq(t, "withSum", views, []int64{628, 150, 300, 0})
	eq(t, "withExists", avatars, []bool{true, false, false, false})

	// Relationship query builders ($user->posts()->where(...)).
	eq(t, "of hasMany", must(UserPosts.Of(&alice).Where(Posts.Published.Eq(true)).Count()), int64(2))
	eq(t, "of belongsToMany", must(UserRoles.Of(&alice).OrderBy(Roles.ID.Asc()).Pluck(Roles.Name)), []string{"admin", "editor"})
	eq(t, "of through", must(CountryPosts.Of(&Country{ID: 2}).Pluck(Posts.Title)), []string{"Carol's post"})

	// A relation from a table to itself: users sharing a country with a
	// high-karma user. The inner users table is aliased automatically.
	compatriots := orm.HasMany(Users.CountryID, Users.CountryID, func(*User, []User) {})
	eq(t, "self-referencing whereHas", get(Users.Query().WhereHas(compatriots, func(q UserQuery) UserQuery {
		return q.Where(Users.Karma.Gt(90))
	})), []string{"Carol", "Dave"})
}

func TestWrites(t *testing.T) {
	ctx := setup(t)
	assertLimitedMutations(t, ctx, orm.DefaultConnection)
	assertLikes(t, ctx, orm.DefaultConnection)
	assertInjectionSafety(t, ctx, orm.DefaultConnection)
	assertJSON(t, ctx, orm.DefaultConnection)
	assertBigNumbers(t, ctx, orm.DefaultConnection, driver == "mysql")

	u := User{Name: "Frank", Email: "frank@example.com", Active: true, CountryID: 2}
	if err := Users.Create(&u); err != nil {
		t.Fatal(err)
	}
	eq(t, "auto id", u.ID, int64(6))
	eq(t, "exists", u.Exists() && u.WasRecentlyCreated(), true)
	eq(t, "timestamps", u.CreatedAt.IsZero() || u.UpdatedAt.IsZero(), false)

	u.Karma = 10
	eq(t, "isDirty", Users.Karma.IsDirty(&u), true)
	eq(t, "original", Users.Karma.Original(&u), 0)
	eq(t, "dirty", Users.Dirty(&u), []string{"karma"})
	if err := Users.Save(&u); err != nil {
		t.Fatal(err)
	}
	eq(t, "wasChanged", Users.Karma.WasChanged(&u), true)
	eq(t, "clean after save", Users.IsClean(&u), true)
	eq(t, "persisted", must(Users.Find(6)).Karma, 10)
	if err := Users.Save(&u); err != nil {
		t.Fatal(err)
	}
	eq(t, "no-op save", Users.WasChanged(&u), false)

	u.Settings.V.Labels = []string{"a"}
	ok(Users.Save(&u))
	u.Settings.V.Labels[0] = "b"
	eq(t, "in-place slice edit is dirty", Users.Dirty(&u), []string{"settings"})
	ok(Users.Save(&u))
	eq(t, "in-place slice edit saved", must(Users.Find(6)).Settings.V.Labels, []string{"b"})
	deleted := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	trashed := must(Users.Query().WithTrashed().Find(6))
	trashed.DeletedAt = new(deleted)
	ok(Users.Save(&trashed))
	*trashed.DeletedAt = deleted.Add(time.Hour)
	eq(t, "in-place pointee edit is dirty", Users.Dirty(&trashed), []string{"deleted_at"})
	eq(t, "original is a snapshot", Users.DeletedAt.Original(&trashed).Equal(deleted), true)
	trashed.DeletedAt = nil
	ok(Users.Save(&trashed))
	u = must(Users.Find(6))

	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*3600))
	frank := Users.Where(Users.ID.Eq(6))
	must(frank.Update(Users.CreatedAt.Set(at)))
	eq(t, "offset time reads back as the same instant", must(Users.Find(6)).CreatedAt.Equal(at), true)
	eq(t, "offset time equals its UTC form", must(frank.Where(Users.CreatedAt.Eq(at.UTC())).Count()), int64(1))
	eq(t, "offset time orders as an instant", must(frank.Where(Users.CreatedAt.Gt(time.Date(2026, 3, 1, 10, 30, 0, 0, time.UTC))).Count()), int64(0))
	eq(t, "raw SQL inlines times in UTC", strings.Contains(must(Users.Where(Users.CreatedAt.Eq(at)).ToRawSQL()), "'2026-03-01 10:00:00'"), true)

	ok(Users.Query().Insert(User{ID: 50, Name: "Keyed", Email: "keyed@example.com"}, User{Name: "Generated", Email: "generated@example.com"}))
	eq(t, "mixed keys keep the explicit key", must(Users.Find(50)).Name, "Keyed")
	generated := must(Users.Where(Users.Email.Eq("generated@example.com")).First())
	eq(t, "mixed keys generate the missing key", generated.ID != 0 && generated.ID != 50, true)
	eq(t, "mixed keys cleanup", must(Users.Query().WhereKey(int64(50), generated.ID).ForceDelete()), int64(2))

	eq(t, "mass update", must(Users.Where(Users.CountryID.Eq(2)).Update(Users.Active.Set(false))), int64(3))
	eq(t, "increment", must(Posts.Where(Posts.ID.Eq(1)).Increment(Posts.Views, 10)), int64(1))
	eq(t, "incremented", must(Posts.Find(1)).Views, int64(550))
	must(Posts.Where(Posts.ID.Eq(1)).Decrement(Posts.Views, 50))
	eq(t, "decremented", must(Posts.Find(1)).Views, int64(500))

	// Soft deletes.
	if err := Users.Delete(&u); err != nil {
		t.Fatal(err)
	}
	eq(t, "trashed", Users.Trashed(&u), true)
	_, err := Users.Find(6)
	eq(t, "hidden after delete", errors.Is(err, orm.ErrNotFound), true)
	eq(t, "withTrashed find", must(Users.Query().WithTrashed().Find(6)).Name, "Frank")
	if err := Users.Restore(&u); err != nil {
		t.Fatal(err)
	}
	eq(t, "restored", must(Users.Find(6)).Name, "Frank")
	if err := Users.ForceDelete(&u); err != nil {
		t.Fatal(err)
	}
	eq(t, "force deleted", must(Users.Query().WithTrashed().Where(Users.ID.Eq(6)).Count()), int64(0))
	eq(t, "mass restore", must(Users.Where(Users.Name.Eq("Eve")).Restore()), int64(1))
	eq(t, "restore skips live rows", must(Users.Query().Restore()), int64(0))
	eq(t, "mass soft delete", must(Users.Where(Users.Name.Eq("Eve")).Delete()), int64(1))
	eq(t, "destroy", must(Users.Destroy(4)), 1)
	eq(t, "destroyed is trashed", must(Users.Query().OnlyTrashed().Count()), int64(2))

	// firstOrCreate / updateOrCreate.
	existing := must(Users.Query().FirstOrCreate(orm.Attrs(Users.Email.Set("alice@example.com")), Users.Name.Set("ignored")))
	eq(t, "firstOrCreate existing", existing.Name, "Alice")
	created := must(Users.Query().FirstOrCreate(orm.Attrs(Users.Email.Set("new@example.com")), Users.Name.Set("Newbie")))
	eq(t, "firstOrCreate new", [2]any{created.Name, created.Exists()}, [2]any{"Newbie", true})
	updated := must(Users.Query().UpdateOrCreate(orm.Attrs(Users.Email.Set("bob@example.com")), Users.Karma.Set(1000)))
	eq(t, "updateOrCreate", must(Users.Find(updated.ID)).Karma, 1000)
	fresh := must(Users.Query().FirstOrNew(orm.Attrs(Users.Email.Set("ghost@example.com"))))
	eq(t, "firstOrNew unsaved", fresh.Exists(), false)

	// Bulk inserts and upserts.
	if err := Roles.Query().Upsert([]Role{{ID: 1, Name: "superadmin"}, {ID: 4, Name: "guest"}}, []orm.AnyColumn[Role]{Roles.ID}); err != nil {
		t.Fatal(err)
	}
	eq(t, "upsert", must(Roles.Query().OrderBy(Roles.ID.Asc()).Pluck(Roles.Name)), []string{"superadmin", "editor", "viewer", "guest"})
	if err := Tags.Query().InsertOrIgnore(Tag{Name: "go"}, Tag{Name: "sql"}); err != nil {
		t.Fatal(err)
	}
	eq(t, "insertOrIgnore", must(Tags.Query().Count()), int64(3))
	if err := Videos.Query().Insert(Video{Title: "A"}, Video{Title: "B"}); err != nil {
		t.Fatal(err)
	}
	eq(t, "insert", must(Videos.Query().Count()), int64(3))

	// Replicate, refresh, touch.
	post := must(Posts.Find(1))
	clone := must(Posts.Replicate(&post, Posts.Views))
	eq(t, "replicate", [3]any{clone.ID, clone.Title, clone.Views}, [3]any{int64(0), post.Title, int64(0)})
	if err := Posts.Create(&clone); err != nil {
		t.Fatal(err)
	}
	eq(t, "replica saved", clone.ID, int64(8))
	untouched := must(Posts.Find(5))
	before := untouched.UpdatedAt
	if err := Posts.Touch(&untouched); err != nil {
		t.Fatal(err)
	}
	eq(t, "touch", untouched.UpdatedAt.After(before) && must(Posts.Find(5)).UpdatedAt.Equal(untouched.UpdatedAt), true)
	post.Title = "scribbled"
	if err := Posts.Refresh(&post); err != nil {
		t.Fatal(err)
	}
	eq(t, "refresh", post.Title, "Generics in Go 1.27")
	eq(t, "is", Posts.Is(&post, &Post{ID: 1}), true)

	// UUID keys on a second connection.
	entry := AuditEntry{Action: "login"}
	if err := AuditLog.Create(&entry); err != nil {
		t.Fatal(err)
	}
	eq(t, "uuid v7", len(entry.ID) == 36 && entry.ID[14] == '7', true)
	eq(t, "audit connection", must(AuditLog.Query().Count()), int64(1))
	if _, err := AuditLog.Query().On(orm.DefaultConnection).Count(); err == nil {
		t.Error("On(default) should hit the main DB, which has no audit_log table")
	}
}

func TestRelationWrites(t *testing.T) {
	setup(t)
	dave := must(Users.Find(4))
	bob := must(Users.Find(2))

	p := Post{Title: "Dave's first"}
	if err := UserPosts.Create(&dave, &p); err != nil {
		t.Fatal(err)
	}
	eq(t, "hasMany create", p.UserID, int64(4))
	eq(t, "of count", must(UserPosts.Of(&dave).Count()), int64(1))

	c := Comment{Body: "first!"}
	if err := PostComments.Create(&p, &c); err != nil {
		t.Fatal(err)
	}
	eq(t, "morphMany create", [2]any{c.CommentableID, c.CommentableType}, [2]any{p.ID, "posts"})

	PostAuthor.Associate(&p, &bob)
	eq(t, "associate", [2]any{p.UserID, p.Author.Name}, [2]any{int64(2), "Bob"})
	PostAuthor.Dissociate(&p)
	eq(t, "dissociate", p.UserID, int64(0))

	video := must(Videos.Find(1))
	eq(t, "morphTo associate", CommentCommentable.Associate(&c, &video) && c.CommentableType == "videos", true)

	// Pivot operations.
	roleIDs := func() []int64 {
		ids := must(UserRoles.Of(&bob).Pluck(Roles.ID))
		slices.Sort(ids)
		return ids
	}
	if err := UserRoles.AttachIDs(&bob, 1); err != nil {
		t.Fatal(err)
	}
	if err := UserRoles.Attach(&bob, RoleUser{RoleID: 3, GrantedBy: "test"}); err != nil {
		t.Fatal(err)
	}
	eq(t, "attach", roleIDs(), []int64{1, 2, 3})
	pivot := must(RoleUsers.Where(RoleUsers.UserID.Eq(2), RoleUsers.RoleID.Eq(3)).First())
	eq(t, "pivot timestamps", pivot.CreatedAt.IsZero(), false)

	res := must(UserRoles.Sync(&bob, 1, 3))
	eq(t, "sync", [2][]int64{res.Attached, res.Detached}, [2][]int64{nil, {2}})
	res = must(UserRoles.Toggle(&bob, 1, 2))
	eq(t, "toggle", [2][]int64{res.Attached, res.Detached}, [2][]int64{{2}, {1}})
	eq(t, "after toggle", roleIDs(), []int64{2, 3})
	res = must(UserRoles.Toggle(&bob, 1, 1))
	eq(t, "toggle with a repeated id", [2][]int64{res.Attached, res.Detached}, [2][]int64{{1}, nil})
	res = must(UserRoles.Toggle(&bob, 1))
	eq(t, "toggle back", [2][]int64{res.Attached, res.Detached}, [2][]int64{nil, {1}})
	res = must(UserRoles.SyncWithoutDetaching(&bob, 1))
	eq(t, "syncWithoutDetaching", roleIDs(), []int64{1, 2, 3})
	eq(t, "updateExistingPivot", must(UserRoles.UpdateExistingPivot(&bob, 3, RoleUsers.GrantedBy.Set("changed"))), int64(1))
	eq(t, "detach all", must(UserRoles.Detach(&bob)), int64(3))
}

type guard struct{ created int }

func (g *guard) Creating(_ context.Context, u *User) error {
	if strings.HasSuffix(u.Email, "@spam.test") {
		return errors.New("spam rejected")
	}
	return nil
}

func (g *guard) Created(context.Context, *User) error { g.created++; return nil }

// isolatedUsers is a fresh Table for users, so events and global scopes
// registered in tests don't leak into the shared Users schema.
func isolatedUsers() *orm.Table[User, int64] {
	return &orm.Table[User, int64]{
		Name: Users.Table.Name, PrimaryKey: Users.PrimaryKey, KeyType: Users.KeyType,
		CreatedAt: Users.Table.CreatedAt, UpdatedAt: Users.Table.UpdatedAt, DeletedAt: Users.Table.DeletedAt,
		Columns: Users.Columns, NullZero: Users.NullZero, Ptr: Users.Ptr, State: Users.State,
	}
}

func TestEventsAndScopes(t *testing.T) {
	ctx := setup(t)
	users := isolatedUsers()
	g := &guard{}
	users.Observe(g)
	var saved []string
	users.On(orm.Saved, func(_ context.Context, u *User) error { saved = append(saved, u.Name); return nil })

	err := users.Create(&User{Name: "Spammer", Email: "x@spam.test"})
	eq(t, "creating aborts", err != nil && strings.Contains(err.Error(), "spam"), true)
	if err := users.Create(&User{Name: "Gina", Email: "gina@example.com"}); err != nil {
		t.Fatal(err)
	}
	if err := users.WithContext(orm.WithoutEvents(ctx)).Create(&User{Name: "Quiet", Email: "x2@spam.test"}); err != nil {
		t.Fatal(err)
	}
	eq(t, "created event", g.created, 1)
	eq(t, "saved event", saved, []string{"Gina"})

	users.AddGlobalScope("karma", func(q UserQuery) UserQuery { return q.Where(Users.Karma.Gt(10)) })
	eq(t, "global scope", must(users.Query().Count()), int64(3))
	eq(t, "withoutGlobalScope", must(users.Query().WithoutGlobalScope("karma").Count()), int64(6))
	eq(t, "withoutGlobalScopes", must(users.Query().WithoutGlobalScopes().Count()), int64(7))

	querying := isolatedUsers()
	var seen []int64
	querying.On(orm.Created, func(ctx context.Context, u *User) error {
		n, err := querying.Query().Where(Users.ID.Eq(u.ID)).WithContext(ctx).Count()
		seen = append(seen, n)
		return err
	})
	ok(querying.Create(&User{Name: "Hal", Email: "hal@example.com"}))
	ok(orm.Transaction(ctx, func(ctx context.Context) error {
		return querying.WithContext(ctx).Create(&User{Name: "Ida", Email: "ida@example.com"})
	}))
	eq(t, "created listeners can query", seen, []int64{1, 1})

	var events int
	var stop func()
	stop = orm.Listen(func(orm.QueryEvent) { events++; stop() })
	must(Users.Query().Count())
	must(Users.Query().Count())
	eq(t, "a listener can stop itself", events, 1)
}

func TestPaginationAndIteration(t *testing.T) {
	setup(t)
	for _, name := range []string{"Nil One", "Nil Two"} {
		ok(Users.Create(&User{Name: name, Email: strings.ReplaceAll(strings.ToLower(name), " ", ".") + "@example.com"}))
	}
	total := must(Users.Query().WithTrashed().Count())
	eq(t, "users without a country", must(Users.Where(Users.CountryID.IsNull()).Count()), int64(2))
	for _, desc := range []bool{false, true} {
		order := Users.CountryID.Asc()
		if desc {
			order = Users.CountryID.Desc()
		}
		seen := map[int64]bool{}
		for cursor, pages := "", 0; ; pages++ {
			if pages > int(total) {
				t.Fatal("cursor pages never end")
			}
			page := must(Users.Query().WithTrashed().OrderBy(order, Users.ID.Asc()).CursorPaginate(2, cursor))
			for _, u := range page.Items {
				seen[u.ID] = true
			}
			if cursor = page.NextCursor; cursor == "" {
				break
			}
		}
		eq(t, fmt.Sprintf("cursor pages over NULLs (desc %v)", desc), int64(len(seen)), total)
	}
	rowsSeen := map[any]bool{}
	for cursor, pages := "", 0; ; pages++ {
		if pages > int(total) {
			t.Fatal("row cursor pages never end")
		}
		page := must(orm.From("users").OrderBy("country_id").OrderByDesc("created_at").OrderBy("id").CursorPaginate(2, cursor))
		for _, r := range page.Items {
			rowsSeen[fmt.Sprint(r["id"])] = true
		}
		if cursor = page.NextCursor; cursor == "" {
			break
		}
	}
	eq(t, "row cursor pages over NULLs and times", int64(len(rowsSeen)), total)
	for _, desc := range []bool{false, true} {
		order := Users.CountryID.Asc()
		if desc {
			order = Users.CountryID.Desc()
		}
		ordered := must(Users.Query().WithTrashed().OrderBy(order.NullsLast(), Users.ID.Asc()).Get())
		tail := ordered[len(ordered)-2:]
		eq(t, fmt.Sprintf("nulls last (desc %v)", desc), fmt.Sprint(tail[0].CountryID, tail[1].CountryID), "0 0")
		eq(t, fmt.Sprintf("nulls last keeps the values first (desc %v)", desc), ordered[0].CountryID != 0, true)
	}
	newest := must(Users.Query().WithTrashed().Latest().CursorPaginate(2, ""))
	eq(t, "latest pages by cursor", len(newest.Items), 2)
	_, err := Countries.Query().Latest().Get()
	eq(t, "latest without a created-at column refused", err != nil && strings.Contains(err.Error(), "created-at"), true)
	byTie := map[int64]bool{}
	for page := 1; page <= int(total); page++ {
		for _, u := range must(Users.Query().WithTrashed().OrderBy(Users.Active.Desc()).Paginate(1, page)).Items {
			byTie[u.ID] = true
		}
	}
	eq(t, "offset pages over tied orders cover every row once", int64(len(byTie)), total)
	rowTie := map[string]bool{}
	ok(orm.From("users").Chunk(2, func(rows []map[string]any) error {
		for _, r := range rows {
			rowTie[fmt.Sprint(r["id"])] = true
		}
		return nil
	}))
	eq(t, "unordered row chunks cover every row once", int64(len(rowTie)), total)
	must(Users.Query().Where(Users.CountryID.IsNull()).ForceDelete())
	ordered := Posts.Query().OrderBy(Posts.Views.Desc(), Posts.ID.Asc())
	all := titles(must(ordered.Get()))

	page := must(ordered.Paginate(3, 2))
	eq(t, "paginate", [4]any{page.Total, page.LastPage, page.CurrentPage, titles(page.Items)}, [4]any{int64(7), 3, 2, all[3:6]})
	simple := must(ordered.SimplePaginate(3, 3))
	eq(t, "simplePaginate", [2]any{len(simple.Items), simple.HasMore}, [2]any{1, false})

	var viaCursor []string
	cursor := ""
	for {
		p := must(ordered.CursorPaginate(2, cursor))
		viaCursor = append(viaCursor, titles(p.Items)...)
		if cursor = p.NextCursor; cursor == "" {
			break
		}
	}
	eq(t, "cursorPaginate", viaCursor, all)

	var sizes []int
	if err := Posts.Query().OrderBy(Posts.ID.Asc()).Chunk(2, func(ps []Post) error { sizes = append(sizes, len(ps)); return nil }); err != nil {
		t.Fatal(err)
	}
	eq(t, "chunk", sizes, []int{2, 2, 2, 1})
	sizes = nil
	if err := Posts.Query().ChunkByID(3, func(ps []Post) error {
		sizes = append(sizes, len(ps))
		return orm.ErrStop
	}); err != nil {
		t.Fatal(err)
	}
	eq(t, "chunkById stop", sizes, []int{3})

	n, withComments := 0, 0
	for p, err := range Posts.Query().With(PostComments).Lazy(2) {
		if err != nil {
			t.Fatal(err)
		}
		n++
		withComments += len(p.Comments)
	}
	eq(t, "lazy", [2]int{n, withComments}, [2]int{7, 4})
	n = 0
	for _, err := range Posts.Query().Cursor() {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	eq(t, "cursor", n, 7)
}

func TestTransactions(t *testing.T) {
	ctx := setup(t)
	err := orm.Transaction(ctx, func(ctx context.Context) error {
		if err := Users.WithContext(ctx).Create(&User{Name: "Temp", Email: "temp@example.com"}); err != nil {
			return err
		}
		eq(t, "visible inside", must(Users.Where(Users.Name.Eq("Temp")).WithContext(ctx).Exists()), true)
		return errors.New("roll back")
	})
	eq(t, "error returned", err.Error(), "roll back")
	eq(t, "rolled back", must(Users.Where(Users.Name.Eq("Temp")).Exists()), false)

	if err := orm.Transaction(ctx, func(ctx context.Context) error {
		return Users.WithContext(ctx).Create(&User{Name: "Kept", Email: "kept@example.com"})
	}); err != nil {
		t.Fatal(err)
	}
	eq(t, "committed", must(Users.Where(Users.Name.Eq("Kept")).Exists()), true)
}

func sprint(v any) string { return fmt.Sprintf("%v", v) }

func quoteChar() string {
	if driver == "mysql" {
		return "`"
	}
	return `"`
}

func TestTransactionsAdvanced(t *testing.T) {
	ctx := setup(t)
	var committed []string
	err := orm.Transaction(ctx, func(ctx context.Context) error {
		ok(Users.WithContext(ctx).Create(&User{Name: "Outer", Email: "outer@example.com"}))
		orm.AfterCommit(ctx, func(context.Context) { committed = append(committed, "outer") })
		inner := orm.Transaction(ctx, func(ctx context.Context) error {
			ok(Users.WithContext(ctx).Create(&User{Name: "Inner", Email: "inner@example.com"}))
			orm.AfterCommit(ctx, func(context.Context) { committed = append(committed, "inner") })
			return errors.New("undo inner")
		})
		eq(t, "inner error", inner.Error(), "undo inner")
		eq(t, "inner rolled back", must(Users.Where(Users.Name.Eq("Inner")).WithContext(ctx).Exists()), false)
		eq(t, "outer intact", must(Users.Where(Users.Name.Eq("Outer")).WithContext(ctx).Exists()), true)
		eq(t, "callbacks deferred", len(committed), 0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "afterCommit", committed, []string{"outer"})
	eq(t, "outer committed", must(Users.Where(Users.Name.Eq("Outer")).Exists()), true)

	ran := false
	orm.AfterCommit(ctx, func(context.Context) { ran = true })
	eq(t, "afterCommit outside tx runs now", ran, true)

	var seen []string
	stop := orm.Listen(func(e orm.QueryEvent) { seen = append(seen, e.SQL) })
	must(Users.Query().Count())
	stop()
	must(Users.Query().Count())
	eq(t, "listen", len(seen) == 1 && strings.Contains(seen[0], "COUNT(*)"), true)
}

func TestMoreWrites(t *testing.T) {
	ctx := setup(t)

	// createOrFirst relies on the unique email index.
	existing := must(Users.Query().CreateOrFirst(orm.Attrs(Users.Email.Set("bob@example.com")), Users.Name.Set("Not Bob")))
	eq(t, "createOrFirst existing", existing.Name, "Bob")
	fresh := must(Users.Query().CreateOrFirst(orm.Attrs(Users.Email.Set("zed@example.com")), Users.Name.Set("Zed")))
	eq(t, "createOrFirst new", fresh.ID > 5, true)
	eq(t, "isUniqueViolation", orm.IsUniqueViolation(Users.Create(&User{Name: "Dup", Email: "zed@example.com"})), true)

	quiet := User{Name: "NoStamps", Email: "nostamps@example.com"}
	ok(Users.WithContext(orm.WithoutTimestamps(ctx)).Create(&quiet))
	eq(t, "withoutTimestamps", must(Users.Find(quiet.ID)).CreatedAt.IsZero(), true)

	eq(t, "isNot", Users.IsNot(&existing, &fresh), true)

	// Prune deletes models one by one with events; users are soft deletable, so it force deletes.
	n := must(Users.Where(Users.Email.Like("%nostamps%")).Prune(10))
	eq(t, "prune", n, 1)
	eq(t, "pruned for good", must(Users.Query().WithTrashed().Where(Users.Email.Like("%nostamps%")).Count()), int64(0))

	// insertUsing and truncate.
	copied := must(Tags.Query().InsertUsing([]orm.AnyColumn[Tag]{Tags.Name}, Roles.Query().Where(Roles.ID.Lte(2)), orm.Upper(Roles.Name)))
	eq(t, "insertUsing", copied, int64(2))
	eq(t, "insertUsing rows", must(Tags.Where(Tags.Name.In("ADMIN", "EDITOR")).Count()), int64(2))
	if err := Images.Truncate(); err != nil {
		t.Fatal(err)
	}
	eq(t, "truncate", must(Images.Query().Count()), int64(0))
	img := Image{ImageableID: 1, ImageableType: "users", URL: "again.png"}
	ok(Images.Create(&img))
	eq(t, "truncate resets ids", img.ID, int64(1))

	a, b := orm.NewULID(), orm.NewULID()
	eq(t, "ulid", len(a) == 26 && a != b, true)
}

func TestMoreQueries(t *testing.T) {
	setup(t)

	eq(t, "whereFullText", titles(must(Posts.Query().WhereFullText("generics", Posts.Title).Get())), []string{"Generics in Go 1.27"})
	eq(t, "jsonHasKey", names(must(Users.Where(Users.Settings.JSONHasKey("theme")).OrderBy(Users.ID.Asc()).Get())), []string{"Alice", "Bob"})
	eq(t, "jsonDoesntContain", names(must(Users.Where(Users.Settings.JSON[string]("labels").JSONDoesntContain("admin")).Get())), []string{"Bob"})

	// joinSub: users joined to their published post counts.
	counts := Posts.Query().Select(Posts.UserID).AddSelectAs(Posts.Views, orm.Count[Post]()).Where(Posts.Published.Eq(true)).GroupBy(Posts.UserID)
	prolific := must(Users.Query().JoinSub(counts, "pc", Posts.UserID.EqCol(Users.ID), Posts.Views.Gte(2)).Pluck(Users.Name))
	eq(t, "joinSub", prolific, []string{"Alice"})

	video := must(Videos.Find(1))
	onVideo := must(Comments.Where(CommentCommentable.Is(&video)).Pluck(Comments.Body))
	eq(t, "whereMorphedTo", onVideo, []string{"Nice video"})
}

func TestFactories(t *testing.T) {
	setup(t)
	users := orm.NewFactory(Users.Table, func(n int) User {
		return User{Name: fmt.Sprintf("Factory %d", n), Email: fmt.Sprintf("factory%d@example.com", n), Active: true}
	})
	made := must(users.Count(2).Make())
	eq(t, "make", [2]string{made[0].Name, made[1].Name}, [2]string{"Factory 1", "Factory 2"})
	eq(t, "make unsaved", made[0].Exists(), false)

	var posts int
	created := must(users.Count(4).
		State(func(u *User) { u.Karma = 7 }).
		Sequence(func(u *User) { u.CountryID = 1 }, func(u *User) { u.CountryID = 2 }).
		AfterCreating(func(ctx context.Context, u *User) error {
			posts++
			return UserPosts.WithContext(ctx).Create(u, &Post{Title: "by " + u.Name})
		}).
		Create())
	eq(t, "create", len(created), 4)
	eq(t, "state", created[3].Karma, 7)
	eq(t, "sequence", []int64{created[0].CountryID, created[1].CountryID, created[2].CountryID}, []int64{1, 2, 1})
	eq(t, "afterCreating", must(Posts.Where(Posts.Title.Like("by Factory%")).Count()), int64(4))

	layered := must(users.Count(3).
		Sequence(func(u *User) { u.CountryID = 1 }, func(u *User) { u.CountryID = 2 }).
		Sequence(func(u *User) { u.Karma = 10 }, func(u *User) { u.Karma = 20 }).
		State(func(u *User) { u.CountryID = 9 }).
		Make())
	eq(t, "every sequence applies, in call order with states", [][2]int64{
		{layered[0].CountryID, int64(layered[0].Karma)}, {layered[1].CountryID, int64(layered[1].Karma)}, {layered[2].CountryID, int64(layered[2].Karma)},
	}, [][2]int64{{9, 10}, {9, 20}, {9, 10}})
	_, err := users.Count(-1).Make()
	eq(t, "a negative count is an error", err != nil && strings.Contains(err.Error(), "-1"), true)
	_, err = users.Sequence().Create()
	eq(t, "an empty sequence is an error", err != nil && strings.Contains(err.Error(), "at least one"), true)
}

// TestBoundContexts checks every builder runs inside the transaction its
// WithContext ctx carries: a failing transaction rolls back what each one
// wrote, and a nil context is a programming error.
func TestBoundContexts(t *testing.T) {
	ctx := setup(t)
	users := orm.NewFactory(Users.Table, func(n int) User {
		return User{Name: fmt.Sprintf("Factory %d", n), Email: fmt.Sprintf("bound%d@example.com", n)}
	})
	beforeUsers, beforePosts := must(Users.Query().Count()), must(Posts.Query().Count())
	beforeRoles := must(UserRoles.Of(&User{ID: 1}).Count())
	err := orm.Transaction(ctx, func(ctx context.Context) error {
		ok(Users.WithContext(ctx).Create(&User{Name: "Session", Email: "session@example.com"}))
		must(orm.FromTable(Users).WithContext(ctx).Where("email", orm.Eq, "alice@example.com").Update(map[string]any{"karma": 0}))
		ok(UserPosts.WithContext(ctx).Create(&User{ID: 1}, &Post{Title: "bound"}))
		ok(UserRoles.WithContext(ctx).AttachIDs(&User{ID: 1}, 3))
		must(users.WithContext(ctx).Count(2).Create())
		eq(t, "a bound query sees the transaction's writes", must(Users.WithContext(ctx).Where(Users.Email.Eq("session@example.com")).Count()), int64(1))
		return errors.New("roll back")
	})
	eq(t, "the transaction failed", err != nil, true)
	eq(t, "session writes rolled back", must(Users.Query().Count()), beforeUsers)
	eq(t, "row query writes rolled back", must(Users.Find(1)).Karma, 42)
	eq(t, "relation writes rolled back", must(Posts.Query().Count()), beforePosts)
	eq(t, "pivot writes rolled back", must(UserRoles.Of(&User{ID: 1}).Count()), beforeRoles)
	eq(t, "WithContext(nil) panics", panics(func() { Users.Query().WithContext(nil) }), true) //nolint:staticcheck // a nil context is the case under test
}
