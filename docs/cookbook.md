# Cookbook

Recipes for every feature, using the example schema in
[`example/models`](../example/models). The schema has countries, users,
posts, videos, polymorphic comments, roles with a typed pivot, polymorphic
tags and images, and an audit log on a second connection. Every snippet
mirrors code exercised by [`models_test.go`](../example/models/models_test.go)
or [`schema_test.go`](../schema/schema_test.go), which run against SQLite,
Postgres, MySQL and MariaDB.

- [Setup](#setup)
- [Models](#models)
- [Retrieving](#retrieving)
- [Where clauses](#where-clauses)
- [Selects, joins, grouping, unions](#selects-joins-grouping-unions)
- [Aggregates and single values](#aggregates-and-single-values)
- [Chunking, iterators, pagination](#chunking-iterators-pagination)
- [Inserting and updating](#inserting-and-updating)
- [Deleting and soft deletes](#deleting-and-soft-deletes)
- [Dirty tracking](#dirty-tracking)
- [Relationships](#relationships)
- [Eager loading](#eager-loading)
- [Querying relationships](#querying-relationships)
- [Writing through relationships](#writing-through-relationships)
- [Scopes](#scopes)
- [Events and observers](#events-and-observers)
- [Transactions](#transactions)
- [Connections and dialects](#connections-and-dialects)
- [Factories](#factories)
- [Debugging](#debugging)
- [Schema builder](#schema-builder)
- [Migrations](#migrations)
- [Running the test databases](#running-the-test-databases)

## Setup

```go
import (
	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if _, err := orm.Open(orm.DefaultConnection, "pgx", os.Getenv("DATABASE_URL")); err != nil {
		log.Fatal(err)
	}
	// ... queries need no db handle from here on.
}
```

`orm.Open` picks the dialect from the driver name: `sqlite`/`sqlite3` map to
SQLite, `pgx`/`postgres` to Postgres, and `mysql`/`mariadb` to MySQL. For any
other `*sql.DB`, register it with `orm.AddConnection(name, db, orm.Postgres)`.

> **SQLite tip:** use `?_time_format=sqlite` with `modernc.org/sqlite` so
> `time.Time` values are stored in the format SQLite's date functions
> understand.

## Models

```go
//orm:table users soft_deletes
type User struct {
	orm.Model
	ID        int64              `db:"id"`
	CountryID int64              `db:"country_id,nullzero"`
	Name      string             `db:"name"`
	Email     string             `db:"email"`
	Active    bool               `db:"active"`
	Karma     int                `db:"karma"`
	Settings  orm.JSON[Settings] `db:"settings"`
	CreatedAt time.Time          `db:"created_at"`
	UpdatedAt time.Time          `db:"updated_at"`
	DeletedAt *time.Time         `db:"deleted_at"`

	PostsCount int64 `db:"posts_count,virtual"`

	Posts      []Post
	LatestPost *Post
	Roles      []Role
}
```

Directive options (`//orm:table <name> [options]`):

| Option | Meaning |
|---|---|
| `key=<col>` | primary key (default: field tagged `pk`, else `id`) |
| `key_type=increments\|uuid\|ulid\|manual` | key generation (default: increments for integer keys) |
| `connection=<name>` | registered connection to use |
| `timestamps=false` | don't maintain `created_at` / `updated_at` |
| `soft_deletes` | `deleted_at` soft deletes |
| `morph=<alias>` | value stored in `*_type` columns (default: table name) |
| `schema=<Name>` | generated schema value name (default: CamelCase table) |

Tag options: `pk`, `virtual` (selected only, never written), and `nullzero`
(write the zero value as NULL). Zero `time.Time` values are always written as
NULL, and NULLs are read back as zero values, or as nil for pointer fields.

**Casts.** Use `orm.JSON[T]` for JSON columns, `time.Time` for dates, and
typed string constants for enums. Any custom type implementing `sql.Scanner`
and `driver.Valuer` works as a cast. **Accessors and mutators** are ordinary
Go methods, and **serialization** uses `encoding/json` tags (`json:"-"` hides
a field).

```go
// Encrypted columns (Laravel's 'encrypted' / 'encrypted:array' casts):
// AES-256-GCM of the JSON encoding, keyed by orm.SetEncryptionKey.
orm.SetEncryptionKey([]byte(os.Getenv("APP_KEY")))
// ApiToken orm.Encrypted[map[string]string] `db:"api_token"`

// Hashed passwords (the 'hashed' cast): assigning a plain password
// bcrypt-hashes it on write; an already-hashed value is stored as-is.
// Password orm.Hashed `db:"password"`
u.Password = "secret"
u.Password.Matches("secret") // verify
```

## Retrieving

```go
all, err := Users.All(ctx)
u, err := Users.Find(ctx, 1)                         // orm.ErrNotFound if missing
us, err := Users.Query().FindMany(ctx, 1, 2, 3)
first, err := Users.Where(Users.Active.Eq(true)).First(ctx)
only, err := Users.Where(Users.Email.Eq("a@b.c")).Sole(ctx) // ErrNotFound / ErrMultipleRecords
u, err = Users.Where(Users.Name.Eq("x")).FirstOr(ctx, func() (User, error) { return User{}, nil })

byEmail, err := Users.Query().KeyBy(ctx, Users.Email)  // map[string]User
byUser, err := Posts.Query().Grouped(ctx, Posts.UserID) // map[int64][]Post
names, err := Users.Query().Map(ctx, func(u User) string { return u.Name })
```

## Where clauses

```go
Users.Where(Users.Karma.Gt(10), Users.Active.Eq(true))          // AND
Users.Where(Users.Karma.Gt(90)).OrWhere(Users.Name.Eq("Bob"))
Users.Query().WhereNot(Users.Active.Eq(true))
Users.Where(orm.Or(Users.Karma.Lt(5), Users.Karma.Gt(40)))      // nested group
Users.Where(Users.ID.In(1, 2), Users.Karma.Between(10, 50), Users.Email.Like("%@example.com"))
Users.Where(Users.Settings.IsNull())
Users.Query().WhereKey(1, 4)
Users.Where(Users.CreatedAt.EqCol(Users.UpdatedAt))             // whereColumn
Users.Where(orm.AnyOf("LIKE", "%car%", Users.Name, Users.Email)) // whereAny
Users.Where(orm.Raw[User]("karma % 2 = ?", 1))                  // whereRaw

Posts.Where(orm.Year(Posts.CreatedAt).Eq(2026))                 // whereYear
Posts.Where(orm.Date(Posts.CreatedAt).Eq("2026-01-10"))         // whereDate

Users.Where(Users.Settings.JSON[string]("theme").Eq("dark"))    // settings->theme
Users.Where(Users.Settings.JSON[string]("labels").JSONContains("admin"))
Users.Where(Users.Settings.JSON[string]("labels").JSONDoesntContain("admin"))
Users.Where(Users.Settings.JSON[string]("labels").JSONOverlaps("admin", "editor")) // whereJsonOverlaps
Users.Where(Users.Settings.JSON[string]("labels").JSONLength().Eq(0))
Users.Where(Users.Settings.JSONHasKey("theme"))

Posts.Query().WhereFullText("generics", Posts.Title)

Users.Query().When(onlyActive, func(q orm.Query[User]) orm.Query[User] {
	return q.Where(Users.Active.Eq(true))
})
```

Subqueries:

```go
popularAuthors := Posts.Where(Posts.Views.Gt(200)).Subquery(Posts.UserID)
Users.Where(Users.ID.InSub(popularAuthors))
Users.Where(orm.Exists[User](Posts.Where(Posts.UserID.EqCol(Users.ID), Posts.Views.Gt(500))))
```

## Selects, joins, grouping, unions

```go
Users.Query().Select(Users.ID, Users.Name)

// addSelect a correlated subquery into a virtual column.
latest := Posts.Where(Posts.UserID.EqCol(Users.ID)).Latest().Limit(1).Subquery(Posts.Title)
Users.Query().AddSelectAs(Users.LatestPostTitle, orm.IfNull(latest, ""))

Users.Query().
	Join(Posts, Posts.UserID.EqCol(Users.ID)).
	WhereOf(Posts.Published.Eq(true)).    // conditions on the joined model
	GroupBy(Users.ID).
	Having(orm.Count[User]().Gte(2)).
	Pluck(ctx, Users.Name)

counts := Posts.Query().Select(Posts.UserID).AddSelectAs(Posts.Views, orm.Count[Post]()).GroupBy(Posts.UserID)
Users.Query().JoinSub(counts, "pc", Posts.UserID.EqCol(Users.ID), Posts.Views.Gte(2))

Users.Where(Users.Karma.Gt(90)).Union(Users.Where(Users.Name.Eq("Bob")))
Users.Query().OrderBy(Users.Name.Asc()).Latest().InRandomOrder().Reorder(Users.ID.Desc())
Users.Query().Limit(10).Offset(20) // or ForPage(3, 10)
Users.Query().Distinct().LockForUpdate() // SharedLock()
```

## Aggregates and single values

```go
n, err := Users.Query().Count(ctx)
sum, err := Users.Query().Sum(ctx, Users.Karma)      // int: Sum only takes numeric columns
avg, err := Posts.Query().Avg(ctx, Posts.Views)      // float64
newest, err := Posts.Query().Max(ctx, Posts.CreatedAt)
ok, err := Users.Where(Users.Name.Eq("Alice")).Exists(ctx)
emails, err := Users.Query().Pluck(ctx, Users.Email)  // []string
perUser, err := Posts.Query().GroupBy(Posts.UserID).PluckMap(ctx, Posts.UserID, orm.Count[Post]())
name, err := Users.Query().OrderBy(Users.Karma.Desc()).Value(ctx, Users.Name)
```

## Chunking, iterators, pagination

```go
err := Posts.Query().OrderBy(Posts.ID.Asc()).Chunk(ctx, 100, func(ps []Post) error { return nil })
err = Posts.Query().ChunkByID(ctx, 100, func(ps []Post) error { return orm.ErrStop }) // stop early
err = Posts.Query().ChunkWhile(ctx, func(prev, cur Post) bool { return prev.UserID == cur.UserID },
	func(ps []Post) error { return nil }) // chunkWhile, ordered by key

for p, err := range Posts.Query().With(PostComments).Lazy(ctx, 500) { _, _ = p, err } // lazyById
for p, err := range Posts.Query().Cursor(ctx) { _, _ = p, err }                       // one query, streamed

page, err := Posts.Query().Latest().Paginate(ctx, 20, 2)   // Items, Total, LastPage, From, To, HasMore
simple, err := Posts.Query().SimplePaginate(ctx, 20, 2)    // HasMore, no count query
cur, err := Posts.Query().OrderBy(Posts.Views.Desc(), Posts.ID.Asc()).CursorPaginate(ctx, 20, cursor)
next := cur.NextCursor
```

`Paginate` is length-aware on every engine — SQL, ClickHouse, MongoDB and
Qdrant alike — and serialises like Laravel's paginator (`data`, `total`,
`per_page`, `current_page`, `last_page`, `from`, `to`, `has_more`). Row
queries page the same way and return `Page[map[string]any]`; a grouped or
distinct row query counts groups or distinct rows. Page and size must be
at least 1. `CursorPaginate`, `ChunkByID` and `Lazy` page by key instead
of offset on every engine too; on Qdrant, which can only order by an
indexed payload field, ordering by the key (or not at all) uses its native
point-id scroll.

```go
page, err := orm.From("events").On("mongo").OrderBy("created_at").Paginate(ctx, 50, 3)
err = orm.FromTable(Users).Chunk(ctx, 500, func(rows []map[string]any) error { return nil })
for row, err := range orm.FromTable(Users).Lazy(ctx, 500) { _, _ = row, err } // by key column
for row, err := range orm.From("users").Cursor(ctx) { _, _ = row, err }      // one query, streamed
```

## Inserting and updating

```go
u := User{Name: "Frank", Email: "frank@example.com"}
err := Users.Create(ctx, &u)   // auto id / uuid / ulid, timestamps, events
u.Karma = 10
err = Users.Save(ctx, &u)      // UPDATE only dirty columns

Users.Where(Users.CountryID.Eq(2)).Update(ctx, Users.Active.Set(false)) // mass update, maintains updated_at
Posts.Where(Posts.ID.Eq(1)).Increment(ctx, Posts.Views, 10)
Posts.Where(Posts.ID.Eq(1)).Decrement(ctx, Posts.Views, 1, Posts.Title.Set("edited"))

Videos.Query().Insert(ctx, Video{Title: "A"}, Video{Title: "B"})   // no events / timestamps
id, err := Videos.Query().InsertGetID(ctx, Video{Title: "C"})       // insertGetId
Tags.Query().InsertOrIgnore(ctx, Tag{Name: "go"})
Roles.Query().Upsert(ctx, []Role{{ID: 1, Name: "superadmin"}}, []orm.AnyColumn[Role]{Roles.ID})
Tags.Query().InsertUsing(ctx, []orm.AnyColumn[Tag]{Tags.Name}, Roles.Query(), orm.Upper(Roles.Name))

u, err = Users.Query().FirstOrNew(ctx, orm.Attrs(Users.Email.Set("a@b.c")), Users.Name.Set("A"))
u, err = Users.Query().FirstOrCreate(ctx, orm.Attrs(Users.Email.Set("a@b.c")), Users.Name.Set("A"))
u, err = Users.Query().UpdateOrCreate(ctx, orm.Attrs(Users.Email.Set("a@b.c")), Users.Karma.Set(1000))
u, err = Users.Query().CreateOrFirst(ctx, orm.Attrs(Users.Email.Set("a@b.c")))

clone, err := Posts.Replicate(ctx, &post, Posts.Views)    // unsaved copy without key/timestamps/views
err = Posts.Touch(ctx, &post)
err = Posts.Refresh(ctx, &post)
fresh, err := Posts.Fresh(ctx, &post)
same := Posts.Is(&a, &b)

fallback, err := Users.Query().FindOr(ctx, 99, func() (User, error) { return User{Name: "nobody"}, nil })

err = Users.Create(orm.WithoutTimestamps(ctx), &u)
err = Images.Truncate(ctx)
err = Images.Query().On("archive").Truncate(ctx) // on another connection; a document store deletes every document
```

## Deleting and soft deletes

```go
err := Users.Delete(ctx, &u)              // soft delete (soft_deletes directive)
Users.Trashed(&u)
Users.Query().WithTrashed().Find(ctx, u.ID)
Users.Query().OnlyTrashed().Get(ctx)
err = Users.Restore(ctx, &u)
err = Users.ForceDelete(ctx, &u)
n, err := Users.Destroy(ctx, 4, 5)        // loads and deletes each, with events

Users.Where(Users.Name.Eq("Eve")).Delete(ctx)      // mass (soft) delete
Users.Where(Users.Name.Eq("Eve")).Restore(ctx)
Users.Where(Users.Name.Eq("Eve")).ForceDelete(ctx)

n, err = Users.Where(Users.CreatedAt.Lt(cutoff)).Prune(ctx, 500) // Prunable: one by one, with events
```

## Dirty tracking

```go
u.Karma = 10
Users.Karma.IsDirty(&u)       // true
Users.Karma.Original(&u)      // value when loaded
Users.Dirty(&u)               // []string{"karma"}
Users.IsDirty(&u, Users.Karma, Users.Name)
Users.Save(ctx, &u)
Users.Karma.WasChanged(&u)    // true
Users.IsClean(&u)
u.Exists(); u.WasRecentlyCreated()
```

## Relationships

Relations are values, declared once next to your models:

```go
var (
	UserPosts = orm.HasMany(Users.ID, Posts.UserID,
		func(u *User, ps []Post) { u.Posts = ps })

	UserLatestPost = orm.HasOne(Users.ID, Posts.UserID,
		func(u *User, p *Post) { u.LatestPost = p }).OfMany(Posts.ID, true) // latestOfMany

	UserCountry = orm.BelongsTo(Users.CountryID, Countries.ID,
		func(u *User, c *Country) { u.Country = c })

	PostAuthor = orm.BelongsTo(Posts.UserID, Users.ID,
		func(p *Post, u *User) { p.Author = u }).
		WithDefault(func(*Post) *User { return &User{Name: "Guest Author"} })

	// Many-to-many through a typed pivot model (role_user).
	UserRoles = orm.BelongsToMany(Users.ID, RoleUsers.UserID, Roles.ID, RoleUsers.RoleID,
		func(u *User, rs []Role) { u.Roles = rs }).
		WithPivot(func(r *Role, p RoleUser) { r.Pivot = p })

	CountryPosts = orm.HasManyThrough(Countries.ID, Users.CountryID, Users.ID, Posts.UserID,
		func(c *Country, ps []Post) { c.Posts = ps })

	// Polymorphic.
	PostComments = orm.MorphMany(Posts.ID, Comments.CommentableID, Comments.CommentableType,
		func(p *Post, cs []Comment) { p.Comments = cs })
	UserAvatar = orm.MorphOne(Users.ID, Images.ImageableID, Images.ImageableType,
		func(u *User, i *Image) { u.Avatar = i })
	CommentCommentable = orm.MorphTo(Comments.CommentableID, Comments.CommentableType,
		func(c *Comment, owner any) { c.Commentable = owner }, // *Post or *Video
		orm.Target(Posts.ID), orm.Target(Videos.ID))
	PostTags = orm.MorphToMany(Posts.ID, Taggables.TaggableID, Taggables.TaggableType, Tags.ID, Taggables.TagID,
		func(p *Post, ts []Tag) { p.Tags = ts })
	TagPosts = orm.MorphedByMany(Tags.ID, Taggables.TagID, Posts.ID, Taggables.TaggableID, Taggables.TaggableType,
		func(t *Tag, ps []Post) { t.Posts = ps })
)
```

Relation options: `Scoped(...)` for constrained relations, `WithAttributes(...)`
(attributes set on create), `Chaperone(...)` (sets the inverse on children),
`WherePivot(...)`, and `PostAuthor.Touches()` (`$touches`).

## Eager loading

```go
users, err := Users.Query().
	With(UserPosts, Published, func(q orm.Query[Post]) orm.Query[Post] {
		return q.OrderBy(Posts.Views.Desc()).With(PostComments) // nested
	}).
	With(UserLatestPost).
	With(UserRoles).
	Get(ctx)

err = orm.Load(ctx, users, UserCountry) // $users->load('country')
```

## Querying relationships

```go
Users.Query().WhereHas(UserPosts, Published, Popular(100))
Users.Query().WhereDoesntHave(UserPosts)
Users.Where(Users.Name.Eq("Dave")).OrWhereHas(UserRoles)
Users.Query().WhereRelation(UserRoles, Roles.Name.Eq("admin"))
Users.Query().HasCount(UserPosts, ">=", 2)
Users.Where(orm.Or(orm.Has(UserPosts), Users.Karma.Gt(90)))          // compose freely
Posts.Where(PostAuthor.Is(&alice))                                    // whereBelongsTo
Users.Where(UserRoles.AttachedTo(&editor))                            // whereAttachedTo
Comments.Where(CommentCommentable.HasMorph(Posts.ID, Published))      // whereHasMorph
Comments.Where(CommentCommentable.Is(&video))                         // whereMorphedTo

Users.Query().
	WithCount(UserPosts, Users.PostsCount).
	WithSum(UserPosts, Posts.Views, Users.PostViews, Published).
	WithExists(UserAvatar, Users.HasAvatar)
```

Relations pointing at their own table (parents and children, followers)
alias the inner table automatically.

## Writing through relationships

```go
UserPosts.Of(&u).Where(Posts.Published.Eq(true)).Count(ctx) // $user->posts()->...
UserPosts.Create(ctx, &u, &Post{Title: "Hi"})                // sets user_id
UserPosts.SaveMany(ctx, &u, posts)
PostComments.Create(ctx, &post, &Comment{Body: "first!"})     // sets id + type
PostAuthor.Associate(&post, &bob); PostAuthor.Dissociate(&post)
CommentCommentable.Associate(&comment, &video)

UserRoles.AttachIDs(ctx, &u, 1, 2)
UserRoles.Attach(ctx, &u, RoleUser{RoleID: 3, GrantedBy: "admin"}) // with pivot data
UserRoles.Detach(ctx, &u, 2)
res, err := UserRoles.Sync(ctx, &u, 1, 3)       // res.Attached, res.Detached
UserRoles.SyncWithoutDetaching(ctx, &u, 4)
UserRoles.Toggle(ctx, &u, 1, 2)
UserRoles.UpdateExistingPivot(ctx, &u, 3, RoleUsers.GrantedBy.Set("changed"))
```

## Scopes

```go
func Active(q orm.Query[User]) orm.Query[User] { return q.Where(Users.Active.Eq(true)) }
func Popular(min int64) func(orm.Query[Post]) orm.Query[Post] { // dynamic scope
	return func(q orm.Query[Post]) orm.Query[Post] { return q.Where(Posts.Views.Gte(min)) }
}
Users.Query().Scope(Active)

Users.AddGlobalScope("verified", func(q orm.Query[User]) orm.Query[User] { return q.Where(...) })
Users.Query().WithoutGlobalScope("verified")
Users.Query().WithoutGlobalScopes()
```

## Events and observers

```go
Users.On(orm.Creating, func(ctx context.Context, u *User) error {
	if strings.HasSuffix(u.Email, "@spam.test") {
		return errors.New("rejected") // aborts the create
	}
	return nil
})

type UserObserver struct{}
func (UserObserver) Created(ctx context.Context, u *User) error { return nil }
Users.Observe(UserObserver{})

Users.Create(orm.WithoutEvents(ctx), &u)
Users.SaveQuietly(ctx, &u)
```

The available events are `Retrieved`, `Creating`, `Created`, `Updating`, `Updated`, `Saving`, `Saved`,
`Deleting`, `Deleted`, `Trashed`, `ForceDeleting`, `ForceDeleted`, `Restoring`,
`Restored` and `Replicating`.

## Transactions

```go
err := orm.Transaction(ctx, func(ctx context.Context) error {
	if err := Users.Create(ctx, &u); err != nil {
		return err // rolls back
	}
	orm.AfterCommit(ctx, func(ctx context.Context) { sendWelcomeMail(u) })
	return orm.Transaction(ctx, func(ctx context.Context) error { // nested: a savepoint
		return nil
	})
})

// Re-run the whole transaction when the database reports a deadlock or
// lock timeout, up to three times (DB::transaction's $attempts).
err = orm.RetryingTransaction(ctx, 3, func(ctx context.Context) error { return Users.Save(ctx, &u) })
```

Every query that receives the transaction's `ctx` runs inside it.

## Connections and dialects

```go
orm.Open(orm.DefaultConnection, "sqlite", "file:app.db?_time_format=sqlite")
orm.Open("audit", "pgx", auditDSN)
orm.AddConnection("replica", replicaDB, orm.MySQL)

// Read/write split: SELECTs run against readDSN, everything else against
// writeDSN (Laravel's read/write connection config).
write, err := orm.OpenReadWrite(orm.DefaultConnection, "mysql", readDSN, writeDSN)

// Sticky reads (Laravel's 'sticky' => true): open the scope once where the
// request's ctx is created — middleware is the usual place — and every
// read after the request's first write runs on the write connection for
// the rest of the ctx, so it reads back its own writes despite lag.
ctx := orm.Sticky(r.Context()) // in middleware, before calling the handler

//orm:table audit_log connection=audit key_type=uuid timestamps=false
AuditLog.Query().Count(ctx)               // runs on "audit"
Users.Query().On("replica").Get(ctx)      // Model::on()

sqlText, args := Users.Where(Users.Karma.Gt(10)).ToSQLFor(orm.Postgres)

// Session state that lives on one connection (FOREIGN_KEY_CHECKS, PRAGMAs):
conn, err := db.Conn(ctx)
ctx = orm.WithConn(ctx, orm.DefaultConnection, conn) // every query on ctx uses conn
```

`orm.Open` with the `mysql` driver asks the server for its version and picks
`orm.MySQL` or `orm.MariaDB`; MariaDB has its own dialect (shared locks,
`INSERT ... RETURNING`, vector functions). `AddConnection(..., orm.MariaDB)`
or the driver name `mariadb` choose it explicitly.

## Queries without a model

`orm.From` is `DB::table`: the same builder idea over a bare table, with
rows as maps and no model machinery.

```go
rows, err := orm.From("users").
	Where("active", orm.Eq, true).
	WhereIn("country_id", 1, 2).
	WhereNull("deleted_at").
	WhereRaw("karma > ?", 10).
	LeftJoin(Countries, Countries.ID.EqCol(Users.CountryID)).
	OrderBy("name").
	Limit(10).
	Get(ctx) // []map[string]any

n, err := orm.From("users").Where("email", orm.Eq, "a@b.c").Count(ctx)
id, err := orm.From("users").InsertGetID(ctx, map[string]any{"name": "Ada", "email": "ada@example.com"})
n, err = orm.From("users").Where("id", orm.Eq, id).Update(ctx, map[string]any{"name": "Ada L"})
n, err = orm.From("users").Where("id", "=", id).Delete(ctx)
err = orm.From("users").UpdateOrInsert(ctx,
	map[string]any{"email": "ada@example.com"},
	map[string]any{"name": "Ada L"})
```

Operators are `orm.Op` constants (`orm.Eq`, `orm.Neq`, `orm.Gt`, `orm.Gte`,
`orm.Lt`, `orm.Lte`, `orm.Like`, `orm.NotLike`, `orm.Ilike`, `orm.NotIlike`);
string literals still convert. When a model exists, its typed columns carry
over — completion lists the valid columns and the values are compile-checked:

```go
rows, err := orm.From("users").
	SelectCols(Users.Email).
	Join(Countries, Countries.ID.EqCol(Users.CountryID)).
	WhereCond(Users.Active.Eq(true), Users.Karma.Gt(10)).
	OrderByColDesc(Users.Karma).
	Limit(10).
	Get(ctx)
```

`orm.FromTable(Users)` takes the table name and its connection from
the model, so there is no table or `.On(...)` string to mistype. Selected,
joined and ordered typed columns render table-qualified; when joined
tables share a column name, `As` tells them apart:

```go
rows, err := orm.FromTable(Users).
	SelectCols(Users.Name.As("user"), Countries.Name.As("country")).
	Join(Countries, Countries.ID.EqCol(Users.CountryID)).
	OrderByCol(Users.Name).
	Get(ctx) // rows[0]["user"], rows[0]["country"]
```

Grouping selects typed aggregates under a name; `HavingCond` filters on
them portably (Postgres cannot see select aliases in `HAVING`):

```go
rows, err := orm.FromTable(Users).
	SelectCols(Users.CountryID, orm.Count[User]().As("users")).
	GroupByCols(Users.CountryID).
	HavingCond(orm.Count[User]().Gt(1)).
	Get(ctx)
rows, err = orm.From("users").
	SelectRaw("country_id, COUNT(*) AS n").
	GroupBy("country_id").
	HavingRaw("COUNT(*) > ?", 1).
	Get(ctx)
```

Typed writes and plucks reuse the model's assignments and columns:

```go
users := orm.FromTable(Users)
err = users.InsertCols(ctx, Users.Name.Set("Ada"), Users.Email.Set("ada@example.com"))
n, err = users.WhereCond(Users.Email.Eq("ada@example.com")).
	UpdateCols(ctx, Users.Name.Set("Ada L"), Users.Karma.SetExpr(orm.Plus(Users.Karma, 1)))
emails, err := users.OrderByCol(Users.Email).PluckCol(ctx, Users.Email) // []string
```

`InsertCols` and `UpdateCols` write exactly what they are given, like
`Insert` and `Update`: no timestamps, no events. On a document store,
`SetExpr` assignments and `As` aliases are refused, since neither has a
document form.

Aggregates, counters and bulk writes round out `DB::table`:

```go
total, err := orm.From("orders").Sum[int64](ctx, "amount") // the type is the caller's
karma, err := orm.FromTable(Users).SumCol(ctx, Users.Karma)  // inferred: int
avg, err := orm.From("users").Avg(ctx, "karma")                    // float64
n, err := orm.From("posts").Where("id", orm.Eq, 7).Increment(ctx, "views", 1)

err = orm.From("countries").InsertOrIgnore(ctx, map[string]any{"id": 1, "name": "Denmark"})
err = orm.From("countries").Upsert(ctx,
	[]map[string]any{{"id": 1, "name": "Danmark"}, {"id": 4, "name": "Finland"}},
	[]string{"id"}, // conflict columns
	"name")         // columns to update; default: every other inserted column

sql, args := orm.From("users").Where("users.karma", orm.Gt, 10).ToSQL()
```

`UpdateOrInsert` checks existence before writing, so it never inserts a
duplicate on engines that report only changed rows (MySQL). `Count` after
`Distinct` counts distinct rows. On document stores, `InsertOrIgnore`
matches on the key column (`WithKey`, default `id`), `Increment` and
`Decrement` become `$inc` on MongoDB, and aggregates — plus increments on
Qdrant — return `ErrNotTranslatable`. ClickHouse, which has no unique
constraints, emulates `InsertOrIgnore` and `Upsert` by reading the taken
keys first.

## Factories

```go
var UserFactory = orm.NewFactory(Users.Table, func(n int) User {
	return User{Name: fmt.Sprintf("User %d", n), Email: fmt.Sprintf("user%d@example.com", n)}
})

users, err := UserFactory.Count(4).
	State(func(u *User) { u.Karma = 7 }).
	Sequence(func(u *User) { u.CountryID = 1 }, func(u *User) { u.CountryID = 2 }).
	AfterCreating(func(ctx context.Context, u *User) error {
		return UserPosts.Create(ctx, u, &Post{Title: "by " + u.Name})
	}).
	Create(ctx)
draft := UserFactory.MakeOne() // unsaved
```

## Debugging

```go
sqlText, args := Users.Where(Users.Name.Eq("x")).ToSQL()
fmt.Println(Users.Where(Users.Name.Eq("O'Brien")).ToRawSQL()) // args inlined

plan, err := Users.Where(Users.Karma.Gt(10)).Explain(ctx) // []map[string]any

stop := orm.Listen(func(e orm.QueryEvent) { log.Println(e.Duration, e.SQL) }) // DB::listen
defer stop()

orm.EnableQueryLog()                            // DB::enableQueryLog
must(Users.Query().Count(ctx))
for _, e := range orm.QueryLog() {              // DB::getQueryLog
	log.Println(e.Duration, e.SQL)
}
orm.FlushQueryLog()                              // DB::flushQueryLog
orm.DisableQueryLog()
```

Errors from the database come back as `*orm.QueryError`, which carries the
failing SQL. `orm.IsUniqueViolation(err)` detects duplicate-key errors on
every engine.

## Schema builder

```go
err := schema.Create(ctx, "flights", func(t *schema.Blueprint) {
	t.ID()
	t.ForeignID("airline_id").Constrained().CascadeOnDelete()
	t.String("name", 100).Comment("flight name")
	t.Char("code", 3).Unique()
	t.Decimal("price", 8, 2).Default(0)
	t.Boolean("active").Default(true)
	t.Enum("status", "scheduled", "departed")
	t.JSON("meta").Nullable()
	t.UUID("ref").Index()
	t.TimestampTz("departs_at").UseCurrent()
	t.Integer("price_cents").StoredAs("price * 100")
	t.Text("notes").FullText()                 // Postgres / MySQL
	t.Timestamps()
	t.SoftDeletes()
	t.Index("airline_id", "departs_at").Name("flights_airline_departs").Algorithm("btree")
})

err = schema.Table(ctx, "flights", func(t *schema.Blueprint) {
	t.String("gate", 10).Nullable().After("code")
	t.String("name", 200).Change()
	t.RenameColumn("notes", "remarks")
	t.DropColumn("ref")
	t.RenameIndex("flights_code_unique", "flights_code_uq")
	t.DropForeignOn("airline_id")
	t.ForeignID("airline_id").Nullable().Change()
	t.Foreign("airline_id").References("id").On("carriers").NullOnDelete()
})
```

**Column types:** `ID`, `BigIncrements`, `Increments`, `Medium/Small/TinyIncrements`,
`String`, `Char`, `Tiny/Medium/LongText`, `Text`, `Tiny/Small/Medium/Big Integer`,
`Integer`, `Unsigned*`, `Boolean`, `Decimal`, `Float`, `Double`, `Date`, `Time(Tz)`,
`DateTime(Tz)`, `Timestamp(Tz)`, `Year`, `JSON`, `JSONB`, `Binary`, `UUID`, `ULID`,
`UUIDPrimary`, `ULIDPrimary`, `IPAddress`, `MACAddress`, `Enum`, `Set`, `Geometry`,
`Geography`, `Vector`, `ForeignID`, `ForeignUUID`, `ForeignULID`, `ForeignIDFor(Users.Table)`,
`Timestamps(Tz)`, `NullableTimestamps`, `Datetimes`, `SoftDeletes(Tz)`,
`(Nullable)(UUID|ULID)Morphs`, `RememberToken`.

**Column modifiers:** `Nullable`, `Default`, `UseCurrent`, `UseCurrentOnUpdate`,
`Unsigned`, `AutoIncrement`, `From`, `Comment`, `Charset`, `Collation`, `After`,
`First`, `Invisible`, `StoredAs`, `VirtualAs`, `GeneratedAs`, `Always`, `Change`,
`Primary`, `Unique`, `Index`, `FullText`, `SpatialIndex`, `Constrained`.

**Indexes and keys:** `Primary`, `Unique`, `Index`, `FullText`, `SpatialIndex`
(each with `.Name()`, `.Algorithm()`, `.Language()`), `RenameIndex`, `Drop*`,
`Foreign(...).References(...).On(...)` with `CascadeOnDelete`, `NullOnDelete`,
`RestrictOnDelete`, `NoActionOnDelete`, the `*OnUpdate` variants, `Deferrable`
and `InitiallyDeferred`. Also `DropConstrainedForeignID`, `DropForeignIDFor` and
`DropConstrainedForeignIDFor`. Drops take a name (`DropIndex("posts_ref_idx")`,
`DropUnique`, `DropFullText`, `DropSpatialIndex`, `DropForeign`) or the columns the
index was declared on (`DropIndexOn("user_id", "created_at")`, `DropUniqueOn`, ...),
as Laravel does with a string or an array.

**Table options:** `Engine`, `Charset`, `Collation` (MySQL), `Comment`, `Temporary`,
and for ClickHouse MergeTree tables `OrderBy`, `Unsorted`, `PartitionBy`, `TTL` and
`Setting`. The primary key is the ClickHouse sorting key unless `OrderBy` sets one.

Anything an engine cannot express fails with a `*schema.UnsupportedError` naming
the engine and the operation, before anything runs: `Set` outside MySQL, unique
indexes, foreign keys and auto-increment columns on ClickHouse, `NullOnDelete` and
`CascadeOnDelete` on DuckDB, and so on.

**Facade:** `Create`, `CreateIfNotExists`, `Table`, `Drop`, `DropIfExists`,
`DropColumns`, `Rename`, `HasTable`, `HasColumn(s)`, `HasIndex`, `Tables`, `Columns`,
`GetColumns`, `GetIndexes`, `GetForeignKeys`, `GetColumnType`, `WhenTableHasColumn`,
`WhenTableDoesntHaveColumn`, `DropAllTables`, `WithoutForeignKeyConstraints`,
`DriverName` and `Pretend`. Use `schema.On("conn")` for other connections; a
document-store connection (MongoDB, Qdrant) is an error.

`WithoutForeignKeyConstraints` holds one connection for SQLite and MySQL, so the
disabled checks never leak into the pool; schema calls on its ctx use that
connection, plain orm queries do not. On Postgres it runs a transaction with
`SET CONSTRAINTS ALL DEFERRED`, which only defers `Deferrable()` constraints.

SQLite can't `ALTER` columns, primary keys or foreign keys in place. Like
Laravel, `Table` rebuilds the table for those changes, keeping data, column
order, CHECK constraints, generated columns, indexes and the AUTOINCREMENT
counter. Column renames run first as `ALTER TABLE ... RENAME COLUMN`, so SQLite
rewrites every reference to them. The rebuild runs atomically with foreign keys
off and checks them before committing; inside a transaction (a migration) SQLite
cannot turn foreign keys off, so with enforcement on such a migration needs
`NoTransaction: true`.

## Migrations

```go
func init() {
	schema.Register(schema.Migration{
		Name: "2026_01_01_000000_create_flights_table",
		Up: func(ctx context.Context, s *schema.Builder) error {
			return s.Create(ctx, "flights", func(t *schema.Blueprint) { t.ID(); t.Timestamps() })
		},
		Down: func(ctx context.Context, s *schema.Builder) error { return s.DropIfExists(ctx, "flights") },
	})
}
```

Add a `cmd/migrate` (see [`example/migrate`](../example/migrate/main.go)):

```sh
go run ./cmd/migrate                       # migrate (also: -step, -pretend, -isolated)
go run ./cmd/migrate install               # create the migrations table
go run ./cmd/migrate status
go run ./cmd/migrate rollback [-steps N | -batch B]
go run ./cmd/migrate reset | refresh [-steps N] [-seed] | fresh [-seed] | wipe
go run ./cmd/migrate make create_flights_table -dir migrations
go run ./cmd/migrate make:seeder Users -dir seeders
go run ./cmd/migrate make:model LineItem -dir models
go run ./cmd/migrate db:seed [-class Users]
```

Migrations run inside a transaction on SQLite, Postgres and DuckDB, so a failed
migration leaves nothing behind. A migration with `Connection: "audit"` runs
on that connection — a MongoDB or Qdrant one included (see the README's
"Migrating document stores"); a migrator whose own connection is a document
store keeps its log there. `Migrator` exposes the same operations
programmatically, for example to migrate in tests.

`migrate -isolated` (`Migrator.Isolated`) holds a lock row in
`migrations_lock` while it runs, so several instances deploying at once
migrate exactly once; the others report the lock and exit cleanly. A lock
older than `Migrator.LockTimeout` (an hour by default) is taken over, since
a crashed run never releases it. ClickHouse and Qdrant have no unique keys
to lock with and refuse.

Within one `Table` call, index, foreign key and primary key drops run before
column changes, so a column can be dropped or changed in the same call that
drops what covers it. `First` / `After` position columns added to an
existing MySQL or MariaDB table and are refused elsewhere (a new table's
columns are in declaration order); an auto-increment column is always the
primary key, so declaring another one beside it is refused.

## Seeders

Seeders pair with factories to populate a migrated database. Each runs in
its own transaction; a failing seeder stops the run.

```go
var UserFactory = orm.NewFactory(Users.Table, func(n int) User {
	return User{Name: fmt.Sprintf("User %d", n), Email: fmt.Sprintf("user%d@example.com", n)}
})

func init() {
	schema.RegisterSeeder(schema.Seeder{
		Name: "Users",
		Run: func(ctx context.Context) error {
			_, err := UserFactory.Count(50).Create(ctx)
			return err
		},
	})
}

schema.Seed(ctx)                    // every seeder
schema.Seed(ctx, "Users")           // one
schema.SeedOn(ctx, "analytics")     // on a named connection
```

`go run ./cmd/migrate db:seed` runs them all, `-class Users` just one, and
`fresh -seed` / `refresh -seed` seed after migrating. `make:seeder Users`
scaffolds the file above.

## Running the test databases

```sh
docker compose up -d --wait   # Postgres 17 :54329, MySQL 8.4 :33069, MariaDB 11.8 :33070
just test-all
```

Each service creates two empty databases, `storm` and `storm_audit`.
Point the suite at any server with `STORM_TEST_DRIVER`, `STORM_TEST_DSN`
and `STORM_TEST_AUDIT_DSN`.
