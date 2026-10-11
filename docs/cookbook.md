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

SQLite has no time type and compares times as text, so storm binds every
time on SQLite as UTC `YYYY-MM-DD HH:MM:SS[.fraction]`, the form SQLite's
date functions (and Laravel) use: rows written by plain SQL, by Laravel or
by storm compare and order correctly against each other.

`orm.Open` also turns off SQLite's double-quoted string quirk (`_dqs=0` for
`modernc.org/sqlite`), under which a misspelled `"column"` silently reads
as the string `'column'`, and refuses a SQLite connection that still has it
on. For a `*sql.DB` you open yourself, add `_dqs=0` to the DSN.

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
| `time_precision=6` | keep 6 fractional second digits in the maintained timestamps (default whole seconds); pair with `t.Timestamps(6)` |
| `soft_deletes` | `deleted_at` soft deletes |
| `morph=<alias>` | value stored in `*_type` columns (default: table name) |
| `schema=<Name>` | generated schema value name (default: CamelCase table) |

Tag options: `pk`, `virtual` (selected only, never written), and `nullzero`
(write the zero value as NULL). Zero `time.Time` values are always written as
NULL, and NULLs are read back as zero values, or as nil for pointer fields.
Fields of an embedded struct (a shared `Timestamps` struct, say) are columns
too. ormgen type-checks the package, so a named key type such as
`type UserID int64` still auto-increments, and it refuses anything it cannot
generate correctly: an unknown option or tag, two `pk` fields, a field name
that would hide a table method such as `Find`, or a generated name the
package already declares.

**Casts.** Use `orm.JSON[T]` for JSON columns, `time.Time` for dates, and
typed string constants for enums. Any custom type implementing `sql.Scanner`
and `driver.Valuer` works as a cast. **Accessors and mutators** are ordinary
Go methods, and **serialization** uses `encoding/json` tags (`json:"-"` hides
a field).

```go
// Encrypted columns (Laravel's 'encrypted' / 'encrypted:array' casts):
// AES-256-GCM of the JSON encoding, keyed by orm.SetEncryptionKey.
key, err := base64.StdEncoding.DecodeString(os.Getenv("APP_KEY")) // 32 random bytes
err = orm.SetEncryptionKey(key)
// ApiToken orm.Encrypted[map[string]string] `db:"api_token"`

// Hashed passwords (the 'hashed' cast): the field only ever holds a
// bcrypt hash, so the plain password never sits in the model.
// Password orm.Hashed `db:"password"`
u.Password, err = orm.HashPassword("secret")
u.Password.Matches("secret") // verify
```

## Retrieving

```go
all, err := Users.All()
u, err := Users.Find(1)                         // orm.ErrNotFound if missing
us, err := Users.FindMany(1, 2, 3)
first, err := Users.Where(Users.Active.Eq(true)).First()
only, err := Users.Where(Users.Email.Eq("a@b.c")).Sole() // ErrNotFound / ErrMultipleRecords
u, err = Users.Where(Users.Name.Eq("x")).FirstOr(func() (User, error) { return User{}, nil })

byEmail, err := Users.Query().KeyBy(Users.Email)  // map[string]User
byUser, err := Posts.Query().Grouped(Posts.UserID) // map[int64][]Post
names, err := Users.Query().Map(func(u User) string { return u.Name })
```

Tables, queries and columns carry the model's key type
(`orm.Table[User, int64]`, `orm.Query[User, int64]`), so every key
lookup is typed: `Users.Find("1")` and
`Users.Where(...).Find("1")` do not compile when the key is an
`int64`, and the same holds for `FindMany`, `FindOr`, `Destroy`,
`WhereKey` and `WhereKeyNot`. A table without a primary key has key type
`orm.NoKey`. ormgen also emits `UserQuery` (= `orm.Query[User, int64]`)
for writing scopes.

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
Users.Where(orm.AnyOf(orm.ILike, "%car%", Users.Name, Users.Email)) // whereAny
Users.Where(Users.Name.Contains("50%"), Users.Email.EndsWith("@example.com")) // literal text
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

Users.Query().When(onlyActive, func(q UserQuery) UserQuery {
	return q.Where(Users.Active.Eq(true))
})
```

`Like` and `NotLike` take a SQL pattern (`%` any run, `_` one character,
`\` escaping either) and are case-sensitive on every engine; `ILike` and
`NotILike` ignore case on every engine. `Contains`, `StartsWith` and
`EndsWith` match their text literally (`orm.EscapeLike` escapes it), and
wildcards match across newlines everywhere. An operator outside the `orm.`
constants fails the query with an error.


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
	Pluck(Users.Name)

counts := Posts.Query().Select(Posts.UserID).AddSelectAs(Posts.Views, orm.Count[Post]()).GroupBy(Posts.UserID)
Users.Query().JoinSub(counts, "pc", Posts.UserID.EqCol(Users.ID), Posts.Views.Gte(2))

// A model query returns its own model; Joined returns both sides of each row,
// each from its own table's columns, Right nil where a LEFT JOIN found none.
pairs, err := orm.Joined(Users.Query().LeftJoin(Posts, Posts.UserID.EqCol(Users.ID)), Posts.Table)
for _, p := range pairs { _, _ = p.Left.Name, p.Right } // Right is *Post

// A lateral subquery sees the outer row; read its columns with From(alias).
latest := Posts.Where(Posts.UserID.EqCol(Users.ID)).OrderBy(Posts.CreatedAt.Desc()).Limit(1)
Users.Query().LeftJoinLateral(latest, "lp").AddSelectAs(Users.LatestPostTitle, Posts.Title.From("lp"))

Users.Where(Users.Karma.Gt(90)).Union(Users.Where(Users.Name.Eq("Bob")))
Users.Query().OrderBy(Users.Name.Asc()).Latest().InRandomOrder().Reorder(Users.ID.Desc())
Users.Query().Limit(10).Offset(20) // or ForPage(3, 10)
Users.Query().Distinct().LockForUpdate() // SharedLock()
```

A locking read waits for rows another transaction has locked, unless told
otherwise: `NoWait()` fails at once, and `SkipLocked()` leaves those rows
out, so workers of a job queue each claim a different job:

```go
err := orm.Transaction(ctx, func(ctx context.Context) error {
	job, err := Jobs.WithContext(ctx).Query().Where(Jobs.Done.Eq(false)).
		OrderBy(Jobs.ID.Asc()).LockForUpdate().SkipLocked().First()
	if err != nil {
		return err
	}
	// ... run it, mark it done
})
```

Postgres, MySQL 8 and MariaDB 10.6+ render `NOWAIT` / `SKIP LOCKED`; SQLite
locks the whole database, so there the locks are no-ops, as in Laravel.
`NoWait` and `SkipLocked` without a lock are an error, and a document store
refuses row locks rather than ignoring them.

## Aggregates and single values

```go
n, err := Users.Query().Count()
sum, err := Users.Query().Sum(Users.Karma)      // int: Sum only takes numeric columns
avg, err := Posts.Query().Avg(Posts.Views)      // float64
newest, err := Posts.Query().Max(Posts.CreatedAt)
ok, err := Users.Where(Users.Name.Eq("Alice")).Exists()
emails, err := Users.Query().Pluck(Users.Email)  // []string
perUser, err := Posts.Query().GroupBy(Posts.UserID).PluckMap(Posts.UserID, orm.Count[Post]())
name, err := Users.Query().OrderBy(Users.Karma.Desc()).Value(Users.Name)
```

`Sum` of no rows is zero; `Min`, `Max` and `Avg` of no rows (or only
NULLs) have no value and return `orm.ErrNotFound`, never a zero that
could be real.

A plain order puts NULLs where the engine does — first ascending on
SQLite, MySQL and MariaDB, last on Postgres, DuckDB and ClickHouse — which
is what their indexes are built for. `Users.CountryID.Asc().NullsLast()`
sorts them last on every engine; on MongoDB, which sorts NULLs and missing
fields first ascending, such an order runs as an aggregation that sorts on
a computed "is null" field first. Offset pagination and chunking end the
order with the primary key, so ties never skip or repeat rows, and cursor
pagination sorts nullable columns NULLs last itself.

## Chunking, iterators, pagination

```go
err := Posts.Query().OrderBy(Posts.ID.Asc()).Chunk(100, func(ps []Post) error { return nil })
err = Posts.Query().ChunkByID(100, func(ps []Post) error { return orm.ErrStop }) // stop early
err = Posts.Query().ChunkWhile(func(prev, cur Post) bool { return prev.UserID == cur.UserID },
	func(ps []Post) error { return nil }) // chunkWhile, ordered by key

for p, err := range Posts.Query().With(PostComments).Lazy(500) { _, _ = p, err } // lazyById
for p, err := range Posts.Query().Cursor() { _, _ = p, err }                       // one query, streamed

page, err := Posts.Query().Latest().Paginate(20, 2)   // Items, Total, LastPage, From, To, HasMore
simple, err := Posts.Query().SimplePaginate(20, 2)    // HasMore, no count query
cur, err := Posts.Query().OrderBy(Posts.Views.Desc(), Posts.ID.Asc()).CursorPaginate(20, cursor)
next, prev := cur.NextCursor, cur.PrevCursor // pass either back as cursor
```

`Paginate` is length-aware on every engine — SQL, ClickHouse, MongoDB and
Qdrant alike — and serialises like Laravel's paginator (`data`, `total`,
`per_page`, `current_page`, `last_page`, `from`, `to`, `has_more`). Row
queries page the same way and return `Page[map[string]any]`; a grouped or
distinct row query counts groups or distinct rows. Page and size must be
at least 1. `CursorPaginate`, `ChunkByID` and `Lazy` page by key instead
of offset on every engine too; on Qdrant, which can only order by an
indexed payload field, ordering by the key (or not at all) uses its native
point-id scroll. A cursor page serialises as `data`, `per_page`, `next_cursor` and
`prev_cursor`: `PrevCursor` walks back towards the first page (which has
none), and `NextCursor` is empty on the last page. Nullable order columns
page with their NULLs last in either direction. Qdrant scrolls forward
only, so it refuses a previous-page cursor as not translatable.

```go
page, err := orm.From("events").On("mongo").OrderBy("created_at").Paginate(50, 3)
err = orm.FromTable(Users).Chunk(500, func(rows []map[string]any) error { return nil })
for row, err := range orm.FromTable(Users).Lazy(500) { _, _ = row, err } // by key column
for row, err := range orm.From("users").Cursor() { _, _ = row, err }      // one query, streamed
```

## Inserting and updating

```go
u := User{Name: "Frank", Email: "frank@example.com"}
err := Users.Create(&u)   // auto id / uuid / ulid, timestamps, events
u.Karma = 10
err = Users.Save(&u)      // UPDATE only dirty columns

Users.Where(Users.CountryID.Eq(2)).Update(Users.Active.Set(false)) // mass update, maintains updated_at
Posts.Where(Posts.ID.Eq(1)).Increment(Posts.Views, 10)
Posts.Where(Posts.ID.Eq(1)).Decrement(Posts.Views, 1, Posts.Title.Set("edited"))

Videos.Query().Insert(Video{Title: "A"}, Video{Title: "B"})   // no events / timestamps
id, err := Videos.Query().InsertGetID(Video{Title: "C"})       // insertGetId
Tags.Query().InsertOrIgnore(Tag{Name: "go"})
Roles.Query().Upsert([]Role{{ID: 1, Name: "superadmin"}}, []orm.AnyColumn[Role]{Roles.ID})
Tags.Query().InsertUsing([]orm.AnyColumn[Tag]{Tags.Name}, Roles.Query(), orm.Upper(Roles.Name))

u, err = Users.Query().FirstOrNew(orm.Attrs(Users.Email.Set("a@b.c")), Users.Name.Set("A"))
u, err = Users.Query().FirstOrCreate(orm.Attrs(Users.Email.Set("a@b.c")), Users.Name.Set("A"))
u, err = Users.Query().UpdateOrCreate(orm.Attrs(Users.Email.Set("a@b.c")), Users.Karma.Set(1000))
u, err = Users.Query().CreateOrFirst(orm.Attrs(Users.Email.Set("a@b.c")))

clone, err := Posts.Replicate(&post, Posts.Views)    // unsaved copy without key/timestamps/views
err = Posts.Touch(&post)
err = Posts.Refresh(&post)
fresh, err := Posts.Fresh(&post)
same := Posts.Is(&a, &b)

fallback, err := Users.Query().FindOr(99, func() (User, error) { return User{Name: "nobody"}, nil })

err = Users.WithContext(orm.WithoutTimestamps(ctx)).Create(&u)
err = Images.Truncate()
err = Images.Query().On("archive").Truncate() // on another connection; a document store deletes every document
```

## Deleting and soft deletes

```go
err := Users.Delete(&u)              // soft delete (soft_deletes directive)
Users.Trashed(&u)
Users.Query().WithTrashed().Find(u.ID)
Users.Query().OnlyTrashed().Get()
err = Users.Restore(&u)
err = Users.ForceDelete(&u)
n, err := Users.Destroy(4, 5)        // loads and deletes each, with events

Users.Where(Users.Name.Eq("Eve")).Delete()      // mass (soft) delete
Users.Where(Users.Name.Eq("Eve")).Restore()
Users.Where(Users.Name.Eq("Eve")).ForceDelete()

n, err = Users.Where(Users.CreatedAt.Lt(cutoff)).Prune(500) // Prunable: one by one, with events
```

## Dirty tracking

```go
u.Karma = 10
Users.Karma.IsDirty(&u)       // true
Users.Karma.Original(&u)      // value when loaded
Users.Dirty(&u)               // []string{"karma"}
Users.IsDirty(&u, Users.Karma, Users.Name)
Users.Save(&u)
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

`OfMany(col, useMax)` turns a `HasOne` into Eloquent's `latestOfMany` /
`oldestOfMany` / `ofMany`: of the related rows the relation's own
constraints allow, it keeps the one with the greatest (or least) `col`, ties
going to the greatest (or least) key. `col` can be any ordered column,
`time.Time` included. The pick is made in SQL, so an eager load fetches one
row per parent, `WhereHas(UserLatestPost, ...)` and `WithCount` look at the
picked row only, and the scopes of `With(UserLatestPost, scope)` filter the
pick rather than choosing among the rows they keep: a user whose latest post
fails the scope gets nil. (Document stores have no correlated subqueries, so
there the candidates are loaded and the pick is made in Go, with the same
result.)

## Eager loading

```go
users, err := Users.Query().
	With(UserPosts, Published, func(q PostQuery) PostQuery {
		return q.OrderBy(Posts.Views.Desc()).With(PostComments) // nested
	}).
	With(UserLatestPost).
	With(UserRoles).
	Get()

err = orm.Load(ctx, users, UserCountry) // $users->load('country')
// A Limit or Offset in an eager scope applies to each parent, for every
// relation kind: each user's two most viewed posts, each user's newest role.
users, err = Users.Query().
	With(UserPosts, func(q PostQuery) PostQuery { return q.OrderBy(Posts.Views.Desc()).Limit(2) }).
	With(UserRoles, func(q RoleQuery) RoleQuery { return q.OrderBy(Roles.ID.Desc()).Limit(1) }).
	Get()

err = orm.LoadMissing(ctx, users, UserPosts) // only the users whose posts are not loaded yet

// MorphTo owners are of several models, so scopes go on each target
// (morphWith / constrain); the untyped With scope is refused.
comments, err := Comments.Query().With(CommentCommentable.Constrain(
	orm.Target(Posts.ID, Published, func(q PostQuery) PostQuery { return q.With(PostAuthor) }),
)).Get()
err = orm.LoadCount(ctx, users, UserPosts, Users.PostsCount, Published)      // $users->loadCount('posts')
err = orm.LoadSum(ctx, users, UserPosts, Posts.Views, Users.PostViews)       // also LoadAvg, LoadMin, LoadMax, LoadExists
```

## Querying relationships

```go
Users.Query().WhereHas(UserPosts, Published, Popular(100))
Users.Query().WhereDoesntHave(UserPosts)
Users.Where(Users.Name.Eq("Dave")).OrWhereHas(UserRoles)
Users.Query().WhereRelation(UserRoles, Roles.Name.Eq("admin"))
Users.Query().HasCount(UserPosts, orm.Gte, 2)
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
UserPosts.Of(&u).Where(Posts.Published.Eq(true)).Count() // $user->posts()->...
UserPosts.Create(&u, &Post{Title: "Hi"})                // sets user_id
UserPosts.SaveMany(&u, posts)
PostComments.Create(&post, &Comment{Body: "first!"})     // sets id + type
PostAuthor.Associate(&post, &bob); PostAuthor.Dissociate(&post)
CommentCommentable.Associate(&comment, &video)

UserRoles.AttachIDs(&u, 1, 2)
UserRoles.Attach(&u, RoleUser{RoleID: 3, GrantedBy: "admin"}) // with pivot data
UserRoles.Detach(&u, 2)
res, err := UserRoles.Sync(&u, 1, 3)       // res.Attached, res.Detached
UserRoles.SyncWithoutDetaching(&u, 4)
UserRoles.Toggle(&u, 1, 2)
UserRoles.UpdateExistingPivot(&u, 3, RoleUsers.GrantedBy.Set("changed"))
```

## Scopes

```go
func Active(q UserQuery) UserQuery { return q.Where(Users.Active.Eq(true)) }
func Popular(min int64) func(PostQuery) PostQuery { // dynamic scope
	return func(q PostQuery) PostQuery { return q.Where(Posts.Views.Gte(min)) }
}
Users.Query().Scope(Active)

Users.AddGlobalScope("verified", func(q UserQuery) UserQuery { return q.Where(...) })
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

Users.WithContext(orm.WithoutEvents(ctx)).Create(&u)
Users.SaveQuietly(&u)
```

The available events are `Retrieved`, `Creating`, `Created`, `Updating`, `Updated`, `Saving`, `Saved`,
`Deleting`, `Deleted`, `Trashed`, `ForceDeleting`, `ForceDeleted`, `Restoring`,
`Restored` and `Replicating`.

## Transactions

```go
err := orm.Transaction(ctx, func(ctx context.Context) error {
	if err := Users.WithContext(ctx).Create(&u); err != nil {
		return err // rolls back
	}
	orm.AfterCommit(ctx, func(ctx context.Context) { sendWelcomeMail(u) })
	return orm.Transaction(ctx, func(ctx context.Context) error { // nested: a savepoint
		return nil
	})
})

// Re-run the whole transaction when the database reports a deadlock or
// lock timeout, up to three times (DB::transaction's $attempts).
err = orm.RetryingTransaction(ctx, 3, func(ctx context.Context) error { return Users.WithContext(ctx).Save(&u) })
```

Every query that receives the transaction's `ctx` runs inside it.

On MongoDB (a replica set or sharded cluster) `orm.TransactionOn(ctx,
"mongo", ...)` is a real multi-document transaction. MongoDB has no
savepoints, so a nested transaction joins the outer one, and if it fails
the whole transaction rolls back with `orm.ErrNestedFailed` instead of
committing half of it. Qdrant has no transactions and refuses with
`ErrNotTranslatable`; `orm.SupportsTransactions` reports which connections
can, and seeders run in a transaction exactly where it is supported.

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
AuditLog.Query().Count()               // runs on "audit"
Users.Query().On("replica").Get()      // Model::on()

sqlText, args, err := Users.Where(Users.Karma.Gt(10)).ToSQLFor(orm.Postgres)

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
	Get() // []map[string]any

n, err := orm.From("users").Where("email", orm.Eq, "a@b.c").Count()
id, err := orm.From("users").InsertGetID(map[string]any{"name": "Ada", "email": "ada@example.com"})
n, err = orm.From("users").Where("id", orm.Eq, id).Update(map[string]any{"name": "Ada L"})
n, err = orm.From("users").Where("id", "=", id).Delete()
err = orm.From("users").UpdateOrInsert(
	map[string]any{"email": "ada@example.com"},
	map[string]any{"name": "Ada L"})
```

Operators are `orm.Op` constants (`orm.Eq`, `orm.Neq`, `orm.Gt`, `orm.Gte`,
`orm.Lt`, `orm.Lte`, `orm.Like`, `orm.NotLike`, `orm.ILike`, `orm.NotILike`);
string literals still convert. When a model exists, its typed columns carry
over — completion lists the valid columns and the values are compile-checked:

```go
rows, err := orm.From("users").
	SelectCols(Users.Email).
	Join(Countries, Countries.ID.EqCol(Users.CountryID)).
	WhereCond(Users.Active.Eq(true), Users.Karma.Gt(10)).
	OrderByColDesc(Users.Karma).
	Limit(10).
	Get()
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
	Get() // rows[0]["user"], rows[0]["country"]
```

Grouping selects typed aggregates under a name; `HavingCond` filters on
them portably (Postgres cannot see select aliases in `HAVING`):

```go
rows, err := orm.FromTable(Users).
	SelectCols(Users.CountryID, orm.Count[User]().As("users")).
	GroupByCols(Users.CountryID).
	HavingCond(orm.Count[User]().Gt(1)).
	Get()
rows, err = orm.From("users").
	SelectRaw("country_id, COUNT(*) AS n").
	GroupBy("country_id").
	HavingRaw("COUNT(*) > ?", 1).
	Get()
```

Typed writes and plucks reuse the model's assignments and columns:

```go
users := orm.FromTable(Users)
err = users.InsertCols(Users.Name.Set("Ada"), Users.Email.Set("ada@example.com"))
n, err = users.WhereCond(Users.Email.Eq("ada@example.com")).
	UpdateCols(Users.Name.Set("Ada L"), Users.Karma.SetExpr(orm.Plus(Users.Karma, 1)))
emails, err := users.OrderByCol(Users.Email).PluckCol(Users.Email) // []string
```

`InsertCols` and `UpdateCols` write exactly what they are given, like
`Insert` and `Update`: no timestamps, no events. On a document store,
`SetExpr` assignments and `As` aliases are refused, since neither has a
document form.

Aggregates, counters and bulk writes round out `DB::table`:

```go
total, err := orm.From("orders").Sum[int64]("amount") // the type is the caller's
karma, err := orm.FromTable(Users).SumCol(Users.Karma)  // inferred: int
avg, err := orm.From("users").Avg("karma")                    // float64
n, err := orm.From("posts").Where("id", orm.Eq, 7).Increment("views", 1)

err = orm.From("countries").InsertOrIgnore(map[string]any{"id": 1, "name": "Denmark"})
err = orm.From("countries").Upsert(
	[]map[string]any{{"id": 1, "name": "Danmark"}, {"id": 4, "name": "Finland"}},
	[]string{"id"}, // conflict columns
	"name")         // columns to update; default: every other inserted column

sql, args, err := orm.From("users").Where("users.karma", orm.Gt, 10).ToSQL()
```

`UpdateOrInsert` checks existence before writing, so it never inserts a
duplicate on engines that report only changed rows (MySQL). `Count` after
`Distinct` counts distinct rows. On document stores, `InsertOrIgnore`
matches on the key column (`WithKey`, default `id`), `Upsert` on
Qdrant matches only that key (the point id), `Increment` and
`Decrement` become `$inc` on MongoDB, `Sum` / `Avg` / `Min` / `Max` run
as an aggregation pipeline on MongoDB, and aggregates and increments on
Qdrant return `ErrNotTranslatable`. ClickHouse, which has no unique
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
		return UserPosts.WithContext(ctx).Create(u, &Post{Title: "by " + u.Name})
	}).
	Create()
draft, err := UserFactory.MakeOne() // unsaved
```

`State` and `Sequence` apply in the order they are called, so a later state
overrides an earlier sequence; a negative `Count` or an empty `Sequence` is
returned as an error by `Make` and `Create`.

## Raw SQL

For what the builders do not cover, `orm.Select` and `orm.Statement` run
SQL on a connection (`DB::select`, `DB::statement`). Placeholders are `?`
on every engine and are bound, never interpolated; a `?` inside a quoted
string stays text, `??` writes a literal `?` (Postgres's jsonb operator),
and a placeholder count that differs from the arguments is an error. The
same rules hold for every `*Raw` builder method. The statements reach
`orm.Listen`, honour transactions and sticky reads, and refuse on document
stores.

```go
rows, err := orm.Select(ctx, "SELECT name FROM users WHERE karma > ?", 10)   // []map[string]any
n, err := orm.StatementOn(ctx, "audit", "DELETE FROM audit_log WHERE at < ?", cutoff)
```

## Debugging

```go
sqlText, args, err := Users.Where(Users.Name.Eq("x")).ToSQL()
raw, err := Users.Where(Users.Name.Eq("O'Brien")).ToRawSQL() // args inlined

plan, err := Users.Where(Users.Karma.Gt(10)).Explain() // []map[string]any

stop := orm.Listen(func(e orm.QueryEvent) { log.Println(e.Duration, e.SQL) }) // DB::listen
defer stop()

orm.EnableQueryLog()                            // DB::enableQueryLog
must(Users.Query().Count())
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

A foreign key's columns get an index (`flights_airline_id_index`) on
Postgres and SQLite, which, unlike MySQL and MariaDB, do not index them on
their own; without it, loading a parent's children or deleting a parent
reads the whole child table. An index that already leads with those
columns counts, and the implied one is created only if missing.

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

Names longer than the engine keeps fail loudly instead of being cut
short: Postgres keeps 63 bytes (it would truncate silently, so two long
index names could collide) and MySQL and MariaDB 64 characters. That
covers generated index and foreign key names (`users_email_unique`);
give a long one its own with `Name` or `Named`. Queries refuse over-long
names on those engines too.

## Migrations

```go
func init() {
	schema.Register(schema.Migration{
		Name: "2026_01_01_000000_create_flights_table",
		Up: func(ctx context.Context, s *schema.Builder) error {
			return s.WithContext(ctx).Create("flights", func(t *schema.Blueprint) { t.ID(); t.Timestamps() })
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
go run ./cmd/migrate reset | refresh [-steps N] [-seed] | fresh [-seed] [-drop-views] | wipe [-drop-views]
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

Inside `Up` and `Down`, run orm queries with the ctx they receive
(`orm.From("users").WithContext(ctx)...`) so they join the migration's
transaction. `-pretend` prints the schema statements without running them
and runs `Up` in a transaction it always rolls back, undoing those orm
writes; on a connection without transactions (ClickHouse, a document
store) the ctx is `orm.ReadOnly` and a write fails with `orm.ErrReadOnly`
rather than applying. Each command accepts only its own flags.

Views are created over a SELECT in the engine's SQL, listed apart from
tables, and dropped one by one or all at once; `migrate:fresh -drop-views`
(`Migrator.DropViews`) drops them before the tables:

```go
err = schema.Default().CreateView(ctx, "active_users", "SELECT * FROM users WHERE active")
views, err := schema.Default().Views(ctx)
err = schema.Default().DropViewIfExists(ctx, "active_users")
```

`CreateDatabase(ctx, name)` and `DropDatabaseIfExists(ctx, name)` manage
databases on Postgres, MySQL, MariaDB and ClickHouse servers; SQLite and
DuckDB databases are files you open with `orm.Open`, so they refuse.

Indexes can be partial or cover an expression:

```go
t.Unique("email").Where("deleted_at IS NULL")    // unique among live rows (Postgres, SQLite)
t.RawIndex("lower(email)", "users_email_lower")  // expression index (not MariaDB)
```

Engines without the form refuse it with a `schema.UnsupportedError`.

Time columns take an optional precision, the fractional second digits
stored: `t.Timestamp("at", 6)`, `t.Timestamps(3)`, `t.DateTime("seen",
6).UseCurrent()` (MySQL's default becomes `CURRENT_TIMESTAMP(6)`). The
default is whole seconds, as in Laravel; precision runs from 0 to 6, and
a microsecond time stored in a precision-6 column reads back unchanged on
every engine.

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
			_, err := UserFactory.Count(50).WithContext(ctx).Create()
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
