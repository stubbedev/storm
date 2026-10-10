# storm

An Eloquent-style ORM, schema builder and migrator for Go, built on Go 1.27
generic methods and fully type-checked.

```go
users, err := Users.Query().
	Scope(Active).
	Where(Users.Karma.Gt(10)).
	WhereHas(UserPosts, Published).
	With(UserPosts, func(q orm.Query[Post]) orm.Query[Post] {
		return q.OrderBy(Posts.Views.Desc()).With(PostComments)
	}).
	WithCount(UserPosts, Users.PostsCount).
	Latest().
	Paginate(ctx, 20, page)

emails, err := Users.Query().Pluck(ctx, Users.Email) // []string, inferred from the column
total, err := Posts.Query().Sum(ctx, Posts.Views)     // int64; Sum(ctx, Users.Email) does not compile
```

Every column is typed (`orm.Column[User, string]`), so these mistakes are
compile errors rather than runtime SQL errors:

```go
Users.Query().Where(Posts.Title.Eq("x"))       // a Post condition in a User query
Users.Query().Where(Users.Karma.Eq("lots"))    // string for an int column
Users.Query().Sum(ctx, Users.Email)            // string does not satisfy orm.Number
Users.Query().With(PostComments)               // a Post relation on a User query
```

Supported stores are **SQLite**, **PostgreSQL**, **MySQL**, **MariaDB**,
**DuckDB** and **ClickHouse** through SQL, and **MongoDB** and **Qdrant**
through a translation layer — querying, writing, paginating and migrating
all go through the same typed API. The dialect follows the connection
(`orm.Open` tells MySQL from MariaDB by asking the server), the same query
renders correctly for each, and the full test suite runs against all eight.

## Contents

- [Requirements and install](#requirements-and-install)
- [Quick start](#quick-start)
- [Replicas, sticky reads and transactions](#replicas-sticky-reads-and-transactions)
- [Seeding](#seeding)
- [Library-only usage](#library-only-usage)
- [Queries without a model](#queries-without-a-model)
- [Encrypted and hashed columns](#encrypted-and-hashed-columns)
- [Packages](#packages)
- [Editor hints](#editor-hints)
- [Eloquent feature map](#eloquent-feature-map)
- [What Go doesn't allow](#what-go-doesnt-allow)
- [Testing](#testing)
- [Cookbook](docs/cookbook.md): recipes for every feature

## Requirements and install

Go 1.27 or newer is required, because the API relies on generic methods.

```sh
go get github.com/stubbedev/storm
```

Bring your own `database/sql` driver: `modernc.org/sqlite` (or any SQLite
driver), `github.com/jackc/pgx/v5/stdlib`, or `github.com/go-sql-driver/mysql`.

## Quick start

**1. Define models.** A model is a plain struct that embeds `orm.Model`,
plus a directive. The directive options mirror Laravel 13's `#[Table]` /
`#[Connection]` attributes and the `SoftDeletes` / `HasUuids` traits.

```go
package models

//go:generate go run github.com/stubbedev/storm/cmd/ormgen

//orm:table users soft_deletes
type User struct {
	orm.Model
	ID         int64      `db:"id"`
	CountryID  int64      `db:"country_id,nullzero"` // 0 is written as NULL
	Name       string     `db:"name"`
	Email      string     `db:"email"`
	Active     bool       `db:"active"`
	CreatedAt  time.Time  `db:"created_at"`
	UpdatedAt  time.Time  `db:"updated_at"`
	DeletedAt  *time.Time `db:"deleted_at"`
	PostsCount int64      `db:"posts_count,virtual"` // filled by WithCount

	Posts []Post // relations have no db tag
}
```

`go generate` writes `orm_gen.go`, which defines a `Users` schema value with
typed columns (`Users.Email`, `Users.CreatedAt`, ...) and table metadata.

**2. Declare relations and scopes.**

```go
var UserPosts = orm.HasMany(Users.ID, Posts.UserID,
	func(u *User, ps []Post) { u.Posts = ps })

func Active(q orm.Query[User]) orm.Query[User] { return q.Where(Users.Active.Eq(true)) }
```

**3. Connect once.** The driver picks the dialect.

```go
orm.Open(orm.DefaultConnection, "pgx", os.Getenv("DATABASE_URL"))
orm.Open("analytics", "mysql", os.Getenv("ANALYTICS_DSN")) // used by //orm:table x connection=analytics
```

**4. Migrate.**

```go
schema.Register(schema.Migration{
	Name: "2026_01_01_000000_create_users_table",
	Up: func(ctx context.Context, s *schema.Builder) error {
		return s.Create(ctx, "users", func(t *schema.Blueprint) {
			t.ID()
			t.ForeignID("country_id").Nullable().Constrained().NullOnDelete()
			t.String("name")
			t.String("email").Unique()
			t.Timestamps()
			t.SoftDeletes()
		})
	},
	Down: func(ctx context.Context, s *schema.Builder) error { return s.DropIfExists(ctx, "users") },
})

schema.Run(ctx, schema.NewMigrator(), os.Args[1:], os.Stdout) // migrate | rollback | fresh | status | make ...
// or call the methods directly: schema.NewMigrator().Migrate(ctx, false)
```

**5. Query.** No database handle is passed around; connections resolve by
name, and transactions travel in `ctx`.

```go
u := User{Name: "Ada", Email: "ada@example.com"}
err := Users.Create(ctx, &u)               // u.ID, timestamps filled in

err = orm.Transaction(ctx, func(ctx context.Context) error {
	u.Name = "Ada Lovelace"
	return Users.Save(ctx, &u)              // updates only dirty columns
})
```

The `example/` directory contains a complete schema covering every relation
type, its migrations, a migration CLI (`go run ./example/migrate`), and the
test suite that exercises all of it.

## Replicas, sticky reads and transactions

One connection can carry two pools: `SELECT`s run against the read DSN,
everything else (including `INSERT ... RETURNING`) against the write DSN.

```go
write, err := orm.OpenReadWrite(orm.DefaultConnection, "mysql", readDSN, writeDSN)
```

Because replication is asynchronous, a read straight after a write can land
on a replica that has not applied it yet. `orm.Sticky` gives a ctx Laravel's
`sticky` semantics: after the first write, the rest of that ctx reads from
the write pool. Open the scope once where the request's ctx is created
(middleware is the usual place); routing from there is automatic.

```go
ctx := orm.Sticky(r.Context()) // in middleware

// reads hit the replica...
users, err := Users.Query().Get(ctx)
// ...until a write happens
err = Users.Create(ctx, &u)
// ...after which this read sees u despite replication lag
users, err = Users.Query().Get(ctx)
```

A plain ctx keeps reading the replica; other requests are unaffected.
Inside a transaction everything runs on the transaction anyway, so
read-your-own-writes is guaranteed there without sticky. Transactions nest
as savepoints, can run callbacks after commit, and can re-run themselves on
deadlock:

```go
err := orm.RetryingTransaction(ctx, 3, func(ctx context.Context) error {
	return Users.Save(ctx, &u) // re-run on deadlock or lock timeout, up to 3 times
})
```

Every executed statement is reported to `orm.Listen` callbacks, and
`orm.EnableQueryLog` / `orm.QueryLog` / `orm.FlushQueryLog` record them for
debugging; `Query.Explain(ctx)` returns the database's plan as maps.

## Seeding

Seeders pair with factories and run each in their own transaction; a failing
seeder stops the run. Register them next to their definition, typically from
`init()`:

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
```

The same CLI that migrates also seeds:

```sh
schema.Run(ctx, m, []string{"db:seed"}, os.Stdout)          // every seeder
schema.Run(ctx, m, []string{"db:seed", "-class", "Users"}, os.Stdout)
schema.Run(ctx, m, []string{"fresh", "-seed"}, os.Stdout)    // drop, migrate, seed
schema.Run(ctx, m, []string{"make:seeder", "Users"}, os.Stdout) // scaffold a seeder
schema.Run(ctx, m, []string{"make:model", "LineItem"}, os.Stdout) // scaffold a model
```

`schema.Seed(ctx, names...)` and `schema.SeedOn(ctx, "analytics")` expose it
programmatically.

## Library-only usage

Nothing here requires a CLI. `schema.Run` is a thin flag parser over the
same calls, so an application can migrate and seed itself at startup, and
tests can set up their database without shelling out:

```go
import (
	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"

	_ "yourapp/migrations" // init() calls schema.Register
	_ "yourapp/seeders"    // init() calls schema.RegisterSeeder
)

func main() {
	orm.Open(orm.DefaultConnection, "pgx", os.Getenv("DATABASE_URL"))

	if _, err := schema.NewMigrator().Migrate(ctx, false); err != nil {
		log.Fatal(err) // no-op when the schema is current
	}
	if _, err := schema.Seed(ctx); err != nil {
		log.Fatal(err)
	}
}
```

Every `schema.Run` command has a method behind it: `Migrator.Migrate`,
`Rollback`, `Reset`, `Refresh`, `Fresh`, `Status`, `Pretend`, `schema.Seed`,
and the `Builder` DDL (`schema.On(conn).Create/...`) can be called directly
without migrations at all. Only the `make:*` scaffolds and `go generate`
are dev-time codegen with no runtime role.

## Queries without a model

Not every table deserves a struct. `orm.From` is `DB::table`: the same
builder over a bare table, with rows as maps and no model machinery.

```go
rows, err := orm.From("users").
	Where("active", orm.Eq, true).
	WhereIn("country_id", 1, 2).
	WhereNull("deleted_at").
	WhereRaw("karma > ?", 10).
	OrderBy("name").
	Limit(10).
	Get(ctx) // []map[string]any

id, err := orm.From("users").InsertGetID(ctx, map[string]any{"name": "Ada", "email": "a@x.io"})
n, err := orm.From("users").Where("id", orm.Eq, id).Update(ctx, map[string]any{"name": "Ada L"})
err = orm.From("users").UpdateOrInsert(ctx,
	map[string]any{"email": "a@x.io"},
	map[string]any{"name": "Ada L"})
```

The operator is an `orm.Op` (`orm.Eq`, `orm.Neq`, `orm.Gt`, `orm.Gte`,
`orm.Lt`, `orm.Lte`, `orm.Like`, `orm.NotLike`, `orm.Ilike`,
`orm.NotIlike`) — typing `orm.` lists exactly the valid ones — and a plain
string literal still converts, so `"="` keeps working.

When a model does exist, its typed columns slot into the same builder:
completion after `Users.` lists exactly the valid columns, the compared
and written values are compile-checked, and selected, joined and ordered
columns render table-qualified. Joins take the joined model's schema and
typed conditions, exactly as on a model query — `EqCol` only accepts a
column of the same value type:

```go
rows, err := orm.FromTable(Users). // table and connection from the model
	SelectCols(Users.Name.As("user"), Countries.Name.As("country")).
	Join(Countries, Countries.ID.EqCol(Users.CountryID)).
	WhereCond(Users.Karma.Gt(10)).
	OrderByCol(Users.Name).
	Get(ctx) // []map[string]any{{"user": "Alice", "country": "Denmark"}, ...}

emails, err := orm.FromTable(Users).PluckCol(ctx, Users.Email) // []string
err = orm.FromTable(Users).InsertCols(ctx, Users.Name.Set("Ada"), Users.Email.Set("a@x.io"))
n, err := orm.FromTable(Users).WhereCond(Users.Email.Eq("a@x.io")).
	UpdateCols(ctx, Users.Name.Set("Ada L"), Users.Karma.SetExpr(orm.Plus(Users.Karma, 1)))
```

Grouping takes typed aggregates named with `As`, and every page comes back
length-aware on every engine, document stores included:

```go
perCountry, err := orm.FromTable(Users).
	SelectCols(Users.CountryID, orm.Count[User]().As("users"), orm.Sum(Users.Karma).As("karma")).
	GroupByCols(Users.CountryID).
	HavingCond(orm.Count[User]().Gt(1)).
	Get(ctx)

page, err := orm.FromTable(Users).OrderByCol(Users.ID).Paginate(ctx, 20, 2)
// page.Items, page.Total, page.LastPage, page.From, page.To, page.HasMore
```

The typed variants are `FromTable`, `WhereCond`, `OrWhereCond`,
`SelectCols` (with `As` on any column or aggregate), `Join`, `LeftJoin`,
`RightJoin`, `CrossJoin`, `GroupByCols`, `HavingCond`, `OrderByCol`,
`OrderByColDesc`, `PluckCol`, `SumCol`/`AvgCol`/`MinCol`/`MaxCol`,
`InsertCols` and `UpdateCols`. Like their string counterparts,
`InsertCols` and `UpdateCols` write exactly what they are given — no
timestamps or events.

It also has `First`, `Find`, `Sole`, `Value` / `ValueCol`, `Pluck`,
`Count`, `Exists`, `DoesntExist`, `Sum`/`Avg`/`Min`/`Max`, an `Or` form of
every `Where` (`OrWhereIn`, `OrWhereNull`, `OrWhereBetween`, ...),
`WhereColumn`, `WhereExists`, `Distinct`, `SelectRaw`, `GroupBy`,
`Having`, `HavingRaw`, `Union` / `UnionAll`, `Latest` / `Oldest`,
`InRandomOrder`, `Reorder`, `When` / `Unless`, `LockForUpdate` /
`SharedLock`, `ForPage`, `Paginate`, `SimplePaginate`, `CursorPaginate`,
`Chunk`, `ChunkByID`, `Lazy`, `Cursor`, `Insert`, `InsertOrIgnore`,
`Upsert`, `InsertGetID`, `InsertUsing`, `Update`, `Increment`,
`Decrement`, `UpdateOrInsert`, `Delete`, `Explain` and `ToSQL`; see the
cookbook.
Column names may be qualified (`"users.country_id"`, `"users.*"`), and
every method runs on document stores too, except those with no document
form (joins, grouping, unions, locks, random order, raw SQL, column
comparisons, subqueries, aggregates, expression updates), which return
`ErrNotTranslatable`.

## Analytical engines

**DuckDB** (`github.com/marcboeker/go-duckdb/v2`, CGo) is an embedded OLAP
engine: open a file and query it. CSV, Parquet and JSON are tables through
table functions, so `orm.From` doubles as the flat-file reader.

```go
orm.Open("lake", "duckdb", "analytics.duckdb")

n, err := orm.From("read_csv('events.csv')").On("lake").
	Where("kind", "=", "click").
	Count(ctx)
```

Auto-increment keys draw from per-table sequences the schema builder
creates (and drops with the table); migrations and the full query surface
work. DuckDB enforces only `NO ACTION`/`RESTRICT` foreign keys, cannot add
or drop constraints on existing tables, and cannot alter a table other
tables reference; the schema builder refuses what it cannot express.

**ClickHouse** (`github.com/ClickHouse/clickhouse-go/v2`) speaks the same
builder with the engine's physics showing through: `Update` runs as
`ALTER TABLE ... UPDATE`, `Delete` as a lightweight `DELETE FROM` — both
made synchronous with `mutations_sync`, with affected rows counted before
the mutation since the driver reports none — and there are no
transactions or generated integer keys (use UUID or manual keys). With no
unique constraints to lean on, `Upsert` and `InsertOrIgnore` read which
keys exist first, insert the new rows and update the rest row by row; a
concurrent writer can race that read. Tables default to
`ENGINE = MergeTree` sorted by the primary key; `OrderBy`, `Unsorted`,
`PartitionBy`, `TTL` and `Setting` shape the storage, and
`Engine("Memory")` and friends take no sorting key. Plain indexes become
`bloom_filter` data skipping indexes; unique indexes, foreign keys and
auto-increment columns are refused.

## Vector search

`orm.Vector[orm.D3]` (any `Dn` dimension) is a fixed-size float vector
column, and its size is part of the type: `orm.NewVector3(x, y, z)` and
`orm.NewVector[orm.D3](...)` build one from literals (panicking on the wrong
count), `orm.VectorOf[orm.D3](floats)` checks runtime data, and scanning or
decoding a value of another size fails. Store it in pgvector
(`t.Vector("embedding", 3)` plus `CREATE EXTENSION vector`), DuckDB
(`float[3]`), ClickHouse (`Array(Float64)`) or Qdrant, and order by
distance for a KNN query:

```go
type Doc struct {
	orm.Model
	ID        int64             `db:"id"`
	Embedding orm.Vector[orm.D3] `db:"embedding"`
}

docs, err := Docs.Query().
	OrderBy(Docs.Embedding.Nearest(orm.NewVector3(0.2, 0.1, 0.9), orm.Cosine)).
	Limit(10).
	Get(ctx)
```

Metrics are `orm.L2` (pgvector `<->`), `orm.Cosine` (`<=>`) and
`orm.InnerProduct` (`<#>`); DuckDB uses `array_distance` and friends,
ClickHouse `L2Distance` / `cosineDistance` / `dotProduct`, MySQL the
`DISTANCE()` of HeatWave, MariaDB 11.7+ `VEC_DISTANCE_*`, and SQLite computes L2 in SQL over
JSON while refusing the other metrics loudly.

## Document stores

MongoDB and Qdrant run the same typed builder through a translation layer
instead of SQL. Register the connection, point tables at it with
`connection=`, and query as usual:

```go
orm.OpenMongo("mongo", "mongodb://localhost:27017", "app")

//orm:table events connection=mongo key_type=uuid
type Event struct {
	orm.Model
	ID   string `db:"id"`
	Kind string `db:"kind"`
}

events, err := Events.Query().Where(Events.Kind.Eq("click")).Limit(10).Get(ctx)
err = Events.Query().Where(Events.Kind.Eq("click")).Update(ctx, Events.Kind.Set("tap"))
```

What translates: `Where` (comparisons, `In`, `Between`, null tests,
`And` / `Or` / `Not`), `OrderBy` on columns, `Limit` / `Offset`, `Select`,
`Upsert` and `InsertOrIgnore`, `Increment` / `Decrement` (`$inc` on
MongoDB), and the model lifecycle — `Create`, `Save` (dirty columns only),
`Delete`, soft deletes and global scopes, events, `Count`, `Exists`,
`Pluck` / `PluckMap` / `Value` of columns, `Sum` / `Avg` / `Min` / `Max`
(an aggregation pipeline on MongoDB), length-aware and simple pagination,
chunking (`ChunkByID` and `Lazy` included), transactions (MongoDB replica
sets), and `orm.From(...)` table queries. Keys must be
UUID, ULID or manual (assigned automatically when missing). Every
operation reaches `orm.Listen` and the query log with a readable rendering
of the plan. What refuses, with a precise error naming the part
(`orm.IsNotTranslatable`): joins, unions, `groupBy`, raw fragments,
subqueries, expressions in place of columns, `SetExpr`, and aggregates
and `Increment` on Qdrant, which has no server-side aggregation or atomic
payload arithmetic. A custom store implements `orm.DocStore`;
`UpdateDocs` receives an `orm.DocUpdate` with the fields to set and the
amounts to add.

**Qdrant** is the vector native: collections are tables, payload fields are
the columns, and an `orm.Vector` column becomes the collection's vector —
`Nearest` ordering runs a real similarity search.

```go
store, _ := orm.OpenQdrant("qdrant", "http://localhost:6333")

//orm:table documents connection=qdrant key_type=uuid
docs, err := Docs.Query().
	OrderBy(Docs.Vec.Nearest(orm.NewVector3(1, 0, 0), orm.Cosine)).
	Limit(10).
	Get(ctx)
```

On Qdrant a NULL test matches a field that is missing, null or an empty
array — the filter Qdrant offers for all three. `ILIKE` and `!=` translate
on MongoDB.

### Migrating document stores

The schema builder and migrator drive MongoDB and Qdrant with the same
blueprints as SQL, so one migration set covers every store:

```go
schema.On("mongo").Create(ctx, "events", func(t *schema.Blueprint) {
	t.UUID("id").Primary()
	t.String("kind").Index()
	t.Integer("score").Unsigned()
	t.Enum("level", "info", "warn").Nullable()
})
schema.On("qdrant").Create(ctx, "documents", func(t *schema.Blueprint) {
	t.UUID("id").Primary()           // the point id
	t.String("title").Index()        // a keyword payload index
	t.Vector("vec", 3)
	t.HnswIndex("vec", orm.Cosine)   // the collection's vector size and metric
})
```

On **MongoDB** the declared columns become a `$jsonSchema` validator — types,
nullability, `Unsigned` (minimum 0) and enum values are enforced on write —
indexes become MongoDB indexes, and `Table` renames and drops fields in every
document, changes declarations and manages indexes. On **Qdrant** payloads
are schemaless: indexes become typed payload indexes, the vector column and
its `HnswIndex` size the collection, and dropped columns leave every point.
`Tables`, `GetColumns`, `GetIndexes`, `HasIndex`, `Rename` (MongoDB),
`DropAllTables` and `Fresh` work too. What a store cannot express is refused
with a `schema.UnsupportedError`: generated integer keys, defaults, foreign
keys, unique or multi-field indexes and renames on Qdrant, vector indexes on
MongoDB. The migrator keeps its log in the store when its connection is one.
`store.EnsureCollection(ctx, "documents", 3, orm.Cosine)` remains as a
shortcut outside migrations; it fails if the collection exists with another
size or metric.

## Encrypted and hashed columns

```go
orm.SetEncryptionKey([]byte(os.Getenv("APP_KEY"))) // call once at startup

// type User struct {
// 	orm.Model
// 	ApiToken orm.Encrypted[map[string]string] `db:"api_token"` // AES-256-GCM at rest
// 	Password orm.Hashed                     `db:"password"`    // bcrypt at rest
// }

u.Password = "secret"          // assigning plain text hashes it on write
u.Password.Matches("secret")   // verify
```

`orm.Encrypted[V]` works for any JSON-encodable value; `orm.Hashed` stores a
loaded hash as-is, so re-saving a model never double-hashes. `orm.JSON[V]`
remains the cast for plain JSON columns, and any `sql.Scanner` /
`driver.Valuer` type slots in as a custom cast.

## Packages

| Package | Purpose |
|---|---|
| `orm` | Query builder, models, relations, events, scopes, pagination, transactions, factories |
| `schema` | Blueprint DDL, introspection, migrations, artisan-style CLI (`schema.Run`) |
| `cmd/ormgen` | Generates typed columns and table metadata from model structs |

## Editor hints

The whole API is generic over the model, so gopls carries the model type
through every chain: hovering a query variable shows
`orm.Query[models.User]`, `Pluck(ctx, Users.Email)` shows `[]string`, and a
condition shows `orm.Cond[models.User]`. ormgen also documents every
generated symbol, so hovering `Users.Email` shows the column, table and
field type at once.

To see the types without hovering, turn on gopls inlay hints:

```jsonc
// VS Code: settings.json (or the equivalent ui.inlay_hint in Neovim)
"gopls": {
  "ui.inlayhints": {
    "assignVariableTypes": true,     // q := Users.Query()  // Query[User]
    "compositeLiteralTypes": true,
    "compositeLiteralFields": true, // User{Name: ...}     // Name:
    "parameterNames": true
  }
}
```

Completion after `Users.` lists every column with its type in the popup;
`Users.Where(Users.` offers exactly the typed columns, and passing the
wrong value type (`Users.Karma.Gt("ten")`) is a compile error, not a
runtime one. The same holds inside `orm.From` chains:
`WhereCond(Users.Karma.Gt(10))`, `SelectCols(Users.Email)` and
`UpdateCols(ctx, Users.Karma.Set(5))` take the same typed columns, and `Where("active", orm.Eq, true)` completes the
valid operators after `orm.`.

## Eloquent feature map

| Eloquent | storm |
|---|---|
| `User::query()`, `User::where(...)` | `Users.Query()`, `Users.Where(...)` |
| `where`, `orWhere`, `whereNot`, nested closures | `Where`, `OrWhere`, `WhereNot`, `orm.And` / `orm.Or` |
| `whereIn`, `whereBetween`, `whereNull`, `whereLike` | `Col.In`, `Col.Between`, `Col.IsNull`, `Col.Like` |
| `whereColumn` | `Col.EqCol(other)`, `Col.Cmp(op, other)` |
| `whereDate/Year/Month/Day/Time` | `orm.Date(col).Eq(...)`, `orm.Year(col)`, ... |
| `whereJsonContains/Length/ContainsKey/DoesntContain/Overlaps`, `->` paths | `Col.JSON[T]("a","b")`, `.JSONContains`, `.JSONDoesntContain`, `.JSONOverlaps`, `.JSONLength`, `.JSONHasKey` |
| `whereFullText` | `WhereFullText(term, cols...)` |
| `whereAny`, `whereAll` | `orm.AnyOf`, `orm.AllOf` |
| `whereExists`, `whereIn(subquery)` | `orm.Exists`, `Col.InSub(q.Subquery(col))` |
| `select`, `addSelect`, `selectRaw`, `distinct` | `Select`, `AddSelect`, `AddSelectAs(col, expr)`, `Distinct` |
| `join`, `leftJoin`, `rightJoin`, `crossJoin`, `joinSub` | `Join`, `LeftJoin`, `RightJoin`, `CrossJoin`, `JoinSub` |
| `groupBy`, `having`, `orderBy`, `latest`, `inRandomOrder`, `reorder` | same names |
| `union`, `unionAll`, `when`, `unless`, `lockForUpdate`, `sharedLock` | same names |
| `get`, `first`, `firstOrFail`, `find`, `findMany`, `sole`, `value`, `pluck` | `Get`, `First` (`ErrNotFound`), `Find`, `FindMany`, `FindOr`, `Sole`, `Value`, `Pluck`, `PluckMap` |
| `count`, `sum`, `avg`, `min`, `max`, `exists` | same names; `Sum` and `Avg` only accept numeric columns |
| `chunk`, `chunkById`, `chunkWhile`, `lazy`, `cursor` | `Chunk`, `ChunkByID`, `ChunkWhile`, `Lazy` and `Cursor` (Go iterators) |
| `paginate`, `simplePaginate`, `cursorPaginate` | same names |
| `create`, `save`, `update`, `delete`, `destroy` | `Create`, `Save`, `Update`, `Delete`, `Destroy` |
| `insert`, `insertOrIgnore`, `insertUsing`, `upsert`, `insertGetId` | same names, `InsertGetID` |
| `increment`, `decrement`, `truncate` | `Increment`, `Decrement`, `Truncate` |
| `firstOrNew/Create`, `updateOrCreate`, `createOrFirst` | same names with typed `Attrs(...)` |
| `isDirty`, `isClean`, `wasChanged`, `getOriginal` | `Users.IsDirty(&u)`, `Users.Email.IsDirty(&u)`, `.Original(&u)` |
| `replicate`, `refresh`, `fresh`, `touch`, `is`, `isNot` | same names |
| Soft deletes, `withTrashed`, `onlyTrashed`, `restore`, `forceDelete` | `soft_deletes` directive and the same methods |
| Timestamps, `withoutTimestamps` | automatic; `orm.WithoutTimestamps(ctx)` |
| UUID / ULID keys | `key_type=uuid` / `key_type=ulid` |
| Local / dynamic / global scopes, `withoutGlobalScope(s)` | `Scope(fn)`, `AddGlobalScope`, `WithoutGlobalScope(s)` |
| Events, observers, `withoutEvents`, `saveQuietly` | `Users.On(orm.Creating, fn)`, `Users.Observe(obs)`, `orm.WithoutEvents(ctx)`, `SaveQuietly` |
| `hasOne`, `hasMany`, `belongsTo`, `belongsToMany` (+ pivot) | `orm.HasOne`, `HasMany`, `BelongsTo`, `BelongsToMany` (typed pivot model) |
| `hasOneThrough`, `hasManyThrough` | `orm.HasOneThrough`, `HasManyThrough` |
| `morphOne`, `morphMany`, `morphTo`, `morphToMany`, `morphedByMany` | same names |
| `latestOfMany`, `ofMany`, `withDefault`, `chaperone`, `touches` | `OfMany`, `WithDefault`, `Chaperone`, `Touches` |
| `with`, nested and constrained eager loading, `load` | `With(rel, scopes...)`, `orm.Load` |
| `has`, `whereHas`, `doesntHave`, `whereRelation`, `whereHasMorph`, `whereMorphedTo` | `HasCount`, `WhereHas`, `WhereDoesntHave`, `WhereRelation`, `Rel.HasMorph`, `Rel.Is` |
| `whereBelongsTo`, `whereAttachedTo` | `PostAuthor.Is(&u)`, `UserRoles.AttachedTo(&role)` |
| `withCount/Sum/Avg/Min/Max/Exists` | same names, into typed virtual columns |
| `$user->posts()->...`, `create`, `save`, `associate` | `UserPosts.Of(&u)...`, `.Create`, `.Save`, `.Associate`, `.Dissociate` |
| `attach`, `detach`, `sync`, `toggle`, `updateExistingPivot` | same names |
| Casts (array/json, dates, enums, encrypted, hashed, custom) | `orm.JSON[T]`, `orm.Encrypted[V]` (`orm.SetEncryptionKey`), `orm.Hashed`, `time.Time`, typed string enums, any `sql.Scanner` / `driver.Valuer` |
| Model factories, seeders, `db:seed`, `--seed` | `orm.NewFactory(...).Count(n).State(...).Sequence(...).Create(ctx)`; `schema.Seeder` + `RegisterSeeder` + `schema.Seed`, `db:seed [-class]`, `fresh -seed` |
| Prunable / MassPrunable | `Prune(ctx, chunk)` / `Delete` |
| `DB::transaction`, nested savepoints, `afterCommit`, deadlock retry | `orm.Transaction` (nests), `orm.AfterCommit`, `orm.RetryingTransaction(ctx, attempts, fn)` |
| `DB::listen`, `toSql`, `toRawSql`, `explain`, query log | `orm.Listen`, `ToSQL`, `ToRawSQL`, `Explain(ctx)`, `orm.EnableQueryLog` / `QueryLog` / `FlushQueryLog` |
| Connections, `Model::on()`, read/write split, `sticky` | `orm.Open` / `AddConnection` / `OpenReadWrite`, `connection=` directive, `Query.On(name)`, `orm.Sticky(ctx)` |
| `DB::table('users')` (no model) | `orm.From("users")` / `orm.FromTable(Users)`: wheres, joins, orders, aggregates, `Get` as `[]map[string]any`, `Insert`, `InsertOrIgnore`, `Upsert`, `InsertGetID`, `Update`, `Increment`, `Decrement`, `UpdateOrInsert`, `Delete` |
| `Schema::create/table/drop/rename/hasTable/hasColumn(s)/hasIndex` | `schema.Create`, `Table`, `Drop`, `Rename`, `HasTable`, `HasColumns`, `HasIndex` |
| All Blueprint column types and modifiers | see the cookbook; generated, identity, charset, collation, `after`, `first`, `comment`, `useCurrent`, ... |
| Indexes (primary, unique, index, fullText, spatial), `renameIndex` | same names; SQLite alterations rebuild the table like Laravel |
| Foreign keys, `constrained`, `cascadeOnDelete`, `dropConstrainedForeignId` | same names |
| `getColumns`, `getIndexes`, `getForeignKeys`, `getTables` | same names |
| `migrate`, `rollback`, `reset`, `refresh`, `fresh`, `status`, `--step`, `--pretend`, `make:migration`, `db:wipe`, `db:seed`, `make:seeder`, `make:model` | `schema.Run` commands and `Migrator` methods |

## Engines

Every engine runs the same builder; the matrix shows where behaviour
legitimately differs.

| Engine | Notes |
|---|---|
| SQLite | in-memory or file; JSON1 for JSON columns |
| Postgres (pgvector) | full parity plus `orm.Vector` columns with HNSW indexes (`t.HnswIndex`) |
| MySQL 8 / 9 | `DISTANCE()` vector ordering (HeatWave); fulltext and spatial indexes |
| MariaDB | its own dialect: `INSERT ... RETURNING`, `LOCK IN SHARE MODE`; 11.7+ `VECTOR` columns with `VECTOR INDEX` (`t.HnswIndex`, euclidean or cosine) and `VEC_DISTANCE_*` ordering |
| DuckDB | embedded OLAP; `orm.From("read_csv('f.csv')")` queries flat files; auto-increment via sequences |
| ClickHouse | `Update`/`Delete` are synchronous mutations, counted before they run; upserts read the taken keys first; no transactions or generated integer keys |
| MongoDB | document store: the typed builder translates; joins, unions, subqueries and raw SQL refuse with `orm.ErrNotTranslatable`; keys are uuid/ulid/manual; migrations declare a validator |
| Qdrant | vector store: collections are tables, `Nearest` ordering runs a similarity search; same refusals as MongoDB; migrations create collections and payload indexes |

## What Go doesn't allow

A few Eloquent features depend on PHP runtime features Go deliberately lacks.
Each has a typed equivalent:

- **Magic properties and lazy loading** (`$user->posts`). Relations are loaded
  explicitly with `With` or `orm.Load`, and queried with `UserPosts.Of(&u)`.
- **Dynamic methods** (`whereEmail('x')`, `__call` macros). Use
  `Where(Users.Email.Eq("x"))` and plain Go functions as scopes.
- **Dynamic attributes** (`$user->posts_count`, accessors appended at
  runtime). Declare a `virtual` column for selected extras, and write
  accessors and mutators as ordinary Go methods.
- **Mass assignment / `$fillable`**. Unnecessary, since models are structs and
  updates are typed `Column.Set(v)` assignments.
- **Inheriting static methods** (`User::find()` via `extends Model`). The
  generated schema value plays that role: `Users.Find(ctx, id)`.
- **Variadic generic tuples** (`select(a, b, c)` into an anonymous row). Select into a
  struct's columns, use `PluckMap` for pairs, or add virtual columns.
- **Interfaces with generic methods**. Relation behaviour is reached through
  `Query` methods, not through an interface you could implement yourself.

Engine limitations are surfaced, not hidden. SQLite has no full-text indexes,
so `WhereFullText` falls back to `LIKE`, and its pragma-based foreign-key
toggling has no effect inside a transaction.

## Testing

```sh
just test                      # SQLite, no setup needed
docker compose up -d --wait    # Postgres 17 (pgvector), MySQL 8.4, MariaDB 11.8,
                               # ClickHouse 25, MongoDB 8, Qdrant 1.15
just test-all                  # every engine
```

CI (`.github/workflows/test.yml`) runs the same matrix.
