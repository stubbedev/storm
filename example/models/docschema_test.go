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

// assertDocMigrations migrates a document store through the schema
// package: collections, declared fields, indexes and the migrator.
func assertDocMigrations(t *testing.T, ctx context.Context, conn string) {
	t.Helper()
	s := schema.On(conn)
	mongo := conn == "mongo"
	engine := map[bool]string{true: "mongodb", false: "qdrant"}[mongo]
	refused := func(what string, err error, fragment string) {
		t.Helper()
		var u *schema.UnsupportedError
		eq(t, what+" refused", errors.As(err, &u) && u.Engine == engine && strings.Contains(err.Error(), fragment), true)
	}

	eq(t, conn+" driver name", must(s.DriverName(ctx)), engine)
	ok(s.DropIfExists(ctx, "pets"))
	ok(s.Create(ctx, "pets", func(t *schema.Blueprint) {
		t.UUID("id").Primary()
		t.String("name").Index()
		t.Integer("age").Unsigned()
		t.Enum("kind", "cat", "dog").Nullable()
		t.Text("bio").Nullable()
	}))
	ok(s.CreateIfNotExists(ctx, "pets", func(t *schema.Blueprint) { t.UUID("id").Primary() }))
	eq(t, conn+" hasTable", must(s.HasTable(ctx, "pets")), true)
	eq(t, conn+" tables", slices.Contains(must(s.Tables(ctx)), "pets"), true)
	eq(t, conn+" hasIndex", must(s.HasIndex(ctx, "pets", []string{"name"})), true)
	eq(t, conn+" no foreign keys", len(must(s.GetForeignKeys(ctx, "pets"))), 0)
	ok(s.WithoutForeignKeyConstraints(ctx, func(context.Context) error { return nil }))

	pets := orm.From("pets").On(conn)
	valid := map[string]any{"id": "0198c0de-0000-7000-8000-0000000000a1", "name": "Rex", "age": 3, "kind": "dog", "bio": "good"}
	ok(pets.Insert(valid))
	err := s.Table(ctx, "pets", func(t *schema.Blueprint) {
		t.DropColumn("bio")
		t.Integer("score").Default(0)
	})
	eq(t, conn+" a refused alter fails", err != nil, true)
	eq(t, conn+" a refused alter touches no document", must(pets.Where("id", orm.Eq, valid["id"]).Value("bio")), any("good"))

	if mongo {
		eq(t, "declared columns", must(s.Columns(ctx, "pets")), []string{"id", "name", "age", "kind", "bio"})
		eq(t, "column type", must(s.GetColumnType(ctx, "pets", "age")), "int")
		for what, doc := range map[string]map[string]any{
			"wrong type":    {"id": "x1", "name": 5, "age": 1},
			"negative":      {"id": "x2", "name": "n", "age": -1},
			"not in enum":   {"id": "x3", "name": "n", "age": 1, "kind": "fish"},
			"missing field": {"id": "x4", "age": 1},
			"null not null": {"id": "x5", "name": nil, "age": 1},
		} {
			eq(t, "validator rejects "+what, pets.Insert(doc) != nil, true)
		}
		ok(pets.Insert(map[string]any{"id": "x6", "name": "Tom", "age": 2, "kind": nil, "bio": nil}))
		ok(s.Table(ctx, "pets", func(t *schema.Blueprint) {
			t.String("color").Nullable()
			t.RenameColumn("bio", "about")
			t.DropColumn("age")
			t.DropIndexOn("name")
			t.Unique("name")
		}))
		eq(t, "altered columns", must(s.Columns(ctx, "pets")), []string{"id", "name", "kind", "about", "color"})
		rex := must(pets.Where("id", orm.Eq, valid["id"]).First())
		eq(t, "rename and drop rewrite documents", fmt.Sprintf("%v %v", rex["about"], rex["age"]), "good <nil>")
		eq(t, "new unique index enforced", pets.Insert(map[string]any{"id": "x7", "name": "Rex"}) != nil, true)
		ok(s.Table(ctx, "pets", func(t *schema.Blueprint) { t.String("color").Change() }))
		eq(t, "changed column", must(s.GetColumns(ctx, "pets"))[4].Nullable, false)
		pretend, log := s.Pretend()
		ok(pretend.Table(ctx, "pets", func(t *schema.Blueprint) {
			t.RenameColumn("about", "story")
			t.DropColumn("color")
			t.DropUnique("pets_name_unique")
		}))
		ok(pretend.Rename(ctx, "pets", "beasts"))
		ok(pretend.Drop(ctx, "pets"))
		eq(t, "pretend records document store changes", log(), []string{
			"rename field about to story in pets",
			"drop fields color from pets",
			"declare fields of pets: id, name, kind, story",
			"drop index pets_name_unique on pets",
			"rename collection pets to beasts",
			"drop collection pets",
		})
		eq(t, "pretend changes nothing", must(s.Columns(ctx, "pets")), []string{"id", "name", "kind", "about", "color"})
		ok(s.Rename(ctx, "pets", "animals"))
		eq(t, "renamed", must(s.HasTable(ctx, "animals")) && !must(s.HasTable(ctx, "pets")), true)
		refused("vector index", s.Create(ctx, "embeds", func(t *schema.Blueprint) {
			t.UUID("id").Primary()
			t.Vector("vec", 3)
			t.HnswIndex("vec", orm.Cosine)
		}), "Atlas")
		ok(s.Drop(ctx, "animals"))
	} else {
		eq(t, "indexed fields are known", must(s.Columns(ctx, "pets")), []string{"name"})
		refused("rename field", s.Table(ctx, "pets", func(t *schema.Blueprint) { t.RenameColumn("bio", "about") }), "cannot move")
		refused("unique index", s.Table(ctx, "pets", func(t *schema.Blueprint) { t.Unique("age") }), "unique")
		refused("index of unknown type", s.Table(ctx, "pets", func(t *schema.Blueprint) { t.Index("age") }), `t.Integer("age").Index()`)
		ok(s.Table(ctx, "pets", func(t *schema.Blueprint) {
			t.DropColumn("bio")
			t.DropIndexOn("name")
			t.Integer("age").Index()
		}))
		rex := must(pets.Where("id", orm.Eq, valid["id"]).First())
		eq(t, "dropped payload field", rex["bio"], nil)
		eq(t, "index swapped", must(s.Columns(ctx, "pets")), []string{"age"})
		refused("rename collection", s.Rename(ctx, "pets", "animals"), "no rename")
		ok(s.Drop(ctx, "pets"))

		ok(s.Create(ctx, "embeds", func(t *schema.Blueprint) {
			t.UUID("id").Primary()
			t.String("label")
			t.Vector("vec", 3)
			t.HnswIndex("vec", orm.L2)
		}))
		embeds := orm.From("embeds").On(conn)
		ok(embeds.Insert(
			map[string]any{"id": "0198c0de-0000-7000-8000-0000000000b1", "label": "x", "vec": orm.NewVector3(1, 0, 0)},
			map[string]any{"id": "0198c0de-0000-7000-8000-0000000000b2", "label": "y", "vec": orm.NewVector3(0, 1, 0)}))
		described := must(must(orm.Connection(ctx, conn)).Doc.(orm.DocSchema).DescribeCollection(ctx, "embeds"))
		eq(t, "vector from the blueprint", *described.Vector, orm.DocVector{Size: 3, Metric: orm.L2})
		eq(t, "vector points stored", must(embeds.Count()), int64(2))
		ok(s.Drop(ctx, "embeds"))
	}

	err = s.Drop(ctx, "pets")
	eq(t, conn+" drop missing", err != nil && strings.Contains(err.Error(), "does not exist"), true)
	refused("auto-increment", s.Create(ctx, "bad", func(t *schema.Blueprint) { t.ID() }), "generated integer keys")
	refused("default", s.Create(ctx, "bad", func(t *schema.Blueprint) { t.Boolean("on").Default(true) }), "defaults")
	refused("foreign key", s.Create(ctx, "bad", func(t *schema.Blueprint) {
		t.UUID("id").Primary()
		t.ForeignUUID("owner_id").Constrained("owners")
	}), "foreign")

	migrator := &schema.Migrator{Connection: conn, Table: "schema_log", Migrations: []schema.Migration{
		{
			Name: "2026_01_01_000001_create_toys",
			Up: func(ctx context.Context, s *schema.Builder) error {
				return s.Create(ctx, "toys", func(t *schema.Blueprint) {
					t.UUID("id").Primary()
					t.String("name").Index()
				})
			},
			Down: func(ctx context.Context, s *schema.Builder) error { return s.Drop(ctx, "toys") },
		},
		{
			Name: "2026_01_01_000002_index_toys",
			Up: func(ctx context.Context, s *schema.Builder) error {
				return s.Table(ctx, "toys", func(t *schema.Blueprint) { t.Integer("price").Index() })
			},
			Down: func(ctx context.Context, s *schema.Builder) error {
				return s.Table(ctx, "toys", func(t *schema.Blueprint) { t.DropIndexOn("price") })
			},
		},
	}}
	pending := must(migrator.Pretend(ctx))
	eq(t, conn+" pretend", len(pending) == 2 && strings.HasPrefix(pending[0].Statements[0], "create collection toys"), true)
	eq(t, conn+" pretend runs nothing", must(s.HasTable(ctx, "toys")), false)
	eq(t, conn+" migrate", must(migrator.Migrate(ctx, false)), []string{"2026_01_01_000001_create_toys", "2026_01_01_000002_index_toys"})
	eq(t, conn+" migrated", must(s.HasIndex(ctx, "toys", []string{"price"})), true)
	status := must(migrator.Status(ctx))
	eq(t, conn+" status", status[0].Ran && status[1].Ran && status[1].Batch == 1, true)
	eq(t, conn+" nothing left", len(must(migrator.Migrate(ctx, false))), 0)
	eq(t, conn+" rollback", must(migrator.Rollback(ctx, 0)), []string{"2026_01_01_000002_index_toys", "2026_01_01_000001_create_toys"})
	eq(t, conn+" rolled back", must(s.HasTable(ctx, "toys")), false)
	eq(t, conn+" log emptied", len(must(migrator.Status(ctx))) == 2 && !must(migrator.Status(ctx))[0].Ran, true)
	isolated := migrator.Isolated(ctx, func(ctx context.Context) error {
		_, err := migrator.Migrate(ctx, false)
		return err
	})
	if mongo {
		ok(isolated)
		eq(t, "isolated migrate", must(s.HasTable(ctx, "toys")), true)
		ok(s.Drop(ctx, "toys"))
		ok(s.Drop(ctx, "schema_log_lock"))
	} else {
		refused("migration lock", isolated, "unique keys")
	}
	ok(s.Drop(ctx, "schema_log"))
}
