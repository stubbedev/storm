// Package migrations holds the example schema. Each migration registers
// itself; `go run ./example/migrate make <name>` scaffolds new ones here.
//
// The schema targets the transactional engines (SQLite, Postgres, MySQL,
// MariaDB) the example models run on, and is not meant for the analytical
// ones, which refuse it with a *schema.UnsupportedError: DuckDB enforces
// only NO ACTION and RESTRICT foreign keys, so users' NullOnDelete and
// role_user's CascadeOnDelete cannot be expressed; ClickHouse generates no
// integer keys (every ID() here), and has no foreign keys or unique
// indexes. The models' auto-increment keys need generated keys too.
package migrations

import (
	"context"

	"github.com/stubbedev/storm/schema"
)

type B = schema.Blueprint

func create(name, table string, fn func(*B)) schema.Migration {
	return schema.Migration{
		Name: name,
		Up:   func(ctx context.Context, s *schema.Builder) error { return s.Create(ctx, table, fn) },
		Down: func(ctx context.Context, s *schema.Builder) error { return s.DropIfExists(ctx, table) },
	}
}

func init() {
	schema.Register(
		create("2026_01_01_000000_create_countries_table", "countries", func(t *B) {
			t.ID()
			t.String("name")
		}),
		create("2026_01_01_000001_create_users_table", "users", func(t *B) {
			t.ID()
			t.ForeignID("country_id").Nullable().Constrained("countries").NullOnDelete()
			t.String("name")
			t.String("email").Unique()
			t.Boolean("active").Default(true)
			t.Integer("karma").Default(0)
			t.JSONB("settings").Nullable()
			t.Timestamps()
			t.SoftDeletes()
		}),
		create("2026_01_01_000002_create_posts_table", "posts", func(t *B) {
			t.ID()
			t.UnsignedBigInteger("user_id").Index()
			t.String("title")
			t.Boolean("published").Default(false)
			t.BigInteger("views").Default(0)
			t.Timestamps()
		}),
		schema.Migration{
			Name: "2026_01_01_000011_add_title_fulltext_to_posts_table",
			Up: func(ctx context.Context, s *schema.Builder) error {
				if driver, err := s.DriverName(ctx); err != nil || driver == "sqlite" {
					return err // SQLite has no full text indexes; whereFullText falls back to LIKE
				}
				return s.Table(ctx, "posts", func(t *B) { t.FullText("title") })
			},
			Down: func(ctx context.Context, s *schema.Builder) error {
				if driver, err := s.DriverName(ctx); err != nil || driver == "sqlite" {
					return err
				}
				return s.Table(ctx, "posts", func(t *B) { t.DropFullTextOn("title") })
			},
		},
		create("2026_01_01_000003_create_videos_table", "videos", func(t *B) {
			t.ID()
			t.String("title")
		}),
		create("2026_01_01_000004_create_comments_table", "comments", func(t *B) {
			t.ID()
			t.Morphs("commentable")
			t.Text("body")
			t.Timestamps()
		}),
		create("2026_01_01_000005_create_roles_table", "roles", func(t *B) {
			t.ID()
			t.String("name").Unique()
		}),
		create("2026_01_01_000006_create_role_user_table", "role_user", func(t *B) {
			t.ForeignID("user_id").Constrained().CascadeOnDelete()
			t.ForeignID("role_id").Constrained().CascadeOnDelete()
			t.String("granted_by").Default("")
			t.Timestamps()
			t.Primary("user_id", "role_id")
		}),
		create("2026_01_01_000007_create_tags_table", "tags", func(t *B) {
			t.ID()
			t.String("name").Unique()
		}),
		create("2026_01_01_000008_create_taggables_table", "taggables", func(t *B) {
			t.ForeignID("tag_id").Constrained().CascadeOnDelete()
			t.Morphs("taggable")
		}),
		create("2026_01_01_000009_create_images_table", "images", func(t *B) {
			t.ID()
			t.Morphs("imageable")
			t.String("url")
		}),
		schema.Migration{
			Name:       "2026_01_01_000010_create_audit_log_table",
			Connection: "audit",
			Up: func(ctx context.Context, s *schema.Builder) error {
				return s.Create(ctx, "audit_log", func(t *B) {
					t.UUIDPrimary()
					t.String("action")
				})
			},
			Down: func(ctx context.Context, s *schema.Builder) error { return s.DropIfExists(ctx, "audit_log") },
		},
	)
}
