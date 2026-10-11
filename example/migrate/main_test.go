package main

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

func TestDataSource(t *testing.T) {
	t.Setenv("DB_DSN", "")
	if dsn, err := dataSource("sqlite", "DB_DSN", "file:x.db"); err != nil || dsn != "file:x.db" {
		t.Fatalf("sqlite default: %q, %v", dsn, err)
	}
	if _, err := dataSource("pgx", "DB_DSN", "file:x.db"); err == nil {
		t.Fatal("pgx got the SQLite default instead of an error")
	}
	t.Setenv("DB_DSN", "postgres://localhost/app")
	if dsn, err := dataSource("pgx", "DB_DSN", "file:x.db"); err != nil || dsn != "postgres://localhost/app" {
		t.Fatalf("pgx from the environment: %q, %v", dsn, err)
	}
}

func TestSeedingTwice(t *testing.T) {
	ctx := context.Background()
	if _, err := orm.Open(orm.DefaultConnection, "sqlite", "file:"+filepath.Join(t.TempDir(), "seed.db")); err != nil {
		t.Fatal(err)
	}
	if err := schema.Create(ctx, "countries", func(t *schema.Blueprint) {
		t.ID()
		t.String("name")
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := schema.Run(ctx, schema.NewMigrator(), []string{"db:seed"}, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	n, err := orm.From("countries").Count()
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("%d countries after seeding twice, want 2", n)
	}
}
