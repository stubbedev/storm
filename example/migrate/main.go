// Command migrate is the example project's artisan-style migration CLI:
//
//	go run ./example/migrate                 # migrate
//	go run ./example/migrate status
//	go run ./example/migrate rollback -steps 1
//	go run ./example/migrate fresh -seed
//	go run ./example/migrate make create_flights_table -dir example/migrations
//	go run ./example/migrate make:seeder Users -dir example/migrations
//	go run ./example/migrate db:seed
//
// It uses SQLite files by default; set DB_DRIVER, DB_DSN and DB_AUDIT_DSN
// to target Postgres ("pgx") or MySQL ("mysql").
package main

import (
	"cmp"
	"context"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	_ "github.com/stubbedev/storm/example/migrations"
	"github.com/stubbedev/storm/orm"
	"github.com/stubbedev/storm/schema"
)

func main() {
	driver := cmp.Or(os.Getenv("DB_DRIVER"), "sqlite")
	if _, err := orm.Open(orm.DefaultConnection, driver, cmp.Or(os.Getenv("DB_DSN"), "file:example.db?_time_format=sqlite")); err != nil {
		log.Fatal(err)
	}
	if _, err := orm.Open("audit", driver, cmp.Or(os.Getenv("DB_AUDIT_DSN"), "file:example_audit.db?_time_format=sqlite")); err != nil {
		log.Fatal(err)
	}
	if err := schema.Run(context.Background(), schema.NewMigrator(), os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}
