// Command ormgen generates typed table and column descriptors for model
// structs annotated with an orm:table directive — the Go counterpart of
// Laravel 13's #[Table], #[Connection], #[WithoutTimestamps] attributes and
// the SoftDeletes / HasUuids traits:
//
//	//orm:table users key=id connection=default soft_deletes
//	type User struct {
//		orm.Model
//		ID         int64      `db:"id"`
//		Email      string     `db:"email"`
//		CreatedAt  time.Time  `db:"created_at"`
//		UpdatedAt  time.Time  `db:"updated_at"`
//		DeletedAt  *time.Time `db:"deleted_at"`
//		PostsCount int64      `db:"posts_count,virtual"` // filled by WithCount
//		Posts      []Post     // no db tag: not a column (relations)
//	}
//
// Directive options:
//
//	key=<column>          primary key (default: field tagged `db:",pk"`, else "id")
//	key_type=<type>       increments (default for integer keys) | uuid | ulid | manual
//	connection=<name>     registered connection (default: orm.DefaultConnection)
//	timestamps=false      don't maintain created_at / updated_at
//	soft_deletes          use deleted_at for soft deletes (also soft_deletes=true)
//	morph=<alias>         value stored in polymorphic *_type columns (default: table name)
//	time_precision=<n>    fractional second digits of maintained timestamps (default 0)
//	schema=<Name>         name of the generated schema value (default: table in CamelCase)
//
// Column tag options: pk (primary key), virtual (selected only, never
// written), nullzero (write the zero value as NULL — for optional foreign
// keys and other nullable columns held in non-pointer fields). Fields of
// embedded structs are columns too, reached through the embedding.
//
// For each model ormgen emits a schema value named after the table in
// CamelCase (Users) embedding *orm.Table[User, K] — so Users.Query(),
// Users.Find(id), Users.Create(&u) work — with one orm.Column[User, K, V]
// per tagged field, K being the key's Go type. ormgen reads the package as
// the go command builds it, build constraints included, and type-checks
// it, so key types, field types and imports come from the types, not from
// how they are spelled. Anything it cannot generate correctly is an error.
package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
)

func main() {
	dir := flag.String("dir", ".", "package directory to scan")
	out := flag.String("out", "orm_gen.go", "output file name, relative to -dir")
	flag.Parse()
	src, err := generate(*dir, *out)
	if err != nil {
		log.Fatal(err)
	}
	if err := writeAtomic(filepath.Join(*dir, *out), src); err != nil {
		log.Fatal(err)
	}
}

// generate returns the source ormgen writes to out for the package in dir.
func generate(dir, out string) ([]byte, error) {
	if filepath.Base(out) != out {
		return nil, fmt.Errorf("ormgen: -out %q must be a file name in -dir", out)
	}
	l, err := load(dir, out)
	if err != nil {
		return nil, err
	}
	im := newImports(l.pkg)
	ms, err := models(l, im)
	if err != nil {
		return nil, err
	}
	return render(l, ms, im)
}
