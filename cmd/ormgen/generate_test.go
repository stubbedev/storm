package main

import (
	"bytes"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/models/orm_gen.go")

// TestGolden generates testdata/models, which holds a model for every
// directive option and tag option, compares the output with the committed
// orm_gen.go and checks that the package compiles with it.
func TestGolden(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Go toolchain")
	}
	dir := filepath.Join("testdata", "models")
	got, err := generate(dir, "orm_gen.go")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "orm_gen.go")
	if *update {
		if err := writeAtomic(path, got); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("generated code differs from %s; rerun with -update and review the diff:\n%s", path, got)
	}
	if out, err := exec.Command("go", "vet", "./"+filepath.ToSlash(dir)).CombinedOutput(); err != nil {
		t.Fatalf("the generated package does not compile:\n%s", out)
	}
}

// TestRefusals checks that every declaration ormgen cannot generate
// correctly fails with an error saying why, instead of generating code
// that misbehaves or does not compile.
func TestRefusals(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Go toolchain")
	}
	const header = "package bad\n\nimport (\n\t\"time\"\n\n\t\"github.com/stubbedev/storm/orm\"\n)\n\nvar _ time.Time\n\n"
	for _, c := range []struct{ name, src, want string }{
		{"directive on a grouped declaration", "//orm:table users\ntype (\n\tUser struct {\n\t\torm.Model\n\t\tID int64 `db:\"id\"`\n\t}\n\tPost struct {\n\t\torm.Model\n\t\tID int64 `db:\"id\"`\n\t}\n)\n", "grouped type declaration"},
		{"timestamps=no", "//orm:table users timestamps=no\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "takes true or false"},
		{"connection without a value", "//orm:table users connection\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "needs a value"},
		{"duplicate option", "//orm:table users morph=a morph=b\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "given twice"},
		{"unknown option", "//orm:table users softdeletes\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "unknown option \"softdeletes\""},
		{"two directives", "//orm:table users\n//orm:table people\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "two directives"},
		{"no table name", "//orm:table key=id\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "names no table"},
		{"tag without a column", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\",pk\"`\n}\n", "needs a column name"},
		{"misspelled tag option", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n\tCountryID int64 `db:\"country_id,nulzero\"`\n}\n", "unknown option \"nulzero\""},
		{"two pk tags", "//orm:table users\ntype User struct {\n\torm.Model\n\tA int64 `db:\"a,pk\"`\n\tB int64 `db:\"b,pk\"`\n}\n", "all tagged pk"},
		{"key disagreeing with pk", "//orm:table users key=b\ntype User struct {\n\torm.Model\n\tA int64 `db:\"a,pk\"`\n\tB int64 `db:\"b\"`\n}\n", "disagrees with the pk tag"},
		{"virtual pk", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id,pk,virtual\"`\n}\n", "a virtual column"},
		{"increments on a string key", "//orm:table users key_type=increments\ntype User struct {\n\torm.Model\n\tID string `db:\"id\"`\n}\n", "needs an integer key"},
		{"key_type without a key", "//orm:table logs key_type=uuid\ntype Log struct {\n\torm.Model\n\tLine string `db:\"line\"`\n}\n", "needs a primary key column"},
		{"embedded pointer", "type Stamps struct {\n\tAt time.Time `db:\"at\"`\n}\n\n//orm:table users\ntype User struct {\n\torm.Model\n\t*Stamps\n\tID int64 `db:\"id\"`\n}\n", "is a pointer"},
		{"field named like a Table method", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n\tFind string `db:\"find\"`\n}\n", "field Find clashes"},
		{"field named Table", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n\tTable string `db:\"table\"`\n}\n", "field Table clashes"},
		{"duplicate column", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n\tName string `db:\"name\"`\n\tAlias string `db:\"name\"`\n}\n", "two fields are tagged db:\"name\""},
		{"duplicate field through an embedding", "type Named struct {\n\tName string `db:\"title\"`\n}\n\n//orm:table users\ntype User struct {\n\torm.Model\n\tNamed\n\tID int64 `db:\"id\"`\n\tName string `db:\"name\"`\n}\n", "two column fields are named Name"},
		{"two models on one schema", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n\n//orm:table users\ntype Member struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "generates Users, which"},
		{"generated name already declared", "var Users = 1\n\n//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "already declares"},
		{"no tagged fields", "//orm:table users\ntype User struct {\n\torm.Model\n}\n", "no db-tagged fields"},
		{"no orm.Model", "//orm:table users\ntype User struct {\n\tID int64 `db:\"id\"`\n}\n", "must embed orm.Model"},
		{"table _", "//orm:table _\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "set schema=<Name>"},
		{"table with a dot", "//orm:table analytics.events\ntype Event struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "not a Go identifier"},
		{"table starting with a digit", "//orm:table 2fa_codes\ntype Code struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n}\n", "not a Go identifier"},
		{"generic model", "//orm:table things\ntype Thing[T any] struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n\tV T `db:\"v\"`\n}\n", "type parameters"},
		{"directive on a non-struct", "//orm:table users\ntype User int\n", "not a struct"},
		{"field type that does not type-check", "//orm:table users\ntype User struct {\n\torm.Model\n\tID int64 `db:\"id\"`\n\tX Missing `db:\"x\"`\n}\n", "does not type-check"},
		{"no models", "type User struct{}\n\nvar _ orm.Model\n", "no //orm:table structs"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			dir, err := os.MkdirTemp("testdata", "_bad")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			if err := os.WriteFile(filepath.Join(dir, "models.go"), []byte(header+c.src), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = generate(dir, "orm_gen.go")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want an error mentioning %q, got %v", c.want, err)
			}
		})
	}
}

func TestOutMustBeAFileName(t *testing.T) {
	if _, err := generate(".", "../orm_gen.go"); err == nil {
		t.Fatal("an -out outside -dir was accepted")
	}
}
