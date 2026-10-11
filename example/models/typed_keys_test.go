package models_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestKeysAreTyped type-checks snippets against the generated models: a
// key lookup with a value of the wrong type must not compile, on the
// schema or on any query chain, while the right type does.
func TestKeysAreTyped(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Go toolchain")
	}
	for _, c := range []struct {
		name, call string
		compiles   bool
	}{
		{"find on a chain", `models.Users.Where(models.Users.Active.Eq(true)).Find(2)`, true},
		{"find on a chain with text", `models.Users.Where(models.Users.Active.Eq(true)).Find("2")`, false},
		{"find on the schema with text", `models.Users.Find("2")`, false},
		{"findMany with a float", `models.Posts.FindMany(1.5)`, false},
		{"whereKey with text", `models.Users.Query().WhereKey("1").Get()`, false},
		{"destroy with text", `models.Users.Destroy("1")`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, err := os.MkdirTemp(".", "_typecheck")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			src := "package main\n\nimport \"github.com/stubbedev/storm/example/models\"\n\nfunc main() {\n\t_, _ = " + c.call + "\n}\n"
			if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("go", "vet", "./"+filepath.Base(dir)).CombinedOutput()
			switch {
			case c.compiles && err != nil:
				t.Fatalf("%s should compile:\n%s", c.call, out)
			case !c.compiles && err == nil:
				t.Fatalf("%s compiled; a key of the wrong type must not", c.call)
			case !c.compiles && !strings.Contains(string(out), "cannot use"):
				t.Fatalf("%s failed for another reason:\n%s", c.call, out)
			}
		})
	}
}
