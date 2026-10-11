package schema

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"text/template"

	"github.com/stubbedev/storm/orm"
)

// Seeder populates tables after migrations run (Eloquent seeders). Run
// functions typically use factories:
//
//	func init() {
//		schema.RegisterSeeder(schema.Seeder{
//			Name: "Users",
//			Run: func(ctx context.Context) error {
//				_, err := UserFactory.Count(50).WithContext(ctx).Create()
//				return err
//			},
//		})
//	}
type Seeder struct {
	Name string
	Run  func(ctx context.Context) error
}

var (
	seedMu  sync.Mutex
	seeders []Seeder
)

// RegisterSeeder adds seeders to the global registry, typically from init()
// functions next to the seeder functions (see MakeSeeder).
func RegisterSeeder(ss ...Seeder) {
	seedMu.Lock()
	defer seedMu.Unlock()
	seeders = append(seeders, ss...)
}

// RegisteredSeeders returns the globally registered seeders, in
// registration order.
func RegisteredSeeders() []Seeder {
	seedMu.Lock()
	defer seedMu.Unlock()
	return slices.Clone(seeders)
}

// Seed runs every registered seeder (or just the named ones) on the default
// connection, each in its own transaction where the store has transactions
// (db:seed). Unknown names are an error. Returns the names that ran.
func Seed(ctx context.Context, names ...string) ([]string, error) {
	return SeedOn(ctx, orm.DefaultConnection, names...)
}

// SeedOn is Seed for a named connection.
func SeedOn(ctx context.Context, connection string, names ...string) ([]string, error) {
	seedMu.Lock()
	all := slices.Clone(seeders)
	seedMu.Unlock()
	selected := all
	if len(names) > 0 {
		selected = nil
		for _, name := range names {
			i := slices.IndexFunc(all, func(s Seeder) bool { return s.Name == name })
			if i < 0 {
				available := make([]string, len(all))
				for j, s := range all {
					available[j] = s.Name
				}
				return nil, fmt.Errorf("schema: no seeder %q (registered: %s)", name, strings.Join(available, ", "))
			}
			selected = append(selected, all[i])
		}
	}
	transactional, err := orm.SupportsTransactions(ctx, connection)
	if err != nil {
		return nil, err
	}
	var ran []string
	for _, s := range selected {
		run := s.Run
		if transactional {
			run = func(ctx context.Context) error { return orm.TransactionOn(ctx, connection, s.Run) }
		}
		if err := run(ctx); err != nil {
			return ran, fmt.Errorf("schema: seeding %s: %w", s.Name, err)
		}
		ran = append(ran, s.Name)
	}
	return ran, nil
}

// SeedReport runs seeders like Seed and prints what ran.
func SeedReport(ctx context.Context, out io.Writer, names ...string) error {
	ran, err := Seed(ctx, names...)
	return seedReport(out, ran, err)
}

// SeedReportOn is SeedReport for a named connection.
func SeedReportOn(ctx context.Context, out io.Writer, connection string, names ...string) error {
	ran, err := SeedOn(ctx, connection, names...)
	return seedReport(out, ran, err)
}

func seedReport(out io.Writer, ran []string, err error) error {
	for _, n := range ran {
		fmt.Fprintf(out, "%-12s %s\n", "Seeded", n)
	}
	if len(ran) == 0 && err == nil {
		fmt.Fprintln(out, "Nothing to seed.")
	}
	return err
}

var seederTmpl = template.Must(template.New("").Parse(`package {{.Package}}

import (
	"context"

	"github.com/stubbedev/storm/schema"
)

func init() {
	schema.RegisterSeeder(schema.Seeder{
		Name: "{{.Name}}",
		Run: func(ctx context.Context) error {
			return nil
		},
	})
}
`))

// MakeSeeder writes a seeder skeleton (make:seeder) registering
// {{.Name}}.
func MakeSeeder(dir, pkg, name string) (string, error) {
	if err := checkScaffold("seeder", name, seederNameRe, pkg); err != nil {
		return "", err
	}
	data := map[string]any{"Package": pkg, "Name": name}
	file := strings.ToLower(strings.ReplaceAll(name, "-", "_"))
	if !strings.HasSuffix(file, "_seeder") {
		file += "_seeder"
	}
	return writeNew(dir, file+".go", seederTmpl, data)
}

var modelTmpl = template.Must(template.New("").Parse(`package {{.Package}}

import (
	"time"

	"github.com/stubbedev/storm/orm"
)

//orm:table {{.Table}}
type {{.Name}} struct {
	orm.Model
	ID        int64     ` + "`db:\"id\"`" + `
	CreatedAt time.Time ` + "`db:\"created_at\"`" + `
	UpdatedAt time.Time ` + "`db:\"updated_at\"`" + `
}
`))

// MakeModel writes a model skeleton (make:model) with an //orm:table
// directive; go generate then produces the typed columns.
func MakeModel(dir, pkg, name string) (string, error) {
	if err := checkScaffold("model", name, modelNameRe, pkg); err != nil {
		return "", err
	}
	table := pluralize(snake(name))
	data := map[string]any{"Package": pkg, "Name": name, "Table": table}
	return writeNew(dir, snake(name)+".go", modelTmpl, data)
}

// snake converts CamelCase to snake_case.
func snake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r + 'a' - 'A')
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
