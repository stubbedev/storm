package schema

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/stubbedev/eloquent-go/orm"
)

// Run implements an artisan-style migration CLI. A project's cmd/migrate is:
//
//	func main() {
//		orm.Open(orm.DefaultConnection, "sqlite", "file:app.db")
//		err := schema.Run(context.Background(), schema.NewMigrator(), os.Args[1:], os.Stdout)
//		...
//	}
//
// Commands:
//
//	migrate [-step] [-pretend]   run pending migrations
//	rollback [-steps N]           revert the last batch (or N migrations)
//	reset                         revert all migrations
//	refresh [-seed]               reset, then migrate (and seed)
//	fresh [-seed]                 drop all tables, migrate (and seed)
//	wipe                          drop all tables (db:wipe)
//	status                        list migrations and whether they ran
//	db:seed [-class X]            run every seeder (or just X)
//	make <name>                   scaffold a migration file
//	make:migration <name>         scaffold a migration file
//	make:seeder <name>            scaffold a seeder file
//	make:model <name>             scaffold a model struct
func Run(ctx context.Context, m *Migrator, args []string, out io.Writer) error {
	if len(args) == 0 {
		args = []string{"migrate"}
	}
	cmd, rest := strings.TrimPrefix(args[0], "migrate:"), args[1:]
	head, sub, _ := strings.Cut(cmd, ":")
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(out)
	step := fs.Bool("step", false, "run each migration in its own batch")
	pretend := fs.Bool("pretend", false, "print the SQL instead of running it")
	steps := fs.Int("steps", 0, "number of migrations to roll back")
	seed := fs.Bool("seed", false, "run seeders after migrating")
	class := fs.String("class", "", "run only this seeder")
	dir := fs.String("dir", "migrations", "directory for new migrations")
	pkg := fs.String("package", "migrations", "package name for new migrations")
	database := fs.String("database", "", "connection holding the migrations table")
	// Allow flags after positional args (make <name> -dir x).
	var positional []string
	for len(rest) > 0 {
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if rest = fs.Args(); len(rest) > 0 {
			positional, rest = append(positional, rest[0]), rest[1:]
		}
	}

	if *database != "" {
		c := *m
		c.Connection = *database
		m = &c
	}
	report := func(verb string, names []string, err error) error {
		for _, n := range names {
			fmt.Fprintf(out, "%-12s %s\n", verb, n)
		}
		if len(names) == 0 && err == nil {
			fmt.Fprintln(out, "Nothing to do.")
		}
		return err
	}

	seedConn := cmp.Or(m.conn(), orm.DefaultConnection)
	if *database != "" {
		seedConn = *database
	}
	var names []string
	if *class != "" {
		names = []string{*class}
	}

	switch head {
	case "migrate":
		if *pretend {
			pending, err := m.Pretend(ctx)
			for _, p := range pending {
				fmt.Fprintf(out, "-- %s\n%s;\n", p.Name, strings.Join(p.Statements, ";\n"))
			}
			return err
		}
		ran, err := m.Migrate(ctx, *step)
		if err := report("Migrated", ran, err); err != nil || !*seed {
			return err
		}
		return SeedReportOn(ctx, out, seedConn, names...)
	case "rollback":
		ran, err := m.Rollback(ctx, *steps)
		return report("Rolled back", ran, err)
	case "reset":
		ran, err := m.Reset(ctx)
		return report("Rolled back", ran, err)
	case "refresh":
		ran, err := m.Refresh(ctx)
		_ = report("Migrated", ran, err)
		if err != nil || !*seed {
			return err
		}
		return SeedReportOn(ctx, out, seedConn, names...)
	case "fresh":
		ran, err := m.Fresh(ctx)
		_ = report("Migrated", ran, err)
		if err != nil || !*seed {
			return err
		}
		return SeedReportOn(ctx, out, seedConn, names...)
	case "wipe":
		if err := On(m.conn()).DropAllTables(ctx); err != nil {
			return err
		}
		fmt.Fprintln(out, "Dropped all tables.")
		return nil
	case "status":
		st, err := m.Status(ctx)
		for _, s := range st {
			state := "Pending"
			if s.Ran {
				state = fmt.Sprintf("Ran [%d]", s.Batch)
			}
			fmt.Fprintf(out, "%-10s %s\n", state, s.Name)
		}
		return err
	case "db":
		if sub != "seed" {
			return fmt.Errorf("unknown db command %q", sub)
		}
		return SeedReportOn(ctx, out, seedConn, names...)
	case "make":
		if len(positional) != 1 {
			return fmt.Errorf("usage: %s <name>", cmd)
		}
		var (
			path string
			err  error
		)
		switch sub {
		case "", "migration":
			path, err = MakeMigration(*dir, *pkg, positional[0], time.Now().UTC())
		case "seeder":
			path, err = MakeSeeder(*dir, *pkg, positional[0])
		case "model":
			path, err = MakeModel(*dir, *pkg, positional[0])
		default:
			return fmt.Errorf("unknown make command %q", sub)
		}
		if err == nil {
			fmt.Fprintln(out, "Created", path)
		}
		return err
	}
	return fmt.Errorf("unknown command %q", cmd)
}

var (
	createRe = regexp.MustCompile(`^create_(\w+?)_table$`)
	alterRe  = regexp.MustCompile(`^\w+_(?:to|from|in)_(\w+?)(?:_table)?$`)
	makeTmpl = template.Must(template.New("").Parse(`package {{.Package}}

import (
	"context"

	"github.com/stubbedev/eloquent-go/schema"
)

func init() {
	schema.Register(schema.Migration{
		Name: "{{.Name}}",
		Up: func(ctx context.Context, s *schema.Builder) error {
{{- if .Create}}
			return s.Create(ctx, "{{.Table}}", func(t *schema.Blueprint) {
				t.ID()
				t.Timestamps()
			})
{{- else if .Table}}
			return s.Table(ctx, "{{.Table}}", func(t *schema.Blueprint) {
			})
{{- else}}
			return nil
{{- end}}
		},
		Down: func(ctx context.Context, s *schema.Builder) error {
{{- if .Create}}
			return s.DropIfExists(ctx, "{{.Table}}")
{{- else if .Table}}
			return s.Table(ctx, "{{.Table}}", func(t *schema.Blueprint) {
			})
{{- else}}
			return nil
{{- end}}
		},
	})
}
`))
)

// MakeMigration writes a migration skeleton (make:migration), guessing the
// table from names like create_flights_table or add_votes_to_users_table.
func MakeMigration(dir, pkg, name string, at time.Time) (string, error) {
	name = strings.ToLower(strings.ReplaceAll(name, "-", "_"))
	full := at.UTC().Format("2006_01_02_150405") + "_" + name
	data := map[string]any{"Package": pkg, "Name": full}
	if m := createRe.FindStringSubmatch(name); m != nil {
		data["Create"], data["Table"] = true, m[1]
	} else if m := alterRe.FindStringSubmatch(name); m != nil {
		data["Table"] = m[1]
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	path := filepath.Join(dir, full+".go")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // the path is assembled from the -dir flag and a generated file name
	if err != nil {
		return "", err
	}
	defer f.Close()
	return path, makeTmpl.Execute(f, data)
}
