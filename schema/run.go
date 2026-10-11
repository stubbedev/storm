package schema

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/stubbedev/storm/orm"
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
//	migrate [-step] [-pretend] [-isolated]
//	                              run pending migrations; -isolated holds a
//	                              lock so concurrent deploys run them once
//	install                       create the migrations table
//	rollback [-steps N] [-batch B]
//	                              revert the last batch, N migrations or batch B
//	reset                         revert all migrations
//	refresh [-steps N] [-seed]    revert all (or N) migrations, migrate again
//	fresh [-seed] [-drop-views]   drop all tables (and views), migrate (and seed)
//	wipe [-drop-views]            drop all tables (and views) (db:wipe)
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
	steps := fs.Int("steps", 0, "number of migrations to roll back or refresh")
	batch := fs.Int("batch", 0, "roll back this batch only")
	isolated := fs.Bool("isolated", false, "hold the migration lock; skip when another run holds it")
	dropViews := fs.Bool("drop-views", false, "fresh also drops views")
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

	if err := checkCommand(fs, head, sub); err != nil {
		return err
	}
	if *database != "" || *dropViews {
		c := *m
		c.Connection = cmp.Or(*database, c.Connection)
		c.DropViews = c.DropViews || *dropViews
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
		migrate := func(ctx context.Context) error {
			ran, err := m.Migrate(ctx, *step)
			if err := report("Migrated", ran, err); err != nil || !*seed {
				return err
			}
			return SeedReportOn(ctx, out, seedConn, names...)
		}
		if !*isolated {
			return migrate(ctx)
		}
		err := m.Isolated(ctx, migrate)
		if errors.Is(err, ErrMigrationLocked) {
			fmt.Fprintln(out, "Another migration run holds the lock; nothing to do.")
			return nil
		}
		return err
	case "install":
		if err := m.Install(ctx); err != nil {
			return err
		}
		fmt.Fprintln(out, "Migration table created.")
		return nil
	case "rollback":
		if *batch > 0 {
			ran, err := m.RollbackBatch(ctx, *batch)
			return report("Rolled back", ran, err)
		}
		ran, err := m.Rollback(ctx, *steps)
		return report("Rolled back", ran, err)
	case "reset":
		ran, err := m.Reset(ctx)
		return report("Rolled back", ran, err)
	case "refresh":
		ran, err := m.Refresh(ctx, *steps)
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
		if *dropViews {
			if err := On(m.conn()).DropAllViews(ctx); err != nil {
				return err
			}
		}
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
		}
		if err == nil {
			fmt.Fprintln(out, "Created", path)
		}
		return err
	}
	return fmt.Errorf("unknown command %q", cmd)
}

// commandFlags lists the flags each command reads; any other flag, or a
// sub-command where none exists, is refused rather than ignored (a
// rollback -pretend must not roll back for real).
var commandFlags = map[string][]string{
	"migrate":  {"step", "pretend", "isolated", "seed", "class", "database"},
	"install":  {"database"},
	"rollback": {"steps", "batch", "database"},
	"reset":    {"database"},
	"refresh":  {"steps", "seed", "class", "database"},
	"fresh":    {"seed", "class", "drop-views", "database"},
	"wipe":     {"drop-views", "database"},
	"status":   {"database"},
	"db":       {"class", "database"},
	"make":     {"dir", "package"},
}

var subCommands = map[string][]string{"db": {"seed"}, "make": {"", "migration", "seeder", "model"}}

func checkCommand(fs *flag.FlagSet, head, sub string) error {
	allowed, known := commandFlags[head]
	if !known {
		return fmt.Errorf("unknown command %q", head)
	}
	if subs := subCommands[head]; !slices.Contains(subs, sub) && (sub != "" || subs != nil) {
		return fmt.Errorf("unknown command %q", head+":"+sub)
	}
	var refused []string
	fs.Visit(func(f *flag.Flag) {
		if !slices.Contains(allowed, f.Name) {
			refused = append(refused, "-"+f.Name)
		}
	})
	if len(refused) > 0 {
		return fmt.Errorf("%s does not take %s", strings.TrimSuffix(head+":"+sub, ":"), strings.Join(refused, ", "))
	}
	return nil
}

var (
	createRe = regexp.MustCompile(`^create_(\w+?)_table$`)
	alterRe  = regexp.MustCompile(`^\w+_(?:to|from|in)_(\w+?)(?:_table)?$`)
	makeTmpl = template.Must(template.New("").Parse(`package {{.Package}}

import (
	"context"

	"github.com/stubbedev/storm/schema"
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
	return writeNew(dir, full+".go", makeTmpl, data)
}

// writeNew renders t into dir/file, refusing to overwrite an existing
// file. The source is rendered before the file is created, and a failed
// write removes the file, so no half-written source is left behind.
func writeNew(dir, file string, t *template.Template, data any) (string, error) {
	var src bytes.Buffer
	if err := t.Execute(&src, data); err != nil {
		return "", fmt.Errorf("schema: rendering %s: %w", file, err)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("schema: creating %s: %w", dir, err)
	}
	path := filepath.Join(dir, file)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // the path is assembled from the -dir flag and a generated file name
	if err != nil {
		return "", fmt.Errorf("schema: creating %s: %w", path, err)
	}
	_, err = f.Write(src.Bytes())
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", errors.Join(fmt.Errorf("schema: writing %s: %w", path, err), os.Remove(path))
	}
	return path, nil
}
