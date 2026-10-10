package schema

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"github.com/stubbedev/eloquent-go/orm"
)

// Migration is one reversible schema change. Name orders migrations; use the
// Laravel convention 2026_01_31_120000_create_users_table.
type Migration struct {
	Name       string
	Connection string // empty: the migrator's connection
	Up         func(ctx context.Context, s *Builder) error
	Down       func(ctx context.Context, s *Builder) error
	// NoTransaction disables wrapping the migration in a transaction on
	// engines with transactional DDL (SQLite, Postgres, DuckDB).
	NoTransaction bool
}

var (
	regMu    sync.Mutex
	registry []Migration
)

// Register adds migrations to the global registry, typically from init()
// functions in a migrations package (see `make` in Run).
func Register(ms ...Migration) {
	regMu.Lock()
	defer regMu.Unlock()
	registry = append(registry, ms...)
}

// Registered returns the globally registered migrations.
func Registered() []Migration {
	regMu.Lock()
	defer regMu.Unlock()
	return slices.Clone(registry)
}

// Migrator runs migrations and records them in a migrations table.
type Migrator struct {
	Connection string // where the migrations table lives; default connection if empty
	Table      string // default "migrations"
	Migrations []Migration
}

// NewMigrator returns a migrator for ms, or for the registered migrations
// when none are given.
func NewMigrator(ms ...Migration) *Migrator {
	if len(ms) == 0 {
		ms = Registered()
	}
	return &Migrator{Migrations: ms}
}

// Status is one migration's state (migrate:status).
type Status struct {
	Name  string
	Ran   bool
	Batch int
}

// PendingMigration is the SQL a pending migration would run (migrate --pretend).
type PendingMigration struct {
	Name       string
	Statements []string
}

func (m *Migrator) conn() string  { return cmp.Or(m.Connection, orm.DefaultConnection) }
func (m *Migrator) table() string { return cmp.Or(m.Table, "migrations") }

func (m *Migrator) sorted() ([]Migration, error) {
	ms := slices.Clone(m.Migrations)
	slices.SortFunc(ms, func(a, b Migration) int { return cmp.Compare(a.Name, b.Name) })
	for i := 1; i < len(ms); i++ {
		if ms[i].Name == ms[i-1].Name {
			return nil, fmt.Errorf("schema: duplicate migration %q", ms[i].Name)
		}
	}
	return ms, nil
}

// Migrate runs pending migrations in a new batch and returns their names.
// With step, each migration gets its own batch (migrate --step).
func (m *Migrator) Migrate(ctx context.Context, step bool) ([]string, error) {
	if err := m.ensureTable(ctx); err != nil {
		return nil, err
	}
	ran, err := m.ran(ctx)
	if err != nil {
		return nil, err
	}
	all, err := m.sorted()
	if err != nil {
		return nil, err
	}
	batch := 1
	for _, b := range ran {
		batch = max(batch, b+1)
	}
	var done []string
	for _, mig := range all {
		if _, ok := ran[mig.Name]; ok {
			continue
		}
		if err := m.run(ctx, mig, true, batch); err != nil {
			return done, fmt.Errorf("schema: migrating %s: %w", mig.Name, err)
		}
		done = append(done, mig.Name)
		if step {
			batch++
		}
	}
	return done, nil
}

// Rollback reverts the last batch, or the last steps migrations when steps > 0.
func (m *Migrator) Rollback(ctx context.Context, steps int) ([]string, error) {
	return m.revert(ctx, func(newest []string, ran map[string]int) []string {
		if steps > 0 {
			return newest[:min(steps, len(newest))]
		}
		if len(newest) == 0 {
			return nil
		}
		last := ran[newest[0]]
		if i := slices.IndexFunc(newest, func(n string) bool { return ran[n] != last }); i >= 0 {
			return newest[:i]
		}
		return newest
	})
}

// Reset reverts every migration that has run, newest first.
func (m *Migrator) Reset(ctx context.Context) ([]string, error) {
	return m.revert(ctx, func(newest []string, _ map[string]int) []string { return newest })
}

// revert rolls back the migrations pick selects from the ones that ran,
// ordered newest first (by batch, then name). A migration that ran but is
// no longer registered is an error.
func (m *Migrator) revert(ctx context.Context, pick func(newest []string, ran map[string]int) []string) ([]string, error) {
	if err := m.ensureTable(ctx); err != nil {
		return nil, err
	}
	ran, err := m.ran(ctx)
	if err != nil {
		return nil, err
	}
	newest := make([]string, 0, len(ran))
	for name := range ran {
		newest = append(newest, name)
	}
	slices.SortFunc(newest, func(a, b string) int {
		return cmp.Or(cmp.Compare(ran[b], ran[a]), cmp.Compare(b, a))
	})
	return m.down(ctx, pick(newest, ran))
}

// Refresh resets and re-runs every migration.
func (m *Migrator) Refresh(ctx context.Context) ([]string, error) {
	if _, err := m.Reset(ctx); err != nil {
		return nil, err
	}
	return m.Migrate(ctx, false)
}

// Fresh drops every table (without running Down) on every connection the
// migrations use, then migrates from scratch.
func (m *Migrator) Fresh(ctx context.Context) ([]string, error) {
	conns := []string{m.conn()}
	for _, mig := range m.Migrations {
		if c := cmp.Or(mig.Connection, m.conn()); !slices.Contains(conns, c) {
			conns = append(conns, c)
		}
	}
	for _, c := range conns {
		if err := On(c).DropAllTables(ctx); err != nil {
			return nil, fmt.Errorf("schema: dropping all tables on %q: %w", c, err)
		}
	}
	return m.Migrate(ctx, false)
}

// Status reports which migrations have run.
func (m *Migrator) Status(ctx context.Context) ([]Status, error) {
	if err := m.ensureTable(ctx); err != nil {
		return nil, err
	}
	ran, err := m.ran(ctx)
	if err != nil {
		return nil, err
	}
	all, err := m.sorted()
	if err != nil {
		return nil, err
	}
	out := make([]Status, len(all))
	for i, mig := range all {
		b, ok := ran[mig.Name]
		out[i] = Status{Name: mig.Name, Ran: ok, Batch: b}
	}
	return out, nil
}

// Pretend returns the SQL pending migrations would run, in migration order,
// without running it or creating the migrations table.
func (m *Migrator) Pretend(ctx context.Context) ([]PendingMigration, error) {
	installed, err := On(m.conn()).HasTable(ctx, m.table())
	if err != nil {
		return nil, err
	}
	ran := map[string]int{}
	if installed {
		if ran, err = m.ran(ctx); err != nil {
			return nil, err
		}
	}
	all, err := m.sorted()
	if err != nil {
		return nil, err
	}
	var out []PendingMigration
	for _, mig := range all {
		if _, ok := ran[mig.Name]; ok {
			continue
		}
		if mig.Up == nil {
			return out, fmt.Errorf("schema: migration %s has no Up function", mig.Name)
		}
		b, log := On(cmp.Or(mig.Connection, m.conn())).Pretend()
		if err := mig.Up(ctx, b); err != nil {
			return out, fmt.Errorf("schema: pretending %s: %w", mig.Name, err)
		}
		out = append(out, PendingMigration{Name: mig.Name, Statements: log()})
	}
	return out, nil
}

func (m *Migrator) down(ctx context.Context, names []string) ([]string, error) {
	byName := map[string]Migration{}
	for _, mig := range m.Migrations {
		byName[mig.Name] = mig
	}
	var done []string
	for _, n := range names {
		mig, ok := byName[n]
		if !ok {
			return done, fmt.Errorf("schema: migration %q has run but is not registered", n)
		}
		if err := m.run(ctx, mig, false, 0); err != nil {
			return done, fmt.Errorf("schema: rolling back %s: %w", n, err)
		}
		done = append(done, n)
	}
	return done, nil
}

// run applies one migration (up or down) and updates the migrations table,
// inside a transaction when the engine supports transactional DDL and the
// migration and repository share a connection.
func (m *Migrator) run(ctx context.Context, mig Migration, up bool, batch int) error {
	conn := cmp.Or(mig.Connection, m.conn())
	fn, direction := mig.Down, "Down"
	if up {
		fn, direction = mig.Up, "Up"
	}
	if fn == nil {
		return fmt.Errorf("schema: migration %s has no %s function", mig.Name, direction)
	}
	apply := func(ctx context.Context) error {
		if err := fn(ctx, On(conn)); err != nil {
			return err
		}
		if up {
			return m.logRan(ctx, mig.Name, batch)
		}
		return m.logReverted(ctx, mig.Name)
	}
	if _, doc, err := On(conn).docStore(ctx); err != nil || doc {
		return thenRun(err, func() error { return apply(ctx) })
	}
	x, err := On(conn).session(ctx)
	if err != nil {
		return err
	}
	if mig.NoTransaction || !x.g.transactionalDDL() || conn != m.conn() {
		return apply(ctx)
	}
	return orm.TransactionOn(ctx, conn, apply)
}

// ensureTable creates the migrations table. Engines that generate no keys
// (ClickHouse) key it by migration name instead of an id.
func (m *Migrator) ensureTable(ctx context.Context) error {
	b := On(m.conn())
	if _, doc, err := b.docStore(ctx); err != nil || doc {
		return thenRun(err, func() error {
			return b.CreateIfNotExists(ctx, m.table(), func(t *Blueprint) {
				t.String("id").Primary()
				t.String("migration").Index()
				t.Integer("batch")
			})
		})
	}
	x, err := b.session(ctx)
	if err != nil {
		return err
	}
	autoKeys := x.dialect.AutoKeys()
	return b.CreateIfNotExists(ctx, m.table(), func(t *Blueprint) {
		if autoKeys {
			t.Increments("id")
			t.String("migration")
		} else {
			t.String("migration").Primary()
		}
		t.Integer("batch")
	})
}

func (m *Migrator) ran(ctx context.Context) (map[string]int, error) {
	if _, doc, err := On(m.conn()).docStore(ctx); err != nil || doc {
		if err != nil {
			return nil, err
		}
		return m.ranDocs(ctx)
	}
	x, err := On(m.conn()).session(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	err = x.query(ctx, fmt.Sprintf("SELECT %s, %s FROM %s", x.g.quote("migration"), x.g.quote("batch"), x.g.quote(m.table())),
		nil, func(r *sql.Rows) error {
			var name string
			var batch int
			if err := r.Scan(&name, &batch); err != nil {
				return err
			}
			out[name] = batch
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("schema: reading migrations table %s on %q: %w", m.table(), m.conn(), err)
	}
	return out, nil
}

// record formats a statement against the migrations table: the first %s is
// the table, then the quoted columns, then one placeholder per arg.
func (m *Migrator) record(ctx context.Context, format string, cols []string, args ...any) error {
	x, err := On(m.conn()).session(ctx)
	if err != nil {
		return err
	}
	parts := []any{x.g.quote(m.table())}
	for _, col := range cols {
		parts = append(parts, x.g.quote(col))
	}
	for i := range args {
		parts = append(parts, x.dialect.Placeholder(i+1))
	}
	return x.execArgs(ctx, fmt.Sprintf(format, parts...), args...)
}

// ranDocs reads the migrations log from a document store.
func (m *Migrator) ranDocs(ctx context.Context) (map[string]int, error) {
	rows, err := orm.From(m.table()).On(m.conn()).Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("schema: reading migrations collection %s on %q: %w", m.table(), m.conn(), err)
	}
	out := make(map[string]int, len(rows))
	for _, row := range rows {
		name, ok := row["migration"].(string)
		if !ok {
			return nil, fmt.Errorf("schema: migrations collection %s on %q holds a document without a migration name", m.table(), m.conn())
		}
		batch, err := strconv.Atoi(fmt.Sprint(row["batch"]))
		if err != nil {
			return nil, fmt.Errorf("schema: migration %s on %q has batch %v: %w", name, m.conn(), row["batch"], err)
		}
		out[name] = batch
	}
	return out, nil
}

// logRan records a migration as run in batch. On a document store the
// entry is keyed by an id derived from the name, which every store accepts
// as a key.
func (m *Migrator) logRan(ctx context.Context, name string, batch int) error {
	if _, doc, err := On(m.conn()).docStore(ctx); err != nil || doc {
		return thenRun(err, func() error {
			return orm.From(m.table()).On(m.conn()).Insert(ctx, map[string]any{"id": nameID(name), "migration": name, "batch": batch})
		})
	}
	return m.record(ctx, "INSERT INTO %s (%s, %s) VALUES (%s, %s)", []string{"migration", "batch"}, name, batch)
}

// logReverted removes a migration from the log.
func (m *Migrator) logReverted(ctx context.Context, name string) error {
	if _, doc, err := On(m.conn()).docStore(ctx); err != nil || doc {
		return thenRun(err, func() error {
			_, err := orm.From(m.table()).On(m.conn()).Where("migration", orm.Eq, name).Delete(ctx)
			return err
		})
	}
	return m.record(ctx, "DELETE FROM %s WHERE %s = %s", []string{"migration"}, name)
}

// nameID is a stable UUID for name: SHA-256 in the version 8 (custom)
// layout.
func nameID(name string) string {
	sum := sha256.Sum256([]byte(name))
	sum[6] = sum[6]&0x0f | 0x80
	sum[8] = sum[8]&0x3f | 0x80
	h := hex.EncodeToString(sum[:16])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
