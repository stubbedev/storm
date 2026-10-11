package schema

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/stubbedev/storm/orm"
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
	// LockTimeout is how old an Isolated lock must be before another run
	// may take it over (a crashed run never releases it); default one hour.
	LockTimeout time.Duration
	// DropViews makes Fresh drop the views too (migrate:fresh --drop-views).
	DropViews bool
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

// RollbackBatch reverts exactly the migrations of one batch
// (migrate:rollback --batch).
func (m *Migrator) RollbackBatch(ctx context.Context, batch int) ([]string, error) {
	return m.revert(ctx, func(newest []string, ran map[string]int) []string {
		return slices.DeleteFunc(slices.Clone(newest), func(n string) bool { return ran[n] != batch })
	})
}

// Refresh rolls back the last steps migrations — every one when steps is
// 0 — and migrates again (migrate:refresh --step).
func (m *Migrator) Refresh(ctx context.Context, steps int) ([]string, error) {
	revert := m.Reset
	if steps > 0 {
		revert = func(ctx context.Context) ([]string, error) { return m.Rollback(ctx, steps) }
	}
	if _, err := revert(ctx); err != nil {
		return nil, err
	}
	return m.Migrate(ctx, false)
}

// Install creates the migrations table (migrate:install); Migrate does
// it on first use too.
func (m *Migrator) Install(ctx context.Context) error { return m.ensureTable(ctx) }

// ErrMigrationLocked reports that another Isolated run holds the lock.
var ErrMigrationLocked = errors.New("schema: another migration run holds the lock")

// Isolated runs fn holding the migrations lock (migrate --isolated), so
// concurrent deploys never migrate at once: the lock is a row keyed
// "migrate" in the <table>_lock table, whose unique key makes taking it
// atomic. While another run holds a lock younger than LockTimeout it
// returns ErrMigrationLocked without running fn. Engines without unique
// keys (ClickHouse, Qdrant) cannot hold the lock and refuse.
func (m *Migrator) Isolated(ctx context.Context, fn func(context.Context) error) (err error) {
	if m.LockTimeout < 0 {
		return fmt.Errorf("schema: Migrator.LockTimeout is %s; it must be positive, or zero for an hour", m.LockTimeout)
	}
	b := On(m.conn())
	engine, err := b.DriverName(ctx)
	if err != nil {
		return err
	}
	if engine == "clickhouse" || engine == "qdrant" {
		return &UnsupportedError{Engine: engine, Table: m.lockTable(), Operation: "hold the migration lock", Reason: "it has no unique keys to take a lock atomically"}
	}
	if err := b.CreateIfNotExists(ctx, m.lockTable(), func(t *Blueprint) {
		t.String("id").Primary()
		t.String("owner")
		t.DateTime("acquired_at")
	}); err != nil {
		return err
	}
	owner, err := m.takeLock(ctx)
	if err != nil {
		return err
	}
	stop := m.heartbeat(ctx, owner)
	defer func() {
		err = errors.Join(err, stop(), m.releaseLock(ctx, owner))
	}()
	return fn(ctx)
}

const lockID = "migrate"

func (m *Migrator) lockTable() string { return m.table() + "_lock" }

func (m *Migrator) lockTimeout() time.Duration { return cmp.Or(m.LockTimeout, time.Hour) }

// takeLock inserts the lock row under a fresh owner token. When the row
// exists, a single conditional update takes it over if it is older than
// LockTimeout, so of several waiting runs exactly one wins.
func (m *Migrator) takeLock(ctx context.Context) (string, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("schema: generating a migration lock owner: %w", err)
	}
	owner := hex.EncodeToString(token)
	locks := orm.From(m.lockTable()).On(m.conn()).WithContext(ctx)
	now := time.Now().UTC().Truncate(time.Second)
	insertErr := locks.Insert(map[string]any{"id": lockID, "owner": owner, "acquired_at": now})
	if insertErr == nil {
		return owner, nil
	}
	held := locks.Where("id", orm.Eq, lockID)
	taken, err := held.Where("acquired_at", orm.Lt, now.Add(-m.lockTimeout())).Update(map[string]any{"owner": owner, "acquired_at": now})
	if err != nil {
		return "", fmt.Errorf("schema: taking over a stale migration lock: %w", errors.Join(insertErr, err))
	}
	if taken > 0 {
		return owner, nil
	}
	exists, err := held.Exists()
	switch {
	case err != nil:
		return "", fmt.Errorf("schema: reading the migration lock: %w", errors.Join(insertErr, err))
	case exists:
		return "", ErrMigrationLocked
	}
	return "", fmt.Errorf("schema: taking the migration lock: %w", insertErr)
}

// heartbeat refreshes the lock's acquired_at while the run works, at a
// third of LockTimeout, so a run longer than LockTimeout is not taken over
// as stale; stop ends it and reports the first refresh that failed.
func (m *Migrator) heartbeat(ctx context.Context, owner string) (stop func() error) {
	done := make(chan struct{})
	finished := make(chan struct{})
	var failed error
	go func() {
		defer close(finished)
		tick := time.NewTicker(m.lockTimeout() / 3)
		defer tick.Stop()
		mine := orm.From(m.lockTable()).On(m.conn()).WithContext(context.WithoutCancel(ctx)).Where("id", orm.Eq, lockID).Where("owner", orm.Eq, owner)
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if _, err := mine.Update(map[string]any{"acquired_at": time.Now().UTC().Truncate(time.Second)}); err != nil && failed == nil {
					failed = fmt.Errorf("schema: refreshing the migration lock: %w", err)
				}
			}
		}
	}()
	return func() error {
		close(done)
		<-finished
		return failed
	}
}

// releaseLock deletes the lock only while owner still holds it; a lock
// taken over in the meantime means another run may have migrated too.
func (m *Migrator) releaseLock(ctx context.Context, owner string) error {
	n, err := orm.From(m.lockTable()).On(m.conn()).WithContext(context.WithoutCancel(ctx)).
		Where("id", orm.Eq, lockID).Where("owner", orm.Eq, owner).Delete()
	switch {
	case err != nil:
		return fmt.Errorf("schema: releasing the migration lock: %w", err)
	case n == 0:
		return errors.New("schema: the migration lock was taken over while this run held it; another run may have migrated concurrently")
	}
	return nil
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
		if m.DropViews {
			if err := On(c).DropAllViews(ctx); err != nil {
				return nil, fmt.Errorf("schema: dropping all views on %q: %w", c, err)
			}
		}
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
		conn := cmp.Or(mig.Connection, m.conn())
		b, log := On(conn).Pretend()
		if err := pretendUp(ctx, conn, func(ctx context.Context) error { return mig.Up(ctx, b) }); err != nil {
			return out, fmt.Errorf("schema: pretending %s: %w", mig.Name, err)
		}
		out = append(out, PendingMigration{Name: mig.Name, Statements: log()})
	}
	return out, nil
}

// errPretended rolls back the transaction a pretended migration ran in.
var errPretended = errors.New("schema: pretend rolls back")

// pretendUp runs up so its orm writes (made with the ctx it is given) leave
// nothing behind: in a transaction that always rolls back on a SQL
// connection with transactions, and on any other connection under
// orm.ReadOnly, where a write fails instead of applying for real.
func pretendUp(ctx context.Context, conn string, up func(context.Context) error) error {
	c, err := orm.Connection(ctx, conn)
	if err != nil {
		return err
	}
	tx, err := orm.SupportsTransactions(ctx, conn)
	if err != nil {
		return err
	}
	if c.Doc != nil || !tx {
		return up(orm.ReadOnly(ctx))
	}
	err = orm.TransactionOn(ctx, conn, func(ctx context.Context) error {
		if err := up(ctx); err != nil {
			return err
		}
		return errPretended
	})
	if errors.Is(err, errPretended) {
		return nil
	}
	return err
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
	body := func(ctx context.Context) error { return fn(ctx, On(conn)) }
	log := func(ctx context.Context) error {
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
	switch {
	case mig.NoTransaction || !x.g.transactionalDDL():
		return apply(ctx)
	case conn == m.conn():
		return orm.TransactionOn(ctx, conn, apply)
	}
	if err := orm.TransactionOn(ctx, conn, body); err != nil {
		return err
	}
	return log(ctx)
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
	rows, err := orm.From(m.table()).On(m.conn()).WithContext(ctx).Get()
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
			return orm.From(m.table()).On(m.conn()).WithContext(ctx).Insert(map[string]any{"id": nameID(name), "migration": name, "batch": batch})
		})
	}
	return m.record(ctx, "INSERT INTO %s (%s, %s) VALUES (%s, %s)", []string{"migration", "batch"}, name, batch)
}

// logReverted removes a migration from the log.
func (m *Migrator) logReverted(ctx context.Context, name string) error {
	if _, doc, err := On(m.conn()).docStore(ctx); err != nil || doc {
		return thenRun(err, func() error {
			_, err := orm.From(m.table()).On(m.conn()).Where("migration", orm.Eq, name).WithContext(ctx).Delete()
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
