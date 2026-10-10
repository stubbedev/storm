package schema

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/stubbedev/eloquent-go/orm"
)

// Builder runs schema operations on one connection (the Schema facade).
type Builder struct {
	conn    string
	pretend *[]string // when set, statements are collected instead of run
}

// On returns a builder for a named connection (Schema::connection()).
func On(conn string) *Builder { return &Builder{conn: conn} }

// Default returns a builder for the default connection.
func Default() *Builder { return On(orm.DefaultConnection) }

// Package-level shortcuts on the default connection.

func Create(ctx context.Context, table string, fn func(*Blueprint)) error {
	return Default().Create(ctx, table, fn)
}

func Table(ctx context.Context, table string, fn func(*Blueprint)) error {
	return Default().Table(ctx, table, fn)
}

func Drop(ctx context.Context, table string) error { return Default().Drop(ctx, table) }

func DropIfExists(ctx context.Context, table string) error {
	return Default().DropIfExists(ctx, table)
}

func Rename(ctx context.Context, from, to string) error { return Default().Rename(ctx, from, to) }

func HasTable(ctx context.Context, table string) (bool, error) {
	return Default().HasTable(ctx, table)
}

func HasColumn(ctx context.Context, table, column string) (bool, error) {
	return Default().HasColumn(ctx, table, column)
}

// Create creates table from the blueprint built by fn.
func (s *Builder) Create(ctx context.Context, table string, fn func(*Blueprint)) error {
	bp := &Blueprint{table: table}
	fn(bp)
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		return thenRun(err, func() error { return s.docCreate(ctx, ds, bp) })
	}
	return s.run(ctx, func(g grammar) ([]string, error) { return g.compileCreate(bp) })
}

// CreateIfNotExists is Create with IF NOT EXISTS; running it again leaves
// the table, its indexes and its sequences untouched.
func (s *Builder) CreateIfNotExists(ctx context.Context, table string, fn func(*Blueprint)) error {
	bp := &Blueprint{table: table, ifNot: true}
	fn(bp)
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		return thenRun(err, func() error { return s.docCreate(ctx, ds, bp) })
	}
	return s.run(ctx, func(g grammar) ([]string, error) { return g.compileCreate(bp) })
}

// Table alters an existing table: add, change, rename or drop columns,
// indexes and keys. Index and constraint drops run first. On SQLite,
// alterations ALTER TABLE cannot express rebuild the table. DuckDB keeps a
// dropped index until commit, blocking column changes in the same
// transaction, so there the drops commit on their own before the rest.
func (s *Builder) Table(ctx context.Context, table string, fn func(*Blueprint)) error {
	bp := &Blueprint{table: table}
	fn(bp)
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		return thenRun(err, func() error { return s.docAlter(ctx, ds, bp) })
	}
	x, err := s.session(ctx)
	if err != nil {
		return err
	}
	drops, stmts, err := x.g.compileAlterPhases(bp)
	if errors.Is(err, errRebuild) {
		return s.rebuildSQLite(ctx, x, bp)
	}
	if err != nil {
		return err
	}
	if x.g.engine == engineDuckDB && len(drops) > 0 && len(stmts) > 0 {
		if err := x.execAll(ctx, drops...); err != nil {
			return err
		}
		return x.execAll(ctx, stmts...)
	}
	return x.execAll(ctx, append(drops, stmts...)...)
}

func (s *Builder) Drop(ctx context.Context, table string) error { return s.drop(ctx, table, false) }

func (s *Builder) DropIfExists(ctx context.Context, table string) error {
	return s.drop(ctx, table, true)
}

// drop drops one table, and on DuckDB the sequences its auto-increment
// columns drew from.
func (s *Builder) drop(ctx context.Context, table string, ifExists bool) error {
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		return thenRun(err, func() error { return s.docDrop(ctx, ds, table, ifExists) })
	}
	x, err := s.session(ctx)
	if err != nil {
		return err
	}
	stmts := []string{x.g.drop(table, ifExists)}
	if x.g.engine == engineDuckDB {
		seqs, err := x.duckdbSequenceDrops(ctx, []string{table})
		if err != nil {
			return err
		}
		stmts = append(stmts, seqs...)
	}
	return x.execAll(ctx, stmts...)
}

// DropColumns drops columns from table.
func (s *Builder) DropColumns(ctx context.Context, table, column string, more ...string) error {
	return s.Table(ctx, table, func(t *Blueprint) { t.DropColumn(column, more...) })
}

// Rename renames a table. On Postgres the primary key, enum checks and
// sequences named after the table are renamed along with it.
func (s *Builder) Rename(ctx context.Context, from, to string) error {
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		return thenRun(err, func() error { return docRefusal(ds, from, "rename collection", ds.RenameCollection(ctx, from, to)) })
	}
	x, err := s.session(ctx)
	if err != nil {
		return err
	}
	stmts := []string{x.g.rename(from, to)}
	if x.g.engine == enginePostgres {
		owned, err := x.postgresOwnedRenames(ctx, from, to)
		if err != nil {
			return err
		}
		stmts = append(stmts, owned...)
	}
	return x.execAll(ctx, stmts...)
}

func (s *Builder) HasTable(ctx context.Context, table string) (bool, error) {
	tables, err := s.Tables(ctx)
	return slices.Contains(tables, table), err
}

func (s *Builder) HasColumn(ctx context.Context, table, column string) (bool, error) {
	return s.HasColumns(ctx, table, column)
}

// HasColumns reports whether table has all columns.
func (s *Builder) HasColumns(ctx context.Context, table string, columns ...string) (bool, error) {
	cols, err := s.Columns(ctx, table)
	if err != nil {
		return false, err
	}
	for _, c := range columns {
		if !slices.Contains(cols, c) {
			return false, nil
		}
	}
	return true, nil
}

// WhenTableHasColumn alters table only if it has column.
func (s *Builder) WhenTableHasColumn(ctx context.Context, table, column string, fn func(*Blueprint)) error {
	ok, err := s.HasColumn(ctx, table, column)
	if err != nil || !ok {
		return err
	}
	return s.Table(ctx, table, fn)
}

// WhenTableDoesntHaveColumn alters table only if it lacks column.
func (s *Builder) WhenTableDoesntHaveColumn(ctx context.Context, table, column string, fn func(*Blueprint)) error {
	ok, err := s.HasColumn(ctx, table, column)
	if err != nil || ok {
		return err
	}
	return s.Table(ctx, table, fn)
}

// DropAllTables drops every table on the connection (used by migrate:fresh):
// with CASCADE on Postgres, children first on DuckDB (which cannot disable
// foreign keys) along with its sequences, and with foreign key checks off
// on SQLite and MySQL.
func (s *Builder) DropAllTables(ctx context.Context) error {
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		return thenRun(err, func() error {
			names, err := docCollections(ctx, ds)
			if err != nil {
				return err
			}
			for _, n := range names {
				if err := docRefusal(ds, n, "drop collection", ds.DropCollection(ctx, n)); err != nil {
					return err
				}
			}
			return nil
		})
	}
	x, err := s.session(ctx)
	if err != nil {
		return err
	}
	tables, err := x.tables(ctx)
	if err != nil || len(tables) == 0 {
		return err
	}
	switch x.g.engine {
	case enginePostgres:
		return x.exec(ctx, "DROP TABLE IF EXISTS "+x.g.list(tables)+" CASCADE")
	case engineDuckDB:
		ordered, err := x.duckdbDropOrder(ctx, tables)
		if err != nil {
			return err
		}
		seqs, err := x.duckdbSequenceDrops(ctx, tables)
		if err != nil {
			return err
		}
		return x.execAll(ctx, append(x.dropEach(ordered), seqs...)...)
	case engineClickHouse:
		return x.exec(ctx, x.dropEach(tables)...)
	}
	return s.WithoutForeignKeyConstraints(ctx, func(ctx context.Context) error {
		return s.run(ctx, func(grammar) ([]string, error) { return x.dropEach(tables), nil })
	})
}

func (x session) dropEach(tables []string) []string {
	stmts := make([]string, len(tables))
	for i, t := range tables {
		stmts[i] = x.g.drop(t, true)
	}
	return stmts
}

// WithoutForeignKeyConstraints runs fn with foreign key checks off.
//
// On SQLite and MySQL/MariaDB the checks are a per-connection setting: fn
// runs on one connection held for its duration (the transaction's, when ctx
// carries one), schema operations and orm queries on fn's ctx use it, and
// the previous setting is restored before the connection goes back to the
// pool; a connection whose setting cannot be restored is closed instead.
// SQLite ignores the setting inside a transaction, so enabled checks there
// are an error. Document stores have no foreign keys and run fn as is.
//
// On Postgres fn runs in a transaction with SET CONSTRAINTS ALL DEFERRED,
// which defers DEFERRABLE constraints until fn returns; constraints declared
// without Deferrable() are still checked per statement. DuckDB cannot
// disable foreign keys, and ClickHouse has none.
func (s *Builder) WithoutForeignKeyConstraints(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok, err := s.docStore(ctx); err != nil || ok {
		return thenRun(err, func() error { return fn(ctx) })
	}
	x, err := s.session(ctx)
	if err != nil {
		return err
	}
	switch x.g.engine {
	case engineClickHouse:
		return fn(ctx)
	case engineDuckDB:
		return x.g.unsupported("", "disable foreign key checks", "DuckDB always enforces foreign keys")
	case enginePostgres:
		return x.withDeferredConstraints(ctx, s, fn)
	}
	if x.pretend != nil {
		return errors.Join(x.exec(ctx, x.g.foreignKeyChecks(false)), fn(ctx), x.exec(ctx, x.g.foreignKeyChecks(true)))
	}
	return s.pinned(ctx, func(ctx context.Context, x session) (bool, error) {
		on, err := x.foreignKeysOn(ctx)
		if err != nil || !on {
			return true, errors.Join(err, fn(ctx))
		}
		if _, inTx := x.db.(*sql.Tx); inTx && x.g.engine == engineSQLite {
			return true, fmt.Errorf("schema: sqlite cannot disable foreign keys on connection %q inside a transaction, where PRAGMA foreign_keys is ignored; run outside it (Migration.NoTransaction)", x.name)
		}
		if err := x.exec(ctx, x.g.foreignKeyChecks(false)); err != nil {
			return false, err
		}
		err = fn(ctx)
		if rerr := x.exec(ctx, x.g.foreignKeyChecks(true)); rerr != nil {
			return false, errors.Join(err, fmt.Errorf("schema: re-enabling foreign key checks on connection %q: %w", x.name, rerr))
		}
		return true, err
	})
}

func (x session) withDeferredConstraints(ctx context.Context, s *Builder, fn func(ctx context.Context) error) error {
	if x.pretend != nil {
		return errors.Join(x.exec(ctx, "SET CONSTRAINTS ALL DEFERRED"), fn(ctx), x.exec(ctx, "SET CONSTRAINTS ALL IMMEDIATE"))
	}
	return orm.TransactionOn(ctx, s.conn, func(ctx context.Context) error {
		x, err := s.session(ctx)
		if err != nil {
			return err
		}
		if err := x.exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
			return err
		}
		if err := fn(ctx); err != nil {
			return err
		}
		return x.exec(ctx, "SET CONSTRAINTS ALL IMMEDIATE")
	})
}

// foreignKeysOn reads the session's foreign key enforcement on SQLite and MySQL.
func (x session) foreignKeysOn(ctx context.Context) (bool, error) {
	q := "SELECT @@SESSION.foreign_key_checks"
	if x.g.engine == engineSQLite {
		q = "PRAGMA foreign_keys"
	}
	var on bool
	err := x.query(ctx, q, nil, func(r *sql.Rows) error { return r.Scan(&on) })
	if err != nil {
		return false, fmt.Errorf("schema: reading foreign key enforcement on connection %q: %w", x.name, err)
	}
	return on, nil
}

// DriverName returns the connection's dialect name: "sqlite", "postgres",
// "mysql", "mariadb", "duckdb" or "clickhouse" (getDriverName()),
// for migrations that differ per engine.
func (s *Builder) DriverName(ctx context.Context) (string, error) {
	if ds, ok, err := s.docStore(ctx); err != nil || ok {
		if err != nil {
			return "", err
		}
		return ds.Engine(), nil
	}
	c, err := orm.Connection(ctx, s.conn)
	if err != nil {
		return "", err
	}
	if c.Dialect == nil {
		return "", s.notSQL()
	}
	return c.Dialect.Name(), nil
}

// Pretend returns a builder that records SQL instead of executing it, and
// a function returning what was recorded. Introspection (SQLite rebuilds,
// DuckDB sequence drops, Postgres renames) still reads the database.
func (s *Builder) Pretend() (*Builder, func() []string) {
	var log []string
	return &Builder{conn: s.conn, pretend: &log}, func() []string { return log }
}

// session is a resolved connection: the handle statements run on, the
// grammar compiling them, and the log statements go to in pretend mode.
type session struct {
	name    string
	db      orm.DB
	dialect orm.Dialect
	g       grammar
	pretend *[]string
}

// session resolves the builder's connection, the single point every schema
// and migrator operation goes through.
func (s *Builder) session(ctx context.Context) (session, error) {
	c, err := orm.Connection(ctx, s.conn)
	if err != nil {
		return session{}, fmt.Errorf("schema: resolving connection %q: %w", s.conn, err)
	}
	if c.Doc != nil || c.Dialect == nil {
		return session{}, s.notSQL()
	}
	g, err := grammarFor(c.Dialect.Name())
	if err != nil {
		return session{}, err
	}
	x := session{name: s.conn, db: c.DB, dialect: c.Dialect, g: g, pretend: s.pretend}
	if x.db == nil && x.pretend == nil {
		return session{}, fmt.Errorf("schema: connection %q has no database handle", s.conn)
	}
	return x, nil
}

func (s *Builder) notSQL() error {
	return fmt.Errorf("schema: connection %q has no SQL dialect", s.conn)
}

// pinned runs fn on a single connection: the transaction's or held one's
// when ctx carries it, otherwise one taken from the pool for fn's duration
// and visible to schema operations on fn's ctx. fn reports whether the
// connection may go back to the pool; one that may not is closed.
func (s *Builder) pinned(ctx context.Context, fn func(ctx context.Context, x session) (reusable bool, err error)) error {
	x, err := s.session(ctx)
	if err != nil {
		return err
	}
	switch x.db.(type) {
	case *sql.Tx, *sql.Conn:
		_, err := fn(ctx, x)
		return err
	}
	pool, ok := x.db.(interface {
		Conn(ctx context.Context) (*sql.Conn, error)
	})
	if !ok {
		return fmt.Errorf("schema: connection %q (%T) cannot hand out a single connection", s.conn, x.db)
	}
	conn, err := pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("schema: taking a connection from %q: %w", s.conn, err)
	}
	x.db = conn
	reusable, err := fn(orm.WithConn(ctx, s.conn, conn), x)
	if reusable {
		return errors.Join(err, conn.Close())
	}
	if derr := conn.Raw(func(any) error { return sqldriver.ErrBadConn }); !errors.Is(derr, sqldriver.ErrBadConn) {
		err = errors.Join(err, fmt.Errorf("schema: discarding connection on %q: %w", s.conn, derr))
	}
	return err
}

func (s *Builder) run(ctx context.Context, compile func(grammar) ([]string, error)) error {
	x, err := s.session(ctx)
	if err != nil {
		return err
	}
	stmts, err := compile(x.g)
	if err != nil {
		return err
	}
	return x.execAll(ctx, stmts...)
}

// execAll runs the statements one blueprint compiled. On engines with
// transactional DDL they run in a transaction (a savepoint on a held
// connection), so a failing statement leaves the table as it was.
func (x session) execAll(ctx context.Context, stmts ...string) error {
	if len(stmts) < 2 || x.pretend != nil || !x.g.transactionalDDL() {
		return x.exec(ctx, stmts...)
	}
	switch x.db.(type) {
	case *sql.Tx:
		return x.exec(ctx, stmts...)
	case *sql.Conn:
		return x.savepoint(ctx, func() error { return x.exec(ctx, stmts...) })
	}
	return orm.TransactionOn(ctx, x.name, func(ctx context.Context) error {
		c, err := orm.Connection(ctx, x.name)
		if err != nil {
			return err
		}
		tx := x
		tx.db = c.DB
		return tx.exec(ctx, stmts...)
	})
}

func (x session) exec(ctx context.Context, stmts ...string) error {
	for _, stmt := range stmts {
		if err := x.execArgs(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// execArgs runs one statement, or records it in pretend mode.
func (x session) execArgs(ctx context.Context, stmt string, args ...any) error {
	if x.pretend != nil {
		*x.pretend = append(*x.pretend, stmt)
		return nil
	}
	start := time.Now()
	_, err := x.db.ExecContext(ctx, stmt, args...)
	orm.Emit(orm.QueryEvent{Connection: x.name, SQL: stmt, Args: args, Duration: time.Since(start), Err: err})
	if err != nil {
		return &orm.QueryError{Err: err, SQL: stmt}
	}
	return nil
}

// query runs a read on the session's handle and calls each for every row.
func (x session) query(ctx context.Context, q string, args []any, each func(*sql.Rows) error) error {
	if x.db == nil {
		return fmt.Errorf("schema: connection %q has no database handle to read from", x.name)
	}
	rows, err := x.db.QueryContext(ctx, q, args...)
	if err != nil {
		return &orm.QueryError{Err: err, SQL: q}
	}
	defer closeRows(rows)
	for rows.Next() {
		if err := each(rows); err != nil {
			return fmt.Errorf("schema: reading %s: %w", q, err)
		}
	}
	return rows.Err()
}
