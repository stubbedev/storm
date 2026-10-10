package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultConnection is the connection used by tables that don't name one.
const DefaultConnection = "default"

// DB is satisfied by *sql.DB, *sql.Tx and *sql.Conn.
type DB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Conn is a registered connection: a handle plus the dialect its SQL is rendered in.
type Conn struct {
	DB DB // used for every statement unless Read is set
	// Read is an optional second pool that SELECTs are routed to
	// (Laravel's read/write connection split).
	Read DB
	// Doc is set for document-store connections (MongoDB, Qdrant); queries
	// on them translate to the store instead of SQL.
	Doc     DocStore
	Dialect Dialect
}

// Reader returns the handle queries run on: Read when the connection has a
// separate read pool, DB otherwise. Writes always go through DB.
func (c Conn) Reader() DB {
	if c.Read != nil {
		return c.Read
	}
	return c.DB
}

// stickyState tracks which connections have been written to inside one
// orm.Sticky scope.
type stickyState struct {
	mu    sync.Mutex
	wrote map[string]bool
}

type stickyKey struct{}

// Sticky returns a ctx with Laravel's "sticky" read/write behaviour: after
// the first write to a connection that has a separate read pool, every later
// read in the same ctx runs on the write pool, so a request reads back its
// own writes despite replication lag. Call it once where a request's ctx is
// created (HTTP middleware is the usual place); routing from there is
// automatic, including writes inside orm.Transaction. Without it, reads
// always use the read pool and only transactions read their own writes.
func Sticky(ctx context.Context) context.Context {
	return context.WithValue(ctx, stickyKey{}, &stickyState{wrote: map[string]bool{}})
}

// markWritten records that a statement wrote to conn in the ctx's sticky
// scope, if there is one.
func markWritten(ctx context.Context, conn string) {
	if st, ok := ctx.Value(stickyKey{}).(*stickyState); ok {
		st.mu.Lock()
		st.wrote[conn] = true
		st.mu.Unlock()
	}
}

// stickyDB picks the handle a read runs on: the write pool when this ctx
// already wrote to the connection, the read pool otherwise.
func stickyDB(ctx context.Context, conn string, c Conn) DB {
	if st, ok := ctx.Value(stickyKey{}).(*stickyState); ok {
		st.mu.Lock()
		wrote := st.wrote[conn]
		st.mu.Unlock()
		if wrote {
			return c.DB
		}
	}
	return c.Reader()
}

var (
	connMu sync.RWMutex
	conns  = map[string]Conn{}
)

// Open opens a database with a database/sql driver and registers it under
// name, picking the dialect from the driver name:
//
//	orm.Open(orm.DefaultConnection, "sqlite", "file:app.db")
//	orm.Open("analytics", "pgx", os.Getenv("ANALYTICS_DSN"))
//
// A MySQL or MariaDB DSN gets clientFoundRows=true unless it sets that parameter, so
// Update and Delete report the rows they matched, as every other engine
// does, rather than only the rows whose values changed.
func Open(name, driver, dsn string) (*sql.DB, error) {
	d, ok := DialectFor(driver)
	if !ok {
		return nil, fmt.Errorf("orm: no dialect known for driver %q; use AddConnection with an explicit dialect", driver)
	}
	if d == MySQL || d == MariaDB {
		dsn = withFoundRows(dsn)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	if d, err = flavour(d, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("orm: opening %s: %w", name, err)
	}
	AddConnection(name, db, d)
	return db, nil
}

func withFoundRows(dsn string) string {
	if strings.Contains(dsn, "clientFoundRows=") {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&clientFoundRows=true"
	}
	return dsn + "?clientFoundRows=true"
}

// flavourTimeout bounds the server version query flavour runs.
const flavourTimeout = 10 * time.Second

// flavour tells MariaDB from MySQL — they share a driver — by asking the
// server, so Open connects for the MySQL driver. Other dialects pass
// through.
func flavour(d Dialect, db *sql.DB) (Dialect, error) {
	if d != MySQL {
		return d, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), flavourTimeout)
	defer cancel()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return nil, fmt.Errorf("reading the server version: %w", err)
	}
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return MariaDB, nil
	}
	return MySQL, nil
}

// SetDefault registers the default connection.
func SetDefault(db DB, d Dialect) { AddConnection(DefaultConnection, db, d) }

// OpenReadWrite opens two pools for one connection: SELECTs run against
// readDSN, everything else against writeDSN (Laravel's 'read'/'write'
// configuration). The returned *sql.DB is the write pool.
func OpenReadWrite(name, driver, readDSN, writeDSN string) (*sql.DB, error) {
	d, ok := DialectFor(driver)
	if !ok {
		return nil, fmt.Errorf("orm: no dialect known for driver %q; use AddConnection with an explicit dialect", driver)
	}
	read, err := sql.Open(driver, readDSN)
	if err != nil {
		return nil, err
	}
	write, err := sql.Open(driver, writeDSN)
	if err != nil {
		_ = read.Close()
		return nil, err
	}
	if d, err = flavour(d, write); err != nil {
		_ = read.Close()
		_ = write.Close()
		return nil, fmt.Errorf("orm: opening %s: %w", name, err)
	}
	connMu.Lock()
	conns[name] = Conn{DB: write, Read: read, Dialect: d}
	connMu.Unlock()
	return write, nil
}

// AddConnection registers a named connection, used by tables declared with
// //orm:table <name> connection=<conn> and by Query.On(<conn>).
func AddConnection(name string, db DB, d Dialect) {
	connMu.Lock()
	defer connMu.Unlock()
	conns[name] = Conn{DB: db, Dialect: d}
}

// Transaction runs fn in a transaction on the default connection. Queries
// that receive the ctx passed to fn run inside the transaction; it commits
// if fn returns nil and rolls back otherwise. Nested calls use savepoints.
func Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return TransactionOn(ctx, DefaultConnection, fn)
}

// txState is carried in the ctx of a running transaction.
type txState struct {
	conn  Conn
	depth int
	after *[]func(context.Context) // afterCommit callbacks, shared with nested levels
	// flat marks a transaction without savepoints (document stores,
	// DuckDB): a nested transaction joins it, and its failure (failed)
	// rolls the whole transaction back.
	flat   bool
	failed bool
}

// DocTransactor is a document store with transactions (MongoDB on a
// replica set); orm.Transaction on its connection runs fn in one.
type DocTransactor interface {
	Transaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// ErrNestedFailed is returned by a transaction without savepoints
// (document stores, DuckDB) whose nested transaction failed: with nothing
// to undo the nested part alone, the whole transaction rolled back.
var ErrNestedFailed = errors.New("orm: a nested transaction failed, so the whole transaction rolled back")

type txKey struct{ conn string }

// TransactionOn is Transaction for a named connection.
func TransactionOn(ctx context.Context, name string, fn func(ctx context.Context) error) (err error) {
	return TransactionAttemptsOn(ctx, name, 1, fn)
}

// RetryingTransaction is Transaction that re-runs fn when the database
// reports a deadlock or lock timeout, up to attempts times in total
// (DB::transaction's $attempts). Nested calls are never retried.
func RetryingTransaction(ctx context.Context, attempts int, fn func(ctx context.Context) error) error {
	return TransactionAttemptsOn(ctx, DefaultConnection, attempts, fn)
}

// RetryingTransactionOn is RetryingTransaction for a named connection.
func RetryingTransactionOn(ctx context.Context, name string, attempts int, fn func(ctx context.Context) error) error {
	return TransactionAttemptsOn(ctx, name, attempts, fn)
}

// TransactionAttemptsOn runs fn in a transaction, retrying on deadlock.
func TransactionAttemptsOn(ctx context.Context, name string, attempts int, fn func(ctx context.Context) error) error {
	for {
		err := TransactionOnce(ctx, name, fn)
		if err == nil || attempts <= 1 || !IsDeadlock(err) {
			return err
		}
		attempts--
	}
}

func TransactionOnce(ctx context.Context, name string, fn func(ctx context.Context) error) (err error) {
	if st, ok := ctx.Value(txKey{name}).(*txState); ok {
		if st.flat {
			if err := fn(ctx); err != nil {
				st.failed = true
				return err
			}
			return nil
		}
		return savepoint(ctx, name, st, fn)
	}
	c, err := lookup(ctx, name)
	if err != nil {
		return err
	}
	if c.Doc != nil {
		return docTransaction(ctx, name, c, fn)
	}
	if _, ok := c.Dialect.(noTransactions); ok {
		return &ErrNotTranslatable{Reason: fmt.Sprintf("transactions on connection %q (%s has none)", name, c.Dialect.Name())}
	}
	beginner, ok := c.DB.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		return fmt.Errorf("orm: connection %q cannot begin a transaction", name)
	}
	tx, err := beginner.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	_, flat := c.Dialect.(noSavepoints)
	st := &txState{conn: Conn{DB: tx, Dialect: c.Dialect}, flat: flat, after: new([]func(context.Context))}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err == nil && st.failed {
			err = ErrNestedFailed
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if err = tx.Commit(); err == nil {
			for _, f := range *st.after {
				f(ctx)
			}
		}
	}()
	return fn(context.WithValue(ctx, txKey{name}, st))
}

// docTransaction runs fn in the document store's own transaction; a store
// without transactions refuses rather than running fn unprotected.
func docTransaction(ctx context.Context, name string, c Conn, fn func(ctx context.Context) error) error {
	tr, ok := c.Doc.(DocTransactor)
	if !ok {
		return &ErrNotTranslatable{Reason: fmt.Sprintf("transactions on connection %q (%T has none)", name, c.Doc)}
	}
	st := &txState{conn: c, flat: true, after: new([]func(context.Context))}
	err := tr.Transaction(ctx, func(tctx context.Context) error {
		if err := fn(context.WithValue(tctx, txKey{name}, st)); err != nil {
			return err
		}
		if st.failed {
			return ErrNestedFailed
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, f := range *st.after {
		f(ctx)
	}
	return nil
}

// noTransactions marks dialects without transactions (ClickHouse), whose
// driver would accept BEGIN and ROLLBACK without undoing anything.
type noTransactions interface{ noTransactions() }

// noSavepoints marks dialects whose transactions cannot nest (DuckDB).
type noSavepoints interface{ noSavepoints() }

// SupportsTransactions reports whether the named connection can run
// orm.Transaction: SQL connections other than ClickHouse, and document
// stores with their own transactions.
func SupportsTransactions(ctx context.Context, name string) (bool, error) {
	c, err := lookup(ctx, name)
	if err != nil {
		return false, err
	}
	if c.Doc == nil {
		_, none := c.Dialect.(noTransactions)
		return !none, nil
	}
	_, ok := c.Doc.(DocTransactor)
	return ok, nil
}

// savepoint runs a nested transaction (DB::transaction inside another).
func savepoint(ctx context.Context, name string, parent *txState, fn func(ctx context.Context) error) (err error) {
	sp := fmt.Sprintf("trans%d", parent.depth+2)
	if _, err := parent.conn.DB.ExecContext(ctx, "SAVEPOINT "+sp); err != nil {
		return err
	}
	mark := len(*parent.after)
	child := &txState{conn: parent.conn, depth: parent.depth + 1, after: parent.after}
	rollback := func() {
		_, _ = parent.conn.DB.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+sp)
		*parent.after = (*parent.after)[:mark]
	}
	defer func() {
		if p := recover(); p != nil {
			rollback()
			panic(p)
		}
	}()
	if err = fn(context.WithValue(ctx, txKey{name}, child)); err != nil {
		rollback()
		return err
	}
	_, err = parent.conn.DB.ExecContext(ctx, "RELEASE SAVEPOINT "+sp)
	return err
}

// AfterCommit runs fn once the outermost transaction on the default
// connection commits, or immediately when ctx carries no transaction.
// Callbacks registered in a rolled back (nested) transaction are discarded.
func AfterCommit(ctx context.Context, fn func(ctx context.Context)) {
	AfterCommitOn(ctx, DefaultConnection, fn)
}

// AfterCommitOn is AfterCommit for a named connection.
func AfterCommitOn(ctx context.Context, conn string, fn func(ctx context.Context)) {
	if st, ok := ctx.Value(txKey{conn}).(*txState); ok {
		*st.after = append(*st.after, fn)
		return
	}
	fn(ctx)
}

// InTransaction reports whether ctx carries a transaction on the default connection.
func InTransaction(ctx context.Context) bool { return InTransactionOn(ctx, DefaultConnection) }

// InTransactionOn reports whether ctx carries a transaction on the named
// connection.
func InTransactionOn(ctx context.Context, name string) bool {
	_, ok := ctx.Value(txKey{name}).(*txState)
	return ok
}

func flatTransaction(ctx context.Context, name string) bool {
	st, ok := ctx.Value(txKey{name}).(*txState)
	return ok && st.flat
}

// QueryEvent describes one executed statement (DB::listen).
type QueryEvent struct {
	Connection string
	SQL        string
	Args       []any
	Duration   time.Duration
	Err        error
}

type listener struct {
	id int
	fn func(QueryEvent)
}

var (
	listenMu  sync.RWMutex
	listeners []listener
	listenSeq int
)

// Listen registers fn to receive every executed statement; call the
// returned function to stop listening.
func Listen(fn func(QueryEvent)) (stop func()) {
	listenMu.Lock()
	defer listenMu.Unlock()
	listenSeq++
	id := listenSeq
	listeners = append(slices.Clip(listeners), listener{id, fn})
	return func() {
		listenMu.Lock()
		defer listenMu.Unlock()
		listeners = slices.DeleteFunc(slices.Clone(listeners), func(l listener) bool { return l.id == id })
	}
}

// Emit reports an executed statement to listeners and, when enabled, the
// query log (used by the schema package too).
func Emit(e QueryEvent) {
	if logEnabled.Load() {
		logMu.Lock()
		queryLogEvents = append(queryLogEvents, e)
		logMu.Unlock()
	}
	listenMu.RLock()
	current := listeners
	listenMu.RUnlock()
	for _, l := range current {
		l.fn(e)
	}
}

var (
	logMu          sync.Mutex
	logEnabled     atomic.Bool
	queryLogEvents []QueryEvent
)

// EnableQueryLog starts recording every executed statement
// (DB::enableQueryLog).
func EnableQueryLog() {
	logMu.Lock()
	queryLogEvents = nil
	logMu.Unlock()
	logEnabled.Store(true)
}

// DisableQueryLog stops recording (DB::disableQueryLog).
func DisableQueryLog() { logEnabled.Store(false) }

// QueryLog returns the statements recorded since the log was enabled or
// flushed (DB::getQueryLog).
func QueryLog() []QueryEvent {
	logMu.Lock()
	defer logMu.Unlock()
	return slices.Clone(queryLogEvents)
}

// FlushQueryLog clears the log (DB::flushQueryLog).
func FlushQueryLog() {
	logMu.Lock()
	queryLogEvents = nil
	logMu.Unlock()
}

// IsDeadlock reports whether err is a deadlock or lock timeout on SQLite,
// Postgres or MySQL/MariaDB — the conditions DB::transaction retries on.
func IsDeadlock(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, s := range []string{
		"SQLSTATE 40P01", "SQLSTATE 40001", "Error 1213",
		"Deadlock found when trying to get lock", "Lock wait timeout exceeded",
		"database is locked",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// IsUniqueViolation reports whether err is a unique constraint violation
// on SQLite, Postgres, MySQL/MariaDB or DuckDB, or a document store's
// DuplicateKeyError.
func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := errors.AsType[*DuplicateKeyError](err); ok {
		return true
	}
	msg := err.Error()
	for _, s := range []string{"UNIQUE constraint failed", "SQLSTATE 23505", "Error 1062", "Duplicate entry", "Duplicate key"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// Connection resolves a registered connection by name, returning the active
// transaction instead when ctx carries one for that connection.
func Connection(ctx context.Context, name string) (Conn, error) { return lookup(ctx, name) }

type heldKey struct{ conn string }

// WithConn returns a ctx in which every statement on the named connection
// runs on conn, one connection taken from its pool — for session state
// that only lives on a connection, like MySQL's FOREIGN_KEY_CHECKS or
// SQLite's PRAGMAs. A transaction begun in that ctx runs on conn too.
func WithConn(ctx context.Context, name string, conn *sql.Conn) context.Context {
	return context.WithValue(ctx, heldKey{name}, conn)
}

// lookup resolves a connection name: an active transaction in ctx wins,
// then a connection held with WithConn, then the registry.
func lookup(ctx context.Context, name string) (Conn, error) {
	if st, ok := ctx.Value(txKey{name}).(*txState); ok {
		return st.conn, nil
	}
	connMu.RLock()
	c, ok := conns[name]
	connMu.RUnlock()
	if !ok {
		return Conn{}, fmt.Errorf("orm: no connection %q registered (call orm.Open, orm.SetDefault or orm.AddConnection)", name)
	}
	if held, ok := ctx.Value(heldKey{name}).(*sql.Conn); ok {
		c.DB, c.Read = held, nil
	}
	return c, nil
}

// sqlConn is lookup for statements that only have an SQL form: on a
// document store they fail as not translatable instead of reaching a
// connection with no database behind it.
func sqlConn(ctx context.Context, name string) (Conn, error) {
	c, err := lookup(ctx, name)
	if err != nil {
		return Conn{}, err
	}
	if c.Doc != nil {
		return Conn{}, &ErrNotTranslatable{Reason: fmt.Sprintf("a SQL-only operation on connection %q", name)}
	}
	return c, nil
}

// dialectOf returns the dialect registered for name, or SQLite when the
// connection is not registered yet (so ToSQL works before startup wiring).
func dialectOf(name string) Dialect {
	connMu.RLock()
	defer connMu.RUnlock()
	if c, ok := conns[name]; ok {
		return c.Dialect
	}
	return SQLite
}
