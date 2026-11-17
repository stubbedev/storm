package orm

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

const defaultStatementCacheSize = 128

type stmtCache struct {
	db      *sql.DB
	size    int
	mu      sync.Mutex
	recent  cachedStmt
	byQuery map[string]*cachedStmt
	closed  bool
}

type cachedStmt struct {
	query      string
	stmt       *sql.Stmt
	refs       int
	evicted    bool
	prev, next *cachedStmt
}

func (s *cachedStmt) unlink() {
	s.prev.next, s.next.prev = s.next, s.prev
	s.prev, s.next = nil, nil
}

func (c *stmtCache) pushFront(s *cachedStmt) {
	s.prev, s.next = &c.recent, c.recent.next
	c.recent.next.prev = s
	c.recent.next = s
}

func preparesStatements(d Dialect) bool {
	switch d.(type) {
	case sqliteDialect, mysqlDialect, mariadbDialect:
		return true
	}
	return false
}

func newStmtCache(db DB, d Dialect, size int) *stmtCache {
	pool, ok := db.(*sql.DB)
	if !ok || size == 0 || !preparesStatements(d) {
		return nil
	}
	c := &stmtCache{db: pool, size: size, byQuery: make(map[string]*cachedStmt, size)}
	c.recent.prev, c.recent.next = &c.recent, &c.recent
	return c
}

func (c *stmtCache) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if len(args) == 0 {
		return c.db.QueryContext(ctx, query)
	}
	s, err := c.acquire(ctx, query)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return c.db.QueryContext(ctx, query, args...)
	}
	defer c.release(s)
	return s.stmt.QueryContext(ctx, args...)
}

func (c *stmtCache) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if len(args) == 0 {
		return c.db.ExecContext(ctx, query)
	}
	s, err := c.acquire(ctx, query)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return c.db.ExecContext(ctx, query, args...)
	}
	defer c.release(s)
	return s.stmt.ExecContext(ctx, args...)
}

func (c *stmtCache) acquire(ctx context.Context, query string) (*cachedStmt, error) {
	if s, closed := c.hit(query); s != nil || closed {
		return s, nil
	}
	stmt, err := c.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("preparing the statement: %w", err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		closeStmts(stmt)
		return nil, nil
	}
	if s, ok := c.byQuery[query]; ok {
		c.use(s)
		c.mu.Unlock()
		closeStmts(stmt)
		return s, nil
	}
	s := &cachedStmt{query: query, stmt: stmt, refs: 1}
	c.byQuery[query] = s
	c.pushFront(s)
	var evicted []*sql.Stmt
	for len(c.byQuery) > c.size {
		if old := c.evict(c.recent.prev); old != nil {
			evicted = append(evicted, old)
		}
	}
	c.mu.Unlock()
	closeStmts(evicted...)
	return s, nil
}

func (c *stmtCache) hit(query string) (*cachedStmt, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, true
	}
	if s, ok := c.byQuery[query]; ok {
		c.use(s)
		return s, false
	}
	return nil, false
}

func (c *stmtCache) use(s *cachedStmt) {
	s.unlink()
	c.pushFront(s)
	s.refs++
}

func (c *stmtCache) evict(s *cachedStmt) *sql.Stmt {
	s.unlink()
	delete(c.byQuery, s.query)
	s.evicted = true
	if s.refs > 0 {
		return nil
	}
	return s.stmt
}

func (c *stmtCache) release(s *cachedStmt) {
	c.mu.Lock()
	s.refs--
	idle := s.evicted && s.refs == 0
	c.mu.Unlock()
	if idle {
		closeStmts(s.stmt)
	}
}

func (c *stmtCache) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	var idle []*sql.Stmt
	for len(c.byQuery) > 0 {
		if s := c.evict(c.recent.next); s != nil {
			idle = append(idle, s)
		}
	}
	c.mu.Unlock()
	closeStmts(idle...)
}

func closeStmts(stmts ...*sql.Stmt) {
	for _, s := range stmts {
		_ = s.Close()
	}
}
