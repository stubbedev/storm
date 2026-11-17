package orm

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func openCacheDB(t *testing.T, name string, size int) (*sql.DB, *stmtCache) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS nums (n INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	AddConnection(name, db, SQLite)
	if err := SetStatementCacheSize(name, size); err != nil {
		t.Fatal(err)
	}
	c, err := lookup(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if c.stmts == nil {
		t.Fatal("the SQLite pool has no statement cache")
	}
	return db, c.stmts
}

func (c *stmtCache) cached() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for s := c.recent.next; s != &c.recent; s = s.next {
		out = append(out, s.query)
	}
	return out
}

func TestStatementCacheReusesStatementsWithArguments(t *testing.T) {
	_, cache := openCacheDB(t, "stmt_reuse", 4)
	ctx := context.Background()
	for i := range 3 {
		if _, err := cache.ExecContext(ctx, `INSERT INTO nums (n) VALUES (?)`, i); err != nil {
			t.Fatal(err)
		}
	}
	first := cache.byQuery[`INSERT INTO nums (n) VALUES (?)`].stmt
	if _, err := cache.ExecContext(ctx, `INSERT INTO nums (n) VALUES (?)`, 3); err != nil {
		t.Fatal(err)
	}
	if got := cache.cached(); len(got) != 1 || cache.byQuery[got[0]].stmt != first {
		t.Fatalf("cached %v, want the one INSERT prepared once", got)
	}
	if _, err := cache.ExecContext(ctx, `DELETE FROM nums WHERE n < 0; DELETE FROM nums WHERE n > 100`); err != nil {
		t.Fatal(err)
	}
	if got := cache.cached(); len(got) != 1 {
		t.Fatalf("a statement without arguments was cached: %v", got)
	}
}

func TestStatementCacheEvictsLeastRecentlyUsed(t *testing.T) {
	_, cache := openCacheDB(t, "stmt_lru", 2)
	ctx := context.Background()
	for _, q := range []string{"SELECT ? + 1", "SELECT ? + 2", "SELECT ? + 1", "SELECT ? + 3"} {
		rows, err := cache.QueryContext(ctx, q, 1)
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	if got, want := cache.cached(), []string{"SELECT ? + 3", "SELECT ? + 1"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("cached %v, want %v", got, want)
	}
}

func TestEvictedStatementLetsOpenRowsFinish(t *testing.T) {
	db, cache := openCacheDB(t, "stmt_evict", 1)
	ctx := context.Background()
	for i := range 50 {
		if _, err := db.Exec(`INSERT INTO nums (n) VALUES (?)`, i); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := cache.QueryContext(ctx, `SELECT n FROM nums WHERE n >= ? ORDER BY n`, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	if !rows.Next() || rows.Scan(&n) != nil || n != 0 {
		t.Fatalf("first row %d, err %v", n, rows.Err())
	}
	other, err := cache.QueryContext(ctx, `SELECT COUNT(*) FROM nums WHERE n >= ?`, 0)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
	if got := cache.cached(); len(got) != 1 || got[0] != `SELECT COUNT(*) FROM nums WHERE n >= ?` {
		t.Fatalf("cached %v, want only the newer statement", got)
	}
	read := 1
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		read++
	}
	if err := rows.Err(); err != nil || read != 50 {
		t.Fatalf("read %d rows of the evicted statement, err %v; want all 50", read, err)
	}
}

func TestStatementCacheUnderConcurrentEviction(t *testing.T) {
	_, cache := openCacheDB(t, "stmt_race", 3)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				var v int
				q := fmt.Sprintf("SELECT ? + %d", (g+i)%7)
				rows, err := cache.QueryContext(ctx, q, i)
				if err == nil {
					if rows.Next() {
						err = rows.Scan(&v)
					}
					err = cmpErr(err, rows.Close())
				}
				if err == nil && v != i+(g+i)%7 {
					err = fmt.Errorf("%s with %d gave %d", q, i, v)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := cache.cached(); len(got) > 3 {
		t.Fatalf("cache holds %d statements, more than its size 3", len(got))
	}
}

func cmpErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestReplacedConnectionClosesItsStatements(t *testing.T) {
	db, cache := openCacheDB(t, "stmt_replace", 4)
	ctx := context.Background()
	if _, err := cache.ExecContext(ctx, `INSERT INTO nums (n) VALUES (?)`, 1); err != nil {
		t.Fatal(err)
	}
	stmt := cache.byQuery[`INSERT INTO nums (n) VALUES (?)`].stmt
	AddConnection("stmt_replace", db, SQLite)
	if len(cache.cached()) != 0 {
		t.Fatal("the replaced connection's cache still holds statements")
	}
	if _, err := stmt.Exec(2); err == nil {
		t.Fatal("the replaced connection's statement is still open")
	}
	if _, err := cache.ExecContext(ctx, `INSERT INTO nums (n) VALUES (?)`, 3); err != nil {
		t.Fatalf("a statement through the replaced connection failed: %v", err)
	}
}

func TestSetStatementCacheSize(t *testing.T) {
	db, _ := openCacheDB(t, "stmt_size", 4)
	if err := SetStatementCacheSize("stmt_size", 0); err != nil {
		t.Fatal(err)
	}
	if c, _ := lookup(context.Background(), "stmt_size"); c.stmts != nil {
		t.Fatal("size 0 left the cache on")
	}
	AddConnection("stmt_size_pg", db, Postgres)
	for _, c := range []struct {
		name string
		size int
		want string
	}{
		{"stmt_size", -1, "negative"},
		{"stmt_size_missing", 8, "no connection"},
		{"stmt_size_pg", 8, "does not cache prepared statements"},
	} {
		if err := SetStatementCacheSize(c.name, c.size); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("SetStatementCacheSize(%q, %d) = %v, want an error containing %q", c.name, c.size, err, c.want)
		}
	}
}
