package orm

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

type benchPost struct {
	Model
	ID        int64     `db:"id"`
	Title     string    `db:"title"`
	Body      *string   `db:"body"`
	Views     int64     `db:"views"`
	Published bool      `db:"published"`
	CreatedAt time.Time `db:"created_at"`
}

var benchPostTable = &Table[benchPost, int64]{
	Name:       "bench_posts",
	PrimaryKey: "id",
	KeyType:    KeyAutoIncrement,
	Columns:    []string{"id", "title", "body", "views", "published", "created_at"},
	Ptr: func(m *benchPost, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "title":
			return &m.Title
		case "body":
			return &m.Body
		case "views":
			return &m.Views
		case "published":
			return &m.Published
		case "created_at":
			return &m.CreatedAt
		}
		return nil
	},
	State: func(m *benchPost) *Model { return &m.Model },
}

var benchPostViews = NewColumn(benchPostTable, "views", func(m *benchPost) *int64 { return &m.Views })

const benchPostRows = 1000

func openBenchPosts(b *testing.B) {
	b.Helper()
	db, err := sql.Open("sqlite", "file::memory:?_time_format=sqlite")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	stmts := []string{`CREATE TABLE bench_posts (id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT, views INTEGER NOT NULL, published BOOLEAN NOT NULL, created_at DATETIME NOT NULL)`}
	for i := 1; i <= benchPostRows; i++ {
		stmts = append(stmts, fmt.Sprintf(`INSERT INTO bench_posts VALUES (%d, 'post %d', 'body of post %d', %d, %t, '2026-01-02 03:04:05')`, i, i, i, i*7, i%2 == 0))
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			b.Fatalf("seed bench_posts: %v", err)
		}
	}
	SetDefault(db, SQLite)
}

func BenchmarkReadRows(b *testing.B) {
	openBenchPosts(b)
	for b.Loop() {
		posts, err := benchPostTable.Query().Get()
		if err != nil {
			b.Fatal(err)
		}
		if len(posts) != benchPostRows {
			b.Fatalf("read %d rows, want %d", len(posts), benchPostRows)
		}
	}
}

func BenchmarkFindByKey(b *testing.B) {
	openBenchPosts(b)
	for b.Loop() {
		if _, err := benchPostTable.Find(500); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoadEditSave(b *testing.B) {
	openBenchPosts(b)
	for b.Loop() {
		p, err := benchPostTable.Find(500)
		if err != nil {
			b.Fatal(err)
		}
		*benchPostViews.Ptr(&p)++
		if err := benchPostTable.Save(&p); err != nil {
			b.Fatal(err)
		}
	}
}
