package orm

import (
	"testing"
	"time"
)

func benchPostRow() benchPost {
	body := "body of post"
	return benchPost{ID: 500, Title: "post 500", Body: &body, Views: 3500, Published: true, CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func BenchmarkSnapshot(b *testing.B) {
	p := benchPostRow()
	b.ReportAllocs()
	for b.Loop() {
		benchPostTable.sync(&p)
	}
}

func BenchmarkDirty(b *testing.B) {
	p := benchPostRow()
	benchPostTable.sync(&p)
	p.Views++
	b.ReportAllocs()
	for b.Loop() {
		if len(benchPostTable.Dirty(&p)) != 1 {
			b.Fatal("want one dirty column")
		}
	}
}

func BenchmarkScanRow(b *testing.B) {
	for _, c := range []struct {
		name string
		srcs []any
	}{
		{"sqlite", []any{int64(500), "post 500", "body of post", int64(3500), int64(1), time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}},
		{"mysql-text", []any{[]byte("500"), []byte("post 500"), []byte("body of post"), []byte("3500"), []byte("1"), []byte("2026-01-02 03:04:05")}},
	} {
		b.Run(c.name, func(b *testing.B) {
			var p benchPost
			dsts := []any{&p.ID, &p.Title, &p.Body, &p.Views, &p.Published, &p.CreatedAt}
			b.ReportAllocs()
			for b.Loop() {
				for i, d := range dsts {
					if err := (nullable{d}).Scan(c.srcs[i]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
