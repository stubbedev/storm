package orm

import (
	"fmt"
	"testing"
	"time"
)

type benchDoc struct {
	Model
	ID     int64     `db:"id"`
	Title  string    `db:"title"`
	Views  int64     `db:"views"`
	Posted time.Time `db:"posted"`
	Vec    Vector[D3]
}

var benchDocTable = &Table[benchDoc, int64]{
	Name:       "bench_documents",
	PrimaryKey: "id",
	KeyType:    KeyAutoIncrement,
	Columns:    []string{"id", "title", "views", "posted", "vec"},
	Ptr: func(m *benchDoc, column string) any {
		switch column {
		case "id":
			return &m.ID
		case "title":
			return &m.Title
		case "views":
			return &m.Views
		case "posted":
			return &m.Posted
		case "vec":
			return &m.Vec
		}
		return nil
	},
	State: func(m *benchDoc) *Model { return &m.Model },
}

type benchDocSchema struct {
	*Table[benchDoc, int64]
	ID     Column[benchDoc, int64, int64]
	Title  Column[benchDoc, int64, string]
	Views  Column[benchDoc, int64, int64]
	Posted Column[benchDoc, int64, time.Time]
	Vec    Column[benchDoc, int64, Vector[D3]]
}

var benchDocs = benchDocSchema{
	Table:  benchDocTable,
	ID:     NewColumn(benchDocTable, "id", func(m *benchDoc) *int64 { return &m.ID }),
	Title:  NewColumn(benchDocTable, "title", func(m *benchDoc) *string { return &m.Title }),
	Views:  NewColumn(benchDocTable, "views", func(m *benchDoc) *int64 { return &m.Views }),
	Posted: NewColumn(benchDocTable, "posted", func(m *benchDoc) *time.Time { return &m.Posted }),
	Vec:    NewColumn(benchDocTable, "vec", func(m *benchDoc) *Vector[D3] { return &m.Vec }),
}

// The plan translation runs on every document-store query; these pin its
// cost so per-condition work cannot quietly turn superlinear.
func BenchmarkPlanCompile(b *testing.B) {
	for _, n := range []int{1, 5, 25} {
		conds := make([]Cond[benchDoc], n)
		for i := range conds {
			conds[i] = benchDocs.Views.Gt(int64(i))
		}
		q := benchDocs.Query().Where(conds...).OrderBy(benchDocs.Posted.Desc()).Limit(20)
		b.Run(fmt.Sprintf("conditions=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := q.plan(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPlanKNN(b *testing.B) {
	q := benchDocs.Query().
		Where(benchDocs.Title.Like("hello")).
		OrderBy(Nearest(benchDocs.Vec, NewVector3(1, 0, 0), Cosine)).
		Limit(10)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := q.plan(); err != nil {
			b.Fatal(err)
		}
	}
}
