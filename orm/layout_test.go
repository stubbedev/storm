package orm

import (
	"slices"
	"strings"
	"testing"
	"time"
)

type layoutRow struct {
	Model
	ID    int64
	Name  string
	Count int64
}

func layoutTable(columns, virtual []string, field func(*layoutRow, int) any) *Table[layoutRow, int64] {
	return &Table[layoutRow, int64]{
		Name: "layout_rows", PrimaryKey: "id", Columns: columns, VirtualColumns: virtual,
		Field: field, State: func(m *layoutRow) *Model { return &m.Model },
	}
}

func layoutFields(m *layoutRow, i int) any {
	switch i {
	case 0:
		return &m.ID
	case 1:
		return &m.Name
	case 2:
		return &m.Count
	}
	return nil
}

func TestFieldIndexesColumnsThenVirtualColumns(t *testing.T) {
	tab := layoutTable([]string{"id", "name"}, []string{"posts_count"}, layoutFields)
	var m layoutRow
	if p, ok := tab.ptr(&m, "posts_count").(*int64); !ok || p != &m.Count {
		t.Fatalf("posts_count resolves to %v, want &m.Count", p)
	}
	if p, ok := tab.ptr(&m, "name").(*string); !ok || p != &m.Name {
		t.Fatalf("name resolves to %v, want &m.Name", p)
	}
	d, err := newModelDests(tab, []string{"posts_count", "id"})
	if err != nil {
		t.Fatal(err)
	}
	d.bind(&m)
	if d.cells[0].dst != any(&m.Count) || d.cells[1].dst != any(&m.ID) {
		t.Fatalf("dests bound to %v, want &m.Count and &m.ID", d.cells)
	}
}

func TestModelDestsRefuseUnknownColumn(t *testing.T) {
	tab := layoutTable([]string{"id", "name"}, nil, layoutFields)
	_, err := newModelDests(tab, []string{"id", "nickname"})
	if err == nil || !strings.Contains(err.Error(), `"nickname"`) {
		t.Fatalf("err = %v, want one naming the unknown column", err)
	}
}

func TestLayoutRefusesMisdeclaredTable(t *testing.T) {
	for _, c := range []struct {
		name    string
		tab     *Table[layoutRow, int64]
		message string
	}{
		{"duplicate column", layoutTable([]string{"id", "name"}, []string{"name"}, layoutFields), `declares column "name" twice`},
		{"column without field", layoutTable([]string{"id", "name", "count", "extra"}, nil, layoutFields), `no field pointer for column "extra"`},
		{"no Field", layoutTable([]string{"id"}, nil, nil), "needs both Field and State"},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				msg, _ := recover().(string)
				if !strings.Contains(msg, c.message) {
					t.Fatalf("panic = %q, want one containing %q", msg, c.message)
				}
			}()
			c.tab.plan()
		})
	}
}

type snapshotStamp struct {
	Note string
	At   time.Time
}

type snapshotRow struct {
	Model
	snapshotStamp
	ID       int64
	Tags     []string
	Labels   map[string]string
	Parent   *int64
	Comments []layoutRow
	cache    map[string]int
}

var snapshotTable = &Table[snapshotRow, int64]{
	Name: "snapshot_rows", PrimaryKey: "id",
	Columns: []string{"id", "tags", "labels", "parent", "at"},
	Field: func(m *snapshotRow, i int) any {
		switch i {
		case 0:
			return &m.ID
		case 1:
			return &m.Tags
		case 2:
			return &m.Labels
		case 3:
			return &m.Parent
		case 4:
			return &m.At
		}
		return nil
	},
	State: func(m *snapshotRow) *Model { return &m.Model },
}

func TestSnapshotCopiesColumnsOnly(t *testing.T) {
	parent := int64(7)
	m := snapshotRow{
		ID: 1, Tags: []string{"a"}, Labels: map[string]string{"k": "v"}, Parent: &parent,
		Comments: []layoutRow{{ID: 9}}, cache: map[string]int{"x": 1},
	}
	m.Note, m.At = "loaded", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	snapshotTable.sync(&m)
	snapshotTable.sync(&m)
	orig, ok := snapshotTable.original(&m)
	if !ok {
		t.Fatal("no snapshot")
	}
	if orig.Comments != nil || orig.cache != nil || orig.Note != "" {
		t.Fatalf("snapshot kept non-column fields: %+v", orig)
	}
	if orig.original != nil || orig.exists || orig.loaded != nil {
		t.Fatalf("snapshot chains to model state: %+v", orig.Model)
	}
	if orig.ID != 1 || !orig.At.Equal(m.At) || orig.Tags[0] != "a" || orig.Labels["k"] != "v" || *orig.Parent != 7 {
		t.Fatalf("snapshot lost column values: %+v", orig)
	}
	if &orig.Tags[0] == &m.Tags[0] || orig.Parent == m.Parent {
		t.Fatal("snapshot shares references with the model")
	}
	m.Tags[0], m.Labels["k"], *m.Parent = "b", "w", 8
	if got, want := snapshotTable.Dirty(&m), []string{"tags", "labels", "parent"}; !slices.Equal(got, want) {
		t.Fatalf("Dirty = %v, want %v", got, want)
	}
}
