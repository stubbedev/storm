package orm

import (
	"strings"
	"testing"
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
