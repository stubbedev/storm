package orm

import (
	"fmt"
	"reflect"
	"slices"
)

type tableLayout struct {
	index   map[string]int
	columns []int
}

func (t *Table[M, K]) plan() *tableLayout {
	t.layoutOnce.Do(func() { t.layout = buildLayout(t) })
	return t.layout
}

func buildLayout[M any, K comparable](t *Table[M, K]) *tableLayout {
	if t.Field == nil || t.State == nil {
		panic(fmt.Sprintf("orm: table %s needs both Field and State", t.Name))
	}
	names := slices.Concat(t.Columns, t.VirtualColumns)
	l := &tableLayout{index: make(map[string]int, len(names)), columns: make([]int, len(t.Columns))}
	probe := new(M)
	for i, name := range names {
		if _, dup := l.index[name]; dup {
			panic(fmt.Sprintf("orm: table %s declares column %q twice", t.Name, name))
		}
		l.index[name] = i
		if i < len(t.Columns) {
			l.columns[i] = i
		}
		p := reflect.ValueOf(t.Field(probe, i))
		if p.Kind() != reflect.Pointer || p.IsNil() {
			panic(fmt.Sprintf("orm: %s.Field returns no field pointer for column %q", t.Name, name))
		}
	}
	return l
}
