package orm

import (
	"fmt"
	"reflect"
	"slices"
	"time"
	"unsafe"
)

type tableLayout struct {
	index   map[string]int
	columns []int
	deep    []int
	clear   []clearedField
}

type clearedField struct {
	path     []int
	settable bool
}

func (c clearedField) zero(v reflect.Value) {
	f := v.FieldByIndex(c.path)
	if c.settable {
		f.SetZero()
		return
	}
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().SetZero() //nolint:gosec
}

type fieldSpan struct {
	offset uintptr
	typ    reflect.Type
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
	base := reflect.ValueOf(probe).Pointer()
	spans := make([]fieldSpan, len(t.Columns))
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
		if i >= len(t.Columns) {
			continue
		}
		spans[i] = fieldSpan{offset: p.Pointer() - base, typ: p.Type().Elem()}
		if holdsReferences(spans[i].typ) {
			l.deep = append(l.deep, i)
		}
	}
	l.clear = nonColumnFields(reflect.ValueOf(probe).Elem(), 0, nil, spans)
	return l
}

func nonColumnFields(v reflect.Value, offset uintptr, path []int, spans []fieldSpan) []clearedField {
	var out []clearedField
	for i := range v.NumField() {
		sf := v.Type().Field(i)
		start := offset + sf.Offset
		fieldPath := append(slices.Clip(path), i)
		switch {
		case sf.Type == reflect.TypeFor[Model]():
		case slices.Contains(spans, fieldSpan{offset: start, typ: sf.Type}):
		case sf.Type.Kind() == reflect.Struct && containsColumn(spans, start, start+sf.Type.Size()):
			out = append(out, nonColumnFields(v.Field(i), start, fieldPath, spans)...)
		case sf.Type.Size() > 0:
			out = append(out, clearedField{path: fieldPath, settable: v.Field(i).CanSet()})
		}
	}
	return out
}

func containsColumn(spans []fieldSpan, start, end uintptr) bool {
	return slices.ContainsFunc(spans, func(s fieldSpan) bool { return s.offset >= start && s.offset < end })
}

func holdsReferences(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return true
	case reflect.Array:
		return holdsReferences(t.Elem())
	case reflect.Struct:
		if t == reflect.TypeFor[time.Time]() {
			return false
		}
		for f := range t.Fields() {
			if holdsReferences(f.Type) {
				return true
			}
		}
	}
	return false
}
