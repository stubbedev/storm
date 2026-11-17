package orm

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestScanCopiesDriverOwnedBytes(t *testing.T) {
	var raw []byte
	var doc json.RawMessage
	var ptr *[]byte
	for _, dst := range []any{&raw, &doc, &ptr} {
		src := []byte(`{"a":1}`)
		if err := (nullable{dst}).Scan(src); err != nil {
			t.Fatal(err)
		}
		src[2] = 'X'
	}
	if string(raw) != `{"a":1}` || string(doc) != `{"a":1}` || string(*ptr) != `{"a":1}` {
		t.Fatalf("scanned bytes alias the driver's buffer: %q %q %q", raw, doc, *ptr)
	}
}

func scanSources() []any {
	at := time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)
	return []any{
		nil, int64(0), int64(1), int64(-42), int64(math.MaxInt64), int64(math.MinInt64),
		uint64(5), uint64(math.MaxUint64), float64(2), float64(2.5), float32(1.5), true, false, at,
		[]byte("42"), []byte("-7"), []byte("+3"), []byte("007"), []byte("4.0"), []byte("1e3"), []byte("0x10"), []byte("1_000"),
		[]byte("abc"), []byte(""), []byte("true"), []byte("1"), []byte("0"), []byte("NaN"), []byte("inf"),
		[]byte{0xff, 0xfe},
		[]byte("2026-01-02 03:04:05"), []byte("2026-01-02T03:04:05Z"), []byte("2026-01-02T03:04:05.5+02:00"),
		"42", "-0", "3.25", "1e400", "true", "f", "nope", "", "9223372036854775808",
		"2026-01-02", "2026-01-02 03:04:05.123456+02:00", "2026-01-02T03:04:05.5Z", "2026-01-02T03:04:05+01:00",
	}
}

func TestScanFastPathMatchesAssign(t *testing.T) {
	int64Val, intVal, stringVal, boolVal, floatVal := int64(9), 9, "old", true, 9.5
	timeVal, bytesVal := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC), []byte("old")
	for _, start := range []any{
		int64(9), 9, "old", true, 9.5, timeVal, []byte("old"),
		(*int64)(nil), (*int)(nil), (*string)(nil), (*bool)(nil), (*float64)(nil), (*time.Time)(nil), (*[]byte)(nil),
		&int64Val, &intVal, &stringVal, &boolVal, &floatVal, &timeVal, &bytesVal,
	} {
		for _, src := range scanSources() {
			fast, slow := cloneStart(start), cloneStart(start)
			fastErr := nullable{fast.Interface()}.Scan(src)
			slowErr := assign(slow.Elem(), src)
			name := fmt.Sprintf("%T from %T(%v)", start, src, src)
			if fmt.Sprint(fastErr) != fmt.Sprint(slowErr) {
				t.Errorf("%s: error %v, assign gives %v", name, fastErr, slowErr)
				continue
			}
			if got, want := fmt.Sprintf("%#v", deref(fast.Elem())), fmt.Sprintf("%#v", deref(slow.Elem())); got != want {
				t.Errorf("%s: scanned %s, assign gives %s", name, got, want)
			}
		}
	}
}

func cloneStart(start any) reflect.Value {
	v := reflect.ValueOf(start)
	p := reflect.New(v.Type())
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		inner := reflect.New(v.Type().Elem())
		inner.Elem().Set(v.Elem())
		p.Elem().Set(inner)
		return p
	}
	p.Elem().Set(v)
	return p
}

func deref(v reflect.Value) any {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return v.Elem().Interface()
	}
	return v.Interface()
}
