package orm

import (
	"encoding/json"
	"testing"
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
