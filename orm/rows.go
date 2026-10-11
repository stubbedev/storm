package orm

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"slices"
)

// resultRows are a query's rows, scanned so that a UUID column a driver
// hands back as 16 raw bytes (DuckDB) reads as its canonical text.
type resultRows struct {
	*sql.Rows
	uuids []bool
}

func (r *resultRows) Scan(dsts ...any) error {
	if r.uuids == nil {
		types, err := r.ColumnTypes()
		if err != nil {
			return fmt.Errorf("orm: reading the result's column types: %w", err)
		}
		r.uuids = make([]bool, len(types))
		for i, t := range types {
			r.uuids[i] = t.DatabaseTypeName() == "UUID"
		}
	}
	if !slices.Contains(r.uuids, true) {
		return r.Rows.Scan(dsts...)
	}
	wrapped := slices.Clone(dsts)
	for i, d := range dsts {
		if i < len(r.uuids) && r.uuids[i] {
			wrapped[i] = uuidText{d}
		}
	}
	return r.Rows.Scan(wrapped...)
}

type uuidText struct{ dst any }

func (u uuidText) Scan(src any) error {
	if b, ok := src.([]byte); ok && len(b) == 16 {
		src = formatUUID(b)
	}
	return nullable(u).Scan(src)
}

func formatUUID(b []byte) string {
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:], b[10:])
	return string(out[:])
}
