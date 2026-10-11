package orm

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// VectorMetric is the distance metric for nearest-neighbour ordering.
type VectorMetric int

const (
	// L2 is squared Euclidean distance (pgvector <->, array_distance).
	L2 VectorMetric = iota
	// Cosine is 1 - cosine similarity (pgvector <=>).
	Cosine
	// InnerProduct is the negative dot product ordering of pgvector (<#>);
	// plain similarity on other engines.
	InnerProduct
)

// Vector is a fixed-size float vector column — pgvector's vector type,
// DuckDB's FLOAT[N], Qdrant's named vectors. Each dimension is its own Go
// type via the D- markers (Go cannot take a number as a type argument), so
// Docs.Embedding (Vector[D3]) and Docs.Fingerprint (Vector[D128]) cannot be
// mixed, and a Vector column cannot be compared to a string.
//
//	type Doc struct {
//		orm.Model
//		ID        int64            `db:"id"`
//		Embedding orm.Vector[orm.D3] `db:"embedding"`
//	}
//
// Ordering with Nearest runs a nearest-neighbour search on the engine —
// an index-backed one with pgvector's HNSW or a Qdrant collection.
type Vector[N Dimension] struct{ comps []float64 }

// Dimension is a vector size marker: orm.Vector[orm.D3] is a
// 3-dimensional vector, and every vector built, scanned or decoded is
// checked against it.
type Dimension interface {
	~int
	Dim() int
}

// Dimension markers for Vector's type argument.
type (
	D2    int
	D3    int
	D4    int
	D5    int
	D6    int
	D7    int
	D8    int
	D9    int
	D10   int
	D12   int
	D16   int
	D24   int
	D32   int
	D48   int
	D64   int
	D96   int
	D128  int
	D160  int
	D256  int
	D384  int
	D512  int
	D640  int
	D768  int
	D896  int
	D1024 int
	D1280 int
	D1536 int
	D2048 int
	D3072 int
	D4096 int
)

func (D2) Dim() int    { return 2 }
func (D3) Dim() int    { return 3 }
func (D4) Dim() int    { return 4 }
func (D5) Dim() int    { return 5 }
func (D6) Dim() int    { return 6 }
func (D7) Dim() int    { return 7 }
func (D8) Dim() int    { return 8 }
func (D9) Dim() int    { return 9 }
func (D10) Dim() int   { return 10 }
func (D12) Dim() int   { return 12 }
func (D16) Dim() int   { return 16 }
func (D24) Dim() int   { return 24 }
func (D32) Dim() int   { return 32 }
func (D48) Dim() int   { return 48 }
func (D64) Dim() int   { return 64 }
func (D96) Dim() int   { return 96 }
func (D128) Dim() int  { return 128 }
func (D160) Dim() int  { return 160 }
func (D256) Dim() int  { return 256 }
func (D384) Dim() int  { return 384 }
func (D512) Dim() int  { return 512 }
func (D640) Dim() int  { return 640 }
func (D768) Dim() int  { return 768 }
func (D896) Dim() int  { return 896 }
func (D1024) Dim() int { return 1024 }
func (D1280) Dim() int { return 1280 }
func (D1536) Dim() int { return 1536 }
func (D2048) Dim() int { return 2048 }
func (D3072) Dim() int { return 3072 }
func (D4096) Dim() int { return 4096 }

func dimOf[N Dimension]() int {
	var n N
	return n.Dim()
}

// VectorOf builds a Vector of dimension N from components that come from
// outside the program, failing when their count is not N.
func VectorOf[N Dimension](fs []float64) (Vector[N], error) {
	var v Vector[N]
	return v, v.set(append([]float64(nil), fs...))
}

// NewVector builds a Vector of dimension N from literal components; it
// panics when their count is not N. Use VectorOf for runtime data.
func NewVector[N Dimension](fs ...float64) Vector[N] {
	v, err := VectorOf[N](fs)
	if err != nil {
		panic(err)
	}
	return v
}

func (v *Vector[N]) set(comps []float64) error {
	if want := dimOf[N](); len(comps) != want {
		return fmt.Errorf("orm: a %d-dimension vector cannot hold %d components", want, len(comps))
	}
	for i, c := range comps {
		if math.IsNaN(c) || math.IsInf(c, 0) {
			return fmt.Errorf("orm: vector component %d is %v; components must be finite", i, c)
		}
	}
	v.comps = comps
	return nil
}

// NewVector3 is shorthand for NewVector[orm.D3].
func NewVector3(x, y, z float64) Vector[D3] { return NewVector[D3](x, y, z) }

// Floats returns the components; nil for the zero Vector.
func (v Vector[N]) Floats() []float64 { return v.comps }

func (v Vector[N]) String() string {
	fs := make([]string, len(v.comps))
	for i, f := range v.comps {
		fs[i] = strconv.FormatFloat(f, 'g', -1, 64)
	}
	return "[" + strings.Join(fs, ",") + "]"
}

// Value stores the vector in the text form pgvector accepts; DuckDB casts
// the same form to FLOAT[N]. The zero Vector is NULL.
func (v Vector[N]) Value() (driver.Value, error) {
	if v.comps == nil {
		return nil, nil
	}
	return v.String(), nil
}

// Scan reads the text form back (pgvector returns it in queries).
func (v *Vector[N]) Scan(src any) error {
	switch s := src.(type) {
	case nil:
		*v = Vector[N]{}
		return nil
	case string:
		return v.parse(s)
	case []byte:
		if len(s) > 0 && s[0] == '[' && s[len(s)-1] == ']' {
			return v.parse(string(s))
		}
		return v.unpack(s)
	case []float64:
		return v.set(append([]float64(nil), s...))
	case []float32:
		comps := make([]float64, len(s))
		for i, f := range s {
			comps[i] = float64(f)
		}
		return v.set(comps)
	case []any:
		comps := make([]float64, len(s))
		for i, f := range s {
			n, err := toFloat(f)
			if err != nil {
				return fmt.Errorf("orm: scanning vector component %d: %w", i, err)
			}
			comps[i] = n
		}
		return v.set(comps)
	}
	return fmt.Errorf("orm: cannot scan %T into a Vector", src)
}

// unpack reads the packed little-endian float32 form MariaDB and MySQL 9
// return for VECTOR columns.
func (v *Vector[N]) unpack(raw []byte) error {
	if len(raw)%4 != 0 {
		return fmt.Errorf("orm: %d bytes are not a packed float32 vector", len(raw))
	}
	comps := make([]float64, len(raw)/4)
	for i := range comps {
		comps[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:])))
	}
	return v.set(comps)
}

// vectorOf reports v's components when v is a non-zero vector value of
// any dimension.
func vectorOf(v any) ([]float64, bool) {
	if vv, ok := v.(interface{ Floats() []float64 }); ok {
		return vv.Floats(), vv.Floats() != nil
	}
	return nil, false
}

func toFloat(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case int:
		return float64(n), nil
	case json.Number:
		return n.Float64()
	case string:
		return strconv.ParseFloat(n, 64)
	}
	return 0, fmt.Errorf("not a number: %T", v)
}

// parse reads the text form: exactly one bracketed, comma-separated list.
func (v *Vector[N]) parse(s string) error {
	t := strings.TrimSpace(s)
	if t == "" {
		*v = Vector[N]{}
		return nil
	}
	inner, opened := strings.CutPrefix(t, "[")
	inner, closed := strings.CutSuffix(inner, "]")
	if !opened || !closed || strings.ContainsAny(inner, "[]") {
		return fmt.Errorf("orm: scanning vector %q: not one bracketed list of numbers", s)
	}
	fs := strings.Split(inner, ",")
	comps := make([]float64, len(fs))
	for i, f := range fs {
		x, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil {
			return fmt.Errorf("orm: scanning vector %q: %w", s, err)
		}
		comps[i] = x
	}
	return v.set(comps)
}

func (v Vector[N]) MarshalJSON() ([]byte, error) {
	if v.comps == nil {
		return []byte("null"), nil
	}
	return json.Marshal(v.comps)
}

func (v *Vector[N]) UnmarshalJSON(b []byte) error {
	var fs []float64
	if err := json.Unmarshal(b, &fs); err != nil {
		return err
	}
	if fs == nil {
		*v = Vector[N]{}
		return nil
	}
	return v.set(fs)
}

// Nearest orders rows by distance of the vector expression e from v under
// the metric (pgvector's nearest-neighbours, DuckDB's array_distance, a
// Qdrant search): orm.Nearest(Docs.Vec, q, orm.Cosine). Only a vector of
// e's own dimension compiles. Combined with Limit it is a KNN query.
func Nearest[M any, N Dimension](e Expr[M, Vector[N]], v Vector[N], metric VectorMetric) Order[M] {
	o := Order[M]{f: func(b *SQL) { b.Dialect.VectorDistance(b, e, v, metric) }}
	if c, ok := e.(interface{ docColumn() string }); ok && c.docColumn() != "" {
		o.vec = &VectorOrder{Column: c.docColumn(), Floats: v.Floats(), Metric: metric}
	}
	return o
}
