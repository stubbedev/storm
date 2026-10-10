package orm

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"
)

// nullable wraps a scan destination so that NULL becomes the field's zero
// value (nil for pointer fields) instead of a scan error, and converts the
// value representations drivers return (e.g. []byte numbers from MySQL,
// int64 booleans from SQLite).
type nullable struct{ dst any }

func (n nullable) Scan(src any) error {
	if s, ok := n.dst.(sql.Scanner); ok {
		return s.Scan(src)
	}
	return assign(reflect.ValueOf(n.dst).Elem(), src)
}

func assign(dst reflect.Value, src any) error {
	if src == nil {
		dst.SetZero()
		return nil
	}
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		if s, ok := reflect.TypeAssert[sql.Scanner](dst); ok {
			return s.Scan(src)
		}
		return assign(dst.Elem(), src)
	}
	sv := reflect.ValueOf(src)
	if sv.Type().AssignableTo(dst.Type()) {
		dst.Set(sv)
		return nil
	}
	if b, ok := src.([]byte); ok {
		src, sv = string(b), reflect.ValueOf(string(b))
	}
	switch dst.Kind() {
	case reflect.String:
		if s, ok := src.(string); ok {
			dst.SetString(s)
			return nil
		}
		dst.SetString(fmt.Sprint(src))
		return nil
	case reflect.Slice:
		if s, ok := src.(string); ok && dst.Type().Elem().Kind() == reflect.Uint8 {
			dst.SetBytes([]byte(s))
			return nil
		}
	case reflect.Bool:
		switch v := src.(type) {
		case bool:
			dst.SetBool(v)
			return nil
		case int64:
			dst.SetBool(v != 0)
			return nil
		case string:
			b, err := strconv.ParseBool(v)
			dst.SetBool(b)
			return err
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n, ok, err := intOf(src); ok {
			if err == nil && dst.OverflowInt(n) {
				err = fmt.Errorf("%d overflows %s", n, dst.Type())
			}
			if err != nil {
				return fmt.Errorf("orm: cannot scan %v into %s: %w", src, dst.Type(), err)
			}
			dst.SetInt(n)
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n, ok, err := uintOf(src); ok {
			if err == nil && dst.OverflowUint(n) {
				err = fmt.Errorf("%d overflows %s", n, dst.Type())
			}
			if err != nil {
				return fmt.Errorf("orm: cannot scan %v into %s: %w", src, dst.Type(), err)
			}
			dst.SetUint(n)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		if f, ok, err := floatOf(src); ok {
			if err != nil {
				return fmt.Errorf("orm: cannot scan %v into %s: %w", src, dst.Type(), err)
			}
			dst.SetFloat(f)
			return nil
		}
	case reflect.Struct:
		if dst.Type() == reflect.TypeFor[time.Time]() {
			if s, ok := src.(string); ok {
				for _, layout := range []string{"2006-01-02 15:04:05.999999999-07:00", "2006-01-02T15:04:05.999999999-07:00", "2006-01-02 15:04:05", "2006-01-02T15:04:05Z", "2006-01-02"} {
					if t, err := time.Parse(layout, s); err == nil {
						dst.Set(reflect.ValueOf(t))
						return nil
					}
				}
			}
		}
	}
	if sv.CanConvert(dst.Type()) {
		dst.Set(sv.Convert(dst.Type()))
		return nil
	}
	// Document stores hand back JSON-shaped values (maps, slices); round-trip
	// them so custom casts (orm.JSON, orm.Vector) hydrate too.
	if b, err := json.Marshal(src); err == nil {
		if err := json.Unmarshal(b, dst.Addr().Interface()); err == nil {
			return nil
		}
	}
	return fmt.Errorf("orm: cannot scan %T into %s", src, dst.Type())
}

// intOf reads src as an exact integer; ok is false when src is not a
// number. Decimal text (MySQL and Postgres aggregates) is parsed exactly,
// never through a float, and a fraction or overflow is an error.
func intOf(src any) (int64, bool, error) {
	switch v := src.(type) {
	case int:
		return int64(v), true, nil
	case int8:
		return int64(v), true, nil
	case int16:
		return int64(v), true, nil
	case int32:
		return int64(v), true, nil
	case int64:
		return v, true, nil
	case uint, uint8, uint16, uint32, uint64:
		u, _, _ := uintOf(v)
		if u > math.MaxInt64 {
			return 0, true, errors.New("overflows int64")
		}
		return int64(u), true, nil
	case float32:
		return intOf(float64(v))
	case float64:
		if v != math.Trunc(v) || v < math.MinInt64 || v >= math.MaxInt64 {
			return 0, true, errors.New("not an int64")
		}
		return int64(v), true, nil
	case bool:
		if v {
			return 1, true, nil
		}
		return 0, true, nil
	case *big.Int:
		if !v.IsInt64() {
			return 0, true, errors.New("overflows int64")
		}
		return v.Int64(), true, nil
	case string, json.Number:
		r, err := exactNumber(fmt.Sprint(v))
		if err != nil {
			return 0, true, err
		}
		if !r.IsInt() || !r.Num().IsInt64() {
			return 0, true, errors.New("not an int64")
		}
		return r.Num().Int64(), true, nil
	}
	return 0, false, nil
}

// uintOf is intOf for unsigned destinations; negatives are errors.
func uintOf(src any) (uint64, bool, error) {
	switch v := src.(type) {
	case uint:
		return uint64(v), true, nil
	case uint8:
		return uint64(v), true, nil
	case uint16:
		return uint64(v), true, nil
	case uint32:
		return uint64(v), true, nil
	case uint64:
		return v, true, nil
	case *big.Int:
		if !v.IsUint64() {
			return 0, true, errors.New("not a uint64")
		}
		return v.Uint64(), true, nil
	case string, json.Number:
		r, err := exactNumber(fmt.Sprint(v))
		if err != nil {
			return 0, true, err
		}
		if !r.IsInt() || !r.Num().IsUint64() {
			return 0, true, errors.New("not a uint64")
		}
		return r.Num().Uint64(), true, nil
	}
	n, ok, err := intOf(src)
	if !ok || err != nil {
		return 0, ok, err
	}
	if n < 0 {
		return 0, true, errors.New("negative")
	}
	return uint64(n), true, nil
}

func floatOf(src any) (float64, bool, error) {
	switch v := src.(type) {
	case float64:
		return v, true, nil
	case float32:
		return float64(v), true, nil
	case string, json.Number:
		f, err := strconv.ParseFloat(fmt.Sprint(v), 64)
		return f, true, err
	}
	if n, ok, err := intOf(src); ok && err == nil {
		return float64(n), true, nil
	}
	if n, ok, err := uintOf(src); ok && err == nil {
		return float64(n), true, nil
	}
	return 0, false, nil
}

func exactNumber(s string) (*big.Rat, error) {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("%q is not a number", s)
	}
	return r, nil
}
