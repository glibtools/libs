package mdb

/*
import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"strconv"
	"time"
)

// varsDigestKeySuffix formats vars digest for inclusion into the cache key.
func varsDigestKeySuffix(d uint64) string {
	// hex is slightly shorter than base10 and stable
	return strconv.FormatUint(d, 16)
}

// varsDigest64 computes a stable 64-bit digest for gorm.Statement Vars.
// Goal: keep cache-key uniqueness properties (different Vars => different digest with extremely high probability)
// while avoiding expensive Dialector.Explain string materialization.
//
// Notes:
// - This is deterministic.
// - It does not aim to reproduce exact SQL literal formatting; only to distinguish different Vars.
// - If a value isn't a common primitive, we fall back to fmt.Sprint(v) to remain stable.
func varsDigest64(vars []interface{}) uint64 {
	h := fnv.New64a()
	// include length to reduce collisions for different slice boundaries
	_ = binary.Write(h, binary.LittleEndian, uint64(len(vars)))

	var b8 [8]byte
	for _, v := range vars {
		if v == nil {
			_, _ = h.Write([]byte{0})
			_, _ = h.Write([]byte{0xFF})
			continue
		}
		rv := reflect.ValueOf(v)
		// unwrap pointers
		for rv.Kind() == reflect.Pointer {
			if rv.IsNil() {
				_, _ = h.Write([]byte{0})
				_, _ = h.Write([]byte{0xFF})
				goto next
			}
			rv = rv.Elem()
		}

		// special-case time.Time (and *time.Time via pointer unwrap above)
		if t, ok := rv.Interface().(time.Time); ok {
			_, _ = h.Write([]byte{7})
			binary.LittleEndian.PutUint64(b8[:], uint64(t.UnixNano()))
			_, _ = h.Write(b8[:])
			_, _ = h.Write([]byte{0xFF})
			continue
		}

		switch rv.Kind() {
		case reflect.Bool:
			_, _ = h.Write([]byte{1})
			if rv.Bool() {
				_, _ = h.Write([]byte{1})
			} else {
				_, _ = h.Write([]byte{0})
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			_, _ = h.Write([]byte{2})
			binary.LittleEndian.PutUint64(b8[:], uint64(rv.Int()))
			_, _ = h.Write(b8[:])
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			_, _ = h.Write([]byte{3})
			binary.LittleEndian.PutUint64(b8[:], rv.Uint())
			_, _ = h.Write(b8[:])
		case reflect.Float32, reflect.Float64:
			_, _ = h.Write([]byte{4})
			binary.LittleEndian.PutUint64(b8[:], math.Float64bits(rv.Convert(reflect.TypeOf(float64(0))).Float()))
			_, _ = h.Write(b8[:])
		case reflect.String:
			_, _ = h.Write([]byte{5})
			s := rv.String()
			binary.LittleEndian.PutUint64(b8[:], uint64(len(s)))
			_, _ = h.Write(b8[:])
			_, _ = h.Write([]byte(s))
		case reflect.Slice:
			// common: []byte
			if rv.Type().Elem().Kind() == reflect.Uint8 {
				_, _ = h.Write([]byte{6})
				bs := rv.Bytes()
				binary.LittleEndian.PutUint64(b8[:], uint64(len(bs)))
				_, _ = h.Write(b8[:])
				_, _ = h.Write(bs)
				break
			}
			fallthrough
		default:
			// last resort: stable string representation
			_, _ = h.Write([]byte{8})
			s := fmt.Sprint(rv.Interface())
			binary.LittleEndian.PutUint64(b8[:], uint64(len(s)))
			_, _ = h.Write(b8[:])
			_, _ = h.Write([]byte(s))
		}

		_, _ = h.Write([]byte{0xFF})

	next:
	}
	return h.Sum64()
}
*/
