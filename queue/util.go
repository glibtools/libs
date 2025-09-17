package queue

import (
	"reflect"
)

var _ = deepCopyConfigs[struct{}]

func deepCopyConfigs[T any](src T) T {
	srcVal := reflect.ValueOf(src)
	for srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}
	dstVal := reflect.New(srcVal.Type()).Elem()

	for i := 0; i < srcVal.NumField(); i++ {
		srcField := srcVal.Field(i)
		dstField := dstVal.Field(i)

		dstField.Set(srcField)
	}

	return dstVal.Interface().(T)
}

// isZeroValue checks if a (reflect.Value) is the zero value for its type.
func isZeroValue(v reflect.Value) bool {
	return reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}

// mergeConfigs merges non-zero fields from src to dst using reflection.
// useAge: mergeConfigs(&dst, &src)
func mergeConfigs(dst, src interface{}) {
	dstVal := reflect.ValueOf(dst).Elem()
	srcVal := reflect.ValueOf(src).Elem()

	for i := 0; i < srcVal.NumField(); i++ {
		srcField := srcVal.Field(i)
		dstField := dstVal.Field(i)

		if !isZeroValue(srcField) {
			dstField.Set(srcField)
		}
	}
}
