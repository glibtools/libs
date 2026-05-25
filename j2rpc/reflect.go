package j2rpc

import (
	"reflect"
)

func IndirectValue(val reflect.Value) reflect.Value {
	for val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	return val
}

func ZeroValue(typ reflect.Type) reflect.Value {
	if typ == nil {
		return reflect.Value{}
	}
	switch typ.Kind() {
	case reflect.Pointer:
		return reflect.New(typ.Elem())
	default:
		return reflect.New(typ).Elem()
	}
}
