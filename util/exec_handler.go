package util

import (
	"reflect"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// ExecSingleArgHandler executes a function with a single argument.
// The argument can be a pointer, slice or map.
// The function should have at most one return value, which should be an error.
// The argument is unmarshalled from the given data.
// If the argument is a pointer, it is unmarshalled directly.
// If the argument is a slice, it is unmarshalled into a slice of the correct type.
// If the argument is a map, it is unmarshalled into a map of the correct type.
// e.g.: ExecSingleArgHandler([]byte(`{"name":"test"}`), func(v util.Map) {})
// e.g.: ExecSingleArgHandler([]byte(`[{"name":"test"}]`), func(v []interface{}) {})
func ExecSingleArgHandler(data []byte, fn any) (err error) {
	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		panic("fn is not a function")
	}
	args := make([]reflect.Type, 0, 1)
	for i := 0; i < fnType.NumIn(); i++ {
		args = append(args, fnType.In(i))
	}
	if len(args) > 1 {
		panic("fn should have at most one argument")
	}
	argParam := func() any {
		var param any
		if len(args) == 0 {
			return nil
		}
		paramType := args[0]
		if paramType.AssignableTo(AnyType[[]byte]()) {
			return data
		}
		switch paramType.Kind() {
		case reflect.Ptr:
			param = reflect.New(paramType.Elem()).Interface()
			if e := Unmarshal(data, param); e != nil {
				return e
			}
		case reflect.Slice:
			sl := reflect.MakeSlice(reflect.SliceOf(paramType.Elem()), 0, 0)
			s2 := reflect.New(sl.Type())
			s2.Elem().Set(sl)
			if e := Unmarshal(data, s2.Interface()); e != nil {
				return e
			}
			param = s2.Elem().Interface()
		case reflect.Map:
			m1 := reflect.MakeMap(reflect.MapOf(paramType.Key(), paramType.Elem()))
			m2 := reflect.New(m1.Type())
			m2.Elem().Set(m1)
			if e := Unmarshal(data, m2.Interface()); e != nil {
				return e
			}
			param = m2.Elem().Interface()
		default:
			v := reflect.New(paramType)
			if e := Unmarshal(data, v.Interface()); e != nil {
				return e
			}
			param = v.Elem().Interface()
		}
		return param
	}()
	callValues := func() []reflect.Value {
		if argParam == nil {
			return nil
		}
		return []reflect.Value{reflect.ValueOf(argParam)}
	}()
	outs := reflect.ValueOf(fn).Call(callValues)
	if len(outs) == 0 {
		return
	}
	if len(outs) > 1 {
		panic("fn should have at most one return value")
	}
	if !outs[0].IsNil() && outs[0].Type().Implements(errorType) {
		err = outs[0].Interface().(error)
	}
	return
}
