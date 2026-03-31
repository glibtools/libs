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
func ExecSingleArgHandler(data []byte, fn any) error {
	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		panic("fn is not a function")
	}

	// 检查参数数量
	numIn := fnType.NumIn()
	if numIn > 1 {
		panic("fn should have at most one argument")
	}

	// 检查返回值数量
	numOut := fnType.NumOut()
	if numOut > 1 {
		panic("fn should have at most one return value")
	}

	// 准备调用参数
	var callArgs []reflect.Value
	if numIn == 1 {
		paramType := fnType.In(0)
		param, err := prepareArgument(data, paramType)
		if err != nil {
			return err
		}
		callArgs = []reflect.Value{reflect.ValueOf(param)}
	}

	// 调用函数
	results := reflect.ValueOf(fn).Call(callArgs)

	// 处理返回值
	if numOut == 1 && !results[0].IsNil() && results[0].Type().Implements(errorType) {
		return results[0].Interface().(error)
	}

	return nil
}

// prepareArgument 根据参数类型准备参数值
func prepareArgument(data []byte, paramType reflect.Type) (any, error) {
	// 特殊情况：[]byte 类型直接返回
	if paramType.AssignableTo(reflect.TypeOf([]byte{})) {
		return data, nil
	}

	switch paramType.Kind() {
	case reflect.String:
		return string(data), nil

	case reflect.Ptr:
		param := reflect.New(paramType.Elem()).Interface()
		if err := Unmarshal(data, param); err != nil {
			return nil, err
		}
		return param, nil

	case reflect.Slice:
		// []byte 类型特殊处理
		if paramType.Elem().Kind() == reflect.Uint8 {
			return data, nil
		}
		// 其他切片类型
		slicePtr := reflect.New(paramType)
		if err := Unmarshal(data, slicePtr.Interface()); err != nil {
			return nil, err
		}
		return slicePtr.Elem().Interface(), nil

	case reflect.Map:
		mapPtr := reflect.New(paramType)
		if err := Unmarshal(data, mapPtr.Interface()); err != nil {
			return nil, err
		}
		return mapPtr.Elem().Interface(), nil

	default:
		// 其他类型
		valuePtr := reflect.New(paramType)
		if err := Unmarshal(data, valuePtr.Interface()); err != nil {
			return nil, err
		}
		return valuePtr.Elem().Interface(), nil
	}
}
