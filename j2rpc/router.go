package j2rpc

import (
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

const Separator = "."

var MethodNameProvider = func(name string) string {
	//default use small camel case
	return strings.ToLower(name[:1]) + name[1:]
}

type ImplRPCMethodProvider interface {
	RPCMethodProvider(methodName string) string
}

type ImplRPCTypeName interface {
	RPCTypeName() string
}

type funcInfo struct {
	isType bool
	name   string
	fn     reflect.Value
	args   []reflect.Value
	argTs  []reflect.Type
}

type rpcRouter struct {
	funcs map[string]funcInfo
	once  sync.Once
}

// lazyInit ...
func (r *rpcRouter) lazyInit() *rpcRouter {
	r.once.Do(func() {
		r.funcs = make(map[string]funcInfo)
	})
	return r
}

func (r *rpcRouter) registerFunc(val interface{}, methodName string, callback func(f funcInfo)) {
	r.lazyInit()
	vVal := reflect.ValueOf(val)
	if vVal.IsNil() {
		return
	}
	vType := reflect.TypeOf(val)
	if vType.Kind() != reflect.Func {
		return
	}
	args := make([]reflect.Value, 0)
	argTs := make([]reflect.Type, 0)
	for i := 0; i < vType.NumIn(); i++ {
		args = append(args, reflect.New(vType.In(i)).Elem())
		argTs = append(argTs, vType.In(i))
	}
	if methodName == "" {
		runtimeFunc := vVal.Pointer()
		methodName = runtime.FuncForPC(runtimeFunc).Name()
		methodName = methodName[strings.LastIndex(methodName, Separator)+1:]
		methodName = MethodNameProvider(methodName)
	}
	if _, has := r.funcs[methodName]; has {
		slog.Warn("Skip existing methodName", slog.String("methodName", methodName))
		return
	}
	info := funcInfo{
		name:  methodName,
		fn:    vVal,
		args:  args,
		argTs: argTs,
	}
	r.funcs[methodName] = info
	if callback != nil {
		callback(info)
	}
}

// registerType ...
func (r *rpcRouter) registerType(val interface{}, typeName string, callback func(f funcInfo)) {
	r.lazyInit()
	if val == nil {
		return
	}
	vVal := reflect.ValueOf(val)
	vType := reflect.TypeOf(val)
	if typeName == "" {
		typeName = IndirectValue(vVal).Type().Name()
	}
	typeName = MethodNameProvider(typeName)
	if _v, ok := val.(ImplRPCTypeName); ok {
		typeName = _v.RPCTypeName()
	}
	numMethod := vType.NumMethod()
	for i := 0; i < numMethod; i++ {
		m := vType.Method(i)
		methodName := fmt.Sprintf("%s%s%s", typeName, Separator, MethodNameProvider(m.Name))
		if _v, ok := val.(ImplRPCMethodProvider); ok {
			methodName = _v.RPCMethodProvider(m.Name)
		}
		if _, has := r.funcs[methodName]; has {
			slog.Warn("Skip existing methodName", slog.String("methodName", methodName))
			continue
		}
		mType := m.Type
		args := []reflect.Value{vVal}
		argTs := []reflect.Type{vType}
		for j := 1; j < mType.NumIn(); j++ {
			args = append(args, reflect.New(mType.In(j)).Elem())
			argTs = append(argTs, mType.In(j))
		}
		info := funcInfo{
			isType: true,
			name:   methodName,
			fn:     m.Func,
			args:   args,
			argTs:  argTs,
		}
		r.funcs[methodName] = info
		if callback != nil {
			callback(info)
		}
	}
}
