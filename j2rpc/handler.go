package j2rpc

import (
	"context"
	"reflect"
)

var (
	contextType    = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType      = reflect.TypeOf((*error)(nil)).Elem()
	rpcContextType = reflect.TypeOf((*Context)(nil)).Elem()
)

type Handler func(c Context)
