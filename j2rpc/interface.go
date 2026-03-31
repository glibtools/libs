package j2rpc

import (
	"context"
	"net/http"
)

type Context interface {
	context.Context
	Abort()
	AddHandler(handlers ...Handler)
	GetContext() context.Context
	GetValue(key string) (interface{}, bool)
	IsAbort() bool
	Msg() *RPCMessage
	Next()
	Request() *http.Request
	Server() Server
	SetValue(key string, val interface{})
	Writer() http.ResponseWriter
	StopWriteStringStatus(status int, str string)
	WriteResponse(args ...interface{})
	Wrote() bool
}

type Group interface {
	Handlers() []Handler
	Use(handlers ...Handler) Group
}

type Server interface {
	Handler(c Context)
	Option() *ServerOption
	RegisterFunc(args ...interface{})
	RegisterType(args ...interface{})
	RegisterTypeBus(bus interface{})
	ServeHTTP(writer http.ResponseWriter, request *http.Request)
	Use(path string, handlers ...Handler) Group
}
