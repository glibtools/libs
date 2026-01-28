package j2rpc

import (
	"net/http"
	"reflect"
	"strings"
	"time"
)

type CallerBody func([]byte) ([]byte, error)

type Option func(option *ServerOption)

type ServerOption struct {
	CallerAfterReadBody CallerBody
	CallerBeforeWrite   CallerBody
}

type server struct {
	groups map[string]Group
	router *rpcRouter
	option *ServerOption
}

// Handler ...
func (s *server) Handler(c Context) {
	if ctx, ok := c.(*rpcContext); ok {
		ctx.server = s
	}
	c.AddHandler(s.getOrNewGroup("").Handlers()...)
	c.Next()
}

func (s *server) Option() *ServerOption { return s.option }

func (s *server) RegisterFunc(args ...interface{}) {
	name, fn := argsToNameInterface(args...)
	s.router.registerFunc(fn, name, func(f funcInfo) { s.Use(f.name, s.handleCallFunc(f)) })
}

func (s *server) RegisterType(args ...interface{}) {
	name, bean := argsToNameInterface(args...)
	s.router.registerType(bean, name, func(f funcInfo) { s.Use(f.name, s.handleCallFunc(f)) })
}

// RegisterTypeBus reg type bus
func (s *server) RegisterTypeBus(bus interface{}) {
	if bus == nil {
		panic("RegisterTypeBus: bus is nil")
	}
	value := reflect.Indirect(reflect.ValueOf(bus))
	for i, structField := range reflect.VisibleFields(value.Type()) {
		field := value.Field(i)
		tag := structField.Tag.Get("j2rpc")
		tag = strings.TrimSpace(tag)
		if !structField.IsExported() || !field.CanInterface() || tag == "-" {
			continue
		}
		if field.CanSet() && field.Kind() == reflect.Ptr && field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}
		tagSlice := strings.Split(tag, ",")
		var tagName string
		for _, v := range tagSlice {
			if strings.HasPrefix(v, "name:") || !strings.Contains(v, ":") {
				tagName = strings.TrimPrefix(v, "name:")
				break
			}
		}
		s.RegisterType(field.Interface(), tagName)
	}
}

func (s *server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	s.Handler(NewContext(request.Context(), writer, request))
}

func (s *server) Use(path string, handlers ...Handler) Group {
	return s.getOrNewGroup(path).Use(handlers...)
}

func (s *server) getOrNewGroup(path string) Group {
	if group, ok := s.groups[path]; ok {
		return group
	}
	group := NewGroup(path)
	s.groups[path] = group
	return group
}

func (s *server) handleCallFunc(f funcInfo) Handler {
	return func(c Context) {
		defer c.Next()
		if c.Wrote() {
			return
		}
		argValues := make([]reflect.Value, 0, len(f.args))
		argTypes := make([]reflect.Type, len(f.args))
		copy(argTypes, f.argTs)
		if len(argTypes) > 0 && f.isType {
			argTypes = argTypes[1:]
			argValues = append(argValues, f.args[0])
		}
		if len(argTypes) > 0 && argTypes[0].Implements(contextType) {
			var ctxVal reflect.Value
			switch {
			case argTypes[0].Implements(rpcContextType):
				ctxVal = reflect.ValueOf(c)
			default:
				_ctx, ok := c.(*rpcContext)
				if !ok {
					c.WriteResponse(NewError(ErrInternal, "context error"))
					return
				}
				ctxVal = reflect.ValueOf(_ctx.GetContext())
			}
			argTypes = argTypes[1:]
			argValues = append(argValues, ctxVal)
		}
		values, err := parsePositionalArguments(c.Msg().Params, argTypes)
		if err != nil {
			c.WriteResponse(NewError(ErrBadParams, err.Error()))
			return
		}
		argValues = append(argValues, values...)
		results := f.fn.Call(argValues)
		if len(results) > 0 {
			last := results[len(results)-1]
			if last.Type().Implements(errorType) {
				if !last.IsNil() {
					c.WriteResponse(last.Interface().(error))
					return
				}
				results = results[:len(results)-1]
			}
		}
		if len(results) == 0 {
			c.WriteResponse()
			return
		}
		ret := results[0]
		if (ret.Kind() == reflect.Interface ||
			ret.Kind() == reflect.Ptr) && ret.IsNil() {
			c.WriteResponse()
			return
		}
		data, e := JSONEncode(ret.Interface())
		if e != nil {
			c.WriteResponse(NewError(ErrInternal, e.Error()))
			return
		}
		c.WriteResponse(data)
	}
}

func (s *server) handleReadBody() Handler {
	return func(c Context) {
		defer c.Next()
		ctx, ok := c.(*rpcContext)
		if !ok {
			return
		}
		if e := ctx.ReadBody(); e != nil {
			return
		}
		if c.Wrote() {
			return
		}
		c.SetValue(TimeBeginContextKey, time.Now())
		method := ctx.Msg().Method
		group, has := s.groups[method]
		if !has {
			c.WriteResponse(NewError(ErrNoMethod, "wrong method"))
			return
		}
		handlers := make([]Handler, 0)
		for _key, _group := range s.groups {
			if _key != "" && method != _key && strings.HasPrefix(method, _key+Separator) {
				handlers = append(handlers, _group.Handlers()...)
			}
		}
		handlers = append(handlers, group.Handlers()...)
		c.AddHandler(handlers...)
	}
}

func NewServer(opt ...Option) Server {
	s := &server{
		groups: make(map[string]Group),
		router: &rpcRouter{},
	}
	s.Use("", s.handleReadBody())
	s.option = &ServerOption{}
	for _, o := range opt {
		o(s.option)
	}
	return s
}

func WithCallerAfterReadBody(fn CallerBody) Option {
	return func(option *ServerOption) {
		option.CallerAfterReadBody = fn
	}
}

func WithCallerBeforeWrite(fn CallerBody) Option {
	return func(option *ServerOption) {
		option.CallerBeforeWrite = fn
	}
}
