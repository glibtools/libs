package giris

import (
	"errors"
	"reflect"
	"strings"

	"github.com/kataras/iris/v12"

	"github.com/glibtools/libs/j2rpc"
)

var (
	contextType    = reflect.TypeOf((iris.Context)(nil))
	errorType      = reflect.TypeOf((*error)(nil)).Elem()
	supportMethods = []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"}
)

// JSONResponse ...
type JSONResponse struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg,omitempty"`
	Data interface{} `json:"data,omitempty"`
}

// BindRestAPIs ...
func BindRestAPIs(g iris.Party, api interface{}) {
	apiType := reflect.TypeOf(api)
	for i := 0; i < reflect.TypeOf(api).NumMethod(); i++ {
		bindRestAPIsHandler(g, apiType.Method(i).Name, ParseRestFunc2handler(apiType.Method(i).Func.Interface(), api))
	}
}

// ParseRestFunc2handler fn param like those
// func Handler(c iris.Context,bound any) (any, error)
// func Handler(c iris.Context,bound any) any
// func Handler(c iris.Context,bound any) error
// func Handler(c iris.Context,bound any)
// func Handler(bound any) (any, error)
// func Handler(bound any) any
// func Handler(bound any) error
// func Handler(bound any)
// func Handler() (any, error)
// func Handler() any
// func Handler() error
// func Handler()
func ParseRestFunc2handler(fn any, receivers ...any) iris.Handler {
	var receiver any
	if len(receivers) > 0 && receivers[0] != nil {
		receiver = receivers[0]
	}
	return func(c iris.Context) {
		fnType := reflect.TypeOf(fn)
		if fnType.Kind() != reflect.Func {
			RestJSONError(c, 500, "server error, handler is not a function", true)
			return
		}
		fnValue := reflect.ValueOf(fn)
		if fnValue.IsNil() {
			RestJSONError(c, 500, "server error, handler is nil", true)
			return
		}
		nm := fnType.NumIn()
		args := make([]reflect.Value, 0, nm)
		if receiver != nil {
			args = append(args, reflect.ValueOf(receiver))
			nm--
		}
		if nm > 0 && fnType.In(len(args)) == contextType {
			args = append(args, reflect.ValueOf(c))
			nm--
		}
		if nm > 1 {
			RestJSONError(c, 500, "server error, handler's bound param is more than one", true)
			return
		}
		if nm == 1 {
			var err error
			args = append(args, reflectNewByType(
				fnType.In(len(args)),
				func(v interface{}) {
					c.RecordRequestBody(true)
					var body []byte
					body, err = c.GetBody()
					if err != nil {
						return
					}
					if err = j2rpc.JSONDecode(body, v); err != nil {
						return
					}
				}),
			)
			if err != nil {
				RestJSONError(c, 400, err)
				return
			}
		}
		result := fnValue.Call(args)
		_ = parseRestFuncResult(result, c)
	}
}

func RestJSONError(c iris.Context, args ...interface{}) {
	var (
		code      = 400
		msg       string
		data      interface{}
		writeCode = false
	)
	for _, arg := range args {
		switch v := arg.(type) {
		case int:
			code = v
		case string:
			msg = v
		case error:
			msg = v.Error()
		case bool:
			writeCode = v
		default:
			data = v
		}
	}
	_ = c.JSON(JSONResponse{Code: code, Msg: msg, Data: data})
	if writeCode {
		c.StatusCode(code)
		c.StopExecution()
	}
}

func RestJSONSuccess(c iris.Context, vs ...interface{}) error {
	const successCode = 200
	if len(vs) == 0 {
		return c.JSON(JSONResponse{Code: successCode})
	}
	return c.JSON(JSONResponse{Code: successCode, Data: vs[0]})
}

func bindRestAPIsHandler(g iris.Party, pathName string, handler iris.Handler) {
	for _, v := range supportMethods {
		if strings.HasPrefix(strings.ToUpper(pathName), v) {
			g.Handle(v, "/"+smallCamelCase(pathName[len(v):]), handler)
			return
		}
	}
	g.Post("/"+smallCamelCase(pathName), handler)
}

func parseRestFuncResult(result []reflect.Value, c iris.Context) error {
	if len(result) == 0 {
		return RestJSONSuccess(c)
	}
	last := result[len(result)-1]
	if last.Type().Implements(errorType) {
		if !last.IsZero() {
			errVal := last.Interface()
			if rpcErr := *((*j2rpc.ItfJ2rpcError)(nil)); errors.As(errVal.(error), &rpcErr) {
				return c.JSON(JSONResponse{
					Code: rpcErr.ErrorCode(),
					Msg:  rpcErr.Error(),
					Data: rpcErr.ErrorData(),
				})
			}
			return c.JSON(JSONResponse{Code: 500, Msg: errVal.(error).Error()})
		}
		result = result[:len(result)-1]
	}
	if len(result) == 0 {
		return RestJSONSuccess(c)
	}
	if result[0].IsZero() {
		return RestJSONSuccess(c)
	}
	return RestJSONSuccess(c, result[0].Interface())
}

func reflectNewByType(t reflect.Type, bind func(v interface{})) reflect.Value {
	value := func() reflect.Value {
		if t.Kind() == reflect.Ptr {
			return reflect.New(t.Elem())
		}
		return reflect.New(t)
	}()
	bind(value.Interface())
	return func() reflect.Value {
		if t.Kind() == reflect.Ptr {
			return value
		}
		return value.Elem()
	}()
}

func smallCamelCase(str string) string {
	if len(str) == 0 {
		return str
	}
	if len(str) == 1 {
		return strings.ToLower(str)
	}
	return strings.ToLower(str[:1]) + str[1:]
}
