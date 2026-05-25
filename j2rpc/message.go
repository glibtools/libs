package j2rpc

import (
	"strings"
)

const (
	ErrParse          ErrorCode = -32700
	ErrInvalidRequest ErrorCode = -32600
	ErrNoMethod       ErrorCode = -32601
	ErrBadParams      ErrorCode = -32602
	ErrInternal       ErrorCode = -32603
	ErrServer         ErrorCode = -32000

	ErrAuthorization ErrorCode = 401
	ErrForbidden     ErrorCode = 403
)

// Error ... Error codes
type Error struct {
	Code    ErrorCode   `json:"code"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func (e *Error) Error() string { return e.Message }

func (e *Error) ErrorCode() int { return int(e.Code) }

func (e *Error) ErrorData() interface{} { return e.Data }

// ErrorCode ... Error codes
type ErrorCode int

type ItfJ2rpcError interface {
	ErrorCode() int
	Error() string
	ErrorData() interface{}
}

type RPCMessage struct {
	ID      RawMessage `json:"id,omitempty"`
	Version string     `json:"jsonrpc,omitempty"`
	Method  string     `json:"method,omitempty"`
	Params  RawMessage `json:"params,omitempty"`
	Result  RawMessage `json:"result,omitempty"`
	Error   *Error     `json:"error,omitempty"`
}

// FormatMethod ...
func (r *RPCMessage) FormatMethod() {
	if r.Method == "" {
		return
	}
	if !strings.Contains(r.Method, Separator) {
		r.Method = MethodNameProvider(r.Method)
		return
	}
	list := strings.Split(r.Method, Separator)
	for i, v := range list {
		list[i] = MethodNameProvider(v)
	}
	r.Method = strings.Join(list, Separator)
}

func (r *RPCMessage) hasValidID() bool { return len(r.ID) > 0 && r.ID[0] != '{' && r.ID[0] != '[' }

// NewError ...
func NewError(code ErrorCode, Msg string, data ...interface{}) *Error {
	ee := &Error{Code: code, Message: Msg}
	if len(data) > 0 {
		ee.Data = data[0]
	}
	return ee
}
