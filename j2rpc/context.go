package j2rpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	BodyContextKey = "___j2rpc.body"

	TimeBeginContextKey = "___j2rpc.timeBegin"
)

type rpcContext struct {
	context.Context
	sync.RWMutex
	handlers []Handler
	hid      uint32
	writer   http.ResponseWriter
	req      *http.Request
	store    map[string]interface{}
	msg      *RPCMessage
	abort    bool
	wrote    bool
	server   Server
}

// Abort ...
func (r *rpcContext) Abort() {
	r.Lock()
	r.abort = true
	r.Unlock()
}

// AddHandler add handler
func (r *rpcContext) AddHandler(handlers ...Handler) {
	if len(handlers) == 0 {
		return
	}
	r.Lock()
	if r.handlers == nil {
		r.handlers = make([]Handler, 0)
	}
	r.handlers = append(r.handlers, handlers...)
	r.Unlock()
}

// GetContext ...
func (r *rpcContext) GetContext() context.Context { return r.Context }

func (r *rpcContext) GetValue(key string) (interface{}, bool) {
	r.RLock()
	defer r.RUnlock()
	v, ok := r.store[key]
	return v, ok
}

func (r *rpcContext) IsAbort() bool {
	r.RLock()
	defer r.RUnlock()
	return r.abort
}

func (r *rpcContext) Msg() *RPCMessage {
	r.RLock()
	msg := r.msg
	r.RUnlock()
	if msg == nil {
		return &RPCMessage{ID: RawMessage{'0'}}
	}
	return &RPCMessage{
		ID:      msg.ID,
		Version: msg.Version,
		Method:  msg.Method,
		Params:  msg.Params,
		Result:  msg.Result,
		Error:   msg.Error,
	}
}

func (r *rpcContext) Next() {
	if r.IsAbort() {
		return
	}
	r.RLock()
	index, handles := atomic.LoadUint32(&r.hid), r.handlers
	r.RUnlock()
	if int(index) >= len(handles) {
		return
	}
	//{
	//	h := runtime.FuncForPC(reflect.ValueOf(handles[index]).Pointer())
	//	file, line := h.FileLine(h.Entry())
	//	_, _ = file, line
	//	log.Printf("rpcContext.Next() index:%d, %s:%d, %s\n", index, file, line, h.Name())
	//}
	atomic.AddUint32(&r.hid, 1)
	handles[index](r)
}

// ReadBody read body
func (r *rpcContext) ReadBody() (err error) {
	r.RLock()
	_msg := r.msg
	r.RUnlock()
	if _msg != nil {
		return
	}
	status, err := getValidateRequestStatus(r.req)
	if err != nil {
		r.StopWriteStringStatus(status, err.Error())
		return
	}
	if r.req.Body == nil {
		r.StopWriteStringStatus(http.StatusBadRequest, "missing request body")
		return
	}
	defer func() { _ = r.req.Body.Close() }()
	body, err := io.ReadAll(r.req.Body)
	if err != nil {
		r.StopWriteStringStatus(http.StatusBadRequest, err.Error())
		return
	}
	//if we need reuse body, set body to req.Body
	//r.req.Body = io.NopCloser(io.NewSectionReader(bytes.NewReader(body), 0, int64(len(body))))
	if callerAfterReadBody := r.Server().Option().CallerAfterReadBody; callerAfterReadBody != nil {
		if body, err = callerAfterReadBody(bytes.TrimSpace(body)); err != nil {
			r.StopWriteStringStatus(http.StatusBadRequest, err.Error())
			return
		}
	}
	r.SetValue(BodyContextKey, body)
	msg := &RPCMessage{}
	if err = JSONDecode(body, msg); err != nil {
		r.StopWriteStringStatus(http.StatusBadRequest, err.Error())
		return
	}
	if !msg.hasValidID() {
		r.StopWriteStringStatus(http.StatusBadRequest, "invalid request id")
		return
	}
	msg.Method = strings.TrimSpace(msg.Method)
	if msg.Method == "" {
		r.StopWriteStringStatus(http.StatusBadRequest, "missing method")
		return
	}
	msg.FormatMethod()
	r.SetMsg(msg)
	return
}

func (r *rpcContext) Request() *http.Request { return r.req }

func (r *rpcContext) Server() Server { return r.server }

func (r *rpcContext) SetMsg(msg *RPCMessage) {
	r.Lock()
	r.msg = &RPCMessage{
		ID:      msg.ID,
		Version: msg.Version,
		Method:  msg.Method,
		Params:  msg.Params,
		Result:  msg.Result,
		Error:   msg.Error,
	}
	r.Unlock()
}

func (r *rpcContext) SetValue(key string, val interface{}) {
	r.Lock()
	if r.store == nil {
		r.store = make(map[string]interface{})
	}
	r.store[key] = val
	r.Unlock()
}

func (r *rpcContext) SetWrote(wrote bool) {
	r.Lock()
	r.wrote = wrote
	r.Unlock()
}

// StopWriteStringStatus stop and write string and set status
func (r *rpcContext) StopWriteStringStatus(status int, str string) {
	if r.Wrote() {
		return
	}
	r.Abort()
	r.Writer().WriteHeader(status)
	_, _ = r.Writer().Write([]byte(str))
	r.SetWrote(true)
}

func (r *rpcContext) Writer() http.ResponseWriter { return r.writer }

// WriteResponse write response
func (r *rpcContext) WriteResponse(args ...interface{}) {
	if r.Wrote() {
		return
	}
	msg := r.Msg()
	for _, arg := range args {
		switch _arg := arg.(type) {
		case ItfJ2rpcError:
			msg.Error = NewError(ErrorCode(_arg.ErrorCode()), _arg.Error(), _arg.ErrorData())
		case error:
			msg.Error = NewError(ErrInternal, _arg.Error())
		case []byte:
			msg.Result = _arg
		case RawMessage:
			msg.Result = _arg
		}
	}
	if msg.Error == nil && msg.Result == nil {
		ret, _ := JSONEncode("Success")
		msg.Result = ret
	}
	if msg.Error != nil {
		msg.Result = nil
	}
	r.SetMsg(msg)
	msg.Method = ""
	msg.Params = nil
	data, err := JSONEncode(msg)
	if err != nil {
		r.StopWriteStringStatus(http.StatusInternalServerError, err.Error())
		return
	}
	if callerBeforeWrite := r.Server().Option().CallerBeforeWrite; callerBeforeWrite != nil {
		if data, err = callerBeforeWrite(bytes.TrimSpace(data)); err != nil {
			r.StopWriteStringStatus(http.StatusInternalServerError, err.Error())
			return
		}
	}
	r.Writer().Header().Set("X-Content-Type-Options", "nosniff")
	r.Writer().Header().Set("X-Content-Length", strconv.Itoa(len(data)))
	r.Writer().Header().Set("Content-Type", "application/json; charset=utf-8")
	r.Writer().WriteHeader(http.StatusOK)
	_, _ = r.Writer().Write(data)
	r.SetWrote(true)
}

func (r *rpcContext) Wrote() bool {
	r.RLock()
	defer r.RUnlock()
	return r.wrote
}

func NewContext(ctx context.Context, writer http.ResponseWriter, req *http.Request) Context {
	return newRpcContext(ctx, writer, req)
}

func newRpcContext(ctx context.Context, writer http.ResponseWriter, req *http.Request) *rpcContext {
	return &rpcContext{
		Context: ctx,
		writer:  writer,
		req:     req,
		store:   make(map[string]interface{}),
	}
}
