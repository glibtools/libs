package j2rpc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
)

const maxRequestContentLength = 1 << 20 * 5

func argsToNameInterface(args ...interface{}) (name string, bean interface{}) {
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			name = v
		default:
			bean = v
		}
	}
	return
}

func getValidateRequestStatus(r *http.Request) (int, error) {
	if r.Method == http.MethodOptions {
		return 0, nil
	}
	if r.Method != http.MethodPost {
		return http.StatusMethodNotAllowed, errors.New("method isn't allowed")
	}
	if r.ContentLength > maxRequestContentLength {
		err := fmt.Errorf("content length too large (%d>%d)", r.ContentLength, maxRequestContentLength)
		return http.StatusRequestEntityTooLarge, err
	}
	return 0, nil
}

func parseArgumentArray(dec *Decoder, types []reflect.Type) ([]reflect.Value, error) {
	args := make([]reflect.Value, 0, len(types))
	for i := 0; dec.More(); i++ {
		if i >= len(types) {
			return args, fmt.Errorf("too many arguments, want at most %d", len(types))
		}
		agv := reflect.New(types[i])
		if err := dec.Decode(agv.Interface()); err != nil {
			return args, fmt.Errorf("invalid argument %d: %s", i, err.Error())
		}
		if agv.IsNil() && types[i].Kind() != reflect.Ptr {
			return args, fmt.Errorf("missing value for required argument %d", i)
		}
		args = append(args, agv.Elem())
	}
	_, err := dec.Token()
	return args, err
}

func parsePositionalArguments(rawArgs RawMessage, types []reflect.Type) ([]reflect.Value, error) {
	dec := NewDecoder(bytes.NewReader(rawArgs))
	var args []reflect.Value
	tok, err := dec.Token()
	switch {
	case err == io.EOF || (err == nil && tok == nil):
	case err != nil:
		return nil, err
	case tok == Delim('['):
		if args, err = parseArgumentArray(dec, types); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("non-array args")
	}
	for i := len(args); i < len(types); i++ {
		args = append(args, ZeroValue(types[i]))
	}
	return args, nil
}
