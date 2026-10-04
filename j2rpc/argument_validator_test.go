package j2rpc

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestOptionalArgumentValidator(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		called := false
		options := []Option{}
		if enabled {
			options = append(options, WithValidateArguments(func([]reflect.Value) error { return NewError(400, "rejected") }))
		}
		s := NewServer(options...)
		s.RegisterFunc("fixture", func(int) string { called = true; return "ok" })
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader(`{"id":1,"method":"fixture","params":[1]}`)))
		if called == enabled {
			t.Fatal("optional argument validation behavior")
		}
	}
}
