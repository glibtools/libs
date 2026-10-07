package j2rpc

import (
	"errors"
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

func TestOptionalContextArgumentValidator(t *testing.T) {
	var order []string
	s := NewServer(
		WithValidateArgumentsContext(func(c Context, values []reflect.Value) error {
			order = append(order, "context")
			if c == nil || len(values) != 1 {
				t.Error("context validator input")
			}
			if values[0].Int() == 2 {
				return NewError(423, "held")
			}
			return nil
		}),
		WithValidateArguments(func([]reflect.Value) error { order = append(order, "plain"); return nil }),
	)
	s.Use("fixture", func(c Context) { order = append(order, "middleware"); c.Next() })
	s.RegisterFunc("fixture", func(int) string { order = append(order, "call"); return "ok" })
	for arg, want := range map[string]string{"1": "middleware,context,plain,call", "2": "middleware,context"} {
		order = nil
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader(`{"id":1,"method":"fixture","params":[`+arg+`]}`)))
		if strings.Join(order, ",") != want {
			t.Fatalf("validator order %q", order)
		}
		if arg == "2" && !strings.Contains(w.Body.String(), "423") {
			t.Fatal("context validator error not returned")
		}
	}
}

func TestOptionalResponseHook(t *testing.T) {
	const marker = "synthetic-private-marker"
	serve := func(hook func(Context, []byte) ([]byte, error), body string) (int, string) {
		opts := []Option{WithCallerBeforeWrite(func(b []byte) ([]byte, error) { return append([]byte("safe:"), b...), nil })}
		if hook != nil {
			opts = append(opts, WithResponseHook(hook))
		}
		s := NewServer(opts...)
		s.RegisterFunc("fixture", func(int) string { return "ok" })
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader(body)))
		return w.Code, w.Body.String()
	}
	valid := `{"id":1,"method":"fixture","params":[1]}`
	bad := `{"id":1,"method":"fixture","params":["` + marker + `"` // truncated JSON

	// Without a hook the historical behavior is unchanged.
	if code, out := serve(nil, valid); code != 200 || !strings.HasPrefix(out, "safe:") {
		t.Fatalf("baseline response changed: %d", code)
	}
	if code, out := serve(nil, `{"id":1}`); code != 400 || out != "missing method" {
		t.Fatalf("baseline early error changed: %d %q", code, out)
	}

	var seen string
	hook := func(c Context, b []byte) ([]byte, error) {
		if c == nil || c.Request() == nil {
			t.Error("hook without request context")
		}
		seen = string(b)
		return []byte("sealed"), nil
	}
	if code, out := serve(hook, valid); code != 200 || out != "sealed" || !strings.HasPrefix(seen, "safe:") {
		t.Fatalf("hook not applied after CallerBeforeWrite: %d %q", code, out)
	}
	for _, body := range []string{bad, `{"id":1}`, ""} {
		code, out := serve(hook, body)
		if code != 400 || strings.Contains(out, marker) || out != "Bad Request" {
			t.Fatalf("early error leaked detail: %d %q", code, out)
		}
	}
	failing := func(Context, []byte) ([]byte, error) { return nil, errors.New(marker) }
	if code, out := serve(failing, valid); code != 500 || strings.Contains(out, marker) {
		t.Fatalf("hook failure leaked detail: %d %q", code, out)
	}
}
