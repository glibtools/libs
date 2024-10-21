package giris

import (
	"net/http"

	"github.com/kataras/iris/v12"

	"github.com/glibtools/libs/j2rpc"
)

func HttpHandler2IrisHandler(h http.Handler) iris.Handler {
	return func(c iris.Context) {
		h.ServeHTTP(c.ResponseWriter(), c.Request())
	}
}

func RPCServer2IrisHandler(s j2rpc.Server) iris.Handler {
	return func(c iris.Context) {
		s.Handler(j2rpc.NewContext(c, c.ResponseWriter(), c.Request()))
	}
}
