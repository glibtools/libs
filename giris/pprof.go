package giris

import (
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/kataras/iris/v12"
)

// AllocsHandler will pass the call from /debug/pprof/allocs to pprof
func AllocsHandler() iris.Handler { return HttpHandler2IrisHandler(pprof.Handler("allocs")) }

// BlockHandler will pass the call from /debug/pprof/block to pprof
func BlockHandler() iris.Handler { return HttpHandler2IrisHandler(pprof.Handler("block")) }

// CmdlineHandler will pass the call from /debug/pprof/cmdline to pprof
func CmdlineHandler() iris.Handler { return HttpHandler2IrisHandler(http.HandlerFunc(pprof.Cmdline)) }

// GoroutineHandler will pass the call from /debug/pprof/goroutine to pprof
func GoroutineHandler() iris.Handler { return HttpHandler2IrisHandler(pprof.Handler("goroutine")) }

// HeapHandler will pass the call from /debug/pprof/heap to pprof
func HeapHandler() iris.Handler { return HttpHandler2IrisHandler(pprof.Handler("heap")) }

// IndexHandler will pass the call from /debug/pprof to pprof
func IndexHandler() iris.Handler { return HttpHandler2IrisHandler(http.HandlerFunc(pprof.Index)) }

// MutexHandler will pass the call from /debug/pprof/mutex to pprof
func MutexHandler() iris.Handler { return HttpHandler2IrisHandler(pprof.Handler("mutex")) }

// PProfWrapGroup adds several routes from package `net/http/pprof` to *gin.RouterGroup object
func PProfWrapGroup(party iris.Party) {
	routers := []struct {
		Method  string
		Path    string
		Handler iris.Handler
	}{
		{"GET", "/debug/pprof/", IndexHandler()},
		{"GET", "/debug/pprof/heap", HeapHandler()},
		{"GET", "/debug/pprof/goroutine", GoroutineHandler()},
		{"GET", "/debug/pprof/allocs", AllocsHandler()},
		{"GET", "/debug/pprof/block", BlockHandler()},
		{"GET", "/debug/pprof/threadcreate", ThreadCreateHandler()},
		{"GET", "/debug/pprof/cmdline", CmdlineHandler()},
		{"GET", "/debug/pprof/profile", ProfileHandler()},
		{"GET", "/debug/pprof/symbol", SymbolHandler()},
		{"POST", "/debug/pprof/symbol", SymbolHandler()},
		{"GET", "/debug/pprof/trace", TraceHandler()},
		{"GET", "/debug/pprof/mutex", MutexHandler()},
	}

	basePath := strings.TrimSuffix(party.GetRelPath(), "/")
	var prefix string

	switch {
	case basePath == "":
		prefix = ""
	case strings.HasSuffix(basePath, "/debug"):
		prefix = "/debug"
	case strings.HasSuffix(basePath, "/debug/pprof"):
		prefix = "/debug/pprof"
	}

	for _, r := range routers {
		party.Handle(r.Method, strings.TrimPrefix(r.Path, prefix), r.Handler)
	}
}

// ProfileHandler will pass the call from /debug/pprof/profile to pprof
func ProfileHandler() iris.Handler { return HttpHandler2IrisHandler(http.HandlerFunc(pprof.Profile)) }

// SymbolHandler will pass the call from /debug/pprof/symbol to pprof
func SymbolHandler() iris.Handler { return HttpHandler2IrisHandler(http.HandlerFunc(pprof.Symbol)) }

// ThreadCreateHandler will pass the call from /debug/pprof/threadcreate to pprof
func ThreadCreateHandler() iris.Handler {
	return HttpHandler2IrisHandler(pprof.Handler("threadcreate"))
}

// TraceHandler will pass the call from /debug/pprof/trace to pprof
func TraceHandler() iris.Handler { return HttpHandler2IrisHandler(http.HandlerFunc(pprof.Trace)) }
