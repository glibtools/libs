package giris

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/kataras/iris/v12"
	"github.com/kataras/iris/v12/context"
	"github.com/kataras/iris/v12/middleware/accesslog"
	"github.com/kataras/iris/v12/middleware/rate"

	"github.com/glibtools/libs/j2rpc"
	"github.com/glibtools/libs/jwt"
	"github.com/glibtools/libs/util"
)

type midLogger struct {
	logger    *accesslog.AccessLog
	webLogger util.ItfLogger
}

func (m *midLogger) Constructor() { m.webLogger = util.ZapLogger("web", "debug") }

// Logger ......
func (m *midLogger) Logger() *accesslog.AccessLog {
	ac := accesslog.New(m.WebLogger().Writer())
	// The default configuration:
	ac.Delim = '|'
	ac.TimeFormat = "2006-01-02 15:04:05.000"
	ac.Async = false
	ac.IP = true
	ac.BytesReceivedBody = true
	ac.BytesSentBody = true
	ac.BytesReceived = true
	ac.BytesSent = true
	ac.BodyMinify = true
	ac.RequestBody = true
	ac.ResponseBody = false
	ac.KeepMultiLineError = true
	ac.PanicLog = accesslog.LogHandler

	// Default line format if formatter is missing:
	// Time|Latency|Code|Method|Path|IP|Path Params Query Fields|Bytes Received|Bytes Sent|Request|Response|
	//
	// Set Custom Formatter:
	//ac.SetFormatter(&accesslog.JSON{Indent: "", HumanTime: true})
	// ac.SetFormatter(&accesslog.CSV{})
	const defaultTmplText = "{{.Now.Format .TimeFormat}} | {{.Latency}}" +
		" | {{.Code}} | {{.Method}} | {{.Path}} | {{.IP}}" +
		" | {{.RequestValuesLine}} | {{.BytesReceivedLine}} | {{.BytesSentLine}}" +
		" | {{.Request}} | {{.Response}} |\n"
	ac.SetFormatter(&accesslog.Template{Text: defaultTmplText})

	m.logger = ac
	return m.logger
}

func (m *midLogger) WebLogger() util.ItfLogger {
	return m.webLogger
}

func ConcurrentLimit(n int) iris.Handler {
	ch := make(chan struct{}, n)
	return func(c iris.Context) {
		select {
		case ch <- struct{}{}:
			c.Next()
			<-ch
		default:
			_ = c.StopWithJSON(http.StatusTooManyRequests, &j2rpc.RPCMessage{
				ID:    json.RawMessage("1"),
				Error: j2rpc.NewError(http.StatusTooManyRequests, "系统繁忙,请稍后再试"),
			})
		}
	}
}

func RateLimiter(limit float64, burst int, options ...rate.Option) context.Handler {
	ops := []rate.Option{
		rate.PurgeEvery(5*time.Minute, 15*time.Minute),
		rate.ExceedHandler(func(c iris.Context) {
			_ = c.StopWithJSON(http.StatusTooManyRequests, &j2rpc.RPCMessage{
				ID:    json.RawMessage("1"),
				Error: j2rpc.NewError(http.StatusTooManyRequests, "请求过于频繁"),
			})
		}),
	}
	ops = append(ops, options...)
	rateHandler := rate.Limit(limit, burst, ops...)
	return func(c *context.Context) {
		identifier := fmt.Sprintf("%s:%s", c.Path(), c.RemoteAddr())
		if jt := c.Values().GetString(jwt.TokenKey); jt != "" {
			identifier = fmt.Sprintf("%s:%s", c.Path(), jt)
		}
		c.Header("X-RateLimit-Identifier", identifier)
		rate.SetIdentifier(c, identifier)
		rateHandler(c)
	}
}
