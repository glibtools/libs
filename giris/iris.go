package giris

import (
	stdContext "context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/kataras/iris/v12"
	"github.com/kataras/iris/v12/core/host"
	"github.com/kataras/iris/v12/middleware/cors"
	"github.com/kataras/iris/v12/middleware/logger"
	"github.com/kataras/iris/v12/middleware/rate"
	"github.com/kataras/iris/v12/middleware/recover"

	"github.com/glibtools/libs/j2rpc"
	"github.com/glibtools/libs/jwt"
	"github.com/glibtools/libs/util"
)

var App = NewApp()

type AppStart struct {
	IrisApp   *iris.Application
	WebLogger *midLogger
	Captcha   *Captcha
	RPC       j2rpc.Server

	rootParty iris.Party
	stop      chan struct{}
}

func (a *AppStart) Constructor() {
	a.WebLogger.Constructor()
	a.defaultMiddlewares(a.IrisApp)
	a.RouteApp()
}

func (a *AppStart) RootParty() iris.Party { return a.rootParty }

// RouteApp ......
func (a *AppStart) RouteApp() iris.Party {
	p := a.IrisApp.Party("/")
	a.rootParty = p
	p.Use(
		ConcurrentLimit(2000),
		func(c iris.Context) {
			func() {
				if jt := c.Values().GetString(jwt.TokenKey); jt != "" {
					rate.SetIdentifier(c, fmt.Sprintf("%s:%s", c.Path(), jt))
					return
				}
				rate.SetIdentifier(c, fmt.Sprintf("%s:%s", c.Path(), c.RemoteAddr()))
			}()
			c.Next()
		},
		RateLimiter(),
		func(c iris.Context) {
			c.Header("X-Server", "")
			c.Header("X-Server", "lucky")
			c.Next()
		},
		//iris.Compression,
	)
	p.Get("/ping", func(c iris.Context) { _, _ = c.WriteString("pong") })
	p.Get("/captcha", a.Captcha.Captcha())
	if a.RPC != nil {
		p.Post("/rpc", RPCServer2IrisHandler(a.RPC))
	}
	return p
}

// Shutdown ...
func (a *AppStart) Shutdown() {
	close(a.stop)
}

// StartApp ......
func (a *AppStart) StartApp(opt StartOption) {
	if len(opt.PPROF) > 0 {
		//go get -u github.com/google/pprof
		//go install github.com/google/pprof@latest
		//需要安装 graphviz
		//pprof -http=:8080 http://127.0.0.1:1011/debug/pprof/profile\?seconds\=10
		pprofServer := &http.Server{
			Addr: fmt.Sprintf(":%s", opt.PPROF),
		}
		go func() { _ = pprofServer.ListenAndServe() }()
		iris.RegisterOnInterrupt(func() {
			log.Println("[pprof] Interrupt...")
			_ = pprofServer.Shutdown(stdContext.Background())
		})
	}
	serverHost := fmt.Sprintf(":%s", opt.PORT)
	go func() {
		a.IrisApp.ConfigureHost(func(su *host.Supervisor) {
			su.RegisterOnShutdown(func() {
				log.Println("[iris app] Shutdown...")
				close(a.stop)
			})
		})

		err := a.IrisApp.Run(
			Addr(serverHost),
			iris.WithoutServerError(iris.ErrServerClosed),
			iris.WithSocketSharding,
			iris.WithKeepAlive(time.Second*60),
			iris.WithTimeout(time.Second*10),
			iris.WithOptimizations,
			iris.WithRemoteAddrHeader(
				"CF-Connecting-IP",
				"True-Client-Ip",
				"X-Appengine-Remote-Addr",
				"X-Forwarded-For",
				"X-Real-Ip",
			),
			iris.WithConfiguration(iris.Configuration{
				RemoteAddrHeadersForce: true,
			}),
		)
		if err != nil {
			log.Printf("ServerStart start error: %s\n", err.Error())
			return
		}
	}()
}

// Wait ...
func (a *AppStart) Wait() { <-a.stop }

func (a *AppStart) defaultMiddlewares(p iris.Party) {
	p.Logger().SetTimeFormat("2006-01-02 15:04:05.000")
	p.Logger().SetOutput(a.WebLogger.WebLogger().Writer())
	p.Logger().SetLevel("info")
	p.UseRouter(
		recover.New(),
		logger.New(),
		cors.New().ExposeHeaders("X-Server", "Authorization", "X-Authorization", "Request-Id", "X-Request-Id").
			AllowHeaders(
				"Authorization", "X-Authorization", "Request-Id", "X-Request-Id", "X-Server",
				"token", "Accept", "Accept-Language", "Content-Language", "Content-Type",
			).
			Handler(),
	)
}

type StartOption struct {
	PORT  string
	PPROF string
}

func Addr(addr string, hostConfigs ...host.Configurator) iris.Runner {
	return func(app *iris.Application) error {
		return app.NewHost(AddrServer(addr)).
			Configure(hostConfigs...).
			ListenAndServe()
	}
}

func AddrServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		ReadTimeout:       time.Second * 10,
		ReadHeaderTimeout: time.Second * 5,
		WriteTimeout:      time.Second * 10,
		IdleTimeout:       time.Second * 60,
	}
}

func Logger() util.ItfLogger { return App.WebLogger.WebLogger() }

func NewApp() *AppStart {
	return &AppStart{
		IrisApp:   iris.New(),
		WebLogger: &midLogger{},
		Captcha:   &Captcha{},
		stop:      make(chan struct{}, 1),
	}
}
