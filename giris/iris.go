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
	"github.com/kataras/iris/v12/middleware/recover"
	"github.com/spf13/viper"

	"github.com/glibtools/libs/config"
	"github.com/glibtools/libs/j2rpc"
	"github.com/glibtools/libs/util"
)

var App = NewApp()
var DefaultRPCServerOption = &RPCServerOption{}

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

	app := a.IrisApp
	app.Logger().SetTimeFormat("2006-01-02 15:04:05.000")
	app.Logger().SetOutput(a.WebLogger.WebLogger().Writer())
	app.Logger().SetLevel(config.C.V().GetString("app.log_level"))

	a.DefaultMiddlewares(a.IrisApp)
}

func (a *AppStart) DefaultMiddlewares(p iris.Party) {
	p.UseRouter(logger.New(), recover.New(),
		cors.New().ExposeHeaders("X-Server", "Authorization", "X-Authorization", "Request-Id", "X-Request-Id").
			AllowHeaders("Authorization", "X-Authorization", "Request-Id", "X-Request-Id", "X-Server", "Token",
				"Accept", "Accept-Language", "Content-Language", "Content-Type").Handler(),
	)
}

// ResetRPCServer ...
func (a *AppStart) ResetRPCServer(appBus interface{}, args ...interface{}) {
	var callBack J2rpcServerCallBackFunc = nil
	var options []j2rpc.Option = nil
	for _, arg := range args {
		switch argV := arg.(type) {
		case j2rpc.Option:
			options = append(options, argV)
		case J2rpcServerCallBackFunc:
			callBack = argV
		default:
			log.Printf("ResetRPCServer: unknown type %T\n", argV)
		}
	}
	s := j2rpc.NewServer(options...)
	s.Use("", j2rpc.Recover(a.IrisApp.Logger()))
	if callBack != nil {
		callBack(s)
	}
	s.RegisterTypeBus(appBus)
	a.RPC = s
}

func (a *AppStart) RootParty() iris.Party {
	if a.rootParty == nil {
		a.rootParty = a.IrisApp.Party("/")
	}
	return a.rootParty
}

// RouteApp ......
func (a *AppStart) RouteApp(middleware ...iris.Handler) iris.Party {
	p := a.RootParty()
	p.Use(middleware...)
	p.Get("/ping", func(c iris.Context) { _, _ = c.WriteString("pong") })
	p.Get("/captcha", a.Captcha.Captcha())
	if a.RPC != nil {
		p.Post("/rpc", RPCServer2IrisHandler(a.RPC))
	}
	return p
}

func (a *AppStart) SetRootParty(rootParty iris.Party) {
	a.rootParty = rootParty
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

		log.Println("[iris app] Start...")

		err := a.IrisApp.Run(
			Addr(serverHost),
			iris.WithoutServerError(iris.ErrServerClosed),
			iris.WithSocketSharding,
			iris.WithKeepAlive(time.Second*60),
			//iris.WithTimeout(time.Second*10),
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

type J2rpcServerCallBackFunc func(s j2rpc.Server)

type RPCServerOption struct {
	ConcurrentLimit int
	RateLimitValue  float64
	RateLimitBurst  int
	ServerHost      string
	PPROFPort       string

	//ResetRPCArgs type is j2rpc.Option or J2rpcServerCallBackFunc
	ResetRPCArgs []interface{}

	BeforeRouteAppFunc func(p iris.Party)

	AfterRouteAppFunc func(p iris.Party)
}

// AddRPCServerArgsCallBack ...
func (r *RPCServerOption) AddRPCServerArgsCallBack(args ...J2rpcServerCallBackFunc) {
	for _, arg := range args {
		r.ResetRPCArgs = append(r.ResetRPCArgs, arg)
	}
}

// AddRPCServerArgsOption ...
func (r *RPCServerOption) AddRPCServerArgsOption(args ...j2rpc.Option) {
	for _, arg := range args {
		r.ResetRPCArgs = append(r.ResetRPCArgs, arg)
	}
}

func (r *RPCServerOption) Prepare(vp *viper.Viper) {
	if r.ConcurrentLimit == 0 {
		r.ConcurrentLimit = vp.GetInt("server.concurrent_limit")
	}
	if r.RateLimitValue == 0 {
		r.RateLimitValue = vp.GetFloat64("server.rate_limit_value")
	}
	if r.RateLimitBurst == 0 {
		r.RateLimitBurst = vp.GetInt("server.rate_limit_burst")
	}
	if r.ServerHost == "" {
		r.ServerHost = vp.GetString("server.host")
	}
	if r.PPROFPort == "" {
		r.PPROFPort = vp.GetString("server.pprof_port")
	}

	if r.ConcurrentLimit <= 10 {
		r.ConcurrentLimit = 2000
	}
	if r.RateLimitValue <= 0 {
		r.RateLimitValue = 20
	}
	if r.RateLimitBurst <= 0 {
		r.RateLimitBurst = 50
	}
	if r.ServerHost == "" {
		r.ServerHost = "80"
	}
	if r.PPROFPort == "" {
		r.PPROFPort = "81"
	}
}

func (r *RPCServerOption) StartIrisRPC(vp *viper.Viper, appBus interface{}) {
	r.Prepare(vp)
	app := App
	app.ResetRPCServer(appBus, r.ResetRPCArgs...)
	app.Constructor()
	if r.BeforeRouteAppFunc != nil {
		r.BeforeRouteAppFunc(app.RootParty())
	}
	app.RouteApp(
		ConcurrentLimit(r.ConcurrentLimit),
		RateLimiter(r.RateLimitValue, r.RateLimitBurst),
	)
	if r.AfterRouteAppFunc != nil {
		r.AfterRouteAppFunc(app.RootParty())
	}
	app.StartApp(StartOption{PORT: r.ServerHost, PPROF: r.PPROFPort})
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
