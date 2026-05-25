package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/hibiken/asynq"

	"github.com/glibtools/libs/config"
	"github.com/glibtools/libs/util"
)

var (
	appName string

	DefaultAQServerConfigFunc = func() asynq.Config {
		return asynq.Config{
			Concurrency: runtime.NumCPU(),

			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},

			Logger: util.ZapLogger("asynq"),

			LogLevel: asynq.WarnLevel,

			DelayedTaskCheckInterval: time.Second * 1,
		}
	}
)

type AQHandlerMap map[string]asynq.HandlerFunc

func AQHandle(mux *asynq.ServeMux, pattern string, handler asynq.Handler) {
	mux.Handle(Pattern(pattern), handler)
}

func AQHandleFunc(mux *asynq.ServeMux, pattern string, handler asynq.HandlerFunc) {
	mux.HandleFunc(Pattern(pattern), handler)
}

func GetAppName() string {
	if appName == "" {
		return config.Viper().GetString("app.name")
	}
	return appName
}

func LoadRedisClientOpt() asynq.RedisClientOpt {
	v := config.Viper()
	host := v.GetString("redis.host")
	if host == "" {
		host = v.GetString("app.host")
	}
	return asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%d", host, v.GetInt("redis.port")),
		Password: v.GetString("redis.pwd"),
		DB:       v.GetInt("redis.db"),
	}
}

func NewAQClient(c ...asynq.RedisClientOpt) *asynq.Client {
	opt := LoadRedisClientOpt()
	if len(c) > 0 {
		opt = c[0]
	}
	return asynq.NewClient(opt)
}

func NewAQServer(c ...asynq.Config) *asynq.Server {
	cfg := DefaultAQServerConfigFunc()
	if len(c) > 0 {
		mergeConfigs(&cfg, &(c[0]))
	}
	return asynq.NewServer(LoadRedisClientOpt(), cfg)
}

func NewAQServerMux(hm AQHandlerMap) *asynq.ServeMux {
	mux := asynq.NewServeMux()
	for k, v := range hm {
		mux.HandleFunc(Pattern(k), v)
	}
	return mux
}

func NewAQTask(typ string, data interface{}, opts ...asynq.Option) *asynq.Task {
	var payload []byte
	switch v := data.(type) {
	case []byte:
		payload = v
	case string:
		payload = []byte(v)
	default:
		payload, _ = json.Marshal(v)
	}
	return asynq.NewTask(Pattern(typ), payload, opts...)
}

func Pattern(typ string) string {
	return fmt.Sprintf("%s:%s", GetAppName(), typ)
}

// SetAppName sets the app name.
func SetAppName(name string) {
	appName = name
}

func WrapAQHandler(fn any) asynq.HandlerFunc {
	return func(ctx context.Context, task *asynq.Task) error {
		return util.ExecSingleArgHandler(task.Payload(), fn)
	}
}
