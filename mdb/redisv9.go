package mdb

import (
	"context"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
)

var Rdb = new(RedisV9)

type RedisOptions struct {
	Addr     string
	Username string
	Password string
	DB       int
}

// Copy ...
func (r *RedisOptions) Copy() *RedisOptions {
	return &RedisOptions{
		Addr:     r.Addr,
		Username: r.Username,
		Password: r.Password,
		DB:       r.DB,
	}
}

type RedisV9 struct {
	m   map[int]*redis.Client
	mu  sync.RWMutex
	opt *RedisOptions
}

// Close ...
func (r *RedisV9) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, c := range r.m {
		_ = c.Close()
		delete(r.m, i)
	}
}

func (r *RedisV9) GetClient(dbs ...int) *redis.Client {
	if r.opt == nil {
		log.Fatalf("RedisV9 not initialize")
		return nil
	}
	db := 0
	if len(dbs) > 0 {
		db = dbs[0]
	}
	r.mu.RLock()
	c := r.m[db]
	r.mu.RUnlock()
	if c != nil {
		return c
	}
	r.mu.Lock()
	opt := r.opt.Copy()
	opt.DB = db
	c = getRedisClient(opt)
	r.m[db] = c
	r.mu.Unlock()
	return c
}

func (r *RedisV9) Initialize(opt *RedisOptions) *RedisV9 {
	r.opt = opt
	r.m = make(map[int]*redis.Client)
	r.GetClient(r.opt.DB)
	return r
}

func (r *RedisV9) Opt() *RedisOptions {
	return r.opt
}

func getRedisClient(opt *RedisOptions) *redis.Client {
	redisOpt := &redis.Options{
		Network:               "",
		Addr:                  opt.Addr,
		ClientName:            "",
		Dialer:                nil,
		OnConnect:             nil,
		Username:              opt.Username,
		Password:              opt.Password,
		CredentialsProvider:   nil,
		DB:                    opt.DB,
		MaxRetries:            0,
		MinRetryBackoff:       0,
		MaxRetryBackoff:       0,
		DialTimeout:           0,
		ReadTimeout:           0,
		WriteTimeout:          0,
		ContextTimeoutEnabled: false,
		PoolFIFO:              true,
		PoolSize:              0,
		PoolTimeout:           0,
		MinIdleConns:          2,
		MaxIdleConns:          100,
		ConnMaxIdleTime:       0,
		ConnMaxLifetime:       0,
		TLSConfig:             nil,
		Limiter:               nil,
	}
	c := redis.NewClient(redisOpt)
	e := c.Ping(context.Background()).Err()
	if e != nil {
		log.Fatalf("RedisV9 initialize failed: %s", e.Error())
	}
	return c
}
