package mdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/glibtools/libs/util"
)

var SeparatorColon = ":"

type RedisStore struct {
	c   *redis.Client
	opt *RedisStoreOption
}

func (r *RedisStore) ClearAll() { r.c.FlushDB(context.Background()) }

func (r *RedisStore) Del(key string) {
	k, f, y := r.GetKeyFieldOrNot(key)
	if !y {
		return
	}
	r.c.HDel(context.Background(), k, f)
}

func (r *RedisStore) DropPrefix(prefix ...string) {
	if len(prefix) == 0 {
		return
	}
	for _, s := range prefix {
		keys := r.ScanKeys(s)
		if len(keys) == 0 {
			continue
		}
		r.c.Del(context.Background(), keys...)
	}
}

func (r *RedisStore) Get(key string) (data []byte, ok bool) {
	k, f, y := r.GetKeyFieldOrNot(key)
	if !y {
		return
	}

	v, err := r.c.HGet(context.Background(), k, f).Result()
	if err != nil {
		return
	}
	var rv RedisValue
	if err = util.Unmarshal([]byte(v), &rv); err != nil {
		return
	}

	if rv.isExpired() {
		r.Del(key)
		return
	}

	data = rv.Val
	ok = true
	return
}

func (r *RedisStore) GetKeyFieldOrNot(key string) (k, f string, y bool) {
	sep := r.opt.Separator
	s1 := strings.Split(key, sep)
	if len(s1) < 2 {
		return
	}
	k = strings.Join(s1[:len(s1)-1], sep)
	f = s1[len(s1)-1]
	y = true
	return
}

func (r *RedisStore) ScanKeys(matchParam string) []string {
	m := make([]string, 0)
	ctx := context.Background()
	iter := r.c.Scan(ctx, 0, fmt.Sprintf("*%s*", matchParam), 1000).Iterator()
	for iter.Next(ctx) {
		m = append(m, iter.Val())
	}
	return m
}

func (r *RedisStore) Set(key string, data []byte, ttl ...int64) {
	k, f, y := r.GetKeyFieldOrNot(key)
	if !y {
		return
	}

	var expireAt int64 = 0
	if len(ttl) > 0 && ttl[0] > 0 {
		expireAt = time.Now().Unix() + ttl[0]
	}
	rv := RedisValue{
		Val:      data,
		ExpireAt: expireAt,
	}
	v, err := util.Marshal(rv)
	if err != nil {
		return
	}
	r.c.HSet(context.Background(), k, f, string(v))
}

func (r *RedisStore) StoreGC(prefix string) {
	keys := r.ScanKeys(prefix)
	if len(keys) == 0 {
		return
	}
	r.gc(keys)
}

// background run gc
func (r *RedisStore) gc(keys []string) {
	for _, key := range keys {
		deleter := HDeleter(r.c, key)
		HScanCallback(r.c, key, "*", func(k, v string) {
			var rv RedisValue
			err := util.Unmarshal([]byte(v), &rv)
			if err != nil || rv.isExpired() {
				deleter(k)
			}
		})
		deleter()
	}
}

type RedisStoreOption struct {
	Separator string
}

type RedisValue struct {
	Val      util.RawMessage `json:"val"`
	ExpireAt int64           `json:"expire_at"`
}

// isExpired ...
func (r *RedisValue) isExpired() bool {
	return r.ExpireAt > 0 && r.ExpireAt < time.Now().Unix()
}

func GetRedisStore(c *redis.Client, opts ...*RedisStoreOption) *RedisStore {
	var opt *RedisStoreOption
	if len(opts) > 0 {
		opt = opts[0]
	}
	key := fmt.Sprintf("%s:%d", c.Options().Addr, c.Options().DB)
	if opt != nil {
		key = fmt.Sprintf("%s-%s", key, opt.Separator)
	}
	return util.LoadSingleInstance(key, func() *RedisStore {
		return NewRedisStore(c, opts...)
	})
}

func HDeleter(c *redis.Client, key string, args ...interface{}) func(fields ...string) {
	ctx := context.Background()
	var l = 100
	for _, arg := range args {
		switch _arg := arg.(type) {
		case int:
			l = _arg
		}
	}
	sl := make([]string, 0, l)
	return func(fields ...string) {
		sl = append(sl, fields...)
		if len(sl) <= 0 {
			return
		}
		if len(sl) < l && len(fields) > 0 {
			return
		}
		c.HDel(ctx, key, sl...)
		sl = sl[:0]
	}
}

func HScanCallback(c *redis.Client, key, match string, fn func(k, v string)) {
	ctx := context.Background()
	iter := c.HScan(ctx, key, 0, match, 1000).Iterator()
	s := make([]string, 0)
	for iter.Next(ctx) {
		s = append(s, iter.Val())
		n := len(s)
		if n > 1 && n%2 == 0 {
			fn(s[n-2], s[n-1])
		}
	}
}

func NewRedisStore(c *redis.Client, opts ...*RedisStoreOption) *RedisStore {
	var opt *RedisStoreOption
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt == nil {
		opt = &RedisStoreOption{Separator: ":"}
	}
	return &RedisStore{c: c, opt: opt}
}
