package jwt

import (
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/glibtools/libs/mdb"
	"github.com/glibtools/libs/util"
)

type ItfTokenStore interface {
	SetToken(key string, t *Token)
	GetToken(key string) (t *Token, has bool)
	DelToken(key string)
	ClearExpiredToken()
}

type RedisTokenStore struct {
	store *mdb.RedisStore
	mem   *mdb.FreeStore
}

func (r *RedisTokenStore) ClearExpiredToken() {
	r.store.StoreGC(PrefixJWTKeyFunc())
	r.mem.ClearAll()
}

func (r *RedisTokenStore) DelToken(key string) {
	r.store.Del(key)
	r.mem.Del(key)
}

func (r *RedisTokenStore) GetToken(key string) (t *Token, has bool) {
	data, ok := r.mem.Get(key)
	if !ok {
		data, ok = r.store.Get(key)
		if !ok {
			return
		}
		r.mem.Set(key, data, -1)
	}
	t = &Token{}
	err := util.Unmarshal(data, t)
	if err != nil {
		return nil, false
	}
	has = true
	return
}

func (r *RedisTokenStore) SetToken(key string, t *Token) {
	data := util.JsMarshal(t)
	r.store.Set(key, data, -1)
	r.mem.Set(key, data, -1)
}

type defaultTokenStore struct {
	mu     sync.RWMutex
	tokens map[string]*Token
}

func (d *defaultTokenStore) ClearExpiredToken() {
	d.mu.Lock()
	for k, v := range d.tokens {
		if v.expired() {
			delete(d.tokens, k)
		}
	}
	d.mu.Unlock()
}

func (d *defaultTokenStore) DelToken(key string) {
	d.mu.Lock()
	delete(d.tokens, key)
	d.mu.Unlock()
}

func (d *defaultTokenStore) GetToken(key string) (t *Token, has bool) {
	d.mu.RLock()
	t, has = d.tokens[key]
	d.mu.RUnlock()
	return
}

func (d *defaultTokenStore) SetToken(key string, t *Token) {
	d.mu.Lock()
	d.tokens[key] = t
	d.mu.Unlock()
}

func NewRedisTokenStore(c *redis.Client) *RedisTokenStore {
	return &RedisTokenStore{
		store: mdb.NewRedisStore(c, &mdb.RedisStoreOption{Separator: mdb.SeparatorColon}),
		mem:   mdb.NewFreeCacheStore(),
	}
}

func newDefaultTokenStore() *defaultTokenStore {
	return &defaultTokenStore{tokens: make(map[string]*Token)}
}
