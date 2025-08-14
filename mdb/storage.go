package mdb

import (
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/coocood/freecache"
	"github.com/karlseguin/ccache/v3"

	"github.com/glibtools/libs/util"
)

type CCStore struct {
	c    *ccache.Cache[[]byte]
	once sync.Once
}

func (c *CCStore) ClearAll() {
	c.c.Clear()
}

func (c *CCStore) Del(key string) {
	c.c.Delete(key)
}

func (c *CCStore) DropPrefix(prefix ...string) {
	for _, s := range prefix {
		c.c.DeletePrefix(s)
	}
}

func (c *CCStore) Get(key string) (data []byte, ok bool) {
	item := c.c.Get(key)
	if item == nil {
		return
	}
	if item.Expired() {
		c.Del(key)
		return
	}
	data = item.Value()
	ok = true
	return
}

// GetCCache ...
func (c *CCStore) GetCCache() *ccache.Cache[[]byte] { return c.lazyInitialize().c }

func (c *CCStore) Set(key string, data []byte, ttl ...int64) {
	var t = time.Hour
	if len(ttl) > 0 && ttl[0] > 0 {
		t = time.Duration(ttl[0]) * time.Second
	}
	c.c.Set(key, data, t)
}

// lazyInitialize ...
func (c *CCStore) lazyInitialize() *CCStore {
	c.once.Do(func() {
		if c.c == nil {
			c.c = ccache.New(ccache.Configure[[]byte]().
				MaxSize(10 * 10000).
				ItemsToPrune(1000))
		}
	})
	return c
}

type FreeStore struct {
	c    *freecache.Cache
	once sync.Once
}

func (f *FreeStore) ClearAll() { f.c.Clear() }

func (f *FreeStore) Del(key string) { f.c.Del([]byte(key)) }

func (f *FreeStore) DropPrefix(prefix ...string) {
	it := f.c.NewIterator()
	for {
		entry := it.Next()
		if entry == nil {
			break
		}
		k := string(entry.Key)
		for _, p := range prefix {
			if strings.HasPrefix(k, p) {
				f.Del(k)
			}
		}
	}
}

func (f *FreeStore) Get(key string) (data []byte, ok bool) {
	d, err := f.c.Get([]byte(key))
	return d, err == nil
}

func (f *FreeStore) GetFC() *freecache.Cache { return f.lazyInitialize().c }

func (f *FreeStore) Set(key string, data []byte, ttl ...int64) {
	exp := 3600
	if len(ttl) > 0 {
		exp = int(ttl[0])
	}
	_ = f.c.Set([]byte(key), data, exp)
}

// lazyInitialize ...
func (f *FreeStore) lazyInitialize() *FreeStore {
	f.once.Do(func() {
		if f.c == nil {
			// 1024M
			f.c = freecache.NewCache(1024 * util.MB)
			debug.SetGCPercent(20)
		}
	})
	return f
}

func GetCCacheStore() *CCStore { return util.LoadSingle(NewCCacheStore) }

func GetFreeCacheStore() *FreeStore { return util.LoadSingle(NewFreeCacheStore) }

func NewCCacheStore() *CCStore { return new(CCStore).lazyInitialize() }

func NewFreeCacheStore() *FreeStore { return new(FreeStore).lazyInitialize() }

func NewFreeCacheStoreWithSize(size int) *FreeStore { return &FreeStore{c: freecache.NewCache(size)} }
