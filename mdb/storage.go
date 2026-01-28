package mdb

import (
	"bytes"
	"runtime"
	"runtime/debug"
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
			c.c = ccache.New(ccache.Configure[[]byte]().MaxSize(10 * 10000))
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
	bps := make([][]byte, 0)
	uniq := make(map[string]struct{}, len(prefix))
	for _, p := range prefix {
		uniq[p] = struct{}{}
	}
	for p := range uniq {
		if len(p) == 0 {
			continue
		}
		bps = append(bps, []byte(p))
	}

	it := f.c.NewIterator()
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan []byte, 4096)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for k := range jobs {
				f.c.Del(k)
			}
		}()
	}

	for {
		e := it.Next()
		if e == nil {
			break
		}
		key := e.Key
		for _, bp := range bps {
			if bytes.HasPrefix(key, bp) {
				k := make([]byte, len(key)) // 迭代器复用 buffer，必须拷贝
				copy(k, key)
				jobs <- k
				break
			}
		}
	}
	close(jobs)
	wg.Wait()
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

// lazyInitialize 初始化去重表和 TTL（可按需调整）
func (f *FreeStore) lazyInitialize() *FreeStore {
	f.once.Do(func() {
		if f.c == nil {
			f.c = freecache.NewCache(1024 * 1024 * 1024) // 1GiB 示例
		}
		debug.SetGCPercent(20)
	})
	return f
}

type ItfStorageCache interface {
	Get(key string) ([]byte, bool)
	Set(key string, val []byte, ttl ...int64) // ttl in seconds
	Del(key string)
	DropPrefix(prefix ...string)
}

func GetCCacheStore() *CCStore { return util.LoadSingle(NewCCacheStore) }

func GetFreeCacheStore() *FreeStore { return util.LoadSingle(NewFreeCacheStore) }

func NewCCacheStore() *CCStore { return new(CCStore).lazyInitialize() }

func NewFreeCacheStore() *FreeStore { return new(FreeStore).lazyInitialize() }

func NewFreeCacheStoreWithSize(size int) *FreeStore { return &FreeStore{c: freecache.NewCache(size)} }
