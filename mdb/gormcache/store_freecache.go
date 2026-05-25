package gormcache

import (
	"bytes"
	"sync/atomic"

	"github.com/coocood/freecache"
)

// FreeCacheStore adapts freecache.Cache to Store.
type FreeCacheStore struct {
	cache     *freecache.Cache
	size      int
	setErrors atomic.Uint64
}

// Capacity returns the configured freecache capacity in bytes.
func (s *FreeCacheStore) Capacity() int {
	return s.size
}

// Del removes key from the cache.
func (s *FreeCacheStore) Del(key string) {
	s.cache.Del([]byte(key))
}

// DropPrefix is a compatibility/manual invalidation path. The gormcache plugin
// itself avoids this scan by using epoch-based keys.
func (s *FreeCacheStore) DropPrefix(prefix ...string) {
	prefixes := bytePrefixes(prefix)
	if len(prefixes) == 0 {
		return
	}
	keys := make([][]byte, 0)
	it := s.cache.NewIterator()
	for {
		entry := it.Next()
		if entry == nil {
			break
		}
		if hasAnyBytePrefix(entry.Key, prefixes) {
			keys = append(keys, entry.Key)
		}
	}
	for _, key := range keys {
		s.cache.Del(key)
	}
}

// Evictions returns how many entries the underlying freecache backend evicted.
func (s *FreeCacheStore) Evictions() uint64 {
	_vv := s.cache.EvacuateCount()
	if _vv < 0 {
		_vv = 0
	}
	return uint64(_vv)
}

// Get returns a cached value for key.
func (s *FreeCacheStore) Get(key string) ([]byte, bool) {
	val, err := s.cache.Get([]byte(key))
	return val, err == nil
}

// Set stores val for key. ttlSeconds is the lifetime in seconds; 0 or omitted
// uses freecache's "no expiration" behavior.
func (s *FreeCacheStore) Set(key string, val []byte, ttlSeconds ...int64) {
	var _ttl int64 = defaultTTLSeconds
	if len(ttlSeconds) > 0 {
		_ttl = ttlSeconds[0]
	}
	if err := s.cache.Set([]byte(key), val, int(_ttl)); err != nil {
		s.setErrors.Add(1)
	}
}

// SetErrors returns how many writes the underlying freecache backend rejected.
func (s *FreeCacheStore) SetErrors() uint64 {
	return s.setErrors.Load()
}

// NewFreeCacheStore creates a bounded in-process Store backed by freecache.
// freecache rejects keys larger than 65535 bytes and entries larger than 1/1024 of size.
func NewFreeCacheStore(size int) *FreeCacheStore {
	return &FreeCacheStore{cache: freecache.NewCache(size), size: size}
}

func bytePrefixes(prefix []string) [][]byte {
	items := make([][]byte, 0, len(prefix))
	for _, item := range prefix {
		if item != "" {
			items = append(items, []byte(item))
		}
	}
	return items
}

func hasAnyBytePrefix(key []byte, prefix [][]byte) bool {
	for _, item := range prefix {
		if bytes.HasPrefix(key, item) {
			return true
		}
	}
	return false
}
