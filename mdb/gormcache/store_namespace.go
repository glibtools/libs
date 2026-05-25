package gormcache

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const (
	namespaceMagic   uint32 = 0x67636e73 // "gcns"
	namespaceGenSize        = 8
	namespaceHdrSize        = 4 + namespaceGenSize
)

// NamespaceStore adds O(1) namespace invalidation to any Store.
// The gormcache plugin already has its own table/row/global epoch invalidation,
// so this wrapper is retained for manual cache users that need DropPrefix
// semantics without scanning the underlying backend.
type NamespaceStore struct {
	inner        Store
	namespaceFor func(key string) string
	gen          sync.Map
}

// Del removes key from the underlying store.
func (s *NamespaceStore) Del(key string) {
	s.inner.Del(key)
}

// DropPrefix invalidates namespaces in O(1) without scanning the underlying store.
func (s *NamespaceStore) DropPrefix(prefix ...string) {
	for _, item := range prefix {
		if item != "" {
			s.counter(item).Add(1)
		}
	}
}

// Get returns a cached value when its namespace generation is still current.
func (s *NamespaceStore) Get(key string) ([]byte, bool) {
	raw, ok := s.inner.Get(key)
	if !ok || len(raw) < namespaceHdrSize {
		return nil, false
	}
	if binary.LittleEndian.Uint32(raw) != namespaceMagic {
		return nil, false
	}
	if binary.LittleEndian.Uint64(raw[4:]) != s.counter(s.namespaceFor(key)).Load() {
		return nil, false
	}
	return raw[namespaceHdrSize:], true
}

// Set stores val with the current namespace generation. ttlSeconds is the
// lifetime in seconds; 0 or omitted forwards as no-TTL to the inner store.
func (s *NamespaceStore) Set(key string, val []byte, ttlSeconds ...int64) {
	buf := make([]byte, namespaceHdrSize+len(val))
	binary.LittleEndian.PutUint32(buf, namespaceMagic)
	binary.LittleEndian.PutUint64(buf[4:], s.counter(s.namespaceFor(key)).Load())
	copy(buf[namespaceHdrSize:], val)
	s.inner.Set(key, buf, ttlSeconds...)
}

func (s *NamespaceStore) counter(namespace string) *atomic.Uint64 {
	if val, ok := s.gen.Load(namespace); ok {
		return val.(*atomic.Uint64)
	}
	val, _ := s.gen.LoadOrStore(namespace, new(atomic.Uint64))
	return val.(*atomic.Uint64)
}

// NewNamespaceStore wraps inner and maps each key to a bounded namespace.
func NewNamespaceStore(inner Store, namespaceFor func(key string) string) *NamespaceStore {
	if inner == nil || namespaceFor == nil {
		panic("gormcache: NewNamespaceStore needs non-nil inner and namespaceFor")
	}
	return &NamespaceStore{inner: inner, namespaceFor: namespaceFor}
}
