package gormcache

// Store is the cache backend required by the GORM plugin.
// Plugin invalidation uses table/row/global epochs in cache keys, so it does not
// call DropPrefix on normal query/write paths. DropPrefix is kept for manual
// invalidation and compatibility with callers that need namespace-style clears.
type Store interface {
	// Get returns the cached value for key.
	Get(key string) ([]byte, bool)
	// Set stores val for key. ttlSeconds is the lifetime in seconds; 0 or
	// omitted means the backend default (typically "no expiration").
	Set(key string, val []byte, ttlSeconds ...int64)
	// Del removes key from the cache.
	Del(key string)
	// DropPrefix removes or invalidates cached values whose key has one of prefix.
	DropPrefix(prefix ...string)
}
