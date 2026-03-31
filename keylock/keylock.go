package keylock

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

type KeyLock struct {
	mu    sync.Mutex
	locks map[interface{}]*entry
}

// GetNum returns the current reference count for a key.
//
// Note: this is a debugging/introspection helper only; the number includes both
// current holder(s) and waiters that have reserved the entry.
func (l *KeyLock) GetNum(key interface{}) uint32 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.locks[key]; ok {
		return uint32(atomic.LoadInt32(&e.refs))
	}
	return 0
}

// Len returns how many key entries currently exist.
func (l *KeyLock) Len() int {
	l.mu.Lock()
	n := len(l.locks)
	l.mu.Unlock()
	return n
}

// Lock acquires the mutex for the given key and returns an unlock function.
//
// Key must be comparable (map key requirement). Like sync.Mutex, re-locking the
// same key in the same goroutine without unlocking will deadlock.
func (l *KeyLock) Lock(key interface{}) func() {
	e := l.getOrCreateEntry(key)
	e.m.Lock()
	return func() {
		e.m.Unlock()
		l.releaseEntry(key, e)
	}
}

// TryLock attempts to acquire the mutex for the given key without blocking.
// It returns (unlock, true) on success; otherwise (nil, false).
func (l *KeyLock) TryLock(key interface{}) (func(), bool) {
	e := l.getOrCreateEntry(key)
	if !e.m.TryLock() {
		l.releaseEntry(key, e)
		return nil, false
	}
	return func() {
		e.m.Unlock()
		l.releaseEntry(key, e)
	}, true
}

func (l *KeyLock) getOrCreateEntry(key interface{}) *entry {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = make(map[interface{}]*entry)
	}
	e, ok := l.locks[key]
	if !ok {
		e = &entry{}
		// Eagerly init tryMutex to avoid concurrent init races.
		e.m.initLocked()
		l.locks[key] = e
	}
	atomic.AddInt32(&e.refs, 1)
	l.mu.Unlock()
	return e
}

func (l *KeyLock) releaseEntry(key interface{}, e *entry) {
	l.mu.Lock()
	if atomic.AddInt32(&e.refs, -1) == 0 {
		// Only delete if the map still points at the same entry.
		if cur, ok := l.locks[key]; ok && cur == e {
			delete(l.locks, key)
		}
	}
	l.mu.Unlock()
}

type entry struct {
	refs int32
	m    tryMutex
}

// tryMutex is a small mutex implementation that supports TryLock.
//
// It is not re-entrant.
// State: 0 => unlocked, 1 => locked.
// A buffered channel is used as a one-token semaphore to block waiters.
//
// This type is intentionally not exported.
type tryMutex struct {
	state uint32
	ch    chan struct{}
}

func (m *tryMutex) Lock() {
	<-m.ch
	atomic.StoreUint32(&m.state, 1)
}

func (m *tryMutex) TryLock() bool {
	select {
	case <-m.ch:
		atomic.StoreUint32(&m.state, 1)
		return true
	default:
		return false
	}
}

func (m *tryMutex) Unlock() {
	atomic.StoreUint32(&m.state, 0)
	// Return token; if this blocks, Unlock was called without holding the lock.
	m.ch <- struct{}{}
}

// initLocked initializes the semaphore channel.
// It must be called exactly once for a given tryMutex, before it is used by
// multiple goroutines (we do this when creating the entry under KeyLock.mu).
func (m *tryMutex) initLocked() {
	m.ch = make(chan struct{}, 1)
	// Start unlocked by having one token available.
	m.ch <- struct{}{}
}

// NewKeyLock creates a new key lock.
func NewKeyLock() *KeyLock {
	return &KeyLock{locks: make(map[interface{}]*entry)}
}

func WaitSignal() {
	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTSTP,
	)
	<-notifier
}
