package keylock

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

var mutexPool = sync.Pool{
	New: func() interface{} { return &sync.Mutex{} },
}

type KeyLock struct {
	mu    sync.RWMutex
	locks map[interface{}]*lockObj
}

// GetNum get the lock count
func (l *KeyLock) GetNum(key interface{}) uint32 {
	l.mu.RLock()
	var n uint32 = 0
	val, ok := l.locks[key]
	if ok {
		n = val.getNumber()
	}
	l.mu.RUnlock()
	return n
}

// Len get the lock count
func (l *KeyLock) Len() int {
	l.mu.RLock()
	n := len(l.locks)
	l.mu.RUnlock()
	return n
}

func (l *KeyLock) Lock(key interface{}) func() {
	l.mu.Lock()
	lock, has := l.locks[key]
	if !has {
		lock = newLockObj()
		l.locks[key] = lock
	}
	l.mu.Unlock()
	lock.Lock()
	lock.increment()
	return func() {
		lock.decrement()
		lock.Unlock()
		l.cleanup()
	}
}

// clean up the lock object
func (l *KeyLock) cleanup() {
	l.mu.Lock()
	for key, value := range l.locks {
		lock := value
		if lock.isZero() {
			delete(l.locks, key)
			mutexPool.Put(lock.Mutex)
		}
	}
	l.mu.Unlock()
}

type lockObj struct {
	*sync.Mutex
	num uint32
}

// decrease the lock count
func (l *lockObj) decrement() {
	atomic.AddUint32(&l.num, ^uint32(0))
}

func (l *lockObj) getNumber() uint32 {
	return atomic.LoadUint32(&l.num)
}

// increase the lock count
func (l *lockObj) increment() {
	atomic.AddUint32(&l.num, 1)
}

// check if the lock count is zero
func (l *lockObj) isZero() bool {
	return atomic.LoadUint32(&l.num) == 0
}

// NewKeyLock create a new key lock
func NewKeyLock() *KeyLock {
	return &KeyLock{
		locks: make(map[interface{}]*lockObj),
	}
}

func WaitSignal() {
	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTSTP,
		//os.Kill,
		//syscall.SIGKILL,
	)
	<-notifier
}

// create a new lock object
func newLockObj() *lockObj {
	return &lockObj{Mutex: mutexPool.Get().(*sync.Mutex)}
}
