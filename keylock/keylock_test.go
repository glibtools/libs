package keylock

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyLock_Churn_ReleasesEntries(t *testing.T) {
	kl := NewKeyLock()

	const (
		workers   = 32
		iters     = 2000
		keySpace  = 200
		holdNanos = 50
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < iters; i++ {
				k := r.Intn(keySpace)
				unlock := kl.Lock(k)
				// tiny critical section
				time.Sleep(time.Nanosecond * holdNanos)
				unlock()
				if i%200 == 0 {
					_ = kl.Len()
					_ = kl.GetNum(k)
				}
			}
		}(int64(w) + 1)
	}
	wg.Wait()

	// After all goroutines exit, all entries should be released.
	if kl.Len() != 0 {
		t.Fatalf("Len()=%d want=0", kl.Len())
	}
}

func TestKeyLock_DifferentKeys_DoNotBlockEachOther(t *testing.T) {
	kl := NewKeyLock()

	unlockA := kl.Lock("a")
	defer unlockA()

	var progressed int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			u := kl.Lock("b")
			atomic.AddInt32(&progressed, 1)
			u()
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	if atomic.LoadInt32(&progressed) == 0 {
		t.Fatalf("expected progress on key 'b' while 'a' is held")
	}
}

func TestKeyLock_MutualExclusion_SameKey(t *testing.T) {
	kl := NewKeyLock()
	const n = 200

	var inCS int32
	var maxInCS int32
	var sum int32

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			unlock := kl.Lock("k")
			cur := atomic.AddInt32(&inCS, 1)
			for {
				m := atomic.LoadInt32(&maxInCS)
				if cur <= m || atomic.CompareAndSwapInt32(&maxInCS, m, cur) {
					break
				}
			}
			atomic.AddInt32(&sum, 1)
			atomic.AddInt32(&inCS, -1)
			unlock()
		}()
	}
	wg.Wait()

	if sum != n {
		t.Fatalf("sum=%d want=%d", sum, n)
	}
	if maxInCS != 1 {
		t.Fatalf("maxInCS=%d want=1", maxInCS)
	}
	if kl.Len() != 0 {
		t.Fatalf("Len()=%d want=0", kl.Len())
	}
}

func TestKeyLock_TryLock(t *testing.T) {
	kl := NewKeyLock()

	unlock := kl.Lock("k")

	failed := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		start := time.Now()
		for time.Since(start) < 200*time.Millisecond {
			if u, ok := kl.TryLock("k"); ok {
				u()
				select {
				case failed <- "TryLock succeeded while lock should be held":
				default:
				}
				return
			}
			runtime.Gosched()
		}
	}()

	<-done
	unlock()

	select {
	case msg := <-failed:
		t.Fatal(msg)
	default:
	}

	u2, ok := kl.TryLock("k")
	if !ok || u2 == nil {
		t.Fatalf("TryLock after unlock: ok=%v unlockIsNil=%v; want ok=true", ok, u2 == nil)
	}
	u2()

	if kl.Len() != 0 {
		t.Fatalf("Len()=%d want=0", kl.Len())
	}
}

func TestTryMutex_TryLockNonBlocking(t *testing.T) {
	var m tryMutex
	m.initLocked()
	m.Lock()
	defer m.Unlock()

	start := time.Now()
	for i := 0; i < 1000; i++ {
		if m.TryLock() {
			t.Fatalf("TryLock succeeded while locked")
		}
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("TryLock seems to block; took=%s", time.Since(start))
	}
}
