package keylock

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
)

/**
测试
在多个pool Submit 任务时，不会发生死锁
locker1:=Lock("key1")
defer locker1()
locker2:=Lock("key2")
defer locker2()
*/

// NOTE:
// We intentionally don't test "cross key order" (some tasks lock key1->key2 while
// others lock key2->key1) because that pattern can legitimately deadlock in any
// mutex-based design. If you need multi-key locking without deadlocks, enforce a
// global key ordering in callers.

func TestAntsPoolSubmit_NoDeadlock(t *testing.T) {
	// Small pools, heavy contention.
	poolA, err := ants.NewPool(3, ants.WithPreAlloc(true), ants.WithNonblocking(true))
	if err != nil {
		t.Fatalf("new poolA: %v", err)
	}
	defer poolA.Release()

	poolB, err := ants.NewPool(3, ants.WithPreAlloc(true), ants.WithNonblocking(true))
	if err != nil {
		t.Fatalf("new poolB: %v", err)
	}
	defer poolB.Release()

	kl := NewKeyLock()

	const tasksPerPool = 200
	var wg sync.WaitGroup
	wg.Add(tasksPerPool * 2)

	worker := func(id int) func() {
		return func() {
			defer wg.Done()

			u1 := kl.Lock("key1")
			defer u1()
			time.Sleep(time.Millisecond)
			u2 := kl.Lock("key2")
			defer u2()

			if u3, ok := kl.TryLock("key3"); ok {
				u3()
			}
		}
	}

	// Submit with retry because pool is nonblocking and small.
	submitWithRetry := func(p *ants.Pool, fn func()) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			err := p.Submit(fn)
			if err == nil {
				return
			}
			if errors.Is(err, ants.ErrPoolClosed) {
				t.Fatalf("submit on closed pool: %v", err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("submit timeout: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}

	// Submit from multiple goroutines to simulate "多个 pool Submit".
	var submitWG sync.WaitGroup
	submitWG.Add(2)
	go func() {
		defer submitWG.Done()
		for i := 0; i < tasksPerPool; i++ {
			submitWithRetry(poolA, worker(i))
		}
	}()
	go func() {
		defer submitWG.Done()
		for i := 0; i < tasksPerPool; i++ {
			submitWithRetry(poolB, worker(i+tasksPerPool))
		}
	}()

	submitWG.Wait()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(10 * time.Second):
		t.Fatalf("possible deadlock: tasks did not complete in time")
	}

	if kl.Len() != 0 {
		t.Fatalf("expected all lock entries released, Len()=%d", kl.Len())
	}
}
