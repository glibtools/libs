package util

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/emirpasic/gods/queues/linkedlistqueue"
)

type CloseableChannel[T any] struct {
	ch    chan T
	close atomic.Bool
	mu    sync.RWMutex
	queue *linkedlistqueue.Queue
}

// Channel returns the channel of the CloseableChannel.
func (c *CloseableChannel[T]) Channel() <-chan T { return c.ch }

func (c *CloseableChannel[T]) Close() {
	c.LockWrapFunc(func() {
		c.close.Store(true)
		close(c.ch)
	})
}

// Dequeue removes a value from the front of the queue
func (c *CloseableChannel[T]) Dequeue() (val T, ok bool) {
	c.LockWrapFunc(func() {
		if c.queue.Empty() {
			return
		}
		var _val interface{}
		_val, ok = c.queue.Dequeue()
		if !ok {
			return
		}
		val = _val.(T)
	})
	return
}

// Enqueue adds a value to the end of the queue
func (c *CloseableChannel[T]) Enqueue(value T) {
	c.LockWrapFunc(func() { c.queue.Enqueue(value) })
}

func (c *CloseableChannel[T]) IsClosed() bool { return c.close.Load() }

func (c *CloseableChannel[T]) LockWrapFunc(fn func()) {
	if c.IsClosed() {
		return
	}
	c.mu.Lock()
	if c.IsClosed() {
		c.mu.Unlock()
		return
	}
	fn()
	c.mu.Unlock()
}

func (c *CloseableChannel[T]) RLockWrapFunc(fn func()) {
	if c.IsClosed() {
		return
	}
	c.mu.RLock()
	if c.IsClosed() {
		c.mu.RUnlock()
		return
	}
	fn()
	c.mu.RUnlock()
}

func (c *CloseableChannel[T]) Send(val T) { c.Enqueue(val) }

func (c *CloseableChannel[T]) SendWithDefaultTimeout(val T) { c.SendWithTimeout(val, 0) }

// SendWithTimeout send val to the channel with timeout
func (c *CloseableChannel[T]) SendWithTimeout(val T, timeout time.Duration) {
	c.RLockWrapFunc(func() { c.sendWithTimeout(val, timeout) })
}

func (c *CloseableChannel[T]) dequeueSend() (ok bool) {
	c.LockWrapFunc(func() {
		if c.queue.Empty() {
			return
		}
		var _val interface{}
		_val, ok = c.queue.Dequeue()
		if !ok {
			return
		}
		c.sendWithTimeout(_val.(T), 0)
	})
	return
}

// runBackgroundLoop runs a background loop that will send all values
// that are enqueued to the channel
// if the channel is closed, the loop will exit
// if enqueue is empty, the loop will wait for a value to be enqueued
func (c *CloseableChannel[T]) runBackgroundLoop() {
	for {
		if c.IsClosed() {
			return
		}
		ok := c.dequeueSend()
		if !ok {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *CloseableChannel[T]) sendWithTimeout(val T, timeout time.Duration) {
	if timeout < 0 {
		c.ch <- val
		return
	}
	if timeout < time.Millisecond*100 {
		timeout = time.Millisecond * 100
	}
	select {
	case c.ch <- val:
	case <-time.After(timeout):
		break
	}
}

type CloseableType interface {
	IsClosed() bool
	Close()
	Send(val interface{})
	Channel() <-chan interface{}
}

func NewCloseable[T any](ch chan T) *CloseableChannel[T] {
	c := &CloseableChannel[T]{ch: ch, queue: linkedlistqueue.New()}
	go c.runBackgroundLoop()
	return c
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
