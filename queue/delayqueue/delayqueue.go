package delayqueue

import (
	"container/heap"
	"sync"
	"time"
)

// DelayQueue 基于小顶堆的延迟队列，单 timer + 唤醒通道，由 loop 协程独占管理 timer。
type DelayQueue[T any] struct {
	mu sync.RWMutex
	h  minHeap[T]

	// timer 仅能被 loop 协程使用（Stop/Reset/Drain 全在 loop 内）
	timer *time.Timer

	// 唤醒：有更早任务加入/堆从空变非空时触发
	wakeCh chan struct{}

	// 向外发送到期任务
	notify chan Item[T]

	// 关闭信号：Close() 里关闭，loop 收到后退出并关闭 notify
	stopCh chan struct{}

	closed bool
}

// Close 关闭队列（只发退出信号，不直接操作 timer）
func (dq *DelayQueue[T]) Close() {
	dq.mu.Lock()
	if dq.closed {
		dq.mu.Unlock()
		return
	}
	dq.closed = true
	close(dq.stopCh) // 广播退出
	dq.mu.Unlock()
}

// Len 获取队列长度（近似，未计入已到期但未发送的瞬时）
func (dq *DelayQueue[T]) Len() int {
	dq.mu.RLock()
	n := len(dq.h)
	dq.mu.RUnlock()
	return n
}

// PopChan 获取到期任务通道
func (dq *DelayQueue[T]) PopChan() <-chan Item[T] { return dq.notify }

// Push 添加延迟任务（若更早，唤醒 loop 以重置 timer）
func (dq *DelayQueue[T]) Push(v T, at time.Time) {
	dq.mu.Lock()
	if dq.closed {
		dq.mu.Unlock()
		return
	}
	wasEmpty := len(dq.h) == 0
	var oldEarliest time.Time
	if !wasEmpty {
		oldEarliest = dq.h[0].At
	}
	heap.Push(&dq.h, Item[T]{At: at, Value: v})
	needWake := wasEmpty || at.Before(oldEarliest)
	dq.mu.Unlock()

	if needWake {
		select {
		case dq.wakeCh <- struct{}{}:
		default:
		}
	}
}

// loop：唯一操作 timer 的协程；负责定时触发、发送到期任务、在退出时关闭 notify。
func (dq *DelayQueue[T]) loop() {
	var armed bool // 小优化：记录当前是否已武装过 timer

	for {
		var (
			timerCh <-chan time.Time
			wait    time.Duration
			hasTask bool
		)

		// 只读状态，然后尽快释放读锁
		dq.mu.RLock()
		if dq.closed {
			dq.mu.RUnlock()
			close(dq.notify) // 只由 loop 关闭，避免并发关闭
			return
		}
		if n := len(dq.h); n > 0 {
			earliest := dq.h[0].At
			if dur := time.Until(earliest); dur <= 0 {
				dq.mu.RUnlock()
				dq.processExpired()
				continue
			} else {
				hasTask = true
				wait = dur
			}
		}
		dq.mu.RUnlock() // —— 只在这里解一次锁 —— //

		// 不持锁地操作 timer（loop 独占）
		if hasTask {
			if armed { // 之前武装过，先安全停止并清空
				dq.safelyStopTimer()
			}
			dq.timer.Reset(wait)
			timerCh = dq.timer.C
			armed = true
		} else {
			if armed {
				dq.safelyStopTimer()
				armed = false
			}
			timerCh = nil
		}

		select {
		case <-dq.stopCh:
			// 下轮顶部检测 closed=true，统一关闭 notify 并退出
			continue

		case <-dq.wakeCh:
			// 有更早任务或从空变非空，回到顶部重新计算
			continue

		case <-timerCh:
			// 到点：批量发送所有到期任务
			dq.processExpired()
		}
	}
}

// processExpired：弹出并发送所有“已到期”的任务
func (dq *DelayQueue[T]) processExpired() {
	for {
		dq.mu.Lock()
		if dq.closed || len(dq.h) == 0 || dq.h[0].At.After(time.Now()) {
			dq.mu.Unlock()
			return
		}
		item := heap.Pop(&dq.h).(Item[T])
		dq.mu.Unlock()

		// 发送（带 stop 逃生口）；notify 有缓冲，通常不会阻塞 loop
		select {
		case dq.notify <- item:
		case <-dq.stopCh:
			return
		}
	}
}

// safelyStopTimer 安全停止 timer，避免阻塞
func (dq *DelayQueue[T]) safelyStopTimer() {
	if !dq.timer.Stop() {
		// 如果 timer 正在运行，清空通道以避免阻塞
		select {
		case <-dq.timer.C:
		default:
		}
	}
}

// Item 延迟任务项
type Item[T any] struct {
	At    time.Time
	Value T
	index int
}

// 小顶堆实现（全部指针接收器，符合 heap.Interface 要求）
type minHeap[T any] []Item[T]

func (h *minHeap[T]) Len() int { return len(*h) }

func (h *minHeap[T]) Less(i, j int) bool { return (*h)[i].At.Before((*h)[j].At) }

func (h *minHeap[T]) Peek() (Item[T], bool) {
	if len(*h) == 0 {
		var zero Item[T]
		return zero, false
	}
	return (*h)[0], true
}

func (h *minHeap[T]) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h *minHeap[T]) Push(x any) {
	item := x.(Item[T])
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *minHeap[T]) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	(*h)[i].index, (*h)[j].index = i, j
}

// New 创建并启动延迟队列
func New[T any]() *DelayQueue[T] {
	dq := &DelayQueue[T]{
		h:      make(minHeap[T], 0),
		timer:  time.NewTimer(time.Hour), // 初始化为很长；loop 会立即 Stop/Reset
		wakeCh: make(chan struct{}, 1),
		notify: make(chan Item[T], 128),
		stopCh: make(chan struct{}),
	}
	// 立即停掉初始 timer，并清空一次
	if !dq.timer.Stop() {
		select {
		case <-dq.timer.C:
		default:
		}
	}
	go dq.loop()
	return dq
}
