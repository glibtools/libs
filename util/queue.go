package util

import (
	"sync"
	"time"

	"github.com/emirpasic/gods/queues"
	"github.com/emirpasic/gods/queues/linkedlistqueue"
	"github.com/panjf2000/ants/v2"
)

const defaultQueueMaxRetry = 5

var defaultQueueRetryDelayFunc = func(retried int) time.Duration {
	return time.Duration(retried) * time.Second
}

type GQueue struct {
	mu       sync.RWMutex
	opt      GQueueOption
	qu       queues.Queue
	handlers map[string]GQueueHandler
	taskChan chan QueueTask
	workers  map[int]*quWorkers
}

func (g *GQueue) Enqueue(task QueueTask) {
	g.mu.Lock()
	g.qu.Enqueue(task)
	g.mu.Unlock()
}

// Register register task handler
func (g *GQueue) Register(task string, handler GQueueHandler) {
	g.mu.Lock()
	g.handlers[task] = handler
	g.mu.Unlock()
}

// StartServer ...
func (g *GQueue) StartServer() {
	go g.runBackground()
	go g.watcher()
}

// Wait ...
func (g *GQueue) Wait() {
	for _, w := range g.workers {
		w.wait()
	}
}

func (g *GQueue) dequeue() (val QueueTask, ok bool) {
	g.mu.RLock()
	empty := g.qu.Empty()
	g.mu.RUnlock()
	if empty {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.qu.Dequeue()
	if !ok {
		return
	}
	val, ok = v.(QueueTask)
	if !ok {
		return
	}
	return
}

// getHandler ...
func (g *GQueue) getHandler(task string) (handler GQueueHandler, ok bool) {
	g.mu.RLock()
	handler, ok = g.handlers[task]
	g.mu.RUnlock()
	return
}

// runBackground ...
func (g *GQueue) runBackground() {
	for {
		data, ok := g.dequeue()
		if !ok {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		if data.runAt.After(time.Now()) {
			g.Enqueue(data)
			continue
		}
		g.taskChan <- data
	}
}

// runTask ...
func (g *GQueue) runTask(t QueueTask, handler GQueueHandler) {
	defer func() {
		// recover
		if p := recover(); p != nil {
			g.opt.Logger.Errorf("task %s panic: %v", t.Name, p)
		}
	}()
	err := handler(&t)
	if err == nil {
		return
	}
	maxRetry := g.opt.MaxRetry
	if t.maxRetry > 0 {
		maxRetry = t.maxRetry
	}
	if t.Retried >= maxRetry {
		g.opt.Logger.Errorf(
			"task %s retried %d times, but still failed; Error: %s; Data: %s",
			t.Name, t.Retried, err.Error(), string(t.Data),
		)
		return
	}
	// retry
	task := t.Copy()
	task.Retried++
	task.runAt = time.Now().Add(task.retryDelayFunc(task.Retried))
	g.Enqueue(task)
}

func (g *GQueue) runTaskHandler(t QueueTask) {
	handler, ok := g.getHandler(t.Name)
	if !ok {
		return
	}
	worker, ok := g.workers[t.Group]
	if !ok {
		worker = g.workers[1]
	}
	worker.run(func() { g.runTask(t, handler) })
}

// watcher ...watcher
func (g *GQueue) watcher() {
	for {
		data, ok := <-g.taskChan
		if !ok {
			return
		}
		g.runTaskHandler(data)
	}
}

type GQueueHandler func(task *QueueTask) (err error)

type GQueueOption struct {
	MaxWorkers int
	MaxRetry   int
	Logger     ItfLogger
	/**
	 * task groups
	 * key: group id
	 * value: the percentage of workers to be used for this group
	 */
	Groups map[int]int
}

type QueueTask struct {
	Name           string `json:"name,omitempty"`
	Data           []byte `json:"data,omitempty"`
	Retried        int    `json:"retried,omitempty"`
	Group          int    `json:"group,omitempty"`
	maxRetry       int
	retryDelayFunc func(retried int) time.Duration
	runAt          time.Time
}

func (t QueueTask) Copy() QueueTask {
	return QueueTask{
		Name:           t.Name,
		Data:           t.Data,
		Retried:        t.Retried,
		Group:          t.Group,
		maxRetry:       t.maxRetry,
		retryDelayFunc: t.retryDelayFunc,
		runAt:          t.runAt,
	}
}

type QueueTaskOpt func(task *QueueTask)

type quWorkers struct {
	wg  sync.WaitGroup
	chs chan struct{}
}

// run ...
func (w *quWorkers) run(fn func()) {
	w.chs <- struct{}{}
	w.wg.Add(1)
	fn1 := func() {
		fn()
		w.wg.Done()
		<-w.chs
	}
	_ = LoadAntSPool().Submit(fn1)
}

// wait ...
func (w *quWorkers) wait() { w.wg.Wait() }

func LoadAntSPool() *ants.Pool {
	return LoadSingle(func() *ants.Pool {
		pool, _ := ants.NewPool(100)
		return pool
	})
}

func NewGQueue(opt GQueueOption) *GQueue {
	if opt.MaxWorkers <= 0 {
		opt.MaxWorkers = 10
	}
	if opt.Logger == nil {
		opt.Logger = ZapLogger("queue")
	}
	if opt.MaxRetry < 1 {
		opt.MaxRetry = 1
	}
	if len(opt.Groups) < 1 {
		opt.Groups = map[int]int{
			1: 6,
			2: 3,
			3: 1,
		}
	}
	if _, ok := opt.Groups[1]; !ok {
		opt.Groups[1] = 6
	}
	if opt.Groups[1] < 1 {
		opt.Groups[1] = 6
	}
	var workers = make(map[int]*quWorkers)
	for k, v := range opt.Groups {
		workers[k] = newQuWorkers(getNumberFromPercent(opt.MaxWorkers, v))
	}
	g := &GQueue{
		opt:      opt,
		qu:       linkedlistqueue.New(),
		handlers: make(map[string]GQueueHandler),
		taskChan: make(chan QueueTask, opt.MaxWorkers),
		workers:  workers,
	}
	return g
}

func NewQueueTask(name string, data []byte, opt ...QueueTaskOpt) QueueTask {
	task := QueueTask{
		Name:    name,
		Data:    data,
		Retried: 0,
		Group:   1,
	}
	for _, o := range opt {
		o(&task)
	}
	if task.maxRetry <= 0 {
		task.maxRetry = defaultQueueMaxRetry
	}
	if task.retryDelayFunc == nil {
		task.retryDelayFunc = defaultQueueRetryDelayFunc
	}
	if task.runAt.IsZero() {
		task.runAt = time.Now()
	}
	return task
}

func WithQueueTaskGroup(group int) QueueTaskOpt {
	return func(task *QueueTask) {
		task.Group = group
	}
}

func WithQueueTaskMaxRetry(maxRetry int) QueueTaskOpt {
	return func(task *QueueTask) {
		task.maxRetry = maxRetry
	}
}

func WithQueueTaskRetryDelayFunc(retryDelayFunc func(retried int) time.Duration) QueueTaskOpt {
	return func(task *QueueTask) {
		task.retryDelayFunc = retryDelayFunc
	}
}

func WithQueueTaskRunAt(runAt time.Time) QueueTaskOpt {
	return func(task *QueueTask) {
		task.runAt = runAt
	}
}

func getNumberFromPercent(total, percent int) int {
	if percent > 10 || percent < 1 {
		percent = 1
	}
	v := total * percent / 10
	if v < 1 {
		v = 1
	}
	return v
}

func newQuWorkers(maxWorkers int) *quWorkers {
	return &quWorkers{
		wg:  sync.WaitGroup{},
		chs: make(chan struct{}, maxWorkers),
	}
}
