package queue

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andeya/goutil"
	"github.com/emirpasic/gods/queues"
	"github.com/emirpasic/gods/queues/arrayqueue"

	"github.com/glibtools/libs/queue/delayqueue"
	"github.com/glibtools/libs/util"
)

const (
	defaultQueueMaxRetry = 1
	defaultBatchSize     = 10
	maxGroupPercent      = 10
	minGroupPercent      = 1
)

var (
	DefaultGQOptionFunc = func() GQueueOption {
		return GQueueOption{
			MaxConcurrent: runtime.NumCPU(),
			MaxRetry:      defaultQueueMaxRetry,
			Logger:        util.ZapLogger("queue"),
			Groups:        map[int]int{1: 6, 2: 3, 3: 1},
			BatchSize:     defaultBatchSize,
		}
	}

	defaultQueueRetryDelayFunc = func(retried int) time.Duration {
		return time.Duration(retried) * time.Second
	}
)

type GQHandlerMap map[string]GQueueHandler

type GQueue struct {
	opt       GQueueOption
	workers   map[int]*gqWorker
	handlers  sync.Map
	mu        sync.RWMutex
	startOnce sync.Once
	closeCH   chan struct{}
	isClosed  atomic.Bool

	// FIX: 新增，仅用于 StopAndWait 阶段拒绝新任务
	accepting atomic.Bool
}

func (g *GQueue) Enqueue(task GQueueTask) {
	// FIX: StopAndWait 后不再接收新任务
	if !g.accepting.Load() {
		return
	}

	g.mu.RLock()
	worker, ok := g.workers[task.Group]
	if !ok {
		worker = g.workers[1]
	}
	g.mu.RUnlock()
	worker.enqueue(task)
}

func (g *GQueue) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})
	g.mu.RLock()
	defer g.mu.RUnlock()

	totalQueueSize := 0
	groupStats := make(map[int]map[string]interface{})
	for gid, w := range g.workers {
		qsize := w.size()
		totalQueueSize += qsize
		groupStats[gid] = map[string]interface{}{
			"queue_size":    qsize,
			"jobs_buffered": w.jobsLen(),
			"delayed_size":  w.dq.Len(),
		}
	}
	stats["total_queue_size"] = totalQueueSize
	stats["group_stats"] = groupStats
	stats["is_closed"] = g.isClosed.Load()
	return stats
}

// IsClosed 检查队列是否已关闭
func (g *GQueue) IsClosed() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.isClosed.Load()
}

func (g *GQueue) Register(task string, handler GQueueHandler) {
	if task == "" {
		panic("task name cannot be empty")
	}
	if handler == nil {
		panic("handler cannot be nil")
	}
	_, loaded := g.handlers.LoadOrStore(task, handler)
	if loaded {
		g.opt.Logger.Warnf("task handler for '%s' already exists, skipping registration", task)
	}
}

func (g *GQueue) StartServer() {
	g.startOnce.Do(func() {
		g.opt.Logger.Infof("Starting GQueue server with %d workers", len(g.workers))
		for groupID, worker := range g.workers {
			worker.runDequeue(g.opt)
			worker.startDelayConsumer()
			g.opt.Logger.Infof("Worker for group %d started", groupID)
		}
	})
}

func (g *GQueue) Stop() {
	if g.isClosed.Load() {
		return
	}

	g.mu.Lock()
	if g.isClosed.Load() {
		g.mu.Unlock()
		return
	}
	g.isClosed.Store(true)
	g.accepting.Store(false)
	close(g.closeCH)
	g.mu.Unlock()

	g.mu.RLock()
	workers := make([]*gqWorker, 0, len(g.workers))
	for _, w := range g.workers {
		workers = append(workers, w)
	}
	g.mu.RUnlock()

	for _, w := range workers {
		w.dq.Close()
		w.cond.Broadcast()
	}
}

// StopAndWait 停止队列并等待所有任务完成
func (g *GQueue) StopAndWait() {
	g.Stop()
	g.Wait()
}

func (g *GQueue) Wait() {
	g.mu.RLock()
	workers := make([]*gqWorker, 0, len(g.workers))
	for _, w := range g.workers {
		workers = append(workers, w)
	}
	g.mu.RUnlock()
	for _, w := range workers {
		w.inflightMu.Lock()
		w.inflight.Wait()
		w.inflightMu.Unlock()
		w.dequeueWG.Wait()
		w.dqWG.Wait()
	}
}

type GQueueArray struct {
	sync.RWMutex
	queue *arrayqueue.Queue
}

func (q *GQueueArray) Clear() { q.Lock(); q.queue.Clear(); q.Unlock() }

func (q *GQueueArray) Dequeue() (value interface{}, ok bool) {
	q.Lock()
	value, ok = q.queue.Dequeue()
	q.Unlock()
	return
}

func (q *GQueueArray) Empty() bool { q.RLock(); e := q.queue.Empty(); q.RUnlock(); return e }

func (q *GQueueArray) Enqueue(value interface{}) { q.Lock(); q.queue.Enqueue(value); q.Unlock() }

func (q *GQueueArray) Peek() (value interface{}, ok bool) {
	q.RLock()
	value, ok = q.queue.Peek()
	q.RUnlock()
	return
}

func (q *GQueueArray) Size() int { q.RLock(); s := q.queue.Size(); q.RUnlock(); return s }

func (q *GQueueArray) String() string {
	q.RLock()
	str := q.queue.String()
	q.RUnlock()
	return str
}

func (q *GQueueArray) Values() []interface{} { q.RLock(); v := q.queue.Values(); q.RUnlock(); return v }

type GQueueHandler func(task *GQueueTask) (err error)

type GQueueOption struct {
	MaxConcurrent int            `json:"max_concurrent"`
	MaxRetry      int            `json:"max_retry"`
	Logger        util.ItfLogger `json:"-"`
	Groups        map[int]int    `json:"groups"`
	BatchSize     int            `json:"batch_size"`
}

type GQueueTask struct {
	ID             string                          `json:"id"`
	Name           string                          `json:"name"`
	Data           []byte                          `json:"data"`
	Retried        int                             `json:"retried"`
	Group          int                             `json:"group"`
	CreatedAt      time.Time                       `json:"created_at"`
	MaxRetry       int                             `json:"max_retry"`
	RetryDelayFunc func(retried int) time.Duration `json:"-"`
	RunAt          time.Time                       `json:"run_at"`
}

func (t GQueueTask) Copy() *GQueueTask {
	return &GQueueTask{
		ID:             t.ID,
		Name:           t.Name,
		Data:           append([]byte(nil), t.Data...),
		Retried:        t.Retried,
		Group:          t.Group,
		CreatedAt:      t.CreatedAt,
		MaxRetry:       t.MaxRetry,
		RetryDelayFunc: t.RetryDelayFunc,
		RunAt:          t.RunAt,
	}
}

func (t GQueueTask) IsExpired(timeout time.Duration) bool {
	return time.Since(t.CreatedAt) > timeout
}

func (t GQueueTask) String() string {
	return fmt.Sprintf("Task{ID:%s, Name:%s, Group:%d, Retried:%d}", t.ID, t.Name, t.Group, t.Retried)
}

type GQueueTaskOpt func(task *GQueueTask)

type Job func()

type gqWorker struct {
	runner func(fn func())
	// 任务执行的固定协程池（size=limit）
	jobs chan Job
	// 待出队的队列（组内共享）
	queue queues.Queue
	// 延迟队列：用于延时任务
	dq *delayqueue.DelayQueue[GQueueTask]

	logger   util.ItfLogger
	maxRetry int
	gq       *GQueue
	cond     *sync.Cond

	inflightMu sync.Mutex
	inflight   sync.WaitGroup
	dequeueWG  sync.WaitGroup
	dqWG       sync.WaitGroup
}

func (w *gqWorker) dequeue() (task GQueueTask, ok bool) {
	v, ok := w.queue.Dequeue()
	if !ok {
		return
	}
	task, ok = v.(GQueueTask)
	return
}

func (w *gqWorker) enqueue(task GQueueTask) {
	select {
	case <-w.gq.closeCH:
		return
	default:
	}

	if task.RunAt.IsZero() {
		task.RunAt = time.Now()
	}
	if task.RunAt.After(time.Now()) {
		w.dq.Push(task, task.RunAt)
		return
	}

	w.cond.L.Lock()
	w.queue.Enqueue(task)
	w.cond.Signal()
	w.cond.L.Unlock()
}

func (w *gqWorker) jobsLen() int { return len(w.jobs) }

func (w *gqWorker) run(task GQueueTask) {
	select {
	case <-w.gq.closeCH:
		return
	default:
	}

	handler, ok := w.gq.handlers.Load(task.Name)
	if !ok {
		return
	}

	w.inflightMu.Lock()
	if w.gq.isClosed.Load() {
		w.inflightMu.Unlock()
		return
	}
	w.inflight.Add(1)
	w.inflightMu.Unlock()

	w.runner(func() {
		defer w.inflight.Done()
		w.runTask(task, handler.(GQueueHandler))
	})
}

func (w *gqWorker) runDequeue(opt GQueueOption) {
	w.dequeueWG.Add(1)
	go func() {
		defer w.dequeueWG.Done()

		bs := opt.BatchSize
		if bs <= 0 {
			bs = defaultBatchSize
		}
		for {
			select {
			case <-w.gq.closeCH:
				return
			default:
			}

			w.cond.L.Lock()

			for w.queue.Empty() {
				select {
				case <-w.gq.closeCH:
					w.cond.L.Unlock()
					return
				default:
				}
				w.cond.Wait()
			}

			var tasks []GQueueTask
			for i := 0; i < bs; i++ {
				v, ok := w.dequeue()
				if !ok {
					break
				}
				tasks = append(tasks, v)
				if w.queue.Empty() {
					break
				}
			}
			w.cond.L.Unlock()

			if len(tasks) == 0 {
				runtime.Gosched() // 让出 CPU 时间片，避免忙等
				continue
			}

			for _, t := range tasks {
				w.run(t)
			}
		}
	}()
}

func (w *gqWorker) runTask(t GQueueTask, handler GQueueHandler) {
	defer func() {
		if p := recover(); p != nil {
			traceBytes := goutil.PanicTrace(5)
			w.logger.Errorf("task %s panic: %v;\n trace: %s\n", t.Name, p, string(traceBytes))
		}
	}()
	err := handler(&t)
	if err == nil {
		return
	}

	maxRetryNumber := w.maxRetry
	if t.MaxRetry >= 0 {
		maxRetryNumber = t.MaxRetry
	}
	if t.Retried >= maxRetryNumber {
		w.logger.Errorf("task %s retried %d times(max retry %d), but still failed; Error: %s; Data: %s",
			t.Name, t.Retried, maxRetryNumber, err.Error(), string(t.Data))
		return
	}

	// 重试：复制并延后 RunAt
	task := t.Copy()
	task.Retried++
	if task.RetryDelayFunc == nil {
		task.RetryDelayFunc = defaultQueueRetryDelayFunc
	}
	task.RunAt = time.Now().Add(task.RetryDelayFunc(task.Retried))
	w.enqueue(*task)
}

func (w *gqWorker) size() int { return w.queue.Size() }

func (w *gqWorker) startDelayConsumer() {
	w.dqWG.Add(1)
	go func() {
		defer w.dqWG.Done()
		for {
			select {
			case <-w.gq.closeCH:
				return
			case item, ok := <-w.dq.PopChan():
				if !ok {
					return
				}
				if item.At.After(time.Now()) {
					w.enqueue(item.Value)
					continue
				}
				w.run(item.Value)
			}
		}
	}()
}

// EnqueueGq 将任务加入全局队列
func EnqueueGq(task string, payload interface{}, opt ...GQueueTaskOpt) {
	GetGQServer().Enqueue(NewGQTask(task, payload, opt...))
}

// GetGQServer 单例
func GetGQServer(opts ...GQueueOption) *GQueue {
	opt := GQueueOption{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	return util.LoadSingle(func() *GQueue { return NewGQueue(opt) })
}

func NewGQTask(task string, payload interface{}, opt ...GQueueTaskOpt) GQueueTask {
	if task == "" {
		panic("task name cannot be empty")
	}
	var data []byte
	var err error
	switch v := payload.(type) {
	case nil:
		data = nil
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		data, err = util.Marshal(payload)
		if err != nil {
			panic(fmt.Sprintf("failed to marshal payload: %v", err))
		}
	}
	return NewGQueueTaskBytes(task, data, opt...)
}

func NewGQueue(opt1 GQueueOption) *GQueue {
	opt := DefaultGQOptionFunc()
	mergeConfigs(&opt, &opt1)

	// 修正配置
	if opt.MaxConcurrent <= 0 {
		opt.MaxConcurrent = runtime.NumCPU()
	}
	if opt.MaxRetry < 0 {
		opt.MaxRetry = defaultQueueMaxRetry
	}
	if opt.Logger == nil {
		opt.Logger = util.ZapLogger("queue")
	}
	if opt.BatchSize <= 0 {
		opt.BatchSize = defaultBatchSize
	}
	if len(opt.Groups) == 0 {
		opt.Groups = map[int]int{1: 6, 2: 3, 3: 1}
	}
	if _, ok := opt.Groups[1]; !ok || opt.Groups[1] < 1 {
		opt.Groups[1] = 6
	}

	g := &GQueue{
		opt:      opt,
		handlers: sync.Map{},
		workers:  make(map[int]*gqWorker),
		closeCH:  make(chan struct{}),
	}
	g.accepting.Store(true) // FIX

	for groupID, percent := range opt.Groups {
		limit := getNumberFromPercent(opt.MaxConcurrent, percent)
		g.workers[groupID] = newGQWorker(limit, opt.MaxRetry, opt.Logger, g)
	}
	return g
}

func NewGQueueArray() *GQueueArray { return &GQueueArray{queue: arrayqueue.New()} }

func NewGQueueTaskBytes(name string, data []byte, opt ...GQueueTaskOpt) GQueueTask {
	if name == "" {
		panic("task name cannot be empty")
	}
	now := time.Now()
	task := GQueueTask{
		ID:        util.GenNanoid(21),
		Name:      name,
		Data:      data,
		Retried:   0,
		Group:     1,
		CreatedAt: now,
		MaxRetry:  -1,
		RunAt:     now,
	}
	for _, o := range opt {
		if o != nil {
			o(&task)
		}
	}
	if task.RetryDelayFunc == nil {
		task.RetryDelayFunc = defaultQueueRetryDelayFunc
	}
	return task
}

// StartGQServer 批量注册并启动
func StartGQServer(hMap GQHandlerMap) {
	server := GetGQServer()
	for name, h := range hMap {
		server.Register(name, h)
	}
	server.StartServer()
}

func WithGQueueTaskGroup(group int) GQueueTaskOpt {
	return func(task *GQueueTask) { task.Group = group }
}

func WithGQueueTaskID(id string) GQueueTaskOpt {
	return func(task *GQueueTask) {
		if id != "" {
			task.ID = id
		}
	}
}

func WithGQueueTaskMaxRetry(maxRetry int) GQueueTaskOpt {
	return func(task *GQueueTask) { task.MaxRetry = maxRetry }
}

func WithGQueueTaskRetryDelayFunc(retryDelayFunc func(retried int) time.Duration) GQueueTaskOpt {
	return func(task *GQueueTask) { task.RetryDelayFunc = retryDelayFunc }
}

func WithGQueueTaskRunAt(runAt time.Time) GQueueTaskOpt {
	return func(task *GQueueTask) { task.RunAt = runAt }
}

// WrapGQHandler 包装任意函数为队列处理器
func WrapGQHandler(fn any) GQueueHandler {
	return func(task *GQueueTask) (err error) {
		return util.ExecSingleArgHandler(task.Data, fn)
	}
}

func getNumberFromPercent(total, percent int) int {
	// clip 到 [minGroupPercent, maxGroupPercent]
	if percent > maxGroupPercent {
		percent = maxGroupPercent
	}
	if percent < minGroupPercent {
		percent = minGroupPercent
	}
	v := total * percent / maxGroupPercent
	if v < 1 {
		v = 1
	}
	return v
}

func newGQWorker(limit int, maxRetry int, logger util.ItfLogger, gq *GQueue) *gqWorker {
	if limit <= 0 {
		limit = 1
	}
	bufferSize := limit
	if bufferSize > 100 {
		bufferSize = 100
	}
	jobs := make(chan Job, bufferSize)
	for i := 0; i < limit; i++ {
		go workerFunc(jobs, logger)
	}
	return &gqWorker{
		runner:   func(fn func()) { jobs <- fn },
		jobs:     jobs,
		queue:    NewGQueueArray(),
		dq:       delayqueue.New[GQueueTask](),
		logger:   logger,
		maxRetry: maxRetry,
		gq:       gq,
		cond:     sync.NewCond(&sync.Mutex{}),
	}
}

func workerFunc(jobs <-chan Job, logger util.ItfLogger) {
	for job := range jobs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("worker panic recovered: %v", r)
				}
			}()
			job()
		}()
	}
}
