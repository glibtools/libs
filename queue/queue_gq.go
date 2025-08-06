package queue

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andeya/goutil"
	"github.com/emirpasic/gods/queues"
	"github.com/emirpasic/gods/queues/arrayqueue"

	"github.com/glibtools/libs/util"
)

const (
	defaultQueueMaxRetry     = 1
	defaultBatchSize         = 10
	defaultWorkerWaitTimeout = 100 * time.Millisecond
	maxGroupPercent          = 10
	minGroupPercent          = 1
)

var (
	DefaultGQOptionFunc = func() GQueueOption {
		return GQueueOption{
			MaxConcurrent: runtime.NumCPU(),

			MaxRetry: defaultQueueMaxRetry,

			Logger: util.ZapLogger("queue"),

			Groups: map[int]int{
				1: 6,
				2: 3,
				3: 1,
			},
		}
	}

	defaultQueueRetryDelayFunc = func(retried int) time.Duration {
		return time.Duration(retried) * time.Second
	}
)

// GQHandlerMap 任务处理器映射
type GQHandlerMap map[string]GQueueHandler

// GQueue 队列管理器
type GQueue struct {
	opt      GQueueOption
	workers  map[int]*gqWorker
	handlers sync.Map // 使用 sync.Map 提高并发性能
	mu       sync.RWMutex
	closed   int32 // 使用原子操作标记关闭状态
	ctx      context.Context
	cancel   context.CancelFunc
}

func (g *GQueue) Enqueue(task GQueueTask) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mu.RLock()
	worker, ok := g.workers[task.Group]
	if !ok {
		worker = g.workers[1] // 默认使用组1
	}
	g.mu.RUnlock()

	worker.enqueue(task)
}

// GetStats 获取队列统计信息
func (g *GQueue) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	g.mu.RLock()
	defer g.mu.RUnlock()

	totalQueueSize := 0
	groupStats := make(map[int]map[string]interface{})

	for groupID, worker := range g.workers {
		queueSize := worker.queue.Size()
		totalQueueSize += queueSize

		groupStats[groupID] = map[string]interface{}{
			"queue_size": queueSize,
		}
	}

	stats["total_queue_size"] = totalQueueSize
	stats["group_stats"] = groupStats
	stats["is_closed"] = atomic.LoadInt32(&g.closed) == 1

	return stats
}

// Register register task handler
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
	g.mu.Lock()
	defer g.mu.Unlock()

	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.ctx, g.cancel = context.WithCancel(context.Background())

	for _, worker := range g.workers {
		go worker.runDequeue(g.ctx)
	}
}

// Stop 优雅停止队列
func (g *GQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return // 已经关闭
	}

	if g.cancel != nil {
		g.cancel()
	}
}

func (g *GQueue) Wait() {
	g.mu.RLock()
	workers := make([]*gqWorker, 0, len(g.workers))
	for _, worker := range g.workers {
		workers = append(workers, worker)
	}
	g.mu.RUnlock()

	for _, worker := range workers {
		worker.wait()
	}
}

type GQueueArray struct {
	sync.RWMutex
	queue *arrayqueue.Queue
}

func (q *GQueueArray) Clear() {
	q.Lock()
	q.queue.Clear()
	q.Unlock()
}

func (q *GQueueArray) Dequeue() (value interface{}, ok bool) {
	q.Lock()
	value, ok = q.queue.Dequeue()
	q.Unlock()
	return
}

func (q *GQueueArray) Empty() bool {
	q.RLock()
	empty := q.queue.Empty()
	q.RUnlock()
	return empty
}

func (q *GQueueArray) Enqueue(value interface{}) {
	q.Lock()
	q.queue.Enqueue(value)
	q.Unlock()
}

func (q *GQueueArray) Peek() (value interface{}, ok bool) {
	q.RLock()
	value, ok = q.queue.Peek()
	q.RUnlock()
	return
}

func (q *GQueueArray) Size() int {
	q.RLock()
	size := q.queue.Size()
	q.RUnlock()
	return size
}

func (q *GQueueArray) String() string {
	q.RLock()
	str := q.queue.String()
	q.RUnlock()
	return str
}

func (q *GQueueArray) Values() []interface{} {
	q.RLock()
	values := q.queue.Values()
	q.RUnlock()
	return values
}

type GQueueHandler func(task *GQueueTask) (err error)

// GQueueOption 队列配置选项
type GQueueOption struct {
	MaxConcurrent int            `json:"max_concurrent"` // 最大并发数
	MaxRetry      int            `json:"max_retry"`      // 最大重试次数
	Logger        util.ItfLogger `json:"-"`              // 日志器
	Groups        map[int]int    `json:"groups"`         // 任务分组配置
	BatchSize     int            `json:"batch_size"`     // 批处理大小
	WaitTimeout   time.Duration  `json:"wait_timeout"`   // 等待超时时间
}

// GQueueTask 队列任务结构
type GQueueTask struct {
	ID             string                          `json:"id"`         // 任务唯一ID
	Name           string                          `json:"name"`       // 任务名称
	Data           []byte                          `json:"data"`       // 任务数据
	Retried        int                             `json:"retried"`    // 已重试次数
	Group          int                             `json:"group"`      // 任务分组
	CreatedAt      time.Time                       `json:"created_at"` // 创建时间
	MaxRetry       int                             `json:"max_retry"`  // 最大重试次数
	RetryDelayFunc func(retried int) time.Duration `json:"-"`          // 重试延迟函数
	RunAt          time.Time                       `json:"run_at"`     // 执行时间
}

// Copy 复制任务，保持所有字段
func (t GQueueTask) Copy() *GQueueTask {
	return &GQueueTask{
		ID:             t.ID,
		Name:           t.Name,
		Data:           append([]byte(nil), t.Data...), // 深拷贝数据
		Retried:        t.Retried,
		Group:          t.Group,
		CreatedAt:      t.CreatedAt,
		MaxRetry:       t.MaxRetry,
		RetryDelayFunc: t.RetryDelayFunc,
		RunAt:          t.RunAt,
	}
}

// IsExpired 检查任务是否过期
func (t GQueueTask) IsExpired(timeout time.Duration) bool {
	return time.Since(t.CreatedAt) > timeout
}

// String 任务的字符串表示
func (t GQueueTask) String() string {
	return fmt.Sprintf("Task{ID:%s, Name:%s, Group:%d, Retried:%d}",
		t.ID, t.Name, t.Group, t.Retried)
}

type GQueueTaskOpt func(task *GQueueTask)

type Job func()

type gqWorker struct {
	runner func(fn func())

	queue queues.Queue

	logger util.ItfLogger

	maxRetry int

	gq *GQueue

	cond *sync.Cond
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
	w.cond.L.Lock()
	w.queue.Enqueue(task)
	w.cond.Signal()
	w.cond.L.Unlock()
}

func (w *gqWorker) run(task GQueueTask) {
	handler, ok := w.gq.handlers.Load(task.Name)
	if !ok {
		return
	}
	w.runner(func() {
		w.runTask(task, handler.(GQueueHandler))
	})
}

func (w *gqWorker) runDequeue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var tasks []GQueueTask
			for i := 0; i < defaultBatchSize; i++ {
				task, ok := w.dequeue()
				if !ok {
					break
				}
				tasks = append(tasks, task)
			}
			if len(tasks) == 0 {
				w.cond.L.Lock()
				w.cond.Wait()
				w.cond.L.Unlock()
				continue
			}
			for _, task := range tasks {
				if task.RunAt.After(time.Now()) {
					time.AfterFunc(time.Until(task.RunAt), func() {
						w.run(task)
					})
					continue
				}
				w.run(task)
			}
		}
	}
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
		w.logger.Errorf(
			"task %s retried %d times(max retry %d), but still failed; Error: %s; Data: %s",
			t.Name, t.Retried, maxRetryNumber, err.Error(), string(t.Data),
		)
		return
	}
	task := t.Copy()
	task.Retried++
	if task.RetryDelayFunc == nil {
		task.RetryDelayFunc = defaultQueueRetryDelayFunc
	}
	task.RunAt = time.Now().Add(task.RetryDelayFunc(task.Retried))
	w.enqueue(*task)
}

func (w *gqWorker) wait() {
	for {
		_, ok := w.dequeue()
		if !ok {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// EnqueueGq 将任务加入全局队列，带错误处理
func EnqueueGq(task string, payload interface{}, opt ...GQueueTaskOpt) {
	GetGQServer().Enqueue(NewGQTask(task, payload, opt...))
}

// GetGQServer 获取全局队列服务器实例，使用单例模式
func GetGQServer(opts ...GQueueOption) *GQueue {
	opt := GQueueOption{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	return util.LoadSingle(func() *GQueue {
		return NewGQueue(opt)
	})
}

// NewGQTask 创建新任务，优化数据处理
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

// NewGQueue 创建新的队列实例，优化初始化逻辑
func NewGQueue(opt1 GQueueOption) *GQueue {
	opt := DefaultGQOptionFunc()
	mergeConfigs(&opt, &opt1)

	// 验证和设置默认值
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
	if opt.WaitTimeout <= 0 {
		opt.WaitTimeout = defaultWorkerWaitTimeout
	}

	// 设置默认分组
	if len(opt.Groups) == 0 {
		opt.Groups = map[int]int{
			1: 6,
			2: 3,
			3: 1,
		}
	}

	// 确保至少有一个默认分组
	if _, ok := opt.Groups[1]; !ok {
		opt.Groups[1] = 6
	}
	if opt.Groups[1] < 1 {
		opt.Groups[1] = 6
	}

	g := &GQueue{
		opt:      opt,
		handlers: sync.Map{},
		workers:  make(map[int]*gqWorker),
	}

	// 创建工作器
	for groupID, percent := range opt.Groups {
		limit := getNumberFromPercent(opt.MaxConcurrent, percent)
		g.workers[groupID] = newGQWorker(limit, opt.MaxRetry, opt.Logger, g)
	}

	return g
}

func NewGQueueArray() *GQueueArray {
	return &GQueueArray{queue: arrayqueue.New()}
}

// NewGQueueTaskBytes 从字节数据创建任务，添加ID和时间戳
func NewGQueueTaskBytes(name string, data []byte, opt ...GQueueTaskOpt) GQueueTask {
	if name == "" {
		panic("task name cannot be empty")
	}

	now := time.Now()
	task := GQueueTask{
		ID:        generateTaskID(),
		Name:      name,
		Data:      data,
		Retried:   0,
		Group:     1,
		CreatedAt: now,
		MaxRetry:  -1, // -1 表示使用全局设置
		RunAt:     now,
	}

	// 应用选项
	for _, o := range opt {
		if o != nil {
			o(&task)
		}
	}

	// 设置默认重试延迟函数
	if task.RetryDelayFunc == nil {
		task.RetryDelayFunc = defaultQueueRetryDelayFunc
	}

	return task
}

// StartGQServer 启动全局队列服务器
func StartGQServer(hMap GQHandlerMap) {
	server := GetGQServer()

	// 注册所有处理器
	for taskName, handler := range hMap {
		server.Register(taskName, handler)
	}

	server.StartServer()
}

func WithGQueueTaskGroup(group int) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.Group = group
	}
}

// WithGQueueTaskID 设置任务ID
func WithGQueueTaskID(id string) GQueueTaskOpt {
	return func(task *GQueueTask) {
		if id != "" {
			task.ID = id
		}
	}
}

func WithGQueueTaskMaxRetry(maxRetry int) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.MaxRetry = maxRetry
	}
}

func WithGQueueTaskRetryDelayFunc(retryDelayFunc func(retried int) time.Duration) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.RetryDelayFunc = retryDelayFunc
	}
}

func WithGQueueTaskRunAt(runAt time.Time) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.RunAt = runAt
	}
}

// WrapGQHandler 包装任意函数为队列处理器
func WrapGQHandler(fn any) GQueueHandler {
	return func(task *GQueueTask) (err error) {
		return util.ExecSingleArgHandler(task.Data, fn)
	}
}

// generateTaskID 生成唯一的任务ID
func generateTaskID() string {
	return util.GenNanoid(21)
}

// calculateWorkerCount 根据百分比计算工作器数量
func getNumberFromPercent(total, percent int) int {
	if percent > maxGroupPercent || percent < minGroupPercent {
		percent = minGroupPercent
	}
	v := total * percent / maxGroupPercent
	if v < 1 {
		v = 1
	}
	return v
}

// newGQWorker 创建新的队列工作器，采用阻塞策略确保任务不丢失
func newGQWorker(limit int, maxRetry int, logger util.ItfLogger, gq *GQueue) *gqWorker {
	if limit <= 0 {
		limit = 1
	}

	// 使用适当大小的缓冲通道，平衡内存使用和性能
	bufferSize := limit
	if bufferSize > 100 {
		bufferSize = 100 // 限制最大缓冲区大小，避免内存过度使用
	}
	jobs := make(chan Job, bufferSize)

	// 启动工作协程
	for i := 0; i < limit; i++ {
		go workerFunc(jobs, logger)
	}

	runner := func(fn func()) {
		// 直接阻塞等待，这是队列系统的核心特性
		// 当工作器繁忙时，新任务会等待，实现背压控制
		jobs <- fn
	}

	return &gqWorker{
		runner:   runner,
		queue:    NewGQueueArray(),
		logger:   logger,
		maxRetry: maxRetry,
		gq:       gq,
		cond:     sync.NewCond(&sync.Mutex{}),
	}
}

// workerFunc 工作器函数，处理作业，添加日志记录
func workerFunc(jobs <-chan Job, logger util.ItfLogger) {
	for job := range jobs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// 工作器级别的恢复，避免单个任务崩溃影响整个工作器
					logger.Errorf("worker panic recovered: %v", r)
				}
			}()
			job()
		}()
	}
}
