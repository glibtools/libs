package queue

import (
	"runtime"
	"sync"
	"time"

	"github.com/andeya/goutil"
	"github.com/emirpasic/gods/queues"
	"github.com/emirpasic/gods/queues/arrayqueue"

	"github.com/glibtools/libs/util"
)

const defaultQueueMaxRetry = 3

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

type GQHandlerMap map[string]GQueueHandler

type GQueue struct {
	opt GQueueOption

	workers map[int]*gqWorker

	handlers map[string]GQueueHandler
}

func (g *GQueue) Enqueue(task GQueueTask) {
	worker, ok := g.workers[task.Group]
	if !ok {
		worker = g.workers[1]
	}
	worker.enqueue(task)
}

// Register register task handler
func (g *GQueue) Register(task string, handler GQueueHandler) {
	_, ok := g.handlers[task]
	if ok {
		return
	}
	g.handlers[task] = handler
}

func (g *GQueue) StartServer() {
	for _, worker := range g.workers {
		go worker.runDequeue()
	}
}

func (g *GQueue) Wait() {
	for _, worker := range g.workers {
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

type GQueueOption struct {
	MaxConcurrent int

	MaxRetry int

	Logger util.ItfLogger
	/**
	 * task groups
	 * key: group id
	 * value: the percentage of workers to be used for this group
	 */
	Groups map[int]int
}

type GQueueTask struct {
	Name           string `json:"name,omitempty"`
	Data           []byte `json:"data,omitempty"`
	Retried        int    `json:"retried,omitempty"`
	Group          int    `json:"group,omitempty"`
	maxRetry       int
	retryDelayFunc func(retried int) time.Duration
	runAt          time.Time
}

func (t GQueueTask) Copy() *GQueueTask {
	return &GQueueTask{
		Name:           t.Name,
		Data:           t.Data,
		Retried:        t.Retried,
		Group:          t.Group,
		maxRetry:       t.maxRetry,
		retryDelayFunc: t.retryDelayFunc,
		runAt:          t.runAt,
	}
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
	handler, ok := w.gq.handlers[task.Name]
	if !ok {
		return
	}
	w.runner(func() {
		w.runTask(task, handler)
	})
}

func (w *gqWorker) runDequeue() {
	for {
		var tasks []GQueueTask
		for i := 0; i < 10; i++ {
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
			if task.runAt.After(time.Now()) {
				time.AfterFunc(time.Until(task.runAt), func() {
					w.run(task)
				})
				continue
			}
			w.run(task)
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
	if t.Retried >= w.maxRetry || w.maxRetry < 1 {
		w.logger.Errorf(
			"task %s retried %d times, but still failed; Error: %s; Data: %s",
			t.Name, t.Retried, err.Error(), string(t.Data),
		)
		return
	}
	task := t.Copy()
	task.Retried++
	if task.retryDelayFunc == nil {
		task.retryDelayFunc = defaultQueueRetryDelayFunc
	}
	task.runAt = time.Now().Add(task.retryDelayFunc(task.Retried))
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

func EnqueueGq(task string, payload interface{}, opt ...GQueueTaskOpt) {
	GetGQServer().Enqueue(NewGQTask(task, payload, opt...))
}

func GetGQServer(opts ...GQueueOption) (server *GQueue) {
	opt := GQueueOption{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	return util.LoadSingle(func() *GQueue {
		return NewGQueue(opt)
	})
}

func NewGQTask(task string, payload interface{}, opt ...GQueueTaskOpt) GQueueTask {
	var data []byte
	switch v := payload.(type) {
	case nil:
		data = nil
	case []byte:
		data = v
	default:
		data, _ = util.Marshal(payload)
	}
	return NewGQueueTaskBytes(task, data, opt...)
}

func NewGQueue(opt1 GQueueOption) *GQueue {
	opt := DefaultGQOptionFunc()
	mergeConfigs(&opt, &opt1)
	if opt.MaxConcurrent <= 0 {
		opt.MaxConcurrent = runtime.NumCPU()
	}
	if opt.MaxRetry < 1 {
		opt.MaxRetry = defaultQueueMaxRetry
	}
	if opt.Logger == nil {
		opt.Logger = util.ZapLogger("queue")
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
	g := &GQueue{
		opt:      opt,
		handlers: make(map[string]GQueueHandler),
	}
	var workers = make(map[int]*gqWorker)
	for k, v := range opt.Groups {
		limit := getNumberFromPercent(opt.MaxConcurrent, v)
		workers[k] = newGQWorker(
			limit,
			opt.MaxRetry,
			opt.Logger,
			g,
		)
	}
	g.workers = workers
	return g
}

func NewGQueueArray() *GQueueArray {
	return &GQueueArray{queue: arrayqueue.New()}
}

func NewGQueueTaskBytes(name string, data []byte, opt ...GQueueTaskOpt) GQueueTask {
	task := GQueueTask{
		Name:    name,
		Data:    data,
		Retried: 0,
		Group:   1,
	}
	for _, o := range opt {
		o(&task)
	}
	if task.retryDelayFunc == nil {
		task.retryDelayFunc = defaultQueueRetryDelayFunc
	}
	if task.runAt.IsZero() {
		task.runAt = time.Now()
	}
	return task
}

func StartGQServer(hMap GQHandlerMap) {
	server := GetGQServer()
	for task, handler := range hMap {
		server.Register(task, handler)
	}
	server.StartServer()
}

func WithGQueueTaskGroup(group int) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.Group = group
	}
}

func WithGQueueTaskMaxRetry(maxRetry int) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.maxRetry = maxRetry
	}
}

func WithGQueueTaskRetryDelayFunc(retryDelayFunc func(retried int) time.Duration) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.retryDelayFunc = retryDelayFunc
	}
}

func WithGQueueTaskRunAt(runAt time.Time) GQueueTaskOpt {
	return func(task *GQueueTask) {
		task.runAt = runAt
	}
}

func WrapGQHandler(fn any) GQueueHandler {
	return func(task *GQueueTask) (err error) {
		return util.ExecSingleArgHandler(task.Data, fn)
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

func newGQWorker(limit int, maxRetry int, logger util.ItfLogger, gq *GQueue) *gqWorker {
	jobs := make(chan Job, limit)
	for i := 0; i < limit; i++ {
		go worker(jobs)
	}
	runner := func(fn func()) {
		jobs <- fn
	}

	w := &gqWorker{
		runner:   runner,
		queue:    NewGQueueArray(),
		logger:   logger,
		maxRetry: maxRetry,
		gq:       gq,
		cond:     sync.NewCond(&sync.Mutex{}),
	}
	return w
}

func worker(jobs chan Job) {
	for job := range jobs {
		job()
	}
}
