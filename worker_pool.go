package workerpool

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/ngc4736/collections"
	"github.com/ngc4736/collections/linkedlist"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"sync"
	"sync/atomic"
)

const DefaultMaxWorkersCount = 100

type task[T any] func() (T, error)
type Opt[T any] func(*WorkerPool[T])

type WorkerPool[T any] struct {
	wg *sync.WaitGroup

	inputTasks     chan task[T]
	tasksToExecute chan task[T]
	resultQueue    chan T
	errorQueue     chan error

	waitingTasks collections.Dequeue[task[T]]

	workerCount    atomic.Uint32
	maxWorkerCount uint32

	stopSignal chan struct{}

	logger logr.Logger
}

func New[T any](opts ...Opt[T]) *WorkerPool[T] {

	wp := &WorkerPool[T]{
		maxWorkerCount: DefaultMaxWorkersCount,
		logger:         defaultLogger(),
		workerCount:    atomic.Uint32{},
		wg:             &sync.WaitGroup{},
		waitingTasks:   linkedlist.New[task[T]](),
		inputTasks:     make(chan task[T]),
		tasksToExecute: make(chan task[T]),
		resultQueue:    make(chan T),
		errorQueue:     make(chan error),
		stopSignal:     make(chan struct{}),
	}

	for _, o := range opts {
		o(wp)
	}

	wp.logger.Info("created worker pool", "maxWorkerCount", wp.maxWorkerCount)
	return wp
}

func defaultLogger() logr.Logger {
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
		zapcore.Lock(os.Stdout),
		zap.LevelEnablerFunc(func(level zapcore.Level) bool {
			return level == zapcore.FatalLevel
		}),
	)
	silentLogger := zap.New(core)
	defer silentLogger.Sync()

	return zapr.NewLogger(silentLogger)
}

func WithMaxWorkerCount[T any](maxWorkerCount uint32) func(pool *WorkerPool[T]) {
	return func(wp *WorkerPool[T]) {
		wp.maxWorkerCount = maxWorkerCount
	}
}

func WithLogger[T any](logger logr.Logger) func(pool *WorkerPool[T]) {
	return func(wp *WorkerPool[T]) {
		wp.logger = logger
	}
}

func (p *WorkerPool[T]) MaxWorkerCount() uint32 {
	return p.maxWorkerCount
}

func (p *WorkerPool[T]) Logger() logr.Logger {
	return p.logger
}

func (p *WorkerPool[T]) Start() (results <-chan T, errors <-chan error) {
	p.dispatch()
	return p.resultQueue, p.errorQueue
}

func (p *WorkerPool[T]) Submit(task func() (T, error)) {
	logger := p.logger.WithName("submit")
	logger.Info("send task to inputTasks")
	p.inputTasks <- task
}

func (p *WorkerPool[T]) processWaitingTasks() (ok bool) {
	logger := p.logger.WithName("processWaitingTasks")

	waitingTask, ok := p.waitingTasks.First()

	if !ok {
		logger.Info("waitingTasks is empty")
		return true
	}

	var inputTask task[T]

	select {
	case inputTask, ok = <-p.inputTasks:
		if !ok {
			logger.Info("inputTasks closed")
			return false
		}
		logger.Info("push input task to waitingTasks")
		p.waitingTasks.Push(inputTask)
	case p.tasksToExecute <- waitingTask:
		logger.Info("sent waiting task to tasksToExecute")
		p.waitingTasks.PullFirst()
	}

	return true
}

// dispatch sends a submitted Task to a worker
func (p *WorkerPool[T]) dispatch() {
	p.wg.Add(1)

	logger := p.logger.WithName("dispatch")

	go func() {
		defer p.wg.Done()

	Loop:
		for {
			if p.waitingTasks.Size() > 0 {
				if !p.processWaitingTasks() {
					break Loop
				}
				continue
			}

			var inputTask task[T]
			var ok bool

			logger.Info("attempting to receive input task...")
			select {
			case inputTask, ok = <-p.inputTasks:
				// double-check stopSignal
				select {
				case <-p.stopSignal:
					logger.Info("stop signal received")
					break Loop
				default:
				}
				if !ok {
					logger.Info("inputTasks chan is closed")
					break Loop
				}
			case <-p.stopSignal:
				logger.Info("stop signal received")
				break Loop
			}

			logger.Info("input task received")

			// if workerCount < maxWorkerCount init a new worker
			if p.workerCount.Load() < p.maxWorkerCount {
				logger.Info("workers limit not reached yet", "workerCount", p.workerCount.Load())

				p.worker(p.workerCount.Load())
				p.workerCount.Add(1)

				logger.Info("attempting to send task to tasksToExecute...")
				p.tasksToExecute <- inputTask
			} else {
				logger.Info("workers limit reached", "workerCount", p.workerCount.Load())
				logger.Info("push input task to waitingTasks")
				p.waitingTasks.Push(inputTask)
			}
		}
		logger.Info("stop")
	}()
}

func (p *WorkerPool[T]) worker(id uint32) {
	//TODO: after worker completes task, mark it as idle
	// idle worker should wait for a task some time, and then timeout
	p.wg.Add(1)
	logger := p.logger.WithName(fmt.Sprintf("worker-%d", id))

	go func() {
		defer p.wg.Done()

		logger.Info("start")

	Loop:
		for {
			var task task[T]
			var ok bool

			logger.Info("attempting to receive task")
			select {
			case task, ok = <-p.tasksToExecute:
				if !ok {
					logger.Info("task queue is closed")
					break Loop
				}
				logger.Info("received task")
			case <-p.stopSignal:
				logger.Info("stop signal received")
				break Loop
			}

			logger.Info("executing task...")
			result, err := task()
			if err != nil {
				//TODO: process error returned from task
				logger.Error(err, "task failed")
				p.errorQueue <- err
				continue
			}
			logger.Info("completed task")

			logger.Info("attempting to send result", "result", result)
			select {
			case p.resultQueue <- result:
				logger.Info("result sent", "result", result)
			case <-p.stopSignal:
				logger.Info("stop signal received")
				break
			}
		}

		logger.Info("stop")
	}()
}

func (p *WorkerPool[T]) Stop() {
	//TODO: implement graceful shutdown logic:
	// wait for workers to complete tasks in progress (with some timeout)
	// waiting tasks should be returned
	logger := p.logger.WithName("Stop")

	close(p.stopSignal)

	logger.Info("wait for all goroutines to close...")
	p.wg.Wait()
	logger.Info("all goroutines were closed")
}
