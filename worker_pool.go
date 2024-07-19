package workerpool

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/ngc4736/collections"
	linkedlist "github.com/ngc4736/collections/linked-list"
	"sync"
)

type WorkerPool[T any] struct {
	wg *sync.WaitGroup

	inputTasks     chan Task[T]
	tasksToExecute chan Task[T]
	resultQueue    chan T

	waitingTasks collections.Dequeue[Task[T]]

	maxWorkerCount int

	stopSignal chan struct{}

	logger logr.Logger
}

func NewWorkerPool[T any](maxWorkersCount int, logger logr.Logger) *WorkerPool[T] {
	if maxWorkersCount < 1 {
		maxWorkersCount = 1
	}

	logger.Info("create worker pool", "maxWorkerCount", maxWorkersCount)

	return &WorkerPool[T]{
		maxWorkerCount: maxWorkersCount,
		logger:         logger,
		waitingTasks:   linkedlist.New[Task[T]](),
		wg:             &sync.WaitGroup{},
		inputTasks:     make(chan Task[T]),
		tasksToExecute: make(chan Task[T]),
		resultQueue:    make(chan T),
		stopSignal:     make(chan struct{}),
	}
}

func (p *WorkerPool[T]) Start() (results <-chan T) {
	p.dispatch()
	return p.resultQueue
}

func (p *WorkerPool[T]) Submit(task Task[T]) {
	logger := p.logger.WithName("submit")
	logger.Info("send task to inputTasks", "task", task.String())
	p.inputTasks <- task
}

func (p *WorkerPool[T]) processWaitingTasks() (ok bool) {
	logger := p.logger.WithName("processWaitingTasks")

	//TODO: get element from head
	waitingTask, ok := p.waitingTasks.First()

	if !ok {
		logger.Info("waitingTasks is empty")
		return true
	}

	var inputTask Task[T]

	select {
	case inputTask, ok = <-p.inputTasks:
		if !ok {
			logger.Info("inputTasks closed")
			return false
		}
		logger.Info("push input task to waitingTasks", "task", inputTask.String())
		p.waitingTasks.Push(inputTask)
	case p.tasksToExecute <- waitingTask:
		logger.Info("sent waiting task to tasksToExecute", "task", waitingTask.String())
		p.waitingTasks.PullFirst()
	}

	return true
}

// dispatch sends a submitted Task to a worker
func (p *WorkerPool[T]) dispatch() {
	p.wg.Add(1)

	logger := p.logger.WithName("dispatch")

	go func() {
		//var workerCount int
		defer p.wg.Done()

		workersCount := 0

	Loop:
		for {
			if p.waitingTasks.Size() > 0 {
				if !p.processWaitingTasks() {
					break Loop
				}
				continue
			}

			var inputTask Task[T]
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

			logger.Info("input task received", "task", inputTask.String())

			// if workerCount < maxWorkerCount init a new worker
			if workersCount < p.maxWorkerCount {
				logger.Info("workers limit not reached yet", "workersCount", workersCount)

				p.worker(workersCount)
				workersCount++

				logger.Info("attempting to send task to tasksToExecute...", "task", inputTask.String())
				p.tasksToExecute <- inputTask
			} else {
				logger.Info("workers limit reached", "workersCount", workersCount)
				logger.Info("push input task to waitingTasks", "task", inputTask.String())
				p.waitingTasks.Push(inputTask)
			}
		}
		logger.Info("stop")
	}()
}

func (p *WorkerPool[T]) worker(id int) {
	//TODO: after worker completes task, mark it as idle
	// idle worker should wait for a task some time, and then timeout
	p.wg.Add(1)
	logger := p.logger.WithName(fmt.Sprintf("worker-%d", id))

	go func() {
		defer p.wg.Done()

		logger.Info("start")

	Loop:
		for {
			var task Task[T]
			var ok bool

			logger.Info("attempting to receive task")
			select {
			case task, ok = <-p.tasksToExecute:
				if !ok {
					logger.Info("task queue is closed")
					break Loop
				}
				logger.Info("received task", "task", task.String())
			case <-p.stopSignal:
				logger.Info("stop signal received")
				break Loop
			}

			logger.Info("executing task...", "task", task.String())
			result := task.Execute()
			logger.Info("completed task", "task", task.String())

			logger.Info("attempting to send result", "task", task.String(), "result", result)
			select {
			case p.resultQueue <- result:
				logger.Info("result sent", "task", task.String(), "result", result)
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
