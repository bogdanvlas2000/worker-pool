package worker_pool

import (
	"fmt"
	stack "github.com/bogdanvlas2000/collections/stack"
	"github.com/go-logr/logr"
	"sync"
)

type Task struct {
	ID int
}

type Result struct {
	Value string
}

type WorkerPool struct {
	wg *sync.WaitGroup

	inputTasks     chan Task
	tasksToExecute chan Task
	resultQueue    chan Result

	waitingTasks   stack.Stack[Task]
	maxWorkerCount int
	workers        []*worker

	stopSignal chan struct{}

	logger logr.Logger
}

func NewWorkerPool(maxWorkersCount int, logger logr.Logger) *WorkerPool {
	if maxWorkersCount < 1 {
		maxWorkersCount = 1
	}

	logger.Info("create worker pool", "maxWorkerCount", maxWorkersCount)

	pool := WorkerPool{
		maxWorkerCount: maxWorkersCount,
		logger:         logger,
		wg:             &sync.WaitGroup{},
		inputTasks:     make(chan Task, 1),
		tasksToExecute: make(chan Task),
		resultQueue:    make(chan Result),
		stopSignal:     make(chan struct{}),
	}

	return &pool
}

func (p *WorkerPool) Start() <-chan Result {
	p.dispatch()
	return p.resultQueue
}

func (p *WorkerPool) Submit(task Task) {
	logger := p.logger.WithName("submit")
	logger.Info("send task to inputTasks", "taskId", task.ID)
	p.inputTasks <- task
}

func (p *WorkerPool) processWaitingTasks() (ok bool) {
	logger := p.logger.WithName("processWaitingTasks")

	waitingTask, ok := p.waitingTasks.Peek()

	if !ok {
		logger.Info("waitingTasks is empty")
		return true
	}

	var inputTask Task

	select {
	case inputTask, ok = <-p.inputTasks:
		if !ok {
			logger.Info("inputTasks closed")
			return false
		}
		logger.Info("push input task to waitingTasks", "taskId", inputTask.ID)
		p.waitingTasks.Push(inputTask)
	case p.tasksToExecute <- waitingTask:
		logger.Info("sent waiting task to tasksToExecute", "taskId", waitingTask)
		p.waitingTasks.Pop()
	}

	return true
}

// dispatch sends a submitted Task to a worker
func (p *WorkerPool) dispatch() {
	p.wg.Add(1)

	logger := p.logger.WithName("dispatch")

	go func() {
		//var workerCount int
		defer p.wg.Done()

	Loop:
		for {
			if p.waitingTasks.Size() > 0 {
				if !p.processWaitingTasks() {
					break Loop
				}
				continue
			}

			var inputTask Task
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

			logger.Info("input task received", "taskId", inputTask.ID)

			// if workerCount < maxWorkerCount init a new worker
			workersCount := len(p.workers)
			if workersCount < p.maxWorkerCount {
				logger.Info("workers limit not reached yet", "workersCount", workersCount)

				p.initNewWorker(workersCount)

				logger.Info("attempting to send task to tasksToExecute...", "taskId", inputTask.ID)
				p.tasksToExecute <- inputTask
			} else {
				logger.Info("workers limit reached", "workersCount", workersCount)
				logger.Info("push input task to waitingTasks", "taskId", inputTask.ID)
				p.waitingTasks.Push(inputTask)
			}
		}
		logger.Info("stop")
	}()
}

func (p *WorkerPool) initNewWorker(id int) {
	defer p.wg.Add(1)

	w := &worker{
		ID:             len(p.workers),
		logger:         p.logger.WithName(fmt.Sprintf("worker-%d", len(p.workers))),
		tasksToExecute: p.tasksToExecute,
		resultQueue:    p.resultQueue,
		stopSignal:     p.stopSignal,
		wg:             p.wg,
	}
	// add worker
	p.workers = append(p.workers, w)
	p.logger.WithName("initNewWorker").Info("created new worker", "ID", w.ID)
	// start worker
	w.start()
}

func (p *WorkerPool) Stop() {
	logger := p.logger.WithName("Stop")

	close(p.stopSignal)

	logger.Info("wait for all goroutines to close...", "workersCount", len(p.workers))
	//TODO: use wg.Add() and wg.Done() in goroutines
	p.wg.Wait()
	logger.Info("all goroutines were closed")
}
