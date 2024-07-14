package worker_pool

import (
	"fmt"
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

	resultQueue chan Result

	stopSignal chan struct{}

	workers        []*worker
	maxWorkerCount int

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
		inputTasks:     make(chan Task),
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

// dispatch assigns a submitted Task to a worker
func (p *WorkerPool) dispatch() {
	logger := p.logger.WithName("dispatch")

	go func() {
		//var workerCount int

	Loop:
		for {
			var task Task
			var ok bool

			logger.Info("attempting to receive input task...")
			select {
			case task, ok = <-p.inputTasks:
				// double-check stopSignal
				select {
				case <-p.stopSignal:
					logger.Info("stop signal received")
					break Loop
				default:
				}
				if !ok {
					logger.Info("inputTasks is closed")
					break Loop
				}
			case <-p.stopSignal:
				logger.Info("stop signal received")
				break Loop
			}

			logger.Info("input task received", "taskId", task.ID)

			// if workerCount < maxWorkerCount init a new worker
			if len(p.workers) < p.maxWorkerCount {
				workerName := fmt.Sprintf("worker-%d", len(p.workers))
				w := &worker{
					ID:             len(p.workers),
					logger:         p.logger.WithName(workerName),
					tasksToExecute: p.tasksToExecute,
					resultQueue:    p.resultQueue,
				}
				// add worker
				p.workers = append(p.workers, w)
				logger.Info("created new worker", "name", workerName)
				// start worker
				w.start()
			}

			logger.Info("attempting to send task to tasksToExecute...")
			p.tasksToExecute <- task
		}
	}()
}

func (p *WorkerPool) Stop() {
	logger := p.logger.WithName("Stop")

	logger.Info("stop signal received")
	close(p.stopSignal)

	logger.Info("wait for all goroutines to close")
	//TODO: use wg.Add() and wg.Done() in goroutines
	p.wg.Wait()
	logger.Info("all goroutines were closed")
}
