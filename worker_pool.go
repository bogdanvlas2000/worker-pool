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

	tasks []Task

	taskQueue chan Task
	//done      chan bool

	resultQueue chan Result
	stopCh      chan struct{}

	maxWorkersCount int
	workers         []*Worker

	logger logr.Logger
}

func NewWorkerPool(tasks []Task, maxWorkersCount int, logger logr.Logger) (*WorkerPool, error) {
	logger.Info("init worker pool", "tasksCount", len(tasks), "maxWorkersCount", maxWorkersCount)
	return &WorkerPool{
		wg:              &sync.WaitGroup{},
		tasks:           tasks,
		resultQueue:     make(chan Result),
		maxWorkersCount: maxWorkersCount,
		logger:          logger,
		stopCh:          make(chan struct{}),
	}, nil
}

func (p *WorkerPool) Start() {
	p.initWorkers()

	p.taskQueue = p.dispatchTasks(p.tasks)

	p.runWorkers(p.maxWorkersCount)

	p.collectResults()
	p.wg.Wait()
}

func (p *WorkerPool) initWorkers() {
	p.workers = []*Worker{}

	var count int
	if len(p.tasks) < p.maxWorkersCount {
		count = len(p.tasks)
	} else {
		count = p.maxWorkersCount
	}

	for i := 0; i < count; i++ {
		worker := &Worker{
			ID:          i,
			logger:      p.logger.WithName(fmt.Sprintf("worker-%d", i)),
			taskQueue:   p.taskQueue,
			resultQueue: p.resultQueue,
			wg:          p.wg,
		}
		p.workers = append(p.workers, worker)
	}
	p.logger.Info("init workers", "count", count)
}

func (p *WorkerPool) dispatchTasks(tasks []Task) chan Task {
	p.wg.Add(1)

	logger := p.logger.WithName("sender")
	taskQueue := make(chan Task, len(tasks))

	go func() {
		defer p.wg.Done()
		defer close(taskQueue)

		for _, task := range tasks {
			logger.Info("send task", "ID", task.ID)
			taskQueue <- task
		}

		logger.Info("waiting until all tasks are done")
		<-p.stopCh
		logger.Info("stop")
	}()

	return taskQueue
}

func (p *WorkerPool) runWorkers(n int) {
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		worker := Worker{
			logger:      p.logger.WithName(fmt.Sprintf("worker-%d", i)),
			ID:          i,
			taskQueue:   p.taskQueue,
			resultQueue: p.resultQueue,
			wg:          p.wg,
		}
		worker.Start()
	}
}

func (p *WorkerPool) collectResults() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.stopCh)

		var results []Result

		logger := p.logger.WithName("receiver")

		logger.Info("start")

		for {
			logger.Info("attempting to receive result")

			var result Result
			select {
			case result = <-p.resultQueue:
				logger.Info("received result", "value", result.Value)
			case <-p.stopCh:
				logger.Info("stop signal received")
				break
			}

			results = append(results, result)

			remained := len(p.tasks) - len(results)

			if remained == 0 {
				logger.Info("all results received", "results", results)
				break
			}
		}
		logger.Info("stop")
	}()
}
