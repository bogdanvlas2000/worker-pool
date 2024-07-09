package worker_pool

import (
	"flag"
	"fmt"
	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sync"
	"time"
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
	done      chan bool

	resultQueue         chan Result
	remainingTasksCount chan int

	maxWorkersCount int
	workers         []*Worker

	logger logr.Logger
}

func NewWorkerPool(tasks []Task, maxWorkersCount int) *WorkerPool {
	opts := &zap.Options{
		Development: true,
		TimeEncoder: zapcore.TimeEncoderOfLayout(time.RFC3339Nano),
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := zap.New(zap.UseFlagOptions(opts)).WithName("pool")

	return &WorkerPool{
		wg:                  &sync.WaitGroup{},
		tasks:               tasks,
		resultQueue:         make(chan Result),
		remainingTasksCount: make(chan int),
		maxWorkersCount:     maxWorkersCount,
		logger:              logger,
	}
}

func (p *WorkerPool) Start() {
	p.initWorkers()

	defer p.timer()()
	p.taskQueue, p.done = p.dispatchTasks(p.tasks)

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
			ID:             i,
			logger:         p.logger.WithName(fmt.Sprintf("worker-%d", i)),
			taskQueue:      p.taskQueue,
			resultQueue:    p.resultQueue,
			done:           p.done,
			remainingTasks: p.remainingTasksCount,
			wg:             p.wg,
		}
		p.workers = append(p.workers, worker)
	}
	p.logger.Info("init workers", "count", count)
}

func (p *WorkerPool) timer() func() {
	start := time.Now()
	return func() {
		p.logger.WithName("timer").Info("completed", "time", time.Since(start))
	}
}

func (p *WorkerPool) dispatchTasks(tasks []Task) (chan Task, chan bool) {
	p.wg.Add(1)

	logger := p.logger.WithName("sender")
	taskQueue := make(chan Task, len(tasks))
	done := make(chan bool)

	go func() {
		defer p.wg.Done()

		for _, task := range tasks {
			logger.Info("send task", "ID", task.ID)
			taskQueue <- task
		}

		logger.Info("waiting until all tasks are done")
		if <-done {
			logger.Info("all tasks are done")
			close(taskQueue)
			logger.Info("closed taskQueue")
		}
	}()

	return taskQueue, done
}

func (p *WorkerPool) runWorkers(n int) {
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		worker := Worker{
			logger:         p.logger.WithName(fmt.Sprintf("worker-%d", i)),
			ID:             i,
			taskQueue:      p.taskQueue,
			resultQueue:    p.resultQueue,
			done:           p.done,
			remainingTasks: p.remainingTasksCount,
			wg:             p.wg,
		}
		worker.Start()
	}
}

func (p *WorkerPool) collectResults() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var results []Result

		logger := p.logger.WithName("receiver")
		for {
			logger.Info("attempting to receive result")
			result := <-p.resultQueue
			logger.Info("received result", "value", result.Value)

			results = append(results, result)

			remained := len(p.tasks) - len(results)

			logger.Info("remained tasks", "count", remained)
			p.remainingTasksCount <- remained

			if remained == 0 {
				logger.Info("all results received", "results", results)
				break
			}
		}
	}()
}
