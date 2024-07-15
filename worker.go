package worker_pool

import (
	"fmt"
	"github.com/go-logr/logr"
	"sync"
	"time"
)

type worker struct {
	ID     int
	logger logr.Logger

	tasksToExecute <-chan Task
	resultQueue    chan Result

	stopSignal chan struct{}

	wg *sync.WaitGroup
}

func (w *worker) start() {
	go func() {
		defer w.wg.Done()

		w.logger.Info("start")

	Loop:
		for {
			var task Task
			var ok bool

			w.logger.Info("attempting to receive task")
			select {
			case task, ok = <-w.tasksToExecute:
				if !ok {
					w.logger.Info("task queue is closed")
					break Loop
				}
				w.logger.Info("received task", "taskId", task.ID)
			case <-w.stopSignal:
				w.logger.Info("stop signal received")
				break Loop
			}

			w.logger.Info("executing task...", "taskId", task.ID)
			time.Sleep(time.Second)
			w.logger.Info("completed task", "taskId", task.ID)

			result := Result{
				Value: fmt.Sprintf("result of task %d", task.ID),
			}

			w.logger.Info("attempting to send result", "taskId", task.ID, "resultValue", result.Value)
			select {
			case w.resultQueue <- result:
				w.logger.Info("result sent", "taskId", task.ID, "resultValue", result.Value)
			case <-w.stopSignal:
				w.logger.Info("stop signal received")
				break
			}
		}

		w.logger.Info("stop")
	}()
}
