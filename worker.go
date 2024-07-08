package worker_pool

import (
	"fmt"
	"github.com/go-logr/logr"
	"sync"
	"time"
)

type Worker struct {
	logger      logr.Logger
	ID          int
	taskQueue   <-chan Task
	resultQueue chan Result

	remainingTasks chan int
	done           chan bool

	wg *sync.WaitGroup
}

func (w *Worker) Start() {
	go func() {
		defer w.wg.Done()
		w.logger.Info("start")

		for {
			w.logger.Info("attempting to receive task")
			task, ok := <-w.taskQueue
			if !ok {
				w.logger.Info("tasks chan is closed")
				break
			}
			w.logger.Info("received task", "taskId", task.ID)

			time.Sleep(time.Second * 1)
			w.logger.Info("completed task", "taskId", task.ID)

			result := Result{
				Value: fmt.Sprintf("result of task %d", task.ID),
			}

			w.logger.Info("sends result", "taskId", task.ID, "resultValue", result.Value)
			w.resultQueue <- result

			w.logger.Info("attempting to check remaining tasks")
			remained := <-w.remainingTasks
			w.logger.Info("remaining tasks", "count", remained)

			if remained == 0 {
				w.logger.Info("all tasks are done")
				w.done <- true
				close(w.done)
				break
			}
		}
		w.logger.Info("stop", "id", w.ID)
	}()
}
