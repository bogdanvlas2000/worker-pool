package worker_pool

import "testing"

func TestWorkerPool(t *testing.T) {
	tasks := []Task{}

	for i := 0; i < 20; i++ {
		tasks = append(tasks, Task{
			ID: i,
		})
	}

	pool := NewWorkerPool(tasks, 50)
	pool.Start()
}
