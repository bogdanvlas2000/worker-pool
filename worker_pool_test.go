package worker_pool

import "testing"

func TestWorkerPool(t *testing.T) {
	tasks := []Task{
		{ID: 0},
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}

	pool := NewWorkerPool(tasks, 4)
	pool.Start()
}
