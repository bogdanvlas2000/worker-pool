package workerpool

import "fmt"

//TODO: job can return error

type Job[T any] func() (T, error)

type Task[T any] struct {
	id  int
	job Job[T]
}

func NewTask[T any](id int, job Job[T]) Task[T] {
	return Task[T]{
		id:  id,
		job: job,
	}
}

func (t Task[T]) Execute() (T, error) {
	return t.job()
}

func (t Task[T]) String() string {
	return fmt.Sprintf("task-%d", t.id)
}
