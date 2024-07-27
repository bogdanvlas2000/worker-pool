package workerpool

type Dequeue[T any] interface {
	Size() int
	Push(value T)
	Pop() (T, bool)
	Peek() (T, bool)
	PullFirst() (T, bool)
	First() (T, bool)
}
