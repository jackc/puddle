package circ

type Queue[T any] struct {
	arr   []T
	begin int
	len   int
}

func NewQueue[T any](capacity int) *Queue[T] {
	return &Queue[T]{
		// TODO: Do not preallocate whole capacity of Go upstream
		// accepts this: https://github.com/golang/go/issues/55978
		arr: make([]T, capacity),
	}
}

func (q *Queue[T]) Cap() int { return len(q.arr) }
func (q *Queue[T]) Len() int { return q.len }

func (q *Queue[T]) end() int {
	e := q.begin + q.len
	if l := len(q.arr); e >= l {
		e -= l
	}

	return e
}

func (q *Queue[T]) Enqueue(elem T) {
	if q.len == len(q.arr) {
		panic("enqueue: queue is full")
	}

	q.arr[q.end()] = elem
	q.len++
}

func (q *Queue[T]) Dequeue() T {
	if q.len < 1 {
		panic("dequeue: queue is empty")
	}

	elem := q.arr[q.begin]

	// Avoid memory leaks if T is pointer or contains pointers.
	var zeroVal T
	q.arr[q.begin] = zeroVal

	q.len--
	q.begin++
	if q.begin == len(q.arr) {
		q.begin = 0
	}

	return elem
}
