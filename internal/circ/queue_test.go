package circ_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/jackc/puddle/v2/internal/circ"
	"github.com/stretchr/testify/require"
)

func TestQueue_EnqueueDequeue(t *testing.T) {
	r := require.New(t)

	q := circ.NewQueue[int](10)
	r.Equal(10, q.Cap())

	for i := 0; i < 10; i++ {
		q.Enqueue(i)
		r.Equal(i+1, q.Len())
	}

	r.Panics(func() { q.Enqueue(10) })
	r.Equal(10, q.Len())

	for i := 0; i < 10; i++ {
		j := q.Dequeue()
		r.Equal(i, j)

		r.Equal(10-i-1, q.Len())
	}

	r.Panics(func() { q.Dequeue() })
	r.Equal(0, q.Len())
}

func TestQueue_EnqueueDequeueOverflow(t *testing.T) {
	r := require.New(t)

	q := circ.NewQueue[int](10)
	r.Equal(10, q.Cap())

	for i := 0; i < 10; i++ {
		q.Enqueue(i)
		r.Equal(i+1, q.Len())
	}

	r.Panics(func() { q.Enqueue(10) })
	r.Equal(10, q.Len())

	for i := 0; i < 5; i++ {
		j := q.Dequeue()
		r.Equal(i, j)

		r.Equal(10-i-1, q.Len())
	}

	for i := 10; i < 15; i++ {
		q.Enqueue(i)
		r.Equal(i-5+1, q.Len())
	}

	for i := 0; i < 10; i++ {
		j := q.Dequeue()
		r.Equal(i+5, j)

		r.Equal(10-i-1, q.Len())
	}

	r.Panics(func() { q.Dequeue() })
	r.Equal(0, q.Len())
}

func BenchmarkArrayAppend(b *testing.B) {
	arr := make([]int, 0, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arr = append(arr, i)
	}

	// Make sure that the Go compiler doesn't optimize writes above.
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(io.Discard, "%d\n", arr[i])
	}
}

func BenchmarkArrayWrite(b *testing.B) {
	arr := make([]int, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arr[i] = i
	}

	// Make sure that the Go compiler doesn't optimize writes above.
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(io.Discard, "%d\n", arr[i])
	}
}

func BenchmarkEnqueue(b *testing.B) {
	q := circ.NewQueue[int](b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
	}

	// Make sure that the Go compiler doesn't optimize writes above.
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(io.Discard, "%d\n", q.Dequeue())
	}
}

func BenchmarkChanWrite(b *testing.B) {
	// Chennels are another way how to represent a queue.
	ch := make(chan int, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}

	// Make sure that the Go compiler doesn't optimize writes above.
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(io.Discard, "%d\n", <-ch)
	}
}

func BenchmarkDequeue(b *testing.B) {
	q := circ.NewQueue[int](b.N)

	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
	}

	out := make([]int, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out[i] = q.Dequeue()
	}
}
