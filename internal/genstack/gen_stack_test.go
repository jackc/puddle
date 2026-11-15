package genstack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func requirePopEmpty[T any](t testing.TB, s *GenStack[T]) {
	v, ok := s.Pop()
	require.False(t, ok)
	require.Zero(t, v)
}

func requirePop[T any](t testing.TB, s *GenStack[T], expected T) {
	v, ok := s.Pop()
	require.True(t, ok)
	require.Equal(t, expected, v)
}

func TestGenStack_Empty(t *testing.T) {
	s := NewGenStack[int]()
	requirePopEmpty(t, s)
}

func TestGenStack_SingleGen(t *testing.T) {
	r := require.New(t)
	s := NewGenStack[int]()

	s.Push(1)
	s.Push(2)
	r.Equal(2, s.Len())

	requirePop(t, s, 2)
	requirePop(t, s, 1)
	requirePopEmpty(t, s)
}

func TestGenStack_TwoGen(t *testing.T) {
	r := require.New(t)
	s := NewGenStack[int]()

	s.Push(3)
	s.Push(4)
	s.Push(5)
	r.Equal(3, s.Len())
	s.NextGen()
	r.Equal(3, s.Len())
	s.Push(6)
	s.Push(7)
	r.Equal(5, s.Len())

	requirePop(t, s, 5)
	requirePop(t, s, 4)
	requirePop(t, s, 3)
	requirePop(t, s, 7)
	requirePop(t, s, 6)
	requirePopEmpty(t, s)
}

func TestGenStack_MuptiGen(t *testing.T) {
	r := require.New(t)
	s := NewGenStack[int]()

	s.Push(10)
	s.Push(11)
	s.Push(12)
	r.Equal(3, s.Len())
	s.NextGen()
	r.Equal(3, s.Len())
	s.Push(13)
	s.Push(14)
	r.Equal(5, s.Len())
	s.NextGen()
	r.Equal(5, s.Len())
	s.Push(15)
	s.Push(16)
	s.Push(17)
	r.Equal(8, s.Len())

	requirePop(t, s, 12)
	requirePop(t, s, 11)
	requirePop(t, s, 10)
	requirePop(t, s, 14)
	requirePop(t, s, 13)
	requirePop(t, s, 17)
	requirePop(t, s, 16)
	requirePop(t, s, 15)
	requirePopEmpty(t, s)
}

// The oracle is a queue of literal generation stacks, independent of the
// implementation's two-stack representation and generation compaction.
func FuzzGenStackMatchesGenerations(f *testing.F) {
	// 0 = Push, 1 = Pop, 2 = NextGen.
	f.Add([]byte{0, 0, 2, 0, 1, 1, 1})
	f.Add([]byte{0, 0, 2, 0, 2, 0, 1, 2, 1, 1, 1})
	f.Add([]byte{2, 2, 0, 1, 2, 0, 2, 2, 1, 1})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 512 {
			t.Skip()
		}
		stack := NewGenStack[int]()
		generations := [][]int{nil}
		next, count := 0, 0
		pop := func() {
			for len(generations) > 1 && len(generations[0]) == 0 {
				generations = generations[1:]
			}
			got, ok := stack.Pop()
			if count == 0 {
				require.False(t, ok)
				require.Zero(t, got)
				return
			}
			generation := generations[0]
			want := generation[len(generation)-1]
			generations[0] = generation[:len(generation)-1]
			count--
			require.True(t, ok)
			require.Equal(t, want, got)
		}
		for _, operation := range operations {
			switch operation % 3 {
			case 0:
				next++
				stack.Push(next)
				last := len(generations) - 1
				generations[last] = append(generations[last], next)
				count++
			case 1:
				pop()
			case 2:
				stack.NextGen()
				generations = append(generations, nil)
			}
			require.Equal(t, count, stack.Len())
		}
		for count > 0 {
			pop()
			require.Equal(t, count, stack.Len())
		}
		pop()
	})
}
