package puddle

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// Replay the skip-generation and reverse-merge model counterexamples. Reserving
// permits directly pauses ordinary acquires at the real boundary between
// acquireSem acquisition and p.mux admission, without timing or production hooks.
func TestAcquireAllIdlePreservesGenerationsAcrossReservations(t *testing.T) {
	next := 0
	pool, err := NewPool(&Config[int]{
		MaxSize: 3,
		Constructor: func(context.Context) (int, error) {
			next++
			return next, nil
		},
		Destructor: func(int) {},
	})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	for range 3 {
		require.NoError(t, pool.CreateResource(context.Background()))
	}
	require.True(t, pool.acquireSem.TryAcquire(2))
	reservations := 2
	t.Cleanup(func() {
		pool.mux.Lock()
		defer pool.mux.Unlock()
		if reservations > 0 {
			pool.acquireSem.Release(int64(reservations))
		}
	})
	track := func(res *Resource[int]) func() {
		var once sync.Once
		release := func() { once.Do(res.Release) }
		t.Cleanup(release)
		return release
	}

	// The first batch must leave 2 and 1 ahead of the newly released 3.
	first := pool.AcquireAllIdle()
	var releaseFirst []func()
	for _, res := range first {
		releaseFirst = append(releaseFirst, track(res))
	}
	require.Len(t, first, 1)
	require.Equal(t, 3, first[0].Value())
	releaseFirst[0]()

	// The second boundary merges old [1] and new [3]. Reversing that merge
	// would let the pending acquire take 3 before 1.
	second := pool.AcquireAllIdle()
	var releaseSecond []func()
	for _, res := range second {
		releaseSecond = append(releaseSecond, track(res))
	}
	require.Len(t, second, 1)
	require.Equal(t, 2, second[0].Value())
	releaseSecond[0]()

	for _, want := range []int{1, 3} {
		// Continue a caller that already reserved its permit. This is the
		// same idle-resource admission helper used by Acquire/TryAcquire.
		pool.mux.Lock()
		res := pool.tryAcquireIdleResource()
		pool.mux.Unlock()
		require.NotNil(t, res)
		reservations--
		release := track(res)
		require.Equal(t, want, res.Value())
		release()
	}
	remaining := pool.AcquireAllIdle()
	var values []int
	for _, res := range remaining {
		track(res)
		values = append(values, res.Value())
	}
	require.Equal(t, []int{3, 1, 2}, values)
}

func TestAcquireSemAllMatchesAvailablePermits(t *testing.T) {
	for capacity := 1; capacity <= 32; capacity++ {
		for available := 0; available <= capacity; available++ {
			for requested := 1; requested <= capacity+1; requested++ {
				sem := semaphore.NewWeighted(int64(capacity))
				require.True(t, sem.TryAcquire(int64(capacity-available)))
				got := acquireSemAll(sem, requested)
				want := min(available, requested)
				if got != want {
					t.Fatalf("capacity=%d available=%d requested=%d: got %d, want %d",
						capacity, available, requested, got, want)
				}
				// Check actual semaphore consumption, not only the return value.
				require.True(t, sem.TryAcquire(int64(available-got)))
				require.False(t, sem.TryAcquire(1))
				sem.Release(int64(capacity))
			}
		}
	}
}
