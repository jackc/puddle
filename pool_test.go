package puddle_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/puddle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Counter struct {
	mutex sync.Mutex
	n     int
}

// Next increments the counter and returns the value
func (c *Counter) Next() int {
	c.mutex.Lock()
	c.n += 1
	n := c.n
	c.mutex.Unlock()
	return n
}

// Value returns the counter
func (c *Counter) Value() int {
	c.mutex.Lock()
	n := c.n
	c.mutex.Unlock()
	return n
}

func createCreateResourceFunc() (puddle.CreateFunc, *Counter) {
	var c Counter
	f := func(ctx context.Context) (interface{}, error) {
		return c.Next(), nil
	}
	return f, &c
}

func createCreateResourceFuncWithNotifierChan() (puddle.CreateFunc, *Counter, chan int) {
	ch := make(chan int)
	var c Counter
	f := func(ctx context.Context) (interface{}, error) {
		n := c.Next()

		// Because the tests will not read from ch until after the create function f returns.
		go func() { ch <- n }()

		return n, nil
	}
	return f, &c, ch
}

func createCloseResourceFuncWithNotifierChan() (puddle.CloseFunc, *Counter, chan int) {
	ch := make(chan int)
	var c Counter
	f := func(interface{}) {
		n := c.Next()

		// Because the tests will not read from ch until after the close function f returns.
		go func() { ch <- n }()
	}
	return f, &c, ch
}

func stubCloseRes(interface{}) {}

func waitForRead(ch chan int) bool {
	select {
	case <-ch:
		return true
	case <-time.NewTimer(time.Second).C:
		return false
	}
}

func TestPoolAcquireCreatesResourceWhenNoneAvailable(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	defer pool.Close()

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())
	res.Release()
}

func TestPoolAcquireDoesNotCreatesResourceWhenItWouldExceedMaxSize(t *testing.T) {
	createFunc, createCounter := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	pool.SetMaxSize(1)

	wg := &sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				res, err := pool.Acquire(context.Background())
				assert.NoError(t, err)
				assert.Equal(t, 1, res.Value())
				res.Release()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, createCounter.Value())
	assert.Equal(t, 1, pool.Size())
}

func TestPoolAcquireReturnsErrorFromFailedResourceCreate(t *testing.T) {
	errCreateFailed := errors.New("create failed")
	createFunc := func(ctx context.Context) (interface{}, error) {
		return nil, errCreateFailed
	}
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, errCreateFailed, err)
	assert.Nil(t, res)
}

func TestPoolAcquireReusesResources(t *testing.T) {
	createFunc, createCounter := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	res.Release()

	res, err = pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	res.Release()

	assert.Equal(t, 1, createCounter.Value())
}

func TestPoolAcquireContextAlreadyCanceled(t *testing.T) {
	createFunc := func(ctx context.Context) (interface{}, error) {
		panic("should never be called")
	}
	pool := puddle.NewPool(createFunc, stubCloseRes)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := pool.Acquire(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolAcquireContextCanceledDuringCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	timeoutChan := time.After(1 * time.Second)

	var createCalls Counter
	createFunc := func(ctx context.Context) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeoutChan:
		}
		return createCalls.Next(), nil
	}
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Acquire(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolCloseClosesAllAvailableResources(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()

	var closeCalls Counter
	closeFunc := func(interface{}) {
		closeCalls.Next()
	}

	p := puddle.NewPool(createFunc, closeFunc)

	resources := make([]*puddle.Resource, 4)
	for i := range resources {
		var err error
		resources[i], err = p.Acquire(context.Background())
		require.Nil(t, err)
	}

	for _, res := range resources {
		res.Release()
	}

	p.Close()

	assert.Equal(t, len(resources), closeCalls.Value())
}

func TestPoolReleaseClosesResourcePoolIsAlreadyClosed(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	p := puddle.NewPool(createFunc, closeFunc)

	resources := make([]*puddle.Resource, 4)
	for i := range resources {
		var err error
		resources[i], err = p.Acquire(context.Background())
		require.Nil(t, err)
	}

	p.Close()
	assert.Equal(t, 0, closeCalls.Value())

	for _, res := range resources {
		res.Release()
	}

	waitForRead(closeCallsChan)
	waitForRead(closeCallsChan)
	waitForRead(closeCallsChan)
	waitForRead(closeCallsChan)

	assert.Equal(t, len(resources), closeCalls.Value())
}

func TestResourceDestroyRemovesResourceFromPool(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	assert.Equal(t, 1, pool.Size())
	res.Destroy()
	assert.Equal(t, 0, pool.Size())
}

func TestPoolAcquireReturnsErrorWhenPoolIsClosed(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	pool.Close()

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
	assert.Nil(t, res)
}

func BenchmarkPoolAcquireAndRelease(b *testing.B) {
	benchmarks := []struct {
		poolSize    int
		clientCount int
	}{
		{8, 1},
		{8, 4},
		{8, 8},
		{8, 16},
		{8, 32},
		{8, 64},
		{8, 64},
		{8, 128},
		{8, 256},
		{8, 512},
		{8, 1024},
		{8, 2048},

		{64, 1},
		{64, 4},
		{64, 8},
		{64, 16},
		{64, 32},
		{64, 64},
		{64, 64},
		{64, 128},
		{64, 256},
		{64, 512},
		{64, 1024},
		{64, 2048},

		{512, 1},
		{512, 4},
		{512, 8},
		{512, 16},
		{512, 32},
		{512, 64},
		{512, 64},
		{512, 128},
		{512, 256},
		{512, 512},
		{512, 1024},
		{512, 2048},
	}

	for _, bm := range benchmarks {
		name := fmt.Sprintf("PoolSize=%d/ClientCount=%d", bm.poolSize, bm.clientCount)

		b.Run(name, func(b *testing.B) {
			wg := &sync.WaitGroup{}

			createFunc, _ := createCreateResourceFunc()
			pool := puddle.NewPool(createFunc, stubCloseRes)
			pool.SetMaxSize(bm.poolSize)

			for i := 0; i < bm.clientCount; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for j := 0; j < b.N; j++ {
						res, err := pool.Acquire(context.Background())
						if err != nil {
							b.Fatal(err)
						}
						res.Release()
					}
				}()
			}

			wg.Wait()
		})
	}
}
