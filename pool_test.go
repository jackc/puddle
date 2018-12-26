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

func TestPoolGetCreatesResourceWhenNoneAvailable(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	defer pool.Close()

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())
	res.Release()
}

func TestPoolGetDoesNotCreatesResourceWhenItWouldExceedMaxSize(t *testing.T) {
	createFunc, createCounter := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	pool.SetMaxSize(1)

	wg := &sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				res, err := pool.Get(context.Background())
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

func TestPoolGetReturnsErrorFromFailedResourceCreate(t *testing.T) {
	errCreateFailed := errors.New("create failed")
	createFunc := func(ctx context.Context) (interface{}, error) {
		return nil, errCreateFailed
	}
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Get(context.Background())
	assert.Equal(t, errCreateFailed, err)
	assert.Nil(t, res)
}

func TestPoolGetReusesResources(t *testing.T) {
	createFunc, createCounter := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	res.Release()

	res, err = pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	res.Release()

	assert.Equal(t, 1, createCounter.Value())
}

func TestPoolGetContextAlreadyCanceled(t *testing.T) {
	createFunc := func(ctx context.Context) (interface{}, error) {
		panic("should never be called")
	}
	pool := puddle.NewPool(createFunc, stubCloseRes)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := pool.Get(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolGetContextCanceledDuringCreate(t *testing.T) {
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

	res, err := pool.Get(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestResourceReleaseClosesAndRemovesResourceIfOlderThanMaxDuration(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	pool := puddle.NewPool(createFunc, closeFunc)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, pool.Size())

	pool.SetMaxResourceDuration(time.Nanosecond)
	time.Sleep(2 * time.Nanosecond)
	res.Release()

	waitForRead(closeCallsChan)
	assert.Equal(t, 0, pool.Size())
	assert.Equal(t, 1, closeCalls.Value())
}

func TestResourceReleaseClosesAndRemovesResourceWhenResourceCheckoutCountIsMaxResourceCheckouts(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	pool := puddle.NewPool(createFunc, closeFunc)
	pool.SetMaxResourceCheckouts(1)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)

	res.Release()

	waitForRead(closeCallsChan)

	assert.Equal(t, 1, closeCalls.Value())
	assert.Equal(t, 0, pool.Size())
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
		resources[i], err = p.Get(context.Background())
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
		resources[i], err = p.Get(context.Background())
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

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	assert.Equal(t, 1, pool.Size())
	res.Destroy()
	assert.Equal(t, 0, pool.Size())
}

func TestPoolGetReturnsErrorWhenPoolIsClosed(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	pool.Close()

	res, err := pool.Get(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
	assert.Nil(t, res)
}

func BenchmarkPoolGetAndRelease(b *testing.B) {
	benchmarks := []struct {
		poolSize              int
		concurrentClientCount int
		loanDuration          time.Duration
	}{
		// Small pool
		{10, 1, 0},
		{10, 5, 0},
		{10, 10, 0},
		{10, 20, 0},
		{10, 1, 1 * time.Millisecond},
		{10, 5, 1 * time.Millisecond},
		{10, 10, 1 * time.Millisecond},
		{10, 20, 1 * time.Millisecond},

		// large pool
		{100, 1, 0},
		{100, 50, 0},
		{100, 100, 0},
		{100, 200, 0},
		{100, 1, 1 * time.Millisecond},
		{100, 50, 1 * time.Millisecond},
		{100, 100, 1 * time.Millisecond},
		{100, 200, 1 * time.Millisecond},

		// huge pool
		{1000, 1, 0},
		{1000, 500, 0},
		{1000, 1000, 0},
		{1000, 2000, 0},
		{1000, 1, 1 * time.Millisecond},
		{1000, 500, 1 * time.Millisecond},
		{1000, 1000, 1 * time.Millisecond},
		{1000, 2000, 1 * time.Millisecond},
	}

	for _, bm := range benchmarks {
		name := fmt.Sprintf("PoolSize=%d/ConcurrentClientCount=%d/LoanDuration=%v", bm.poolSize, bm.concurrentClientCount, bm.loanDuration)

		createFunc, _ := createCreateResourceFunc()
		pool := puddle.NewPool(createFunc, stubCloseRes)
		pool.SetMaxSize(bm.poolSize)

		borrowAndRelease := func() {
			res, err := pool.Get(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			time.Sleep(bm.loanDuration)
			res.Release()
		}

		b.Run(name, func(b *testing.B) {
			doneChan := make(chan struct{})
			defer close(doneChan)
			for i := 0; i < bm.concurrentClientCount-1; i++ {
				go func() {
					for {
						select {
						case <-doneChan:
							return
						default:
						}

						borrowAndRelease()
					}
				}()
			}

			for i := 0; i < b.N; i++ {
				borrowAndRelease()
			}
		})
	}
}
