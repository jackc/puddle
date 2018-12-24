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
	f := func() (interface{}, error) {
		return c.Next(), nil
	}
	return f, &c
}

func createCreateResourceFuncWithNotifierChan() (puddle.CreateFunc, *Counter, chan int) {
	ch := make(chan int)
	var c Counter
	f := func() (interface{}, error) {
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
	f := func(interface{}) error {
		n := c.Next()

		// Because the tests will not read from ch until after the close function f returns.
		go func() { ch <- n }()

		return nil
	}
	return f, &c, ch
}

func stubCloseRes(interface{}) error { return nil }

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

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)

	pool.Return(res)
}

func TestPoolSetMinSizeImmediatelyCreatesNewResources(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	pool.SetMinSize(2)
	assert.Equal(t, 2, pool.Size())
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
				assert.Equal(t, 1, res)
				pool.Return(res)
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
	createFunc := func() (interface{}, error) {
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
	assert.Equal(t, 1, res)

	pool.Return(res)

	res, err = pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)

	pool.Return(res)

	assert.Equal(t, 1, createCounter.Value())
}

func TestPoolGetContextAlreadyCanceled(t *testing.T) {
	createFunc := func() (interface{}, error) {
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

	var createCalls Counter
	createFunc := func() (interface{}, error) {
		cancel()
		time.Sleep(1 * time.Second)
		return createCalls.Next(), nil
	}
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Get(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolReturnPanicsIfResourceNotPartOfPool(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)

	assert.Panics(t, func() { pool.Return(42) })
}

func TestPoolReturnClosesAndRemovesResourceIfOlderThanMaxDuration(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	pool := puddle.NewPool(createFunc, closeFunc)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, pool.Size())

	pool.SetMaxResourceDuration(time.Nanosecond)
	time.Sleep(2 * time.Nanosecond)
	pool.Return(res)

	waitForRead(closeCallsChan)
	assert.Equal(t, 0, pool.Size())
	assert.Equal(t, 1, closeCalls.Value())
}

func TestPoolReturnClosesAndRemovesResourceWhenResourceCheckoutCountIsMaxResourceCheckouts(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	pool := puddle.NewPool(createFunc, closeFunc)
	pool.SetMaxResourceCheckouts(1)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)

	pool.Return(res)

	waitForRead(closeCallsChan)

	assert.Equal(t, 1, closeCalls.Value())
	assert.Equal(t, 0, pool.Size())
}

func TestPoolCloseClosesAllAvailableResources(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()

	var closeCalls Counter
	closeFunc := func(interface{}) error {
		closeCalls.Next()
		return nil
	}

	p := puddle.NewPool(createFunc, closeFunc)

	resources := make([]interface{}, 4)
	for i := range resources {
		var err error
		resources[i], err = p.Get(context.Background())
		require.Nil(t, err)
	}

	for _, res := range resources {
		p.Return(res)
	}

	p.Close()

	assert.Equal(t, len(resources), closeCalls.Value())
}

func TestPoolReturnClosesResourcePoolIsAlreadyClosed(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	p := puddle.NewPool(createFunc, closeFunc)

	resources := make([]interface{}, 4)
	for i := range resources {
		var err error
		resources[i], err = p.Get(context.Background())
		require.Nil(t, err)
	}

	p.Close()
	assert.Equal(t, 0, closeCalls.Value())

	for _, res := range resources {
		p.Return(res)
	}

	waitForRead(closeCallsChan)
	waitForRead(closeCallsChan)
	waitForRead(closeCallsChan)
	waitForRead(closeCallsChan)

	assert.Equal(t, len(resources), closeCalls.Value())
}

func TestPoolRemovePanicsIfResourceNotPartOfPool(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)

	assert.Panics(t, func() { pool.Remove(42) })
}

func TestPoolRemoveRemovesResourceFromPool(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)

	assert.Equal(t, 1, pool.Size())
	pool.Remove(res)
	assert.Equal(t, 0, pool.Size())
}

func TestPoolRemoveRemovesResourceFromPoolAndStartsNewCreationToMaintainMinSize(t *testing.T) {
	createFunc, createCounter, createCallsChan := createCreateResourceFuncWithNotifierChan()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	pool := puddle.NewPool(createFunc, closeFunc)

	// Ensure there are 2 resources available in pool
	{
		r1, err := pool.Get(context.Background())
		require.Nil(t, err)
		r2, err := pool.Get(context.Background())
		require.Nil(t, err)
		pool.Return(r1)
		pool.Return(r2)
	}

	assert.Equal(t, 2, pool.Size())
	pool.SetMinSize(2)
	assert.Equal(t, 2, pool.Size())

	{
		r1, err := pool.Get(context.Background())
		require.Nil(t, err)
		r2, err := pool.Get(context.Background())
		require.Nil(t, err)
		pool.Remove(r1)
		pool.Remove(r2)
	}

	require.True(t, waitForRead(createCallsChan))
	require.True(t, waitForRead(createCallsChan))
	require.True(t, waitForRead(createCallsChan))
	require.True(t, waitForRead(createCallsChan))
	require.True(t, waitForRead(closeCallsChan))
	require.True(t, waitForRead(closeCallsChan))

	assert.Equal(t, 2, pool.Size())
	assert.Equal(t, 4, createCounter.Value())
	assert.Equal(t, 2, closeCalls.Value())
}

func TestPoolRemoveRemovesResourceFromPoolAndDoesNotStartNewCreationToMaintainMinSizeWhenPoolIsClosed(t *testing.T) {
	createFunc, createCounter, createCallsChan := createCreateResourceFuncWithNotifierChan()
	closeFunc, closeCalls, closeCallsChan := createCloseResourceFuncWithNotifierChan()

	pool := puddle.NewPool(createFunc, closeFunc)

	// Ensure there are 2 resources available in pool
	{
		r1, err := pool.Get(context.Background())
		require.Nil(t, err)
		r2, err := pool.Get(context.Background())
		require.Nil(t, err)
		pool.Return(r1)
		pool.Return(r2)
	}

	assert.Equal(t, 2, pool.Size())
	pool.SetMinSize(2)
	assert.Equal(t, 2, pool.Size())

	{
		r1, err := pool.Get(context.Background())
		require.Nil(t, err)
		r2, err := pool.Get(context.Background())
		require.Nil(t, err)

		pool.Close()

		pool.Remove(r1)
		pool.Remove(r2)
	}

	require.True(t, waitForRead(createCallsChan))
	require.True(t, waitForRead(createCallsChan))
	require.True(t, waitForRead(closeCallsChan))
	require.True(t, waitForRead(closeCallsChan))

	assert.Equal(t, 0, pool.Size())
	assert.Equal(t, 2, createCounter.Value())
	assert.Equal(t, 2, closeCalls.Value())
}

func TestPoolGetReturnsErrorWhenPoolIsClosed(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes)
	pool.Close()

	res, err := pool.Get(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
	assert.Nil(t, res)
}

func TestPoolGetLateFailedCreateErrorIsReported(t *testing.T) {
	errCreateStartedChan := make(chan struct{})
	createWaitChan := make(chan struct{})
	errCreateFailed := errors.New("create failed")
	var createCalls Counter
	createFunc := func() (interface{}, error) {
		n := createCalls.Next()
		if n == 1 {
			return n, nil
		}
		close(errCreateStartedChan)
		<-createWaitChan
		return nil, errCreateFailed
	}
	pool := puddle.NewPool(createFunc, stubCloseRes)

	asyncErrChan := make(chan error)
	pool.SetBackgroundErrorHandler(func(err error) { asyncErrChan <- err })

	res1, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res1)

	go func() {
		<-errCreateStartedChan
		pool.Return(res1)
	}()

	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)
	close(createWaitChan)

	select {
	case err = <-asyncErrChan:
		assert.Equal(t, errCreateFailed, err)
	case <-time.NewTimer(time.Second).C:
		t.Fatal("timed out waiting for async error")
	}
}

func TestPoolCloseResourceCloseErrorIsReported(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	errCloseFailed := errors.New("close failed")
	closeFunc := func(res interface{}) error { return errCloseFailed }
	pool := puddle.NewPool(createFunc, closeFunc)
	asyncErrChan := make(chan error, 1)
	pool.SetBackgroundErrorHandler(func(err error) { asyncErrChan <- err })

	// Get and return a resource to put something in the pool
	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)
	pool.Return(res)

	pool.Close()

	select {
	case err = <-asyncErrChan:
		assert.Equal(t, errCloseFailed, err)
	default:
		t.Fatal("error not reported")
	}
}

func TestPoolReturnClosesResourcePoolIsAlreadyClosedErrorIsReported(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()

	errCloseFailed := errors.New("close failed")
	closeFunc := func(res interface{}) error { return errCloseFailed }
	pool := puddle.NewPool(createFunc, closeFunc)

	asyncErrChan := make(chan error, 1)
	pool.SetBackgroundErrorHandler(func(err error) { asyncErrChan <- err })

	// Get and return a resource to put something in the pool
	res, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res)

	pool.Close()

	pool.Return(res)
	select {
	case err = <-asyncErrChan:
		assert.Equal(t, errCloseFailed, err)
	case <-time.NewTimer(time.Second).C:
		t.Fatal("timed out waiting for async error")
	}
}

func BenchmarkPoolGetAndReturn(b *testing.B) {
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

		borrowAndReturn := func() {
			res, err := pool.Get(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			time.Sleep(bm.loanDuration)
			pool.Return(res)
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

						borrowAndReturn()
					}
				}()
			}

			for i := 0; i < b.N; i++ {
				borrowAndReturn()
			}
		})
	}
}
