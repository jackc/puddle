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

func createCreateResourceFunc() (puddle.Constructor, *Counter) {
	var c Counter
	f := func(ctx context.Context) (interface{}, error) {
		return c.Next(), nil
	}
	return f, &c
}

func createCreateResourceFuncWithNotifierChan() (puddle.Constructor, *Counter, chan int) {
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

func createCloseResourceFuncWithNotifierChan() (puddle.Destructor, *Counter, chan int) {
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

func TestPoolAcquireCreatesResourceWhenNoneIdle(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)
	defer pool.Close()

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())
	res.Release()
}

func TestPoolAcquireDoesNotCreatesResourceWhenItWouldExceedMaxSize(t *testing.T) {
	createFunc, createCounter := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes, 1)

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
	assert.Equal(t, 1, pool.Stat().TotalResources())
}

func TestPoolAcquireWithCancellableContext(t *testing.T) {
	createFunc, createCounter := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes, 1)

	wg := &sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				ctx, cancel := context.WithCancel(context.Background())
				res, err := pool.Acquire(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 1, res.Value())
				res.Release()
				cancel()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	assert.Equal(t, 1, createCounter.Value())
	assert.Equal(t, 1, pool.Stat().TotalResources())
}

func TestPoolAcquireReturnsErrorFromFailedResourceCreate(t *testing.T) {
	errCreateFailed := errors.New("create failed")
	createFunc := func(ctx context.Context) (interface{}, error) {
		return nil, errCreateFailed
	}
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, errCreateFailed, err)
	assert.Nil(t, res)
}

func TestPoolAcquireReusesResources(t *testing.T) {
	createFunc, createCounter := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)

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
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)

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
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)

	res, err := pool.Acquire(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolCloseClosesAllIdleResources(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()

	var closeCalls Counter
	closeFunc := func(interface{}) {
		closeCalls.Next()
	}

	p := puddle.NewPool(createFunc, closeFunc, 10)

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

func TestPoolCloseBlocksUntilAllResourcesReleasedAndClosed(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	var closeCalls Counter
	closeFunc := func(interface{}) {
		closeCalls.Next()
	}

	p := puddle.NewPool(createFunc, closeFunc, 10)

	resources := make([]*puddle.Resource, 4)
	for i := range resources {
		var err error
		resources[i], err = p.Acquire(context.Background())
		require.Nil(t, err)
	}

	for _, res := range resources {
		go func() {
			time.Sleep(100 * time.Millisecond)
			res.Release()
		}()
	}

	p.Close()
	assert.Equal(t, len(resources), closeCalls.Value())
}

func TestPoolStatResources(t *testing.T) {
	startWaitChan := make(chan struct{})
	waitingChan := make(chan struct{})
	endWaitChan := make(chan struct{})

	var createCalls Counter
	createFunc := func(ctx context.Context) (interface{}, error) {
		select {
		case <-startWaitChan:
			close(waitingChan)
			<-endWaitChan
		default:
		}

		return createCalls.Next(), nil
	}
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)
	defer pool.Close()

	resAcquired, err := pool.Acquire(context.Background())
	require.Nil(t, err)

	close(startWaitChan)
	go func() {
		res, err := pool.Acquire(context.Background())
		require.Nil(t, err)
		res.Release()
	}()
	<-waitingChan
	stat := pool.Stat()

	assert.Equal(t, 2, stat.TotalResources())
	assert.Equal(t, 1, stat.ConstructingResources())
	assert.Equal(t, 1, stat.AcquiredResources())
	assert.Equal(t, 0, stat.IdleResources())
	assert.Equal(t, 10, stat.MaxResources())

	resAcquired.Release()

	stat = pool.Stat()
	assert.Equal(t, 2, stat.TotalResources())
	assert.Equal(t, 1, stat.ConstructingResources())
	assert.Equal(t, 0, stat.AcquiredResources())
	assert.Equal(t, 1, stat.IdleResources())
	assert.Equal(t, 10, stat.MaxResources())

	close(endWaitChan)
}

func TestPoolStatCounters(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes, 1)
	defer pool.Close()

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	res.Release()

	stat := pool.Stat()
	assert.Equal(t, int64(1), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.SlowAcquireCount())

	res, err = pool.Acquire(context.Background())
	require.NoError(t, err)
	res.Release()

	stat = pool.Stat()
	assert.Equal(t, int64(2), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.SlowAcquireCount())

	wg := &sync.WaitGroup{}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			res, err = pool.Acquire(context.Background())
			require.NoError(t, err)
			time.Sleep(50 * time.Millisecond)
			res.Release()
			wg.Done()
		}()
	}

	wg.Wait()

	stat = pool.Stat()
	assert.Equal(t, int64(4), stat.AcquireCount())
	assert.Equal(t, int64(2), stat.SlowAcquireCount())
}

func TestResourceDestroyRemovesResourceFromPool(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	var closeCalls Counter
	closeFunc := func(interface{}) {
		closeCalls.Next()
	}

	pool := puddle.NewPool(createFunc, closeFunc, 10)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	res.Hijack()

	assert.Equal(t, 0, pool.Stat().TotalResources())
	assert.Equal(t, 0, closeCalls.Value())
}

func TestResourceHijackRemovesResourceFromPoolButDoesNotDestroy(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	assert.Equal(t, 1, pool.Stat().TotalResources())
	res.Destroy()
	assert.Equal(t, 0, pool.Stat().TotalResources())
}

func TestPoolAcquireReturnsErrorWhenPoolIsClosed(t *testing.T) {
	createFunc, _ := createCreateResourceFunc()
	pool := puddle.NewPool(createFunc, stubCloseRes, 10)
	pool.Close()

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
	assert.Nil(t, res)
}

func BenchmarkPoolAcquireAndRelease(b *testing.B) {
	benchmarks := []struct {
		poolSize    int
		clientCount int
		cancellable bool
	}{
		{8, 2, false},
		{8, 8, false},
		{8, 32, false},
		{8, 128, false},
		{8, 512, false},
		{8, 2048, false},
		{8, 8192, false},

		{64, 2, false},
		{64, 8, false},
		{64, 32, false},
		{64, 128, false},
		{64, 512, false},
		{64, 2048, false},
		{64, 8192, false},

		{512, 2, false},
		{512, 8, false},
		{512, 32, false},
		{512, 128, false},
		{512, 512, false},
		{512, 2048, false},
		{512, 8192, false},

		{8, 2, true},
		{8, 8, true},
		{8, 32, true},
		{8, 128, true},
		{8, 512, true},
		{8, 2048, true},
		{8, 8192, true},

		{64, 2, true},
		{64, 8, true},
		{64, 32, true},
		{64, 128, true},
		{64, 512, true},
		{64, 2048, true},
		{64, 8192, true},

		{512, 2, true},
		{512, 8, true},
		{512, 32, true},
		{512, 128, true},
		{512, 512, true},
		{512, 2048, true},
		{512, 8192, true},
	}

	for _, bm := range benchmarks {
		name := fmt.Sprintf("PoolSize=%d/ClientCount=%d/Cancellable=%v", bm.poolSize, bm.clientCount, bm.cancellable)

		b.Run(name, func(b *testing.B) {
			ctx := context.Background()
			cancel := func() {}
			if bm.cancellable {
				ctx, cancel = context.WithCancel(ctx)
			}

			wg := &sync.WaitGroup{}

			createFunc, _ := createCreateResourceFunc()
			pool := puddle.NewPool(createFunc, stubCloseRes, bm.poolSize)

			for i := 0; i < bm.clientCount; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for j := 0; j < b.N; j++ {
						res, err := pool.Acquire(ctx)
						if err != nil {
							b.Fatal(err)
						}
						res.Release()
					}
				}()
			}

			wg.Wait()
			cancel()
		})
	}
}
