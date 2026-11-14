package puddle_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type Counter struct {
	mutex sync.Mutex
	n     int
}

// Next increments the counter and returns the value
func (c *Counter) Next() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.n += 1
	return c.n
}

// Value returns the counter
func (c *Counter) Value() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.n
}

func createConstructor() (puddle.Constructor[int], *Counter) {
	var c Counter
	f := func(ctx context.Context) (int, error) {
		return c.Next(), nil
	}
	return f, &c
}

func stubDestructor(int) {}

func TestNewPoolRequiresMaxSizeGreaterThan0(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: -1})
	assert.Nil(t, pool)
	assert.Error(t, err)

	pool, err = puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 0})
	assert.Nil(t, pool)
	assert.Error(t, err)
}

func TestPoolAcquireCreatesResourceWhenNoneIdle(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	defer pool.Close()

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())
	assert.WithinDuration(t, time.Now(), res.CreationTime(), time.Second)
	res.Release()
}

func TestPoolAcquireCallsConstructorWithAcquireContextValuesButNotDeadline(t *testing.T) {
	constructor := func(ctx context.Context) (int, error) {
		if ctx.Value("test") != "from Acquire" {
			return 0, errors.New("did not get value from Acquire")
		}
		if _, ok := ctx.Deadline(); ok {
			return 0, errors.New("should not have gotten deadline from Acquire")
		}

		return 1, nil
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.WithValue(context.Background(), "test", "from Acquire")
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	res, err := pool.Acquire(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())
	assert.WithinDuration(t, time.Now(), res.CreationTime(), time.Second)
	res.Release()
}

func TestPoolAcquireCalledConstructorIsNotCanceledByAcquireCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor := func(ctx context.Context) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return 1, ctx.Err()
		}
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
		require.NoError(t, err)
		defer pool.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		res, err := pool.Acquire(ctx)
		assert.Nil(t, res)
		assert.Equal(t, context.DeadlineExceeded, err)

		time.Sleep(75 * time.Millisecond)
		synctest.Wait()
		assert.EqualValues(t, 1, pool.Stat().IdleResources())

		assert.EqualValues(t, 1, pool.Stat().TotalResources())
		assert.EqualValues(t, 1, pool.Stat().CanceledAcquireCount())
	})
}

func TestPoolAcquireDoesNotCreatesResourceWhenItWouldExceedMaxSize(t *testing.T) {
	constructor, createCounter := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
	require.NoError(t, err)

	wg := &sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				res, err := pool.Acquire(context.Background())
				assert.NoError(t, err)
				assert.Equal(t, 1, res.Value())
				res.Release()
			}
		})
	}

	wg.Wait()

	assert.EqualValues(t, 1, createCounter.Value())
	assert.EqualValues(t, 1, pool.Stat().TotalResources())
}

func TestPoolAcquireWithCancellableContext(t *testing.T) {
	constructor, createCounter := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
	require.NoError(t, err)

	wg := &sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				ctx, cancel := context.WithCancel(context.Background())
				res, err := pool.Acquire(ctx)
				assert.NoError(t, err)
				assert.Equal(t, 1, res.Value())
				res.Release()
				cancel()
			}
		})
	}

	wg.Wait()

	assert.EqualValues(t, 1, createCounter.Value())
	assert.EqualValues(t, 1, pool.Stat().TotalResources())
}

func TestPoolAcquireReturnsErrorFromFailedResourceCreate(t *testing.T) {
	errCreateFailed := errors.New("create failed")
	constructor := func(ctx context.Context) (int, error) {
		return 0, errCreateFailed
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, errCreateFailed, err)
	assert.Nil(t, res)
}

func TestPoolAcquireCreatesResourceRespectingContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		finishConstruction := make(chan struct{})
		constructor := func(context.Context) (int, error) {
			cancel()
			<-finishConstruction
			return 1, nil
		}
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
		require.NoError(t, err)
		defer pool.Close()
		var finishOnce sync.Once
		defer finishOnce.Do(func() { close(finishConstruction) })

		res, err := pool.Acquire(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, res)
		assert.EqualValues(t, 1, pool.Stat().ConstructingResources())

		finishOnce.Do(func() { close(finishConstruction) })
		synctest.Wait()
		stat := pool.Stat()
		assert.EqualValues(t, 1, stat.IdleResources())
		assert.EqualValues(t, 1, stat.TotalResources())
	})
}

func TestPoolAcquireReusesResources(t *testing.T) {
	constructor, createCounter := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

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

func TestPoolTryAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, createCounter := createConstructor()
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
		require.NoError(t, err)
		defer pool.Close()

		// Pool is initially empty so TryAcquire fails but starts construction of resource in the background.
		res, err := pool.TryAcquire(context.Background())
		require.EqualError(t, err, puddle.ErrNotAvailable.Error())
		assert.Nil(t, res)

		// Wait for background creation to complete.
		synctest.Wait()

		res, err = pool.TryAcquire(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, res.Value())
		defer res.Release()

		res, err = pool.TryAcquire(context.Background())
		require.EqualError(t, err, puddle.ErrNotAvailable.Error())
		assert.Nil(t, res)

		assert.Equal(t, 1, createCounter.Value())
	})
}

func TestPoolTryAcquireReturnsErrorWhenPoolIsClosed(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	pool.Close()

	res, err := pool.TryAcquire(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
	assert.Nil(t, res)
}

func TestPoolTryAcquireWithFailedResourceCreate(t *testing.T) {
	errCreateFailed := errors.New("create failed")
	constructor := func(ctx context.Context) (int, error) {
		return 0, errCreateFailed
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

	res, err := pool.TryAcquire(context.Background())
	require.EqualError(t, err, puddle.ErrNotAvailable.Error())
	assert.Nil(t, res)
}

func TestPoolTryAcquireDestroysResourceConstructedAfterClose(t *testing.T) {
	constructionStarted := make(chan struct{})
	finishConstruction := make(chan struct{})
	destructionStarted := make(chan struct{})
	finishDestruction := make(chan struct{})
	var acquireCtx context.Context
	var constructorCalls Counter
	var destructorCalls Counter
	constructor := func(ctx context.Context) (int, error) {
		value := constructorCalls.Next()
		if value == 1 {
			acquireCtx = ctx
		} else {
			close(constructionStarted)
			<-finishConstruction
		}
		return value, nil
	}
	destructor := func(value int) {
		if value == 2 {
			close(destructionStarted)
			<-finishDestruction
		}
		destructorCalls.Next()
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: 2})
	require.NoError(t, err)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	var releaseOnce, constructionOnce, destructionOnce sync.Once
	closeFinished := make(chan struct{})
	t.Cleanup(func() {
		constructionOnce.Do(func() { close(finishConstruction) })
		destructionOnce.Do(func() { close(finishDestruction) })
		releaseOnce.Do(res.Release)
		pool.Reset()
	})

	backgroundRes, err := pool.TryAcquire(context.Background())
	require.ErrorIs(t, err, puddle.ErrNotAvailable)
	require.Nil(t, backgroundRes)
	<-constructionStarted

	go func() {
		pool.Close()
		close(closeFinished)
	}()
	// Acquire's constructor context is canceled when Close marks the pool closed.
	select {
	case <-acquireCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the pool context")
	}
	releaseOnce.Do(res.Release)
	constructionOnce.Do(func() { close(finishConstruction) })

	select {
	case <-destructionStarted:
	case <-time.After(time.Second):
		t.Fatal("resource constructed after Close was not destroyed")
	}
	assert.Zero(t, pool.Stat().TotalResources())
	select {
	case <-closeFinished:
		t.Fatal("Close returned before destruction finished")
	default:
	}
	destructionOnce.Do(func() { close(finishDestruction) })
	select {
	case <-closeFinished:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after destruction finished")
	}
	assert.Equal(t, 2, destructorCalls.Value())
}

func TestPoolTryAcquireDestroysResourceConstructedBeforeReset(t *testing.T) {
	constructionStarted := make(chan struct{})
	finishConstruction := make(chan struct{})
	destroyed := make(chan int, 2)
	var constructorCalls Counter
	constructor := func(context.Context) (int, error) {
		value := constructorCalls.Next()
		if value == 1 {
			close(constructionStarted)
			<-finishConstruction
		}
		return value, nil
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{
		Constructor: constructor,
		Destructor:  func(value int) { destroyed <- value },
		MaxSize:     1,
	})
	require.NoError(t, err)
	var constructionOnce sync.Once
	t.Cleanup(func() {
		constructionOnce.Do(func() { close(finishConstruction) })
		pool.Close()
	})

	res, err := pool.TryAcquire(context.Background())
	require.ErrorIs(t, err, puddle.ErrNotAvailable)
	require.Nil(t, res)
	<-constructionStarted
	pool.Reset()
	constructionOnce.Do(func() { close(finishConstruction) })

	select {
	case value := <-destroyed:
		assert.Equal(t, 1, value)
	case <-time.After(time.Second):
		t.Fatal("resource constructed before Reset was not destroyed")
	}
	assert.Zero(t, pool.Stat().TotalResources())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err = pool.Acquire(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Value())
	res.Release()
}

func TestPoolCreateResourceDestroysResourceConstructedAfterClose(t *testing.T) {
	constructionStarted := make(chan struct{})
	idleDestroyed := make(chan struct{})
	destructionStarted := make(chan struct{})
	finishDestruction := make(chan struct{})
	var constructorCalls Counter
	var destructorCalls Counter
	constructor := func(context.Context) (int, error) {
		value := constructorCalls.Next()
		if value == 2 {
			close(constructionStarted)
			<-idleDestroyed
		}
		return value, nil
	}
	destructor := func(value int) {
		if value == 1 {
			close(idleDestroyed)
		}
		if value == 2 {
			close(destructionStarted)
			<-finishDestruction
		}
		destructorCalls.Next()
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: 2})
	require.NoError(t, err)
	var destructionOnce sync.Once
	t.Cleanup(func() {
		destructionOnce.Do(func() { close(finishDestruction) })
		pool.Close()
	})

	require.NoError(t, pool.CreateResource(context.Background()))

	errc := make(chan error, 1)
	go func() { errc <- pool.CreateResource(context.Background()) }()
	select {
	case <-constructionStarted:
	case <-time.After(time.Second):
		t.Fatal("CreateResource did not start construction")
	}

	// Close destroys the idle resource, which releases the constructor only after closed is set.
	closeFinished := make(chan struct{})
	go func() {
		pool.Close()
		close(closeFinished)
	}()
	select {
	case <-destructionStarted:
	case <-time.After(time.Second):
		t.Fatal("resource constructed after Close was not destroyed")
	}
	select {
	case <-closeFinished:
		t.Fatal("Close returned before destruction finished")
	default:
	}
	destructionOnce.Do(func() { close(finishDestruction) })
	select {
	case <-closeFinished:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after destruction finished")
	}
	select {
	case err := <-errc:
		assert.ErrorIs(t, err, puddle.ErrClosedPool)
	case <-time.After(time.Second):
		t.Fatal("CreateResource did not return after Close")
	}
	assert.Equal(t, 2, destructorCalls.Value())
	assert.Zero(t, pool.Stat().TotalResources())
	assert.Zero(t, pool.Stat().IdleResources())
}

func TestPoolCreateResourceDestroysResourceConstructedBeforeReset(t *testing.T) {
	constructionStarted := make(chan struct{})
	finishConstruction := make(chan struct{})
	destroyed := make(chan int, 1)
	var constructorCalls Counter
	constructor := func(context.Context) (int, error) {
		value := constructorCalls.Next()
		if value == 1 {
			close(constructionStarted)
			<-finishConstruction
		}
		return value, nil
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{
		Constructor: constructor,
		Destructor:  func(value int) { destroyed <- value },
		MaxSize:     1,
	})
	require.NoError(t, err)
	var constructionOnce sync.Once
	t.Cleanup(func() {
		constructionOnce.Do(func() { close(finishConstruction) })
		pool.Close()
	})

	errc := make(chan error, 1)
	go func() { errc <- pool.CreateResource(context.Background()) }()
	select {
	case <-constructionStarted:
	case <-time.After(time.Second):
		t.Fatal("CreateResource did not start construction")
	}
	pool.Reset()
	constructionOnce.Do(func() { close(finishConstruction) })

	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("CreateResource did not return after Reset")
	}
	select {
	case value := <-destroyed:
		assert.Equal(t, 1, value)
	case <-time.After(time.Second):
		t.Fatal("resource constructed before Reset was not destroyed")
	}
	assert.Zero(t, pool.Stat().TotalResources())
	assert.Zero(t, pool.Stat().IdleResources())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer res.Release()
	assert.Equal(t, 2, res.Value())
}

func TestPoolAcquireNilContextDoesNotLeavePoolLocked(t *testing.T) {
	constructor, createCounter := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

	assert.Panics(t, func() { pool.Acquire(nil) })

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())
	res.Release()

	assert.Equal(t, 1, createCounter.Value())
}

func TestPoolAcquireContextAlreadyCanceled(t *testing.T) {
	constructor := func(ctx context.Context) (int, error) {
		panic("should never be called")
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := pool.Acquire(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolAcquireContextCanceledDuringCreate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		time.AfterFunc(100*time.Millisecond, cancel)
		timeoutChan := time.After(1 * time.Second)

		var constructorCalls Counter
		constructor := func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-timeoutChan:
			}
			return constructorCalls.Next(), nil
		}
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
		require.NoError(t, err)
		defer pool.Close()

		res, err := pool.Acquire(ctx)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, res)
	})
}

func TestPoolAcquireAllIdle(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	defer pool.Close()

	resources := make([]*puddle.Resource[int], 4)

	resources[0], err = pool.Acquire(context.Background())
	require.NoError(t, err)
	resources[1], err = pool.Acquire(context.Background())
	require.NoError(t, err)
	resources[2], err = pool.Acquire(context.Background())
	require.NoError(t, err)
	resources[3], err = pool.Acquire(context.Background())
	require.NoError(t, err)

	assert.Len(t, pool.AcquireAllIdle(), 0)

	resources[0].Release()
	resources[3].Release()

	assert.ElementsMatch(t, []*puddle.Resource[int]{resources[0], resources[3]}, pool.AcquireAllIdle())

	resources[0].Release()
	resources[3].Release()
	resources[1].Release()
	resources[2].Release()

	assert.ElementsMatch(t, resources, pool.AcquireAllIdle())

	resources[0].Release()
	resources[1].Release()
	resources[2].Release()
	resources[3].Release()
}

func TestPoolAcquireAllIdleWhenClosedIsNil(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	pool.Close()
	assert.Nil(t, pool.AcquireAllIdle())
}

func TestPoolCreateResource(t *testing.T) {
	constructor, counter := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	defer pool.Close()

	err = pool.CreateResource(context.Background())
	require.NoError(t, err)

	stats := pool.Stat()
	assert.EqualValues(t, 1, stats.IdleResources())

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, counter.Value(), res.Value())
	assert.True(t, res.LastUsedNanotime() > 0, "should set LastUsedNanotime so that idle calculations can still work")
	assert.Equal(t, 1, res.Value())
	assert.WithinDuration(t, time.Now(), res.CreationTime(), time.Second)
	res.Release()

	assert.EqualValues(t, 0, pool.Stat().EmptyAcquireCount(), "should have been a warm resource")
}

func TestPoolCreateResourceReturnsErrorFromFailedResourceCreate(t *testing.T) {
	errCreateFailed := errors.New("create failed")
	constructor := func(ctx context.Context) (int, error) {
		return 0, errCreateFailed
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

	err = pool.CreateResource(context.Background())
	assert.Equal(t, errCreateFailed, err)
}

func TestPoolCreateResourceReturnsErrorWhenAlreadyClosed(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	pool.Close()
	err = pool.CreateResource(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
}

func TestPoolCreateResourceReturnsErrorWhenClosedWhileCreatingResource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finishConstruction := make(chan struct{})
		constructor := func(context.Context) (int, error) {
			<-finishConstruction
			return 123, nil
		}
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
		require.NoError(t, err)
		defer pool.Close()
		var finishOnce sync.Once
		defer finishOnce.Do(func() { close(finishConstruction) })

		createErr := make(chan error, 1)
		go func() { createErr <- pool.CreateResource(context.Background()) }()
		synctest.Wait()
		require.EqualValues(t, 1, pool.Stat().ConstructingResources())

		closed := false
		go func() {
			pool.Close()
			closed = true
		}()
		synctest.Wait()
		assert.False(t, closed, "Close must wait for construction and destruction")
		_, err = pool.TryAcquire(context.Background())
		require.ErrorIs(t, err, puddle.ErrClosedPool)

		finishOnce.Do(func() { close(finishConstruction) })
		synctest.Wait()
		assert.True(t, closed)
		assert.ErrorIs(t, <-createErr, puddle.ErrClosedPool)
	})
}

func TestPoolCreateResourceReturnsErrorWhenPoolFull(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 2})
	require.NoError(t, err)
	defer pool.Close()

	err = pool.CreateResource(context.Background())
	require.NoError(t, err)

	stats := pool.Stat()
	assert.EqualValues(t, 1, stats.IdleResources())

	err = pool.CreateResource(context.Background())
	require.NoError(t, err)

	stats = pool.Stat()
	assert.EqualValues(t, 2, stats.IdleResources())

	err = pool.CreateResource(context.Background())
	require.Error(t, err)
}

func TestPoolCloseClosesAllIdleResources(t *testing.T) {
	constructor, _ := createConstructor()

	var destructorCalls Counter
	destructor := func(int) {
		destructorCalls.Next()
	}

	p, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: 10})
	require.NoError(t, err)

	resources := make([]*puddle.Resource[int], 4)
	for i := range resources {
		var err error
		resources[i], err = p.Acquire(context.Background())
		require.Nil(t, err)
	}

	for _, res := range resources {
		res.Release()
	}

	p.Close()

	assert.Equal(t, len(resources), destructorCalls.Value())
}

func TestPoolCloseBlocksUntilAllResourcesReleasedAndClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, _ := createConstructor()
		finishDestruction := make(chan struct{})
		var destructorCalls Counter
		destructor := func(int) {
			<-finishDestruction
			destructorCalls.Next()
		}
		p, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: 10})
		require.NoError(t, err)

		resources := make([]*puddle.Resource[int], 4)
		for i := range resources {
			resources[i], err = p.Acquire(context.Background())
			require.NoError(t, err)
		}

		closed := false
		go func() {
			p.Close()
			closed = true
		}()
		synctest.Wait()
		assert.False(t, closed, "Close must wait for checked-out resources")

		for _, res := range resources {
			res.Release()
		}
		synctest.Wait()
		assert.False(t, closed, "Close must wait for destructors to finish")
		assert.Zero(t, destructorCalls.Value())

		close(finishDestruction)
		synctest.Wait()
		assert.True(t, closed)
		assert.Equal(t, len(resources), destructorCalls.Value())
	})
}

func TestPoolCloseIsSafeToCallMultipleTimes(t *testing.T) {
	constructor, _ := createConstructor()

	p, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

	p.Close()
	p.Close()
}

func TestPoolResetDestroysAllIdleResources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, _ := createConstructor()

		var destructorCalls Counter
		destructor := func(int) {
			destructorCalls.Next()
		}

		p, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: 10})
		require.NoError(t, err)

		resources := make([]*puddle.Resource[int], 4)
		for i := range resources {
			var err error
			resources[i], err = p.Acquire(context.Background())
			require.Nil(t, err)
		}

		for _, res := range resources {
			res.Release()
		}

		require.EqualValues(t, 4, p.Stat().TotalResources())
		p.Reset()
		require.EqualValues(t, 0, p.Stat().TotalResources())

		// Wait for background destruction to complete.
		synctest.Wait()
		require.Equal(t, len(resources), destructorCalls.Value())

		p.Close()
	})
}

func TestPoolResetDestroysCheckedOutResourcesOnReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, _ := createConstructor()

		var destructorCalls Counter
		destructor := func(int) {
			destructorCalls.Next()
		}

		p, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: 10})
		require.NoError(t, err)

		resources := make([]*puddle.Resource[int], 4)
		for i := range resources {
			var err error
			resources[i], err = p.Acquire(context.Background())
			require.Nil(t, err)
		}

		require.EqualValues(t, 4, p.Stat().TotalResources())
		p.Reset()
		require.EqualValues(t, 4, p.Stat().TotalResources())

		for _, res := range resources {
			res.Release()
		}

		require.EqualValues(t, 0, p.Stat().TotalResources())

		// Wait for background destruction to complete.
		synctest.Wait()
		require.Equal(t, len(resources), destructorCalls.Value())

		p.Close()
	})
}

func TestPoolStatResources(t *testing.T) {
	startWaitChan := make(chan struct{})
	waitingChan := make(chan struct{})
	endWaitChan := make(chan struct{})

	var constructorCalls Counter
	constructor := func(ctx context.Context) (int, error) {
		select {
		case <-startWaitChan:
			close(waitingChan)
			<-endWaitChan
		default:
		}

		return constructorCalls.Next(), nil
	}
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
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

	assert.EqualValues(t, 2, stat.TotalResources())
	assert.EqualValues(t, 1, stat.ConstructingResources())
	assert.EqualValues(t, 1, stat.AcquiredResources())
	assert.EqualValues(t, 0, stat.IdleResources())
	assert.EqualValues(t, 10, stat.MaxResources())

	resAcquired.Release()

	stat = pool.Stat()
	assert.EqualValues(t, 2, stat.TotalResources())
	assert.EqualValues(t, 1, stat.ConstructingResources())
	assert.EqualValues(t, 0, stat.AcquiredResources())
	assert.EqualValues(t, 1, stat.IdleResources())
	assert.EqualValues(t, 10, stat.MaxResources())

	close(endWaitChan)
}

func TestPoolStatSuccessfulAcquireCounters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, _ := createConstructor()
		sleepConstructor := func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return constructor(ctx)
		}
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: sleepConstructor, Destructor: stubDestructor, MaxSize: 1})
		require.NoError(t, err)
		defer pool.Close()

		res, err := pool.Acquire(context.Background())
		require.NoError(t, err)
		res.Release()

		stat := pool.Stat()
		assert.EqualValues(t, 1, stat.AcquireCount())
		assert.EqualValues(t, 1, stat.EmptyAcquireCount())
		assert.Equal(t, 10*time.Millisecond, stat.AcquireDuration())
		assert.Equal(t, 10*time.Millisecond, stat.EmptyAcquireWaitTime())

		// Reusing an idle resource does not advance virtual time.
		res, err = pool.Acquire(context.Background())
		require.NoError(t, err)
		res.Release()

		stat = pool.Stat()
		assert.EqualValues(t, 2, stat.AcquireCount())
		assert.EqualValues(t, 1, stat.EmptyAcquireCount())
		assert.Equal(t, 10*time.Millisecond, stat.AcquireDuration())
		assert.Equal(t, 10*time.Millisecond, stat.EmptyAcquireWaitTime())

		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Go(func() {
				res, err := pool.Acquire(context.Background())
				if !assert.NoError(t, err) {
					return
				}
				time.Sleep(50 * time.Millisecond)
				res.Release()
			})
		}
		wg.Wait()

		// One acquire reused the idle resource immediately; the other waited
		// 50ms for it. Both duration counters include the initial construction.
		stat = pool.Stat()
		assert.EqualValues(t, 4, stat.AcquireCount())
		assert.EqualValues(t, 2, stat.EmptyAcquireCount())
		assert.Equal(t, 60*time.Millisecond, stat.AcquireDuration())
		assert.Equal(t, 60*time.Millisecond, stat.EmptyAcquireWaitTime())
	})
}

func TestPoolStatCanceledAcquireBeforeStart(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
	require.NoError(t, err)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pool.Acquire(ctx)
	require.Equal(t, context.Canceled, err)

	stat := pool.Stat()
	assert.Equal(t, int64(0), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.CanceledAcquireCount())
}

func TestPoolStatCanceledAcquireDuringCreate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor := func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}

		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
		require.NoError(t, err)
		defer pool.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		time.AfterFunc(50*time.Millisecond, cancel)
		_, err = pool.Acquire(ctx)
		require.Equal(t, context.Canceled, err)

		synctest.Wait()

		stat := pool.Stat()
		assert.Equal(t, int64(0), stat.AcquireCount())
		assert.Equal(t, int64(1), stat.CanceledAcquireCount())
	})
}

func TestPoolStatCanceledAcquireDuringWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, _ := createConstructor()
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
		require.NoError(t, err)
		defer pool.Close()

		res, err := pool.Acquire(context.Background())
		require.Nil(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		time.AfterFunc(50*time.Millisecond, cancel)
		_, err = pool.Acquire(ctx)
		require.Equal(t, context.Canceled, err)

		res.Release()

		stat := pool.Stat()
		assert.Equal(t, int64(1), stat.AcquireCount())
		assert.Equal(t, int64(1), stat.CanceledAcquireCount())
	})
}

func TestResourceHijackRemovesResourceFromPoolButDoesNotDestroy(t *testing.T) {
	constructor, _ := createConstructor()
	var destructorCalls Counter
	destructor := func(int) {
		destructorCalls.Next()
	}

	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: 10})
	require.NoError(t, err)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	res.Hijack()

	assert.EqualValues(t, 0, pool.Stat().TotalResources())
	assert.EqualValues(t, 0, destructorCalls.Value())

	// Can still call Value, CreationTime and IdleDuration
	res.Value()
	res.CreationTime()
	res.IdleDuration()
}

func TestResourceDestroyRemovesResourceFromPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, _ := createConstructor()
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
		require.NoError(t, err)
		defer pool.Close()

		res, err := pool.Acquire(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, res.Value())

		assert.EqualValues(t, 1, pool.Stat().TotalResources())
		res.Destroy()
		synctest.Wait()

		assert.EqualValues(t, 0, pool.Stat().TotalResources())
	})
}

func TestResourceLastUsageTimeTracking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		constructor, _ := createConstructor()
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
		require.NoError(t, err)
		defer pool.Close()

		res, err := pool.Acquire(context.Background())
		require.NoError(t, err)
		// The process-global time origin predates the bubble, so compare
		// differences between readings made within the same virtual clock.
		t1 := res.LastUsedNanotime()
		time.Sleep(time.Second)
		res.Release()
		time.Sleep(2 * time.Second)

		// Release records the time of the last use.
		res, err = pool.Acquire(context.Background())
		require.NoError(t, err)
		t2 := res.LastUsedNanotime()
		d2 := res.IdleDuration()
		assert.Equal(t, int64(time.Second), t2-t1)
		assert.Equal(t, 2*time.Second, d2)
		res.ReleaseUnused()
		time.Sleep(3 * time.Second)

		// ReleaseUnused does not update usage tracking
		res, err = pool.Acquire(context.Background())
		require.NoError(t, err)
		t3 := res.LastUsedNanotime()
		d3 := res.IdleDuration()
		assert.EqualValues(t, t2, t3)
		assert.Equal(t, 5*time.Second, d3)
		res.Release()

		// Release does update usage tracking
		res, err = pool.Acquire(context.Background())
		require.NoError(t, err)
		t4 := res.LastUsedNanotime()
		assert.Equal(t, int64(5*time.Second), t4-t3)
		assert.Zero(t, res.IdleDuration())
		res.Release()
	})
}

func TestResourcePanicsOnUsageWhenNotAcquired(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	res.Release()

	assert.PanicsWithValue(t, "tried to release resource that is not acquired", res.Release)
	assert.PanicsWithValue(t, "tried to release resource that is not acquired", res.ReleaseUnused)
	assert.PanicsWithValue(t, "tried to destroy resource that is not acquired", res.Destroy)
	assert.PanicsWithValue(t, "tried to hijack resource that is not acquired", res.Hijack)
	assert.PanicsWithValue(t, "tried to access resource that is not acquired or hijacked", func() { res.Value() })
	assert.PanicsWithValue(t, "tried to access resource that is not acquired or hijacked", func() { res.CreationTime() })
	assert.PanicsWithValue(t, "tried to access resource that is not acquired or hijacked", func() { res.LastUsedNanotime() })
	assert.PanicsWithValue(t, "tried to access resource that is not acquired or hijacked", func() { res.IdleDuration() })
}

func TestPoolAcquireReturnsErrorWhenPoolIsClosed(t *testing.T) {
	constructor, _ := createConstructor()
	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 10})
	require.NoError(t, err)
	pool.Close()

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
	assert.Nil(t, res)
}

func TestSignalIsSentWhenResourceFailedToCreate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls Counter
		errOutage := errors.New("outage")
		constructor := func(context.Context) (int, error) {
			if calls.Next() == 2 {
				return 0, errOutage
			}
			return 1, nil
		}
		pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: 1})
		require.NoError(t, err)
		defer pool.Close()

		res, err := pool.Acquire(context.Background())
		require.NoError(t, err)

		results := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Go(func() {
				res, err := pool.Acquire(context.Background())
				if err == nil {
					res.Release()
				}
				results <- err
			})
		}
		synctest.Wait()
		assert.Empty(t, results, "both acquires must be waiting for the only resource")
		assert.Equal(t, 1, calls.Value())

		// Destroy wakes one waiter, whose failed constructor must wake the other.
		res.Destroy()
		wg.Wait()
		errs := []error{<-results, <-results}
		assert.ElementsMatch(t, []error{errOutage, nil}, errs)
		assert.Equal(t, 3, calls.Value())
		assert.EqualValues(t, 1, pool.Stat().IdleResources())
	})
}

func stressTestDur(t testing.TB) time.Duration {
	s := os.Getenv("STRESS_TEST_DURATION")
	if s == "" {
		s = "1s"
	}

	dur, err := time.ParseDuration(s)
	require.Nil(t, err)
	return dur
}

func TestStress(t *testing.T) {
	constructor, _ := createConstructor()
	var destructorCalls Counter
	destructor := func(int) {
		destructorCalls.Next()
	}

	poolSize := runtime.NumCPU()
	if poolSize < 4 {
		poolSize = 4
	}

	pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: destructor, MaxSize: int32(poolSize)})
	require.NoError(t, err)

	finishChan := make(chan struct{})
	wg := &sync.WaitGroup{}

	releaseOrDestroyOrHijack := func(res *puddle.Resource[int]) {
		n := rand.Intn(100)
		if n < 5 {
			res.Hijack()
			destructor(res.Value())
		} else if n < 10 {
			res.Destroy()
		} else {
			res.Release()
		}
	}

	actions := []func(){
		// Acquire
		func() {
			res, err := pool.Acquire(context.Background())
			if err != nil {
				if err != puddle.ErrClosedPool {
					assert.Failf(t, "stress acquire", "pool.Acquire returned unexpected err: %v", err)
				}
				return
			}

			time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
			releaseOrDestroyOrHijack(res)
		},
		// Acquire possibly canceled by context
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.Int63n(2000))*time.Nanosecond)
			defer cancel()
			res, err := pool.Acquire(ctx)
			if err != nil {
				if err != puddle.ErrClosedPool && err != context.Canceled && err != context.DeadlineExceeded {
					assert.Failf(t, "stress acquire possibly canceled by context", "pool.Acquire returned unexpected err: %v", err)
				}
				return
			}

			time.Sleep(time.Duration(rand.Int63n(2000)) * time.Nanosecond)
			releaseOrDestroyOrHijack(res)
		},
		// TryAcquire
		func() {
			res, err := pool.TryAcquire(context.Background())
			if err != nil {
				if err != puddle.ErrClosedPool && err != puddle.ErrNotAvailable {
					assert.Failf(t, "stress TryAcquire", "pool.TryAcquire returned unexpected err: %v", err)
				}
				return
			}

			time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)
			releaseOrDestroyOrHijack(res)
		},
		// AcquireAllIdle (though under heavy load this will almost certainly always get an empty slice)
		func() {
			resources := pool.AcquireAllIdle()
			for _, res := range resources {
				res.Release()
			}
		},
		// Stat
		func() {
			stat := pool.Stat()
			assert.NotNil(t, stat)
		},
		// CreateResource
		func() {
			err := pool.CreateResource(context.Background())
			if err != nil && !errors.Is(err, puddle.ErrClosedPool) && !errors.Is(err, puddle.ErrNotAvailable) {
				t.Error(err)
			}
		},
	}

	workerCount := int(poolSize) * 2

	for i := 0; i < workerCount; i++ {
		wg.Go(func() {
			for {
				select {
				case <-finishChan:
					return
				default:
				}

				actions[rand.Intn(len(actions))]()
			}
		})
	}

	time.AfterFunc(stressTestDur(t), func() { close(finishChan) })
	wg.Wait()
	pool.Close()
}

func TestStress_AcquireAllIdle_TryAcquire(t *testing.T) {
	r := require.New(t)

	pool := testPool[int32](t)

	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			idleRes := pool.AcquireAllIdle()
			r.Less(len(idleRes), 2)
			for _, res := range idleRes {
				res.Release()
			}
		}
	})

	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			res, err := pool.TryAcquire(context.Background())
			if err != nil {
				r.Equal(puddle.ErrNotAvailable, err)
			} else {
				r.NotNil(res)
				res.Release()
			}
		}
	})

	time.AfterFunc(stressTestDur(t), func() { close(done) })
	wg.Wait()
}

func TestStress_AcquireAllIdle_Acquire(t *testing.T) {
	r := require.New(t)

	pool := testPool[int32](t)

	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			idleRes := pool.AcquireAllIdle()
			r.Less(len(idleRes), 2)
			for _, res := range idleRes {
				r.NotNil(res)
				res.Release()
			}
		}
	})

	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			res, err := pool.Acquire(context.Background())
			if err != nil {
				r.Equal(puddle.ErrNotAvailable, err)
			} else {
				r.NotNil(res)
				res.Release()
			}
		}
	})

	time.AfterFunc(stressTestDur(t), func() { close(done) })
	wg.Wait()
}

func startAcceptOnceDummyServer(laddr string) {
	ln, err := net.Listen("tcp", laddr)
	if err != nil {
		log.Fatalln("Listen:", err)
	}

	// Listen one time
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln("Accept:", err)
		}

		for {
			buf := make([]byte, 1)
			_, err := conn.Read(buf)
			if err != nil {
				return
			}
		}
	}()

}

func ExamplePool() {
	// Dummy server
	laddr := "127.0.0.1:8080"
	startAcceptOnceDummyServer(laddr)

	// Pool creation
	constructor := func(context.Context) (any, error) {
		return net.Dial("tcp", laddr)
	}
	destructor := func(value any) {
		value.(net.Conn).Close()
	}
	maxPoolSize := int32(10)

	pool, err := puddle.NewPool(&puddle.Config[any]{Constructor: constructor, Destructor: destructor, MaxSize: int32(maxPoolSize)})
	if err != nil {
		log.Fatalln("NewPool", err)
	}

	// Use pool multiple times
	for i := 0; i < 10; i++ {
		// Acquire resource
		res, err := pool.Acquire(context.Background())
		if err != nil {
			log.Fatalln("Acquire", err)
		}

		// Type-assert value and use
		_, err = res.Value().(net.Conn).Write([]byte{1})
		if err != nil {
			log.Fatalln("Write", err)
		}

		// Release when done.
		res.Release()
	}

	stats := pool.Stat()
	pool.Close()

	fmt.Println("Connections:", stats.TotalResources())
	fmt.Println("Acquires:", stats.AcquireCount())
	// Output:
	// Connections: 1
	// Acquires: 10
}

func BenchmarkPoolAcquireAndRelease(b *testing.B) {
	benchmarks := []struct {
		poolSize    int32
		clientCount int
		cancellable bool
	}{
		{8, 1, false},
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

			constructor, _ := createConstructor()
			pool, err := puddle.NewPool(&puddle.Config[int]{Constructor: constructor, Destructor: stubDestructor, MaxSize: bm.poolSize})
			if err != nil {
				b.Fatal(err)
			}

			for i := 0; i < bm.clientCount; i++ {
				wg.Go(func() {
					for j := 0; j < b.N; j++ {
						res, err := pool.Acquire(ctx)
						if err != nil {
							b.Error(err)
							return
						}
						res.Release()
					}
				})
			}

			wg.Wait()
			cancel()
		})
	}
}

func TestAcquireAllSem(t *testing.T) {
	r := require.New(t)

	sem := semaphore.NewWeighted(5)
	r.Equal(4, puddle.AcquireSemAll(sem, 4))
	sem.Release(4)

	r.Equal(5, puddle.AcquireSemAll(sem, 5))
	sem.Release(5)

	r.Equal(5, puddle.AcquireSemAll(sem, 6))
	sem.Release(5)
}

func testPool[T any](t testing.TB) *puddle.Pool[T] {
	cfg := puddle.Config[T]{
		MaxSize: 1,
		Constructor: func(ctx context.Context) (T, error) {
			var zero T
			return zero, nil
		},
		Destructor: func(T) {},
	}

	pool, err := puddle.NewPool(&cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool
}

func releaser[T any](t testing.TB) chan<- *puddle.Resource[T] {
	startChan := make(chan struct{})
	workChan := make(chan *puddle.Resource[T], 1)

	go func() {
		close(startChan)

		for r := range workChan {
			r.Release()
		}
	}()
	t.Cleanup(func() { close(workChan) })

	// Wait for goroutine start.
	<-startChan
	return workChan
}

func TestReleaseAfterAcquire(t *testing.T) {
	const cnt = 100000

	r := require.New(t)
	ctx := context.Background()
	pool := testPool[int32](t)
	releaseChan := releaser[int32](t)

	res, err := pool.Acquire(ctx)
	r.NoError(err)
	// We need to release the last connection. Otherwise the pool.Close()
	// method will block and this function will never return.
	defer func() { res.Release() }()

	for i := 0; i < cnt; i++ {
		releaseChan <- res
		res, err = pool.Acquire(ctx)
		r.NoError(err)
	}
}

func BenchmarkAcquire_ReleaseAfterAcquire(b *testing.B) {
	r := require.New(b)
	ctx := context.Background()
	pool := testPool[int32](b)
	releaseChan := releaser[int32](b)

	res, err := pool.Acquire(ctx)
	r.NoError(err)
	// We need to release the last connection. Otherwise the pool.Close()
	// method will block and this function will never return.
	defer func() { res.Release() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		releaseChan <- res
		res, err = pool.Acquire(ctx)
		r.NoError(err)
	}
}

func withCPULoad() {
	// Multiply by 2 to similate overload of the system.
	numGoroutines := runtime.NumCPU() * 2

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			wg.Done()

			// Similate computationally intensive task.
			for j := 0; true; j++ {
			}
		}()
	}

	wg.Wait()
}

func BenchmarkAcquire_ReleaseAfterAcquireWithCPULoad(b *testing.B) {
	r := require.New(b)
	ctx := context.Background()
	pool := testPool[int32](b)
	releaseChan := releaser[int32](b)

	withCPULoad()

	res, err := pool.Acquire(ctx)
	r.NoError(err)
	// We need to release the last connection. Otherwise the pool.Close()
	// method will block and this function will never return.
	defer func() { res.Release() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		releaseChan <- res
		res, err = pool.Acquire(ctx)
		r.NoError(err)
	}
}

func BenchmarkAcquire_MultipleCancelled(b *testing.B) {
	const cancelCnt = 64

	r := require.New(b)
	ctx := context.Background()
	pool := testPool[int32](b)
	releaseChan := releaser[int32](b)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	res, err := pool.Acquire(ctx)
	r.NoError(err)
	// We need to release the last connection. Otherwise the pool.Close()
	// method will block and this function will never return.
	defer func() { res.Release() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cancelCnt; j++ {
			_, err = pool.AcquireRaw(cancelCtx)
			r.Equal(context.Canceled, err)
		}

		releaseChan <- res
		res, err = pool.Acquire(ctx)
		r.NoError(err)
	}
}

func BenchmarkAcquire_MultipleCancelledWithCPULoad(b *testing.B) {
	const cancelCnt = 3

	r := require.New(b)
	ctx := context.Background()
	pool := testPool[int32](b)
	releaseChan := releaser[int32](b)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	withCPULoad()

	res, err := pool.Acquire(ctx)
	r.NoError(err)
	// We need to release the last connection. Otherwise the pool.Close()
	// method will block and this function will never return.
	defer func() { res.Release() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < cancelCnt; j++ {
			_, err = pool.AcquireRaw(cancelCtx)
			r.Equal(context.Canceled, err)
		}

		releaseChan <- res
		res, err = pool.Acquire(ctx)
		r.NoError(err)
	}
}
