package puddle_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
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

func createConstructor() (puddle.Constructor, *Counter) {
	var c Counter
	f := func(ctx context.Context) (interface{}, error) {
		return c.Next(), nil
	}
	return f, &c
}

func createConstructorWithNotifierChan() (puddle.Constructor, *Counter, chan int) {
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

func stubDestructor(interface{}) {}

func waitForRead(ch chan int) bool {
	select {
	case <-ch:
		return true
	case <-time.NewTimer(time.Second).C:
		return false
	}
}

func TestPoolAcquireCreatesResourceWhenNoneIdle(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 10)
	defer pool.Close()

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())
	assert.WithinDuration(t, time.Now(), res.CreationTime(), time.Second)
	res.Release()
}

func TestPoolAcquireDoesNotCreatesResourceWhenItWouldExceedMaxSize(t *testing.T) {
	constructor, createCounter := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 1)

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
	constructor, createCounter := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 1)

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
	constructor := func(ctx context.Context) (interface{}, error) {
		return nil, errCreateFailed
	}
	pool := puddle.NewPool(constructor, stubDestructor, 10)

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, errCreateFailed, err)
	assert.Nil(t, res)
}

func TestPoolAcquireReusesResources(t *testing.T) {
	constructor, createCounter := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 10)

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
	constructor := func(ctx context.Context) (interface{}, error) {
		panic("should never be called")
	}
	pool := puddle.NewPool(constructor, stubDestructor, 10)

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

	var constructorCalls Counter
	constructor := func(ctx context.Context) (interface{}, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeoutChan:
		}
		return constructorCalls.Next(), nil
	}
	pool := puddle.NewPool(constructor, stubDestructor, 10)

	res, err := pool.Acquire(ctx)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, res)
}

func TestPoolAcquireAllIdle(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 10)
	defer pool.Close()

	resources := make([]*puddle.Resource, 4)
	var err error

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

	assert.ElementsMatch(t, []*puddle.Resource{resources[0], resources[3]}, pool.AcquireAllIdle())

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

func TestPoolCloseClosesAllIdleResources(t *testing.T) {
	constructor, _ := createConstructor()

	var destructorCalls Counter
	destructor := func(interface{}) {
		destructorCalls.Next()
	}

	p := puddle.NewPool(constructor, destructor, 10)

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

	assert.Equal(t, len(resources), destructorCalls.Value())
}

func TestPoolCloseBlocksUntilAllResourcesReleasedAndClosed(t *testing.T) {
	constructor, _ := createConstructor()
	var destructorCalls Counter
	destructor := func(interface{}) {
		destructorCalls.Next()
	}

	p := puddle.NewPool(constructor, destructor, 10)

	resources := make([]*puddle.Resource, 4)
	for i := range resources {
		var err error
		resources[i], err = p.Acquire(context.Background())
		require.Nil(t, err)
	}

	for _, res := range resources {
		go func(res *puddle.Resource) {
			time.Sleep(100 * time.Millisecond)
			res.Release()
		}(res)
	}

	p.Close()
	assert.Equal(t, len(resources), destructorCalls.Value())
}

func TestPoolStatResources(t *testing.T) {
	startWaitChan := make(chan struct{})
	waitingChan := make(chan struct{})
	endWaitChan := make(chan struct{})

	var constructorCalls Counter
	constructor := func(ctx context.Context) (interface{}, error) {
		select {
		case <-startWaitChan:
			close(waitingChan)
			<-endWaitChan
		default:
		}

		return constructorCalls.Next(), nil
	}
	pool := puddle.NewPool(constructor, stubDestructor, 10)
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

func TestPoolStatSuccessfulAcquireCounters(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 1)
	defer pool.Close()

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	res.Release()

	stat := pool.Stat()
	assert.Equal(t, int64(1), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.EmptyAcquireCount())
	assert.True(t, stat.AcquireDuration() > 0, "expected stat.AcquireDuration() > 0 but %v", stat.AcquireDuration())
	lastAcquireDuration := stat.AcquireDuration()

	res, err = pool.Acquire(context.Background())
	require.NoError(t, err)
	res.Release()

	stat = pool.Stat()
	assert.Equal(t, int64(2), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.EmptyAcquireCount())
	assert.True(t, stat.AcquireDuration() > lastAcquireDuration)
	lastAcquireDuration = stat.AcquireDuration()

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
	assert.Equal(t, int64(2), stat.EmptyAcquireCount())
	assert.True(t, stat.AcquireDuration() > lastAcquireDuration)
	lastAcquireDuration = stat.AcquireDuration()
}

func TestPoolStatCanceledAcquireBeforeStart(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 1)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pool.Acquire(ctx)
	require.Equal(t, context.Canceled, err)

	stat := pool.Stat()
	assert.Equal(t, int64(0), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.CanceledAcquireCount())
}

func TestPoolStatCanceledAcquireDuringCreate(t *testing.T) {
	constructor := func(ctx context.Context) (interface{}, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	pool := puddle.NewPool(constructor, stubDestructor, 1)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := pool.Acquire(ctx)
	require.Equal(t, context.Canceled, err)

	stat := pool.Stat()
	assert.Equal(t, int64(0), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.CanceledAcquireCount())
}

func TestPoolStatCanceledAcquireDuringWait(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 1)
	defer pool.Close()

	res, err := pool.Acquire(context.Background())
	require.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err = pool.Acquire(ctx)
	require.Equal(t, context.Canceled, err)

	res.Release()

	stat := pool.Stat()
	assert.Equal(t, int64(1), stat.AcquireCount())
	assert.Equal(t, int64(1), stat.CanceledAcquireCount())
}

func TestResourceDestroyRemovesResourceFromPool(t *testing.T) {
	constructor, _ := createConstructor()
	var destructorCalls Counter
	destructor := func(interface{}) {
		destructorCalls.Next()
	}

	pool := puddle.NewPool(constructor, destructor, 10)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	res.Hijack()

	assert.Equal(t, 0, pool.Stat().TotalResources())
	assert.Equal(t, 0, destructorCalls.Value())

	// Can still call Value and CreationTime
	res.Value()
	res.CreationTime()
}

func TestResourceHijackRemovesResourceFromPoolButDoesNotDestroy(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 10)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, res.Value())

	assert.Equal(t, 1, pool.Stat().TotalResources())
	res.Destroy()
	assert.Equal(t, 0, pool.Stat().TotalResources())
}

func TestResourcePanicsOnUsageWhenNotAcquired(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 10)

	res, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	res.Release()

	assert.PanicsWithValue(t, "tried to release resource that is not acquired", res.Release)
	assert.PanicsWithValue(t, "tried to destroy resource that is not acquired", res.Destroy)
	assert.PanicsWithValue(t, "tried to hijack resource that is not acquired", res.Hijack)
	assert.PanicsWithValue(t, "tried to access resource that is not acquired or hijacked", func() { res.Value() })
	assert.PanicsWithValue(t, "tried to access resource that is not acquired or hijacked", func() { res.CreationTime() })
}

func TestPoolAcquireReturnsErrorWhenPoolIsClosed(t *testing.T) {
	constructor, _ := createConstructor()
	pool := puddle.NewPool(constructor, stubDestructor, 10)
	pool.Close()

	res, err := pool.Acquire(context.Background())
	assert.Equal(t, puddle.ErrClosedPool, err)
	assert.Nil(t, res)
}

func TestStress(t *testing.T) {
	constructor, _ := createConstructor()
	var destructorCalls Counter
	destructor := func(interface{}) {
		destructorCalls.Next()
	}

	pool := puddle.NewPool(constructor, destructor, 10)

	finishChan := make(chan struct{})
	wg := &sync.WaitGroup{}

	releaseOrDestroyOrHijack := func(res *puddle.Resource) {
		n := rand.Intn(100)
		if n < 5 {
			res.Hijack()
			destructor(res)
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.Int63n(200))*time.Millisecond)
			defer cancel()
			res, err := pool.Acquire(ctx)
			if err != nil {
				if err != puddle.ErrClosedPool && err != context.Canceled && err != context.DeadlineExceeded {
					assert.Failf(t, "stress acquire possibly canceled by context", "pool.Acquire returned unexpected err: %v", err)
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
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for {
				select {
				case <-finishChan:
					wg.Done()
					return
				default:
				}

				actions[rand.Intn(len(actions))]()
			}
		}()
	}

	s := os.Getenv("STRESS_TEST_DURATION")
	if s == "" {
		s = "1s"
	}
	testDuration, err := time.ParseDuration(s)
	require.Nil(t, err)
	time.AfterFunc(testDuration, func() { close(finishChan) })
	wg.Wait()

	pool.Close()
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
	constructor := func(context.Context) (interface{}, error) {
		return net.Dial("tcp", laddr)
	}
	destructor := func(value interface{}) {
		value.(net.Conn).Close()
	}
	maxPoolSize := 10

	pool := puddle.NewPool(constructor, destructor, maxPoolSize)

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

			constructor, _ := createConstructor()
			pool := puddle.NewPool(constructor, stubDestructor, bm.poolSize)

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
