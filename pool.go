package puddle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/puddle/v2/internal/circ"
	"golang.org/x/sync/semaphore"
)

const (
	resourceStatusConstructing = 0
	resourceStatusIdle         = iota
	resourceStatusAcquired     = iota
	resourceStatusHijacked     = iota
)

// ErrClosedPool occurs on an attempt to acquire a connection from a closed pool
// or a pool that is closed while the acquire is waiting.
var ErrClosedPool = errors.New("closed pool")

// ErrNotAvailable occurs on an attempt to acquire a resource from a pool
// that is at maximum capacity and has no available resources.
var ErrNotAvailable = errors.New("resource not available")

// Constructor is a function called by the pool to construct a resource.
type Constructor[T any] func(ctx context.Context) (res T, err error)

// Destructor is a function called by the pool to destroy a resource.
type Destructor[T any] func(res T)

// Resource is the resource handle returned by acquiring from the pool.
type Resource[T any] struct {
	value          T
	pool           *Pool[T]
	creationTime   time.Time
	lastUsedNano   int64
	poolResetCount int
	status         byte
}

// Value returns the resource value.
func (res *Resource[T]) Value() T {
	if !(res.status == resourceStatusAcquired || res.status == resourceStatusHijacked) {
		panic("tried to access resource that is not acquired or hijacked")
	}
	return res.value
}

// Release returns the resource to the pool. res must not be subsequently used.
func (res *Resource[T]) Release() {
	if res.status != resourceStatusAcquired {
		panic("tried to release resource that is not acquired")
	}
	res.pool.releaseAcquiredResource(res, nanotime())
}

// ReleaseUnused returns the resource to the pool without updating when it was last used used. i.e. LastUsedNanotime
// will not change. res must not be subsequently used.
func (res *Resource[T]) ReleaseUnused() {
	if res.status != resourceStatusAcquired {
		panic("tried to release resource that is not acquired")
	}
	res.pool.releaseAcquiredResource(res, res.lastUsedNano)
}

// Destroy returns the resource to the pool for destruction. res must not be
// subsequently used.
func (res *Resource[T]) Destroy() {
	if res.status != resourceStatusAcquired {
		panic("tried to destroy resource that is not acquired")
	}
	go res.pool.destroyAcquiredResource(res)
}

// Hijack assumes ownership of the resource from the pool. Caller is responsible
// for cleanup of resource value.
func (res *Resource[T]) Hijack() {
	if res.status != resourceStatusAcquired {
		panic("tried to hijack resource that is not acquired")
	}
	res.pool.hijackAcquiredResource(res)
}

// CreationTime returns when the resource was created by the pool.
func (res *Resource[T]) CreationTime() time.Time {
	if !(res.status == resourceStatusAcquired || res.status == resourceStatusHijacked) {
		panic("tried to access resource that is not acquired or hijacked")
	}
	return res.creationTime
}

// LastUsedNanotime returns when Release was last called on the resource measured in nanoseconds from an arbitrary time
// (a monotonic time). Returns creation time if Release has never been called. This is only useful to compare with
// other calls to LastUsedNanotime. In almost all cases, IdleDuration should be used instead.
func (res *Resource[T]) LastUsedNanotime() int64 {
	if !(res.status == resourceStatusAcquired || res.status == resourceStatusHijacked) {
		panic("tried to access resource that is not acquired or hijacked")
	}

	return res.lastUsedNano
}

// IdleDuration returns the duration since Release was last called on the resource. This is equivalent to subtracting
// LastUsedNanotime to the current nanotime.
func (res *Resource[T]) IdleDuration() time.Duration {
	if !(res.status == resourceStatusAcquired || res.status == resourceStatusHijacked) {
		panic("tried to access resource that is not acquired or hijacked")
	}

	return time.Duration(nanotime() - res.lastUsedNano)
}

// Pool is a concurrency-safe resource pool.
type Pool[T any] struct {
	mux        sync.Mutex
	acquireSem *semaphore.Weighted
	destructWG sync.WaitGroup

	allResources  []*Resource[T]
	idleResources *circ.Queue[*Resource[T]]

	constructor Constructor[T]
	destructor  Destructor[T]
	maxSize     int32

	acquireCount         int64
	acquireDuration      time.Duration
	emptyAcquireCount    int64
	canceledAcquireCount atomic.Int64

	resetCount int

	baseAcquireCtx       context.Context
	cancelBaseAcquireCtx context.CancelFunc
	closed               bool
}

type Config[T any] struct {
	Constructor Constructor[T]
	Destructor  Destructor[T]
	MaxSize     int32
}

// NewPool creates a new pool. Panics if maxSize is less than 1.
func NewPool[T any](config *Config[T]) (*Pool[T], error) {
	if config.MaxSize < 1 {
		return nil, errors.New("MaxSize must be >= 1")
	}

	baseAcquireCtx, cancelBaseAcquireCtx := context.WithCancel(context.Background())

	return &Pool[T]{
		acquireSem:           semaphore.NewWeighted(int64(config.MaxSize)),
		idleResources:        circ.NewQueue[*Resource[T]](int(config.MaxSize)),
		maxSize:              config.MaxSize,
		constructor:          config.Constructor,
		destructor:           config.Destructor,
		baseAcquireCtx:       baseAcquireCtx,
		cancelBaseAcquireCtx: cancelBaseAcquireCtx,
	}, nil
}

// Close destroys all resources in the pool and rejects future Acquire calls.
// Blocks until all resources are returned to pool and destroyed.
func (p *Pool[T]) Close() {
	defer p.destructWG.Wait()

	p.mux.Lock()
	defer p.mux.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	p.cancelBaseAcquireCtx()

	for p.idleResources.Len() > 0 {
		res := p.idleResources.Dequeue()
		p.allResources = removeResource(p.allResources, res)
		go p.destructResourceValue(res.value)
	}
	p.idleResources = nil
}

// Stat is a snapshot of Pool statistics.
type Stat struct {
	constructingResources int32
	acquiredResources     int32
	idleResources         int32
	maxResources          int32
	acquireCount          int64
	acquireDuration       time.Duration
	emptyAcquireCount     int64
	canceledAcquireCount  int64
}

// TotalResources returns the total number of resources currently in the pool.
// The value is the sum of ConstructingResources, AcquiredResources, and
// IdleResources.
func (s *Stat) TotalResources() int32 {
	return s.constructingResources + s.acquiredResources + s.idleResources
}

// ConstructingResources returns the number of resources with construction in progress in
// the pool.
func (s *Stat) ConstructingResources() int32 {
	return s.constructingResources
}

// AcquiredResources returns the number of currently acquired resources in the pool.
func (s *Stat) AcquiredResources() int32 {
	return s.acquiredResources
}

// IdleResources returns the number of currently idle resources in the pool.
func (s *Stat) IdleResources() int32 {
	return s.idleResources
}

// MaxResources returns the maximum size of the pool.
func (s *Stat) MaxResources() int32 {
	return s.maxResources
}

// AcquireCount returns the cumulative count of successful acquires from the pool.
func (s *Stat) AcquireCount() int64 {
	return s.acquireCount
}

// AcquireDuration returns the total duration of all successful acquires from
// the pool.
func (s *Stat) AcquireDuration() time.Duration {
	return s.acquireDuration
}

// EmptyAcquireCount returns the cumulative count of successful acquires from the pool
// that waited for a resource to be released or constructed because the pool was
// empty.
func (s *Stat) EmptyAcquireCount() int64 {
	return s.emptyAcquireCount
}

// CanceledAcquireCount returns the cumulative count of acquires from the pool
// that were canceled by a context.
func (s *Stat) CanceledAcquireCount() int64 {
	return s.canceledAcquireCount
}

// Stat returns the current pool statistics.
func (p *Pool[T]) Stat() *Stat {
	p.mux.Lock()
	defer p.mux.Unlock()

	s := &Stat{
		maxResources:         p.maxSize,
		acquireCount:         p.acquireCount,
		emptyAcquireCount:    p.emptyAcquireCount,
		canceledAcquireCount: p.canceledAcquireCount.Load(),
		acquireDuration:      p.acquireDuration,
	}

	for _, res := range p.allResources {
		switch res.status {
		case resourceStatusConstructing:
			s.constructingResources += 1
		case resourceStatusIdle:
			s.idleResources += 1
		case resourceStatusAcquired:
			s.acquiredResources += 1
		}
	}

	return s
}

// tryAcquireIdleResource checks if there is any idle resource. If there is
// some, this method removes it from idle list and returns it. If the idle pool
// is empty, this method returns nil and doesn't modify the idleResources slice.
//
// WARNING: Caller of this method must hold the pool mutex!
func (p *Pool[T]) tryAcquireIdleResource() *Resource[T] {
	if p.idleResources.Len() == 0 {
		return nil
	}

	return p.idleResources.Dequeue()
}

// createNewResource creates a new resource and inserts it into list of pool
// resources.
//
// WARNING: Caller of this method must hold the pool mutex!
func (p *Pool[T]) createNewResource() *Resource[T] {
	res := &Resource[T]{
		pool:           p,
		creationTime:   time.Now(),
		lastUsedNano:   nanotime(),
		poolResetCount: p.resetCount,
		status:         resourceStatusConstructing,
	}

	p.allResources = append(p.allResources, res)
	p.destructWG.Add(1)

	return res
}

// Acquire gets a resource from the pool. If no resources are available and the pool is not at maximum capacity it will
// create a new resource. If the pool is at maximum capacity it will block until a resource is available. ctx can be
// used to cancel the Acquire.
//
// If Acquire creates a new resource the resource constructor function will receive a context that delegates Value() to
// ctx. Canceling ctx will cause Acquire to return immediately but it will not cancel the resource creation. This avoids
// the problem of it being impossible to create resources when the time to create a resource is greater than any one
// caller of Acquire is willing to wait.
func (p *Pool[T]) Acquire(ctx context.Context) (_ *Resource[T], err error) {
	select {
	case <-ctx.Done():
		p.canceledAcquireCount.Add(1)
		return nil, ctx.Err()
	default:
	}

	return p.acquire(ctx)
}

// acquire is a continuation of Acquire function that doesn't check context
// validity. This function exists separatly only for benchmarking purposes.
func (p *Pool[T]) acquire(ctx context.Context) (*Resource[T], error) {
	startNano := nanotime()

	var waitedForLock bool
	if !p.acquireSem.TryAcquire(1) {
		waitedForLock = true
		err := p.acquireSem.Acquire(ctx, 1)
		if err != nil {
			p.canceledAcquireCount.Add(1)
			return nil, err
		}
	}

	p.mux.Lock()
	if p.closed {
		p.mux.Unlock()
		p.acquireSem.Release(1)
		return nil, ErrClosedPool
	}

	// If a resource is available in the pool.
	if res := p.tryAcquireIdleResource(); res != nil {
		res.status = resourceStatusAcquired
		if waitedForLock {
			p.emptyAcquireCount += 1
		}
		p.acquireCount += 1
		p.acquireDuration += time.Duration(nanotime() - startNano)
		p.mux.Unlock()
		return res, nil
	}

	if len(p.allResources) >= int(p.maxSize) {
		p.mux.Unlock()
		p.acquireSem.Release(1)
		panic("bug: semaphore allowed more acquires than pool allows")
	}

	// The resource is not available, but there is enough space to create
	// one.
	res := p.createNewResource()
	p.mux.Unlock()

	// Create the resource in a goroutine to immediately return from Acquire if ctx is canceled without also canceling
	// the constructor. See: https://github.com/jackc/pgx/issues/1287 and https://github.com/jackc/pgx/issues/1259
	constructErrCh := make(chan error)
	go func() {
		constructorCtx := newValueCancelCtx(ctx, p.baseAcquireCtx)
		value, err := p.constructResourceValue(constructorCtx)
		if err != nil {
			p.mux.Lock()
			p.allResources = removeResource(p.allResources, res)
			p.destructWG.Done()

			p.mux.Unlock()
			// We won't take the resource so we allow someone else
			// to run Acquire.
			p.acquireSem.Release(1)

			select {
			case constructErrCh <- err:
			case <-ctx.Done():
				// The caller is cancelled, so no-one awaits the
				// error. This branch avoid goroutine leak.
			}
			return
		}

		res.value = value
		res.status = resourceStatusAcquired

		select {
		case constructErrCh <- nil:
			p.mux.Lock()
			p.emptyAcquireCount += 1
			p.acquireCount += 1
			p.acquireDuration += time.Duration(nanotime() - startNano)
			p.mux.Unlock()
		case <-ctx.Done():
			p.releaseAcquiredResource(res, res.lastUsedNano)
		}
	}()

	select {
	case <-ctx.Done():
		p.canceledAcquireCount.Add(1)
		return nil, ctx.Err()
	case err := <-constructErrCh:
		if err != nil {
			return nil, err
		}
		return res, nil
	}
}

// TryAcquire gets a resource from the pool if one is immediately available. If not, it returns ErrNotAvailable. If no
// resources are available but the pool has room to grow, a resource will be created in the background. ctx is only
// used to cancel the background creation.
func (p *Pool[T]) TryAcquire(ctx context.Context) (*Resource[T], error) {
	if !p.acquireSem.TryAcquire(1) {
		return nil, ErrNotAvailable
	}

	p.mux.Lock()
	defer p.mux.Unlock()

	if p.closed {
		p.acquireSem.Release(1)
		return nil, ErrClosedPool
	}

	// If a resource is available now
	if res := p.tryAcquireIdleResource(); res != nil {
		p.acquireCount += 1
		res.status = resourceStatusAcquired
		return res, nil
	}

	if len(p.allResources) >= int(p.maxSize) {
		panic("bug: semaphore allowed more acquires than pool allows")
	}

	res := p.createNewResource()
	go func() {
		value, err := p.constructResourceValue(ctx)
		// We have to create the resource and only then release the
		// semaphore - For the time being there is no resource that
		// someone could acquire.
		defer p.acquireSem.Release(1)
		p.mux.Lock()
		defer p.mux.Unlock()

		if err != nil {
			p.allResources = removeResource(p.allResources, res)
			p.destructWG.Done()
			return
		}

		res.value = value
		res.status = resourceStatusIdle
		p.idleResources.Enqueue(res)
	}()

	return nil, ErrNotAvailable
}

// AcquireAllIdle atomically acquires all currently idle resources. Its intended
// use is for health check and keep-alive functionality. It does not update pool
// statistics.
func (p *Pool[T]) AcquireAllIdle() []*Resource[T] {
	var cnt int
	for p.acquireSem.TryAcquire(1) {
		cnt++
	}
	if cnt == 0 {
		return nil
	}

	resources := make([]*Resource[T], 0, cnt)

	p.mux.Lock()
	defer p.mux.Unlock()

	if p.closed {
		p.acquireSem.Release(int64(cnt))
		return nil
	}

	// Some resources from the maxSize limit do not have to exist (i.e. be
	// idle).
	if diff := cnt - p.idleResources.Len(); diff > 0 {
		p.acquireSem.Release(int64(diff))
		cnt = p.idleResources.Len()
	}

	// We are not guaranteed that idleResources are empty after this loop.
	// But we are guaranteed that all resources remain in idleResources
	// after this loop will (1) either be acquired soon (semaphore was
	// already acquired for them) or (2) were released after start of this
	// function.
	for i := 0; i < cnt; i++ {
		res := p.idleResources.Dequeue()
		res.status = resourceStatusAcquired
		resources = append(resources, res)
	}

	return resources
}

// CreateResource constructs a new resource without acquiring it.
// It goes straight in the IdlePool. It does not check against maxSize.
// It can be useful to maintain warm resources under little load.
func (p *Pool[T]) CreateResource(ctx context.Context) error {
	p.mux.Lock()
	if p.closed {
		p.mux.Unlock()
		return ErrClosedPool
	}
	p.destructWG.Add(1)
	p.mux.Unlock()

	value, err := p.constructResourceValue(ctx)
	if err != nil {
		p.destructWG.Done()
		return err
	}

	res := &Resource[T]{
		pool:           p,
		creationTime:   time.Now(),
		status:         resourceStatusIdle,
		value:          value,
		lastUsedNano:   nanotime(),
		poolResetCount: p.resetCount,
	}

	p.mux.Lock()
	defer p.mux.Unlock()

	// If closed while constructing resource then destroy it and return an error
	if p.closed {
		go p.destructResourceValue(res.value)
		return ErrClosedPool
	}
	p.allResources = append(p.allResources, res)
	p.idleResources.Enqueue(res)

	return nil
}

// Reset destroys all resources, but leaves the pool open. It is intended for use when an error is detected that would
// disrupt all resources (such as a network interruption or a server state change).
//
// It is safe to reset a pool while resources are checked out. Those resources will be destroyed when they are returned
// to the pool.
func (p *Pool[T]) Reset() {
	p.mux.Lock()
	defer p.mux.Unlock()

	p.resetCount++

	for p.idleResources.Len() > 0 {
		res := p.idleResources.Dequeue()
		p.allResources = removeResource(p.allResources, res)
		go p.destructResourceValue(res.value)
	}
}

// releaseAcquiredResource returns res to the the pool.
func (p *Pool[T]) releaseAcquiredResource(res *Resource[T], lastUsedNano int64) {
	defer p.acquireSem.Release(1)
	p.mux.Lock()
	defer p.mux.Unlock()

	if p.closed || res.poolResetCount != p.resetCount {
		p.allResources = removeResource(p.allResources, res)
		go p.destructResourceValue(res.value)
	} else {
		res.lastUsedNano = lastUsedNano
		res.status = resourceStatusIdle
		p.idleResources.Enqueue(res)
	}
}

// Remove removes res from the pool and closes it. If res is not part of the
// pool Remove will panic.
func (p *Pool[T]) destroyAcquiredResource(res *Resource[T]) {
	p.destructResourceValue(res.value)
	defer p.acquireSem.Release(1)
	p.mux.Lock()
	defer p.mux.Unlock()
	p.allResources = removeResource(p.allResources, res)
}

func (p *Pool[T]) hijackAcquiredResource(res *Resource[T]) {
	defer p.acquireSem.Release(1)
	p.mux.Lock()
	defer p.mux.Unlock()

	p.allResources = removeResource(p.allResources, res)
	res.status = resourceStatusHijacked
	p.destructWG.Done() // not responsible for destructing hijacked resources
}

func removeResource[T any](slice []*Resource[T], res *Resource[T]) []*Resource[T] {
	for i := range slice {
		if slice[i] == res {
			slice[i] = slice[len(slice)-1]
			slice[len(slice)-1] = nil // Avoid memory leak
			return slice[:len(slice)-1]
		}
	}

	panic("BUG: removeResource could not find res in slice")
}

func (p *Pool[T]) constructResourceValue(ctx context.Context) (T, error) {
	return p.constructor(ctx)
}

func (p *Pool[T]) destructResourceValue(value T) {
	p.destructor(value)
	p.destructWG.Done()
}
