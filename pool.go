package puddle

import (
	"context"
	"errors"
	"sync"
)

const (
	resourceStatusConstructing = 0
	resourceStatusIdle         = iota
	resourceStatusAcquired     = iota
	resourceStatusHijacked     = iota
)

// ErrClosedPool occurs on an attempt to get a connection from a closed pool.
var ErrClosedPool = errors.New("cannot get from closed pool")

type Constructor func(ctx context.Context) (res interface{}, err error)
type Destructor func(res interface{})

type Resource struct {
	value  interface{}
	pool   *Pool
	status byte
}

func (res *Resource) Value() interface{} {
	return res.value
}

func (res *Resource) Release() {
	if res.status != resourceStatusAcquired {
		panic("tried to release resource that is not acquired")
	}
	res.pool.releaseAcquiredResource(res)
}

func (res *Resource) Destroy() {
	if res.status != resourceStatusAcquired {
		panic("tried to destroy resource that is not acquired")
	}
	res.pool.destroyAcquiredResource(res)
}

// Hijack removes the resource from the pool without destroying it. Caller is
// responsible for cleanup of resource value.
func (res *Resource) Hijack() {
	if res.status != resourceStatusAcquired {
		panic("tried to hijack resource that is not acquired")
	}
	res.pool.hijackAcquiredResource(res)
}

// Pool is a thread-safe resource pool.
type Pool struct {
	cond       *sync.Cond
	destructWG *sync.WaitGroup

	allResources  []*Resource
	idleResources []*Resource

	constructor Constructor
	destructor  Destructor
	maxSize     int

	acquireCount         int64
	slowAcquireCount     int64
	canceledAcquireCount int64

	closed bool
}

func NewPool(constructor Constructor, destructor Destructor, maxSize int) *Pool {
	return &Pool{
		cond:        sync.NewCond(new(sync.Mutex)),
		destructWG:  &sync.WaitGroup{},
		maxSize:     maxSize,
		constructor: constructor,
		destructor:  destructor,
	}
}

// Close closes all resources in the pool and rejects future Acquire calls.
// Blocks until all resources are returned to pool and closed.
func (p *Pool) Close() {
	p.cond.L.Lock()
	p.closed = true

	for _, res := range p.idleResources {
		p.allResources = removeResource(p.allResources, res)
		go p.destructResourceValue(res.value)
	}
	p.idleResources = nil
	p.cond.L.Unlock()

	// Wake up all go routines waiting for a resource to be returned so they can terminate.
	p.cond.Broadcast()

	p.destructWG.Wait()
}

type Stat struct {
	constructingResources int
	acquiredResources     int
	idleResources         int
	maxResources          int
	acquireCount          int64
	slowAcquireCount      int64
	canceledAcquireCount  int64
}

// TotalResource returns the total number of resources in the pool.
func (s *Stat) TotalResources() int {
	return s.constructingResources + s.acquiredResources + s.idleResources
}

// ConstructingResources returns the number of resources with construction in progress in
// the pool.
func (s *Stat) ConstructingResources() int {
	return s.constructingResources
}

// AcquiredResources returns the number of acquired resources in the pool.
func (s *Stat) AcquiredResources() int {
	return s.acquiredResources
}

// IdleResources returns the number of idle resources in the pool.
func (s *Stat) IdleResources() int {
	return s.idleResources
}

// MaxResources returns the maximum size of the pool
func (s *Stat) MaxResources() int {
	return s.maxResources
}

// AcquireCount returns the number of successful acquires from the pool.
func (s *Stat) AcquireCount() int64 {
	return s.acquireCount
}

// SlowAcquireCount returns the number of successful acquires from the pool
// that waited for a resource to be released or constructed.
func (s *Stat) SlowAcquireCount() int64 {
	return s.slowAcquireCount
}

// CanceledAcquireCount returns the number of acquires from the pool
// that were canceled by a context.
func (s *Stat) CanceledAcquireCount() int64 {
	return s.canceledAcquireCount
}

// Stat returns the current pool statistics.
func (p *Pool) Stat() *Stat {
	p.cond.L.Lock()
	s := &Stat{
		maxResources:         p.maxSize,
		acquireCount:         p.acquireCount,
		slowAcquireCount:     p.slowAcquireCount,
		canceledAcquireCount: p.canceledAcquireCount,
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

	p.cond.L.Unlock()
	return s
}

// Acquire gets a resource from the pool. If no resources are available and the pool
// is not at maximum capacity it will create a new resource. If the pool is at
// maximum capacity it will block until a resource is available. ctx can be used
// to cancel the Acquire.
func (p *Pool) Acquire(ctx context.Context) (*Resource, error) {
	p.cond.L.Lock()
	if doneChan := ctx.Done(); doneChan != nil {
		select {
		case <-ctx.Done():
			p.canceledAcquireCount += 1
			p.cond.L.Unlock()
			return nil, ctx.Err()
		default:
		}
	}

	slowAcquire := false

	for {
		if p.closed {
			p.cond.L.Unlock()
			return nil, ErrClosedPool
		}

		// If a resource is available now
		if len(p.idleResources) > 0 {
			res := p.idleResources[len(p.idleResources)-1]
			p.idleResources = p.idleResources[:len(p.idleResources)-1]
			res.status = resourceStatusAcquired
			p.acquireCount += 1
			if slowAcquire {
				p.slowAcquireCount += 1
			}
			p.cond.L.Unlock()
			return res, nil
		}

		slowAcquire = true

		// If there is room to create a resource do so
		if len(p.allResources) < p.maxSize {
			res := &Resource{pool: p, status: resourceStatusConstructing}
			p.allResources = append(p.allResources, res)
			p.destructWG.Add(1)
			p.cond.L.Unlock()

			value, err := p.constructResourceValue(ctx)
			p.cond.L.Lock()
			if err != nil {
				p.allResources = removeResource(p.allResources, res)
				p.destructWG.Done()

				select {
				case <-ctx.Done():
					if err == ctx.Err() {
						p.canceledAcquireCount += 1
					}
				default:
				}

				p.cond.L.Unlock()
				return nil, err
			}

			res.value = value
			res.status = resourceStatusAcquired
			p.acquireCount += 1
			p.slowAcquireCount += 1
			p.cond.L.Unlock()
			return res, nil
		}

		if ctx.Done() == nil {
			p.cond.Wait()
		} else {
			// Convert p.cond.Wait into a channel
			waitChan := make(chan struct{}, 1)
			go func() {
				p.cond.Wait()
				waitChan <- struct{}{}
			}()

			select {
			case <-ctx.Done():
				p.canceledAcquireCount += 1

				// Allow goroutine waiting for signal to exit. Re-signal since we couldn't
				// do anything with it. Another goroutine might be waiting.
				go func() {
					<-waitChan
					p.cond.Signal()
					p.cond.L.Unlock()
				}()

				return nil, ctx.Err()
			case <-waitChan:
			}
		}
	}
}

// releaseAcquiredResource returns res to the the pool.
func (p *Pool) releaseAcquiredResource(res *Resource) {
	p.cond.L.Lock()

	if !p.closed {
		res.status = resourceStatusIdle
		p.idleResources = append(p.idleResources, res)
	} else {
		p.allResources = removeResource(p.allResources, res)
		go p.destructResourceValue(res.value)
	}

	p.cond.L.Unlock()
	p.cond.Signal()
}

// Remove removes res from the pool and closes it. If res is not part of the
// pool Remove will panic.
func (p *Pool) destroyAcquiredResource(res *Resource) {
	p.cond.L.Lock()

	p.allResources = removeResource(p.allResources, res)
	go p.destructResourceValue(res.value)

	p.cond.L.Unlock()
	p.cond.Signal()
}

func (p *Pool) hijackAcquiredResource(res *Resource) {
	p.cond.L.Lock()

	p.allResources = removeResource(p.allResources, res)
	res.status = resourceStatusHijacked
	p.destructWG.Done() // not responsible for destructing hijacked resources

	p.cond.L.Unlock()
	p.cond.Signal()
}

func removeResource(slice []*Resource, res *Resource) []*Resource {
	for i := range slice {
		if slice[i] == res {
			slice[i] = slice[len(slice)-1]
			return slice[:len(slice)-1]
		}
	}

	return slice
}

func (p *Pool) constructResourceValue(ctx context.Context) (interface{}, error) {
	return p.constructor(ctx)
}

func (p *Pool) destructResourceValue(value interface{}) {
	p.destructor(value)
	p.destructWG.Done()
}
