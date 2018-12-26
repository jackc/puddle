package puddle

import (
	"context"
	"errors"
	"sync"
)

const (
	resourceStatusCreating  = 0
	resourceStatusAvailable = iota
	resourceStatusAcquired  = iota
	resourceStatusHijacked  = iota
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

	allResources       []*Resource
	availableResources []*Resource

	maxSize int
	closed  bool

	constructor Constructor
	destructor  Destructor
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

	for _, res := range p.availableResources {
		p.allResources = removeResource(p.allResources, res)
		go p.destructResourceValue(res.value)
	}
	p.availableResources = nil
	p.cond.L.Unlock()

	// Wake up all go routines waiting for a resource to be returned so they can terminate.
	p.cond.Broadcast()

	p.destructWG.Wait()
}

// Size returns the current size of the pool.
func (p *Pool) Size() int {
	p.cond.L.Lock()
	n := len(p.allResources)
	p.cond.L.Unlock()
	return n
}

// MaxSize returns the current maximum size of the pool.
func (p *Pool) MaxSize() int {
	p.cond.L.Lock()
	n := p.maxSize
	p.cond.L.Unlock()
	return n
}

// Acquire gets a resource from the pool. If no resources are available and the pool
// is not at maximum capacity it will create a new resource. If the pool is at
// maximum capacity it will block until a resource is available. ctx can be used
// to cancel the Acquire.
func (p *Pool) Acquire(ctx context.Context) (*Resource, error) {
	if doneChan := ctx.Done(); doneChan != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	p.cond.L.Lock()

	for {
		if p.closed {
			p.cond.L.Unlock()
			return nil, ErrClosedPool
		}

		// If a resource is available now
		if len(p.availableResources) > 0 {
			rw := p.lockedAvailableAcquire()
			p.cond.L.Unlock()
			return rw, nil
		}

		// If there is room to create a resource do so
		if len(p.allResources) < p.maxSize {
			res := &Resource{pool: p, status: resourceStatusCreating}
			p.allResources = append(p.allResources, res)
			p.cond.L.Unlock()

			value, err := p.constructResourceValue(ctx)
			p.cond.L.Lock()
			if err != nil {
				p.allResources = removeResource(p.allResources, res)
				p.cond.L.Unlock()
				return nil, err
			}

			res.value = value
			res.status = resourceStatusAcquired
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

// lockedAvailableAcquire gets the top resource from p.availableResources. p.cond.L
// must already be locked. len(p.availableResources) must be > 0.
func (p *Pool) lockedAvailableAcquire() *Resource {
	rw := p.availableResources[len(p.availableResources)-1]
	p.availableResources = p.availableResources[:len(p.availableResources)-1]
	if rw.status != resourceStatusAvailable {
		panic("BUG: unavailable resource gotten from availableResources")
	}
	rw.status = resourceStatusAcquired
	return rw
}

// releaseAcquiredResource returns res to the the pool.
func (p *Pool) releaseAcquiredResource(res *Resource) {
	p.cond.L.Lock()

	if !p.closed {
		res.status = resourceStatusAvailable
		p.availableResources = append(p.availableResources, res)
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
	value, err := p.constructor(ctx)
	if err != nil {
		return nil, err
	}
	p.destructWG.Add(1)
	return value, nil
}

func (p *Pool) destructResourceValue(value interface{}) {
	p.destructor(value)
	p.destructWG.Done()
}
