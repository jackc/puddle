package puddle

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

const (
	resourceStatusCreating  = 0
	resourceStatusAvailable = iota
	resourceStatusBorrowed  = iota
)

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)

// ErrClosedPool occurs on an attempt to get a connection from a closed pool.
var ErrClosedPool = errors.New("cannot get from closed pool")

type CreateFunc func(ctx context.Context) (res interface{}, err error)
type CloseFunc func(res interface{})

type resourceWrapper struct {
	resource      interface{}
	creationTime  time.Time
	checkoutCount uint64
	status        byte
}

// Pool is a thread-safe resource pool.
type Pool struct {
	cond *sync.Cond

	allResources         map[interface{}]*resourceWrapper
	availableResources   []*resourceWrapper
	minSize              int
	maxSize              int
	maxResourceDuration  time.Duration
	maxResourceCheckouts uint64
	closed               bool

	createRes CreateFunc
	closeRes  CloseFunc
}

func NewPool(createRes CreateFunc, closeRes CloseFunc) *Pool {
	return &Pool{
		cond:                 sync.NewCond(new(sync.Mutex)),
		allResources:         make(map[interface{}]*resourceWrapper),
		maxSize:              maxInt,
		maxResourceDuration:  math.MaxInt64,
		maxResourceCheckouts: math.MaxUint64,
		createRes:            createRes,
		closeRes:             closeRes,
	}
}

// Close closes all resources in the pool and rejects future Get calls.
// Unavailable resources will be closes when they are returned to the pool.
func (p *Pool) Close() {
	p.cond.L.Lock()
	p.closed = true

	for _, rw := range p.availableResources {
		p.closeRes(rw.resource)
		delete(p.allResources, rw.resource)
	}
	p.availableResources = nil
	p.cond.L.Unlock()

	// Wake up all go routines waiting for a resource to be returned so they can terminate.
	p.cond.Broadcast()
}

// Size returns the current size of the pool.
func (p *Pool) Size() int {
	p.cond.L.Lock()
	n := len(p.allResources)
	p.cond.L.Unlock()
	return n
}

// MinSize returns the current minimum size of the pool.
func (p *Pool) MinSize() int {
	p.cond.L.Lock()
	n := p.minSize
	p.cond.L.Unlock()
	return n
}

// SetMinSize sets the minimum size of the pool. It panics if n < 0.
func (p *Pool) SetMinSize(n int) {
	if n < 0 {
		panic("pool MinSize cannot be < 0")
	}
	p.cond.L.Lock()
	p.minSize = n

	p.cond.L.Unlock()
}

// MaxSize returns the current maximum size of the pool.
func (p *Pool) MaxSize() int {
	p.cond.L.Lock()
	n := p.maxSize
	p.cond.L.Unlock()
	return n
}

// SetMaxSize sets the maximum size of the pool. It panics if n < 1.
func (p *Pool) SetMaxSize(n int) {
	if n < 1 {
		panic("pool MaxSize cannot be < 1")
	}
	p.cond.L.Lock()
	p.maxSize = n
	p.cond.L.Unlock()
}

// MaxResourceDuration returns the current maximum resource duration of the pool.
func (p *Pool) MaxResourceDuration() time.Duration {
	p.cond.L.Lock()
	n := p.maxResourceDuration
	p.cond.L.Unlock()
	return n
}

// SetMaxResourceDuration sets the maximum maximum resource duration of the pool. It panics if n < 1.
func (p *Pool) SetMaxResourceDuration(d time.Duration) {
	if d < 0 {
		panic("pool MaxResourceDuration cannot be < 0")
	}
	p.cond.L.Lock()
	p.maxResourceDuration = d
	p.cond.L.Unlock()
}

// MaxResourceCheckouts returns the current maximum uses per resource of the pool.
func (p *Pool) MaxResourceCheckouts() uint64 {
	p.cond.L.Lock()
	n := p.maxResourceCheckouts
	p.cond.L.Unlock()
	return n
}

// SetMaxResourceCheckouts sets the maximum maximum resource duration of the pool. It panics if n < 1.
func (p *Pool) SetMaxResourceCheckouts(n uint64) {
	if n < 0 {
		panic("pool MaxResourceCheckouts cannot be < 1")
	}
	p.cond.L.Lock()
	p.maxResourceCheckouts = n
	p.cond.L.Unlock()
}

// Get gets a resource from the pool. If no resources are available and the pool
// is not at maximum capacity it will create a new resource. If the pool is at
// maximum capacity it will block until a resource is available. ctx can be used
// to cancel the Get.
func (p *Pool) Get(ctx context.Context) (interface{}, error) {
	if doneChan := ctx.Done(); doneChan != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	p.cond.L.Lock()

	if p.closed {
		p.cond.L.Unlock()
		return nil, ErrClosedPool
	}

	// If a resource is available now
	if len(p.availableResources) > 0 {
		res := p.lockedAvailableGet()
		p.cond.L.Unlock()
		return res, nil
	}

	// If there is room to create a resource do so
	if len(p.allResources) < p.maxSize {
		var localVal int
		placeholder := &localVal
		startTime := time.Now()
		p.allResources[placeholder] = &resourceWrapper{resource: placeholder, creationTime: startTime, status: resourceStatusCreating}
		p.cond.L.Unlock()

		res, err := p.createRes(ctx)
		p.cond.L.Lock()
		delete(p.allResources, placeholder)
		if err != nil {
			p.cond.L.Unlock()
			return nil, err
		}

		rw := &resourceWrapper{resource: res, creationTime: startTime, status: resourceStatusBorrowed, checkoutCount: 1}
		p.allResources[res] = rw
		p.cond.L.Unlock()
		return res, nil
	}
	p.cond.L.Unlock()

	// Wait for a resource to be returned to the pool.
	waitResChan := make(chan interface{})
	abortWaitResChan := make(chan struct{})
	go func() {
		p.cond.L.Lock()
		for len(p.availableResources) == 0 {
			p.cond.Wait()
		}
		if p.closed {
			p.cond.L.Unlock()
			return
		}
		res := p.lockedAvailableGet()
		p.cond.L.Unlock()

		select {
		case <-abortWaitResChan:
			p.Return(res)
		case waitResChan <- res:
		}
	}()

	select {
	case <-ctx.Done():
		close(abortWaitResChan)
		return nil, ctx.Err()
	case res := <-waitResChan:
		return res, nil
	}
}

// lockedAvailableGet gets the top resource from p.availableResources. p.cond.L
// must already be locked. len(p.availableResources) must be > 0.
func (p *Pool) lockedAvailableGet() interface{} {
	rw := p.availableResources[len(p.availableResources)-1]
	p.availableResources = p.availableResources[:len(p.availableResources)-1]
	if rw.status != resourceStatusAvailable {
		panic("BUG: unavailable resource gotten from availableResources")
	}
	rw.status = resourceStatusBorrowed
	rw.checkoutCount += 1
	return rw.resource
}

// Return returns res to the the pool. If res is not part of the pool Return
// will panic.
func (p *Pool) Return(res interface{}) {
	p.cond.L.Lock()

	rw, present := p.allResources[res]
	if !present {
		p.cond.L.Unlock()
		panic("Return called on resource that does not belong to pool")
	}

	closeResource := true

	now := time.Now()
	if p.closed {
	} else if now.Sub(rw.creationTime) > p.maxResourceDuration {
	} else if p.maxResourceCheckouts <= rw.checkoutCount { // use <= instead of == as maxResourceCheckouts may be lowered while pool is in use
	} else {
		closeResource = false
	}

	if closeResource {
		delete(p.allResources, rw.resource)
		p.cond.L.Unlock()
		go p.closeRes(rw.resource)
		return
	}

	rw.status = resourceStatusAvailable
	p.availableResources = append(p.availableResources, rw)

	p.cond.L.Unlock()
	p.cond.Signal()
}

// Remove removes res from the pool and closes it. If res is not part of the
// pool Remove will panic.
func (p *Pool) Remove(res interface{}) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	rw, present := p.allResources[res]
	if !present {
		panic("Remove called on resource that does not belong to pool")
	}

	delete(p.allResources, rw.resource)

	// close the resource in the background
	go func() {
		p.closeRes(res)
	}()
}
