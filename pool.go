package puddle

import (
	"context"
	"errors"
	"sync"
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

type Resource struct {
	value  interface{}
	pool   *Pool
	status byte
}

func (res *Resource) Value() interface{} {
	return res.value
}

func (res *Resource) Release() {
	res.pool.releaseBorrowedResource(res)
}

func (res *Resource) Destroy() {
	res.pool.destroyBorrowedResource(res)
}

// Pool is a thread-safe resource pool.
type Pool struct {
	cond *sync.Cond

	allResources       []*Resource
	availableResources []*Resource

	minSize int
	maxSize int
	closed  bool

	createRes CreateFunc
	closeRes  CloseFunc
}

func NewPool(createRes CreateFunc, closeRes CloseFunc) *Pool {
	return &Pool{
		cond:      sync.NewCond(new(sync.Mutex)),
		maxSize:   maxInt,
		createRes: createRes,
		closeRes:  closeRes,
	}
}

// Close closes all resources in the pool and rejects future Acquire calls.
// Unavailable resources will be closes when they are returned to the pool.
func (p *Pool) Close() {
	p.cond.L.Lock()
	p.closed = true

	for _, res := range p.availableResources {
		p.closeRes(res.value)
		p.allResources = removeResource(p.allResources, res)
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

			value, err := p.createRes(ctx)
			p.cond.L.Lock()
			if err != nil {
				p.allResources = removeResource(p.allResources, res)
				p.cond.L.Unlock()
				return nil, err
			}

			res.value = value
			res.status = resourceStatusBorrowed
			p.cond.L.Unlock()
			return res, nil
		}

		// if ctx.Done() == nil {
		// 	p.cond.Wait()
		// } else {

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
		// }
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
	rw.status = resourceStatusBorrowed
	return rw
}

// releaseBorrowedResource returns res to the the pool.
func (p *Pool) releaseBorrowedResource(res *Resource) {
	p.cond.L.Lock()

	if p.closed {
		p.allResources = removeResource(p.allResources, res)
		p.cond.L.Unlock()
		go p.closeRes(res.value)
		return
	}

	res.status = resourceStatusAvailable
	p.availableResources = append(p.availableResources, res)

	p.cond.L.Unlock()
	p.cond.Signal()
}

// Remove removes res from the pool and closes it. If res is not part of the
// pool Remove will panic.
func (p *Pool) destroyBorrowedResource(res *Resource) {
	p.cond.L.Lock()
	p.allResources = removeResource(p.allResources, res)
	p.cond.L.Unlock()
	p.cond.Signal()

	// close the resource in the background
	go p.closeRes(res.value)
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
