package pool

import (
	"context"
	"sync"
)

const (
	resourceStatusCreating  = 0
	resourceStatusAvailable = iota
	resourceStatusBorrowed  = iota
)

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)

type CreateFunc func() (res interface{}, err error)

type resourceWrapper struct {
	resource interface{}
	status   byte
}

// Pool is a thread-safe resource pool.
type Pool struct {
	cond *sync.Cond

	allResources       map[interface{}]*resourceWrapper
	availableResources []*resourceWrapper
	maxSize            int

	create CreateFunc
}

func New(create CreateFunc) *Pool {
	return &Pool{
		cond:         sync.NewCond(new(sync.Mutex)),
		allResources: make(map[interface{}]*resourceWrapper),
		maxSize:      maxInt,
		create:       create,
	}
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

// SetMaxSize sets the maximum size of the pool. It panics if n < 1.
func (p *Pool) SetMaxSize(n int) {
	if n < 1 {
		panic("pool MaxSize cannot be < 1")
	}
	p.cond.L.Lock()
	p.maxSize = n
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

	// If a resource is available now
	if len(p.availableResources) > 0 {
		res := p.lockedAvailableGet()
		p.cond.L.Unlock()
		return res, nil
	}

	// If there is room to create a resource start the process asynchronously
	var errChan chan error
	if len(p.allResources) < p.maxSize {
		errChan = p.startCreate()
	}
	p.cond.L.Unlock()

	// Whether or not we started creating a resource all we can do now is wait.
	resChan := make(chan interface{})
	abortChan := make(chan struct{})

	go func() {
		p.cond.L.Lock()
		for len(p.availableResources) == 0 {
			p.cond.Wait()
		}
		res := p.lockedAvailableGet()
		p.cond.L.Unlock()

		select {
		case <-abortChan:
			p.Return(res)
		case resChan <- res:
		}
	}()

	select {
	case <-ctx.Done():
		close(abortChan)
		return nil, ctx.Err()
	case err := <-errChan:
		close(abortChan)
		return nil, err
	case res := <-resChan:
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
	return rw.resource
}

// startCreate starts creating a new resource. p.cond.L must already be
// locked. The returned error channel will receive any error returned by create.
func (p *Pool) startCreate() chan error {
	// Use a buffered errChan to receive the error so the goroutine doesn't leak if
	// the error channel is never read.
	errChan := make(chan error, 1)

	var localVal int
	placeholder := &localVal
	p.allResources[placeholder] = &resourceWrapper{resource: placeholder, status: resourceStatusCreating}

	go func() {
		res, err := p.create()
		p.cond.L.Lock()
		delete(p.allResources, placeholder)
		if err != nil {
			p.cond.L.Unlock()
			errChan <- err
			return
		}

		rw := &resourceWrapper{resource: res, status: resourceStatusAvailable}
		p.allResources[res] = rw
		p.availableResources = append(p.availableResources, rw)
		p.cond.L.Unlock()
		p.cond.Signal()
	}()

	return errChan
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

	rw.status = resourceStatusAvailable
	p.availableResources = append(p.availableResources, rw)

	p.cond.L.Unlock()
	p.cond.Signal()
}
