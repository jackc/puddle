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

	create CreateFunc
}

func New(create CreateFunc) *Pool {
	return &Pool{
		cond:         sync.NewCond(new(sync.Mutex)),
		allResources: make(map[interface{}]*resourceWrapper),
		create:       create,
	}
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

	if len(p.availableResources) > 0 {
		rw := p.availableResources[len(p.availableResources)-1]
		p.availableResources = p.availableResources[:len(p.availableResources)-1]
		if rw.status != resourceStatusAvailable {
			panic("BUG: unavailable resource gotten from availableResources")
		}
		rw.status = resourceStatusBorrowed
		p.cond.L.Unlock()
		return rw.resource, nil
	}

	// if can create resource

	var localVal int
	placeholder := &localVal
	p.allResources[placeholder] = &resourceWrapper{resource: placeholder, status: resourceStatusCreating}
	p.cond.L.Unlock()

	resChan := make(chan interface{})
	errChan := make(chan error)

	go func() {
		res, err := p.create()
		if err != nil {
			errChan <- err
		}
		resChan <- res
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errChan:
		p.cond.L.Lock()
		delete(p.allResources, placeholder)
		p.cond.L.Unlock()
		return nil, err
	case res := <-resChan:
		p.cond.L.Lock()
		delete(p.allResources, placeholder)
		p.allResources[res] = &resourceWrapper{resource: res, status: resourceStatusBorrowed}
		p.cond.L.Unlock()
		return res, nil
	}

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
}
