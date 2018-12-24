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
	resourceStatusClosing   = iota
)

const maxUint = ^uint(0)
const maxInt = int(maxUint >> 1)

// ErrClosedPool occurs on an attempt to get a connection from a closed pool.
var ErrClosedPool = errors.New("cannot get from closed pool")

type CreateFunc func() (res interface{}, err error)
type CloseFunc func(res interface{}) (err error)

// BackgroundErrorHandler is the type of function that handles background
// errors. It may be called while the pool is locked. Therefore it must not call
// any pool methods and should not perform any lengthy operations.
type BackgroundErrorHandler func(err error)

type resourceWrapper struct {
	resource      interface{}
	creationTime  time.Time
	checkoutCount uint64
	status        byte
}

// Pool is a thread-safe resource pool.
type Pool struct {
	startclosePoolChan  chan struct{}
	finishClosePoolChan chan struct{}

	blockedBorrowResourceChan chan struct{}
	borrowResourceChan        chan interface{}
	returnResourceChan        chan interface{}
	cond                      *sync.Cond

	allResources         map[interface{}]*resourceWrapper
	availableResources   []*resourceWrapper
	minSize              int
	maxSize              int
	maxResourceDuration  time.Duration
	maxResourceCheckouts uint64

	createRes              CreateFunc
	closeRes               CloseFunc
	backgroundErrorHandler BackgroundErrorHandler
}

func NewPool(createRes CreateFunc, closeRes CloseFunc) *Pool {
	pool := &Pool{
		startclosePoolChan:        make(chan struct{}),
		finishClosePoolChan:       make(chan struct{}),
		blockedBorrowResourceChan: make(chan struct{}),
		borrowResourceChan:        make(chan interface{}),
		newResourceChan:           make(chan interface{}),
		returnResourceChan:        make(chan interface{}),
		cond:                      sync.NewCond(new(sync.Mutex)),
		allResources:              make(map[interface{}]*resourceWrapper),
		maxSize:                   maxInt,
		maxResourceDuration:       math.MaxInt64,
		maxResourceCheckouts:      math.MaxUint64,
		createRes:                 createRes,
		closeRes:                  closeRes,
		backgroundErrorHandler:    func(error) {},
	}

	go pool.run()
	return pool
}

func (p *Pool) run() {
	var borrowChan chan interface{}
	var availRW *resourceWrapper

	for {
		select {
		// Successful borrow of available resource
		case borrowChan <- availRW.resource:
			borrowChan = nil
			availRW = nil
			for _, rw := range p.allResources {
				if rw.status == resourceStatusAvailable {
					borrowChan = p.borrowResourceChan
					availRW = rw
					break
				}
			}

			// Client is blocked waiting for resource
		case <-p.blockedBorrowResourceChan:
			if len(p.allResources) < p.maxSize {
				p.startCreate()
			}

			// New resource has been created successfully
		case res := <-p.newResourceChan:
			borrowChan = p.borrowResourceChan
			p.allResources[res] =
			availRW = nil

			// Resource returned from client
		case res <- p.returnResourceChan:
			rw, present := p.allResources[res]
			if !present {
				panic("resource returned that does not belong to pool")
			}
			shouldClose := true

			now := time.Now()
			if now.Sub(rw.creationTime) > p.maxResourceDuration {
			} else if p.maxResourceCheckouts <= rw.checkoutCount { // use <= instead of == as maxResourceCheckouts may be lowered while pool is in use
			} else {
				shouldClose = false
			}

			if shouldClose {
				delete(p.allResources, rw.resource)
				p.ensureMinResources()
				p.cond.L.Unlock()
				p.backgroundClose(rw.resource)
				return
			}

			rw.status = resourceStatusAvailable
			p.availableResources = append(p.availableResources, rw)

		case <-p.startclosePoolChan:
			break
		case <-p.returnResourceChan:
		}

	}

	// TODO -- actually close the resources

	close(p.finishClosePoolChan)
}

// Close closes all resources in the pool. It blocks until any borrowed
// resources are returned and closed.
func (p *Pool) Close() {
	close(p.startclosePoolChan)
	<-p.finishClosePoolChan

	// p.cond.L.Lock()
	// p.closed = true

	// for _, rw := range p.availableResources {
	// 	err := p.closeRes(rw.resource)
	// 	if err != nil {
	// 		p.backgroundErrorHandler(err)
	// 	}
	// 	delete(p.allResources, rw.resource)
	// }
	// p.availableResources = nil
	// p.cond.L.Unlock()

	// // Wake up all go routines waiting for a resource to be returned so they can terminate.
	// p.cond.Broadcast()
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

	p.ensureMinResources()

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

// SetBackgroundErrorHandler assigns a handler for errors that have no other
// place to be reported. For example, Get is called when no resources are
// available. Get begins creating a new resource (in a goroutine). Before the
// new resource is completed, the context passed to Get is canceled. Then the
// new resource creation fails. f will be called with that error.
func (p *Pool) SetBackgroundErrorHandler(f BackgroundErrorHandler) {
	p.cond.L.Lock()
	p.backgroundErrorHandler = f
	p.cond.L.Unlock()
}

// Get gets a resource from the pool. If no resources are available and the pool
// is not at maximum capacity it will create a new resource. If the pool is at
// maximum capacity it will block until a resource is available. ctx can be used
// to cancel the Get.
func (p *Pool) Get(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.startclosePoolChan:
		return nil, ErrClosedPool
	default:
	}

	p.cond.L.Lock()

	// If a resource is available now
	if len(p.availableResources) > 0 {
		res := p.lockedAvailableGet()
		p.cond.L.Unlock()
		return res, nil
	}

	// If there is room to create a resource start the process asynchronously
	var createResChan chan interface{}
	var createErrChan chan error
	if len(p.allResources) < p.maxSize {
		createResChan, createErrChan = p.startCreate()
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

		select {
		case <-p.startclosePoolChan:
			p.cond.L.Unlock()
			return
		default:
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
		p.backgroundFinishCreate(createResChan, createErrChan)
		return nil, ctx.Err()
	case err := <-createErrChan:
		close(abortWaitResChan)
		return nil, err
	case res := <-createResChan:
		close(abortWaitResChan)
		return res, nil
	case res := <-waitResChan:
		p.backgroundFinishCreate(createResChan, createErrChan)
		return res, nil
	}
}

// func (p *Pool) backgroundReportError(errChan chan error) {
// 	go func() {
// 		err := <-errChan
// 		if err != nil {
// 			p.cond.L.Lock()
// 			p.backgroundErrorHandler(err)
// 			p.cond.L.Unlock()
// 		}
// 	}()
// }

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

// startCreate starts creating a new resource. p.cond.L must already be
// locked. The newly created resource will be sent on resChan (already checked
// out) or an error will be sent on errChan.
func (p *Pool) startCreate() (resChan chan interface{}, errChan chan error) {
	resChan = make(chan interface{})
	errChan = make(chan error)

	var localVal int
	placeholder := &localVal
	startTime := time.Now()
	p.allResources[placeholder] = &resourceWrapper{resource: placeholder, creationTime: startTime, status: resourceStatusCreating}

	go func() {
		res, err := p.createRes()
		p.cond.L.Lock()
		delete(p.allResources, placeholder)
		if err != nil {
			p.cond.L.Unlock()
			errChan <- err
			return
		}

		rw := &resourceWrapper{resource: res, creationTime: startTime, status: resourceStatusBorrowed, checkoutCount: 1}
		p.allResources[res] = rw
		p.cond.L.Unlock()
		resChan <- res
	}()

	return resChan, errChan
}

func (p *Pool) backgroundFinishCreate(resChan chan interface{}, errChan chan error) {
	go func() {
		select {
		case res := <-resChan:
			p.Return(res)
		case err := <-errChan:
			p.cond.L.Lock()
			p.backgroundErrorHandler(err)
			p.cond.L.Unlock()
		}
	}()
}

func (p *Pool) backgroundClose(res interface{}) {
	go func() {
		err := p.closeRes(res)
		if err != nil {
			p.backgroundErrorHandler(err)
		}
	}()
}

// Return returns res to the the pool. If res is not part of the pool Return
// will panic.
func (p *Pool) Return(res interface{}) {
	p.returnResourceChan <- res
	p.cond.L.Lock()

	// rw, present := p.allResources[res]
	// if !present {
	// 	p.cond.L.Unlock()
	// 	panic("Return called on resource that does not belong to pool")
	// }

	// if p.closed {
	// 	delete(p.allResources, rw.resource)
	// 	p.cond.L.Unlock()
	// 	p.backgroundClose(rw.resource)
	// 	return
	// }

	closeResource := true

	now := time.Now()
	if now.Sub(rw.creationTime) > p.maxResourceDuration {
	} else if p.maxResourceCheckouts <= rw.checkoutCount { // use <= instead of == as maxResourceCheckouts may be lowered while pool is in use
	} else {
		closeResource = false
	}

	if closeResource {
		delete(p.allResources, rw.resource)
		p.ensureMinResources()
		p.cond.L.Unlock()
		p.backgroundClose(rw.resource)
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
		err := p.closeRes(res)
		if err != nil {
			p.cond.L.Lock()
			p.backgroundErrorHandler(err)
			p.cond.L.Unlock()
		}
	}()

	p.ensureMinResources()
}

// ensureMinResources creates new resources if necessary to get pool up to min size.
// If pool is closed does nothing. p.cond.L must already be locked.
func (p *Pool) ensureMinResources() {
	select {
	case <-p.startclosePoolChan:
	default:
		for len(p.allResources) < p.minSize {
			createResChan, createErrChan := p.startCreate()
			p.backgroundFinishCreate(createResChan, createErrChan)
		}
	}
}
