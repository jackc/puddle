package puddle

import (
	"context"
	"errors"
	"fmt"
	"math"
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

type resourceCreationResult struct {
	placeholder interface{}
	resource    interface{}
	err         error
}

// Pool is a thread-safe resource pool.
type Pool struct {
	closePoolStartedChan  chan struct{}
	closePoolFinishedChan chan struct{}

	lockChan   chan struct{}
	unlockChan chan struct{}

	blockedBorrowResourceChan  chan struct{}
	resourceCreationResultChan chan *resourceCreationResult
	createdResourceChan        chan interface{}
	createResourceErrorChan    chan error

	borrowedResourceChan chan interface{}
	returnedResourceChan chan interface{}

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
		closePoolStartedChan:  make(chan struct{}),
		closePoolFinishedChan: make(chan struct{}),

		lockChan:   make(chan struct{}),
		unlockChan: make(chan struct{}),

		blockedBorrowResourceChan:  make(chan struct{}),
		resourceCreationResultChan: make(chan *resourceCreationResult),
		createResourceErrorChan:    make(chan error),
		createdResourceChan:        make(chan interface{}),
		borrowedResourceChan:       make(chan interface{}),
		returnedResourceChan:       make(chan interface{}),
		allResources:               make(map[interface{}]*resourceWrapper),
		maxSize:                    maxInt,
		maxResourceDuration:        math.MaxInt64,
		maxResourceCheckouts:       math.MaxUint64,
		createRes:                  createRes,
		closeRes:                   closeRes,
		backgroundErrorHandler:     func(error) {},
	}

	go pool.run()
	return pool
}

type stateFunc func(p *Pool) stateFunc

func blockedState(p *Pool) stateFunc {
	select {
	case <-p.blockedBorrowResourceChan:
		// fmt.Println("blockedStart <-p.blockedBorrowResourceChan")
		if len(p.allResources) < p.maxSize {
			p.runStartCreate()
		}
		return blockedState
	case res := <-p.returnedResourceChan:
		// fmt.Println("blockedStart <-p.returnedResourceChan")
		p.runReturnResource(res)
		if len(p.availableResources) > 0 {
			return availableState
		} else {
			return blockedState
		}
	case rcr := <-p.resourceCreationResultChan:
		// fmt.Println("blockedStart <-p.resourceCreationResultChan")

		rw := p.allResources[rcr.placeholder]
		delete(p.allResources, rw.resource)

		if rcr.err != nil {
			select {
			case p.createResourceErrorChan <- rcr.err:
			default:
				fmt.Println("background error report")
				// TODO -- background error report
			}
			return blockedState
		}

		rw.resource = rcr.resource
		rw.status = resourceStatusAvailable
		p.allResources[rw.resource] = rw
		p.availableResources = append(p.availableResources, rw)
		return availableState
	case p.lockChan <- struct{}{}:
		// fmt.Println("blockedStart p.lockChan <- struct{}{}")
		return lockedState
	case <-p.closePoolStartedChan:
		// fmt.Println("blockedStart <-p.closePoolStartedChan")
		return closingState
	}
}

func availableState(p *Pool) stateFunc {
	rw := p.availableResources[len(p.availableResources)-1]

	select {
	case p.borrowedResourceChan <- rw.resource:
		rw.checkoutCount += 1
		rw.status = resourceStatusBorrowed
		p.availableResources = p.availableResources[:len(p.availableResources)-1]
		if len(p.availableResources) > 0 {
			return availableState
		} else {
			return blockedState
		}
	case res := <-p.returnedResourceChan:
		p.runReturnResource(res)
		return availableState
	case rcr := <-p.resourceCreationResultChan:
		rw := p.allResources[rcr.placeholder]
		delete(p.allResources, rw.resource)

		if rcr.err != nil {
			select {
			case p.createResourceErrorChan <- rcr.err:
			default:
				// TODO -- background error report
			}
			return availableState
		}

		rw.resource = rcr.resource
		rw.status = resourceStatusAvailable
		p.allResources[rw.resource] = rw
		p.availableResources = append(p.availableResources, rw)
		return availableState
	case p.lockChan <- struct{}{}:
		return lockedState
	case <-p.closePoolStartedChan:
		return closingState
	}
}

func lockedState(p *Pool) stateFunc {
	p.unlockChan <- struct{}{}
	if len(p.availableResources) > 0 {
		return availableState
	} else {
		return blockedState
	}
}

func closingState(p *Pool) stateFunc {
	select {
	// case borrow
	// case returned resource
	// case created resource
	// case start close
	default:
	}

	close(p.closePoolFinishedChan)
	return nil
}

func (p *Pool) run() {
	state := blockedState

	for state != nil {
		state = state(p)
	}
}

// Close closes all resources in the pool. It blocks until any borrowed
// resources are returned and closed.
func (p *Pool) Close() {
	close(p.closePoolStartedChan)
	<-p.closePoolFinishedChan
}

// Size returns the current size of the pool.
func (p *Pool) Size() int {
	n := 0
	select {
	case <-p.lockChan:
		n = len(p.allResources)
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		// TODO - what should happen when called after close?
	}

	return n
}

// MinSize returns the current minimum size of the pool.
func (p *Pool) MinSize() int {
	n := 0
	select {
	case <-p.lockChan:
		n = p.minSize
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		// TODO - what should happen when called after close?
	}

	return n
}

// SetMinSize sets the minimum size of the pool. It panics if n < 0.
func (p *Pool) SetMinSize(n int) {
	if n < 0 {
		panic("pool MinSize cannot be < 0")
	}

	select {
	case <-p.lockChan:
		p.minSize = n
		p.ensureMinResources()
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		panic("pool is closed")
	}
}

// MaxSize returns the current maximum size of the pool.
func (p *Pool) MaxSize() int {
	n := 0
	select {
	case <-p.lockChan:
		n = p.maxSize
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		// TODO - what should happen when called after close?
	}

	return n
}

// SetMaxSize sets the maximum size of the pool. It panics if n < 1.
func (p *Pool) SetMaxSize(n int) {
	if n < 1 {
		panic("pool MaxSize cannot be < 1")
	}

	select {
	case <-p.lockChan:
		p.maxSize = n
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		panic("pool is closed")
	}
}

// MaxResourceDuration returns the current maximum resource duration of the pool.
func (p *Pool) MaxResourceDuration() time.Duration {
	var d time.Duration
	select {
	case <-p.lockChan:
		d = p.maxResourceDuration
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		// TODO - what should happen when called after close?
	}

	return d
}

// SetMaxResourceDuration sets the maximum maximum resource duration of the pool. It panics if n < 1.
func (p *Pool) SetMaxResourceDuration(d time.Duration) {
	if d < 0 {
		panic("pool MaxResourceDuration cannot be < 0")
	}

	select {
	case <-p.lockChan:
		p.maxResourceDuration = d
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		panic("pool is closed")
	}
}

// MaxResourceCheckouts returns the current maximum uses per resource of the pool.
func (p *Pool) MaxResourceCheckouts() uint64 {
	var n uint64
	select {
	case <-p.lockChan:
		n = p.maxResourceCheckouts
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		// TODO - what should happen when called after close?
	}

	return n
}

// SetMaxResourceCheckouts sets the maximum maximum resource duration of the pool. It panics if n < 1.
func (p *Pool) SetMaxResourceCheckouts(n uint64) {
	if n < 1 {
		panic("pool MaxResourceCheckouts cannot be < 1")
	}

	select {
	case <-p.lockChan:
		p.maxResourceCheckouts = n
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		panic("pool is closed")
	}
}

// SetBackgroundErrorHandler assigns a handler for errors that have no other
// place to be reported. For example, Get is called when no resources are
// available. Get begins creating a new resource (in a goroutine). Before the
// new resource is completed, the context passed to Get is canceled. Then the
// new resource creation fails. f will be called with that error.
func (p *Pool) SetBackgroundErrorHandler(f BackgroundErrorHandler) {
	select {
	case <-p.lockChan:
		p.backgroundErrorHandler = f
		<-p.unlockChan
	case <-p.closePoolStartedChan:
		panic("pool is closed")
	}
}

// Get gets a resource from the pool. If no resources are available and the pool
// is not at maximum capacity it will create a new resource. If the pool is at
// maximum capacity it will block until a resource is available. ctx can be used
// to cancel the Get.
func (p *Pool) Get(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closePoolStartedChan:
		return nil, ErrClosedPool
	case res := <-p.borrowedResourceChan:
		return res, nil
	case p.blockedBorrowResourceChan <- struct{}{}:
	case err := <-p.createResourceErrorChan:
		return nil, err
	}

	// Same select without blocked borrow send
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.closePoolStartedChan:
		return nil, ErrClosedPool
	case res := <-p.borrowedResourceChan:
		return res, nil
	case err := <-p.createResourceErrorChan:
		return nil, err
	}
}

// runStartCreate starts creating a new resource. It must be called inside of
// the run loop. The result will be sent to p.resourceCreationResultChan.
func (p *Pool) runStartCreate() {
	var localVal int
	placeholder := &localVal
	startTime := time.Now()
	p.allResources[placeholder] = &resourceWrapper{resource: placeholder, creationTime: startTime, status: resourceStatusCreating}

	go func(f CreateFunc) {
		res, err := f()
		p.resourceCreationResultChan <- &resourceCreationResult{
			placeholder: placeholder,
			resource:    res,
			err:         err,
		}
	}(p.createRes)
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
	p.returnedResourceChan <- res
}

// runReturnResource handles a returned resource. It must be called inside of
// the run loop.
func (p *Pool) runReturnResource(res interface{}) {
	rw, present := p.allResources[res]
	if !present {
		panic("resource returned that does not belong to pool")
	}

	var closedPool bool
	select {
	case <-p.closePoolStartedChan:
		closedPool = true
	default:
		closedPool = false
	}

	destroyResource := true

	now := time.Now()
	if closedPool {
	} else if now.Sub(rw.creationTime) > p.maxResourceDuration {
	} else if p.maxResourceCheckouts <= rw.checkoutCount { // use <= instead of == as maxResourceCheckouts may be lowered while pool is in use
	} else {
		destroyResource = false
	}

	if destroyResource {
		delete(p.allResources, rw.resource)
		p.ensureMinResources()
		p.backgroundClose(rw.resource)
		return
	}

	rw.status = resourceStatusAvailable
	p.availableResources = append(p.availableResources, rw)
}

// Remove removes res from the pool and closes it. If res is not part of the
// pool Remove will panic.
func (p *Pool) Remove(res interface{}) {
	// p.cond.L.Lock()
	// defer p.cond.L.Unlock()

	// rw, present := p.allResources[res]
	// if !present {
	// 	panic("Remove called on resource that does not belong to pool")
	// }

	// delete(p.allResources, rw.resource)

	// // close the resource in the background
	// go func() {
	// 	err := p.closeRes(res)
	// 	if err != nil {
	// 		p.cond.L.Lock()
	// 		p.backgroundErrorHandler(err)
	// 		p.cond.L.Unlock()
	// 	}
	// }()

	// p.ensureMinResources()
}

// ensureMinResources creates new resources if necessary to get pool up to min size.
// If pool is closed does nothing. p.cond.L must already be locked.
func (p *Pool) ensureMinResources() {
	select {
	case <-p.closePoolStartedChan:
	default:
		for len(p.allResources) < p.minSize {
			p.runStartCreate()
		}
	}
}
