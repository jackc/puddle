package puddle

import (
	"context"
	"time"
)

// Tracer traces pool actions.
type Tracer interface {
	// AcquireStart is called at the beginning of Acquire calls. The return
	// context is used for the rest of the call and will be passed to AcquireEnd
	AcquireStart(ctx context.Context, data AcquireStartData) context.Context
	AcquireEnd(ctx context.Context, data AcquireEndData)
	// ReleaseStart is called at the beginning of Release calls. The return
	// context is used for the rest of the call and will be passed to AcquireEnd
	ReleaseStart(ctx context.Context, data ReleaseStartData) context.Context
	ReleaseEnd(ctx context.Context, data ReleaseEndData)
}

type AcquireStartData struct {
	StartNano int64
}

type AcquireEndData struct {
	WaitDuration    time.Duration
	AcquireDuration time.Duration
	InitDuration    time.Duration
	ResourceStats   ResourceTraceStats
	Err             error
}

type ReleaseTracer interface{}

type ReleaseStartData struct {
	HeldDuration time.Duration
}

type ReleaseEndData struct {
	BlockDuration time.Duration
	Err           error
}

type tracerSpan struct {
	t     Tracer
	start int64
}

func (t tracerSpan) acquireEndErr(ctx context.Context, err error) {
	if t.t == nil {
		return
	}
	t.t.AcquireEnd(ctx, AcquireEndData{
		AcquireDuration: time.Duration(nanotime() - t.start),
		Err:             err,
	})
}

func (t tracerSpan) acquireEnd(ctx context.Context, waitTime time.Duration, stats statFn, isNew bool) {
	if t.t == nil {
		return
	}
	var initDuration time.Duration
	resourceStats := stats()
	if isNew {
		initDuration = time.Since(resourceStats.CreationTime)
	}
	t.t.AcquireEnd(ctx, AcquireEndData{
		WaitDuration:    waitTime,
		AcquireDuration: time.Duration(nanotime() - t.start),
		InitDuration:    initDuration,
		ResourceStats:   resourceStats,
	})
}

type statFn func() ResourceTraceStats

// BaseTracer implements [Tracer] methods as no-ops.
//
// It is intended to be composed with types that only need to implement a subset
// of Tracer methods.
//
// Example usage:
//
//	 // MyTracer only hooks AcquireEnd
//		type MyTracer struct {
//			pool.BaseTracer
//		}
//
//		func AcquireEnd(ctx context.Context, d AcquireEndData) {
//	     /* do something with d */
//		}
type BaseTracer struct{}

func (BaseTracer) AcquireStart(ctx context.Context, _ AcquireStartData) context.Context {
	return ctx
}
func (BaseTracer) AcquireEnd(context.Context, AcquireEndData) {}
func (BaseTracer) ReleaseStart(ctx context.Context, _ ReleaseStartData) context.Context {
	return ctx
}
func (BaseTracer) ReleaseEnd(context.Context, ReleaseEndData) {}

var _ Tracer = BaseTracer{}
