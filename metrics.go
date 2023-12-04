package puddle

import (
	"sync/atomic"
	"time"
)

// AcquireObserver is called after successful resource acquisition.
// When using AcquireObserver, make sure it executes as possible in
// order not to downgrade overall pool performance.
type AcquireObserver func(d time.Duration, isEmptyAcquire bool)

// Metrics hold puddle metrics.
type Metrics struct {
	// externalAcquireObserver is an optional callback func that user
	// may pass to enable extended monitoring, e.g. prom histogram measurement.
	externalAcquireObserver AcquireObserver

	acquireCount         int64
	acquireDuration      time.Duration
	emptyAcquireCount    int64
	canceledAcquireCount atomic.Int64
}

func NewMetrics(opts ...MetricsOption) *Metrics {
	var m Metrics
	for _, o := range opts {
		o(&m)
	}
	return &m
}

func (m *Metrics) observeAcquireCancel() {
	m.canceledAcquireCount.Add(1)
}

// observeAcquireDuration is not thread safe.
func (m *Metrics) observeAcquireDuration(d time.Duration, isEmptyAcquire bool) {
	m.acquireCount++
	m.acquireDuration += d
	if isEmptyAcquire {
		m.emptyAcquireCount++
	}

	if m.externalAcquireObserver != nil {
		m.externalAcquireObserver(d, isEmptyAcquire)
	}
}

type MetricsOption func(m *Metrics)

// WithExternalAcquireObserver sets optional callback function for duration observation.
func WithExternalAcquireObserver(observer AcquireObserver) MetricsOption {
	return func(m *Metrics) { m.externalAcquireObserver = observer }
}
