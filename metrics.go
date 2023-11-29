package puddle

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics hold puddle metrics.
type Metrics struct {
	acquireCount         int64
	acquireDuration      time.Duration
	emptyAcquireCount    int64
	canceledAcquireCount atomic.Int64
	// acquireDurationHistogram can be nil
	acquireDurationHistogram *prometheus.HistogramVec
}

func NewMetrics(opts ...MetricsOption) *Metrics {
	var m Metrics
	for _, o := range opts {
		o(&m)
	}
	return &m
}

func (m *Metrics) observeAcquireDuration(d time.Duration, isEmptyAcquire bool) {
	m.acquireCount++
	m.acquireDuration += d
	if isEmptyAcquire {
		m.emptyAcquireCount++
	}

	if m.acquireDurationHistogram != nil {
		m.acquireDurationHistogram.
			WithLabelValues(strconv.FormatBool(isEmptyAcquire)).
			Observe(float64(d.Nanoseconds()))
	}
}

func (m *Metrics) observeAcquireCancel() {
	m.canceledAcquireCount.Add(1)
}

type MetricsOption func(m *Metrics)

// WithAcquireDurationHistogram turns on recording of pool resource acquire duration.
// Histogram metrics can be very expensive for Prometheus to retain and query.
func WithAcquireDurationHistogram(reg prometheus.Registerer, opts ...HistogramOption) MetricsOption {
	return func(m *Metrics) {
		histOpts := prometheus.HistogramOpts{
			Name:    "puddle_acquire_duration_nanoseconds",
			Help:    "Histogram of a pool resource acquire duration (nanoseconds).",
			Buckets: prometheus.DefBuckets,
		}
		for _, o := range opts {
			o(&histOpts)
		}

		m.acquireDurationHistogram = prometheus.NewHistogramVec(histOpts, []string{"empty"})
		reg.MustRegister(m.acquireDurationHistogram)
	}
}

// A HistogramOption lets you add options to Histogram metrics using With* funcs.
type HistogramOption func(*prometheus.HistogramOpts)

// WithHistogramBuckets allows you to specify custom bucket ranges for histograms.
func WithHistogramBuckets(buckets []float64) HistogramOption {
	return func(o *prometheus.HistogramOpts) { o.Buckets = buckets }
}

// WithHistogramConstLabels allows you to add custom ConstLabels to histograms metrics.
func WithHistogramConstLabels(labels prometheus.Labels) HistogramOption {
	return func(o *prometheus.HistogramOpts) {
		o.ConstLabels = labels
	}
}

// WithHistogramSubsystem allows you to add a Subsystem to histograms metrics.
func WithHistogramSubsystem(subsystem string) HistogramOption {
	return func(o *prometheus.HistogramOpts) {
		o.Subsystem = subsystem
	}
}
