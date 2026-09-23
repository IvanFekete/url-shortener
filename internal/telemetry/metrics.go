package telemetry

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry                                  *prometheus.Registry
	Requests                                  *prometheus.CounterVec
	Latency                                   *prometheus.HistogramVec
	Inflight                                  prometheus.Gauge
	Rejected                                  prometheus.Counter
	DependencyLatency                         *prometheus.HistogramVec
	DependencyErrors                          *prometheus.CounterVec
	Capacity                                  *prometheus.CounterVec
	Cache                                     *prometheus.CounterVec
	Events                                    *prometheus.CounterVec
	Dropped, DeadLetters, Batches             prometheus.Counter
	Queue, Pending, Delayed, DLQ, Concurrency prometheus.Gauge
	EventAge                                  prometheus.Histogram
}

func New() *Metrics {
	r := prometheus.NewRegistry()
	counter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		r.MustRegister(c)
		return c
	}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		r.MustRegister(g)
		return g
	}
	cv := func(name string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: name}, labels)
		r.MustRegister(c)
		return c
	}
	hv := func(name string, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: name, Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5}}, labels)
		r.MustRegister(h)
		return h
	}
	m := &Metrics{Registry: r, Requests: cv("http_requests_total", "route", "method", "status"), Latency: hv("http_request_duration_seconds", "route"),
		Inflight: gauge("http_requests_in_flight", "Requests currently executing"), Rejected: counter("http_requests_rejected_total", "Admission rejections"),
		DependencyLatency: hv("dependency_duration_seconds", "dependency", "operation"), DependencyErrors: cv("dependency_errors_total", "dependency", "operation", "class"),
		Capacity: cv("dynamodb_consumed_capacity_total", "table", "kind"), Cache: cv("cache_requests_total", "result"), Events: cv("analytics_events_total", "result"),
		Dropped: counter("analytics_events_dropped_total", "Redirect events without confirmed SQS acceptance"), DeadLetters: counter("analytics_dead_letters_total", "Failed deliveries at the redrive threshold"),
		Batches: counter("analytics_read_batches_total", "Consumer read batches"), Queue: gauge("analytics_queue_depth", "Approximate visible SQS messages"),
		Pending: gauge("analytics_pending_entries", "Approximate in-flight SQS messages"), Delayed: gauge("analytics_delayed_entries", "Approximate delayed SQS messages"), DLQ: gauge("analytics_dlq_depth", "Approximate messages in the dead letter queue"),
		Concurrency: gauge("analytics_worker_concurrency", "Current worker concurrency limit"),
	}
	m.EventAge = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "analytics_event_age_seconds", Help: "Age of received events; not the queue-wide oldest message", Buckets: []float64{.1, 1, 5, 10, 30, 60, 120, 300, 3600, 86400}})
	r.MustRegister(m.EventAge)
	r.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
func (m *Metrics) Observe(dependency, operation string, start time.Time, err error, class string) {
	m.DependencyLatency.WithLabelValues(dependency, operation).Observe(time.Since(start).Seconds())
	if err != nil {
		m.DependencyErrors.WithLabelValues(dependency, operation, class).Inc()
	}
}
