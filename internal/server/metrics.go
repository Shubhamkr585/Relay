package server

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/shubhamkr585/relay/internal/cluster"
)

// Metrics encapsulates all Prometheus metrics for a Relay server node.
type Metrics struct {
	Registry *prometheus.Registry

	JobsEnqueued         *prometheus.CounterVec
	JobsReserved         *prometheus.CounterVec
	JobsAcked            prometheus.Counter
	JobsNacked           prometheus.Counter
	JobsRetried          *prometheus.CounterVec
	JobsDead             *prometheus.CounterVec
	LeaseExpiries        *prometheus.CounterVec
	StaleLeaseRejections prometheus.Counter
	RaftLeaderChanges    prometheus.Counter

	RaftIsLeader prometheus.Gauge

	RPCDuration       *prometheus.HistogramVec
	RaftApplyDuration prometheus.Histogram
}

// queueDepthCollector dynamically queries local FSM state at scrape time to report relay_queue_depth.
type queueDepthCollector struct {
	fsm  *cluster.FSM
	desc *prometheus.Desc
}

func newQueueDepthCollector(fsm *cluster.FSM) prometheus.Collector {
	return &queueDepthCollector{
		fsm: fsm,
		desc: prometheus.NewDesc(
			"relay_queue_depth",
			"Current depth of jobs by queue and state",
			[]string{"queue", "state"},
			nil,
		),
	}
}

func (c *queueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *queueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	if c.fsm == nil {
		return
	}
	allStats, err := c.fsm.StatsAll()
	if err != nil {
		return
	}
	for queue, s := range allStats {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(s.PendingCount), queue, "pending")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(s.LeasedCount), queue, "leased")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(s.DoneCount), queue, "done")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(s.DeadCount), queue, "dead")
	}
}

// NewMetrics initializes and registers all Prometheus metrics with a dedicated registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		JobsEnqueued: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_jobs_enqueued_total",
				Help: "Total number of jobs enqueued",
			},
			[]string{"queue", "deduplicated"},
		),
		JobsReserved: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_jobs_reserved_total",
				Help: "Total number of jobs reserved by workers",
			},
			[]string{"queue"},
		),
		JobsAcked: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_jobs_acked_total",
				Help: "Total number of jobs successfully acknowledged",
			},
		),
		JobsNacked: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_jobs_nacked_total",
				Help: "Total number of jobs negatively acknowledged",
			},
		),
		JobsRetried: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_jobs_retried_total",
				Help: "Total number of jobs scheduled for retry",
			},
			[]string{"queue"},
		),
		JobsDead: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_jobs_dead_total",
				Help: "Total number of jobs moved to dead letter queue",
			},
			[]string{"queue"},
		),
		LeaseExpiries: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_lease_expiries_total",
				Help: "Total number of lease expirations detected",
			},
			[]string{"queue"},
		),
		StaleLeaseRejections: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_stale_lease_rejections_total",
				Help: "Total number of operations rejected due to stale leases",
			},
		),
		RaftLeaderChanges: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_raft_leader_changes_total",
				Help: "Total number of Raft leadership transitions observed",
			},
		),
		RaftIsLeader: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_raft_is_leader",
				Help: "1 if the current node is Raft leader, 0 otherwise",
			},
		),
		RPCDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "relay_rpc_duration_seconds",
				Help:    "Execution duration of gRPC methods in seconds",
				Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			},
			[]string{"method"},
		),
		RaftApplyDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "relay_raft_apply_duration_seconds",
				Help:    "Execution duration of Raft Apply transactions in seconds",
				Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0},
			},
		),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.JobsEnqueued,
		m.JobsReserved,
		m.JobsAcked,
		m.JobsNacked,
		m.JobsRetried,
		m.JobsDead,
		m.LeaseExpiries,
		m.StaleLeaseRejections,
		m.RaftLeaderChanges,
		m.RaftIsLeader,
		m.RPCDuration,
		m.RaftApplyDuration,
	)

	return m
}

// RegisterFSM registers the dynamic queue depth collector for this node's FSM.
func (m *Metrics) RegisterFSM(fsm *cluster.FSM) {
	if m == nil || fsm == nil {
		return
	}
	m.Registry.MustRegister(newQueueDepthCollector(fsm))
}

// IncEnqueued increments the enqueued jobs counter.
func (m *Metrics) IncEnqueued(queue string, deduplicated bool) {
	if m == nil {
		return
	}
	m.JobsEnqueued.WithLabelValues(queue, strconv.FormatBool(deduplicated)).Inc()
}

// IncReserved increments the reserved jobs counter.
func (m *Metrics) IncReserved(queue string) {
	if m == nil {
		return
	}
	m.JobsReserved.WithLabelValues(queue).Inc()
}

// IncAcked increments the acknowledged jobs counter.
func (m *Metrics) IncAcked() {
	if m == nil {
		return
	}
	m.JobsAcked.Inc()
}

// IncNacked increments the nacked jobs counter.
func (m *Metrics) IncNacked() {
	if m == nil {
		return
	}
	m.JobsNacked.Inc()
}

// IncRetried increments the retried jobs counter.
func (m *Metrics) IncRetried(queue string) {
	if m == nil {
		return
	}
	m.JobsRetried.WithLabelValues(queue).Inc()
}

// IncDead increments the dead-lettered jobs counter.
func (m *Metrics) IncDead(queue string) {
	if m == nil {
		return
	}
	m.JobsDead.WithLabelValues(queue).Inc()
}

// IncLeaseExpiries increments lease expiration counter.
func (m *Metrics) IncLeaseExpiries(queue string, count int) {
	if m == nil {
		return
	}
	m.LeaseExpiries.WithLabelValues(queue).Add(float64(count))
}

// IncStaleLeaseRejections increments stale lease rejection counter.
func (m *Metrics) IncStaleLeaseRejections() {
	if m == nil {
		return
	}
	m.StaleLeaseRejections.Inc()
}

// SetIsLeader sets the leader gauge.
func (m *Metrics) SetIsLeader(isLeader bool) {
	if m == nil {
		return
	}
	if isLeader {
		m.RaftIsLeader.Set(1)
	} else {
		m.RaftIsLeader.Set(0)
	}
}
