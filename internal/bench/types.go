package bench

import (
	"time"
)

// EnvInfo captures the system environment and configuration under test.
type EnvInfo struct {
	CapturedAt          string            `json:"captured_at"`
	Mode                string            `json:"mode"` // "quick", "medium", "full", "partial"
	GitCommit           string            `json:"git_commit"`
	GitDirty            bool              `json:"git_dirty"`
	TimerImplementation string            `json:"timer_implementation"`
	ClockResolution     string            `json:"clock_resolution"`
	ClockResolutionNs   int64             `json:"clock_resolution_ns"`
	GoVersion           string            `json:"go_version"`
	OS                  string            `json:"os"`
	OSCaption           string            `json:"os_caption"`
	Arch                string            `json:"arch"`
	CPUModel            string            `json:"cpu_model"`
	LogicalCores        int               `json:"logical_cores"`
	TotalRAMBytes       uint64            `json:"total_ram_bytes"`
	TotalRAMGB          string            `json:"total_ram_gb"`
	GOMAXPROCS          int               `json:"gomaxprocs"`
	DiskType            string            `json:"disk_type"`
	PowerPlan           string            `json:"power_plan"`
	ACStatus            string            `json:"ac_status"`
	IsOnBattery         bool              `json:"is_on_battery"`
	ConfigLine          string            `json:"config_line"`
	BatchWindow         string            `json:"batch_window"`
	BatchMaxSize        int               `json:"batch_max_size"`
	RaftHeartbeat       string            `json:"raft_heartbeat"`
	RaftElection        string            `json:"raft_election"`
	RaftLeaderLease     string            `json:"raft_leader_lease"`
	RaftTimeouts        map[string]string `json:"raft_timeouts"`
	FsyncMode           string            `json:"fsync_mode"`
	PreflightNote       string            `json:"preflight_note"`
}

// LatencyStats contains calculated latency percentiles.
type LatencyStats struct {
	Count uint64        `json:"count"`
	Min   time.Duration `json:"min"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	P999  time.Duration `json:"p999"`
	Max   time.Duration `json:"max"`
	Mean  time.Duration `json:"mean"`
}

// RepetitionResult captures metrics from a single measurement repetition window.
type RepetitionResult struct {
	Repetition           int            `json:"repetition"`
	Duration             time.Duration  `json:"duration"`
	TotalOps             uint64         `json:"total_ops"`
	Throughput           float64        `json:"throughput"` // ops/sec
	Latency              LatencyStats   `json:"latency"`
	Unreliable           bool           `json:"unreliable"`
	BatchMean            time.Duration  `json:"batch_mean,omitempty"`
	Errors               int            `json:"errors"`
	Timeouts             int            `json:"timeouts"`
	Retries              int            `json:"retries"`
	ErrorBreakdown       map[string]int `json:"error_breakdown,omitempty"`
	ReserveCallsMade     uint64         `json:"reserve_calls_made,omitempty"`
	ReserveErrors        uint64         `json:"reserve_errors,omitempty"`
	AckCallsMade         uint64         `json:"ack_calls_made,omitempty"`
	AckErrors            uint64         `json:"ack_errors,omitempty"`
	EnqueueCallsMade     uint64         `json:"enqueue_calls_made,omitempty"`
	EnqueueErrors        uint64         `json:"enqueue_errors,omitempty"`
	LeaseTimeouts        uint64         `json:"lease_timeouts,omitempty"`
	Valid                bool           `json:"valid"`
	InvalidReason        string         `json:"invalid_reason,omitempty"`
	LastError            string         `json:"last_error,omitempty"`
	ThermalSlices        []float64      `json:"thermal_slices"` // throughput per 5s slice
	RSSBytes             uint64         `json:"rss_bytes"`
	Backlog              int64          `json:"backlog,omitempty"`
	Producers            int            `json:"producers,omitempty"`
	Workers              int            `json:"workers,omitempty"`
	SanityPass           bool           `json:"sanity_pass"`
	SanityNote           string         `json:"sanity_note"`
	LittlesLawConsistent bool           `json:"littles_law_consistent"`
	LittlesLawExpected   time.Duration  `json:"littles_law_expected,omitempty"`
	LittlesLawRatio      float64        `json:"littles_law_ratio,omitempty"`
	LittlesLawNote       string         `json:"littles_law_note,omitempty"`
}

// AggregatedMetric holds median, min, max, and stddev across multiple repetitions.
type AggregatedMetric struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	StdDev float64 `json:"std_dev"`
}

// AggregatedDuration holds median, min, max, and stddev of time durations.
type AggregatedDuration struct {
	Median time.Duration `json:"median"`
	Min    time.Duration `json:"min"`
	Max    time.Duration `json:"max"`
	StdDev time.Duration `json:"std_dev"`
}

// BenchmarkResult encapsulates the full multi-repetition outcome for a specific workload.
type BenchmarkResult struct {
	ID                   string             `json:"id"`
	Name                 string             `json:"name"`
	Description          string             `json:"description"`
	Config               map[string]string  `json:"config"`
	Repetitions          []RepetitionResult `json:"repetitions"`
	Throughput           AggregatedMetric   `json:"throughput"`
	LatencyP50           AggregatedDuration `json:"latency_p50"`
	LatencyP95           AggregatedDuration `json:"latency_p95"`
	LatencyP99           AggregatedDuration `json:"latency_p99"`
	LatencyP999          AggregatedDuration `json:"latency_p999"`
	LatencyMax           AggregatedDuration `json:"latency_max"`
	Unreliable           bool               `json:"unreliable"`
	BatchMean            AggregatedDuration `json:"batch_mean,omitempty"`
	TotalErrors          int                `json:"total_errors"`
	TotalTimeouts        int                `json:"total_timeouts"`
	TotalRetries         int                `json:"total_retries"`
	ErrorBreakdown       map[string]int     `json:"error_breakdown,omitempty"`
	HasErrors            bool               `json:"has_errors"`
	CV                   float64            `json:"cv"`               // Coefficient of variation (%) across repetitions
	Unstable             bool               `json:"unstable"`         // True if CV > 25%
	StabilityStatus      string             `json:"stability_status"` // "STABLE (CV: X.X%)" or "UNSTABLE (CV: X.X%)"
	Valid                bool               `json:"valid"`
	InvalidReason        string             `json:"invalid_reason,omitempty"`
	LittlesLawConsistent bool               `json:"littles_law_consistent"`
	LittlesLawNote       string             `json:"littles_law_note,omitempty"`
	OfferedJobs          uint64             `json:"offered_jobs,omitempty"`
	AchievedJobs         uint64             `json:"achieved_jobs,omitempty"`
	Backlog              int64              `json:"backlog,omitempty"`
	ReserveCallsMade     uint64             `json:"reserve_calls_made,omitempty"`
	ReserveErrors        uint64             `json:"reserve_errors,omitempty"`
	AckCallsMade         uint64             `json:"ack_calls_made,omitempty"`
	AckErrors            uint64             `json:"ack_errors,omitempty"`
	EnqueueCallsMade     uint64             `json:"enqueue_calls_made,omitempty"`
	EnqueueErrors        uint64             `json:"enqueue_errors,omitempty"`
	LeaseTimeouts        uint64             `json:"lease_timeouts,omitempty"`
	OverallPass          bool               `json:"overall_pass"`
	Notes                string             `json:"notes,omitempty"`
}

// FailoverTrial represents a single leader assassination and failover observation.
type FailoverTrial struct {
	Trial            int           `json:"trial"`
	KilledLeader     string        `json:"killed_leader"`
	NewLeader        string        `json:"new_leader"`
	FailoverDuration time.Duration `json:"failover_duration"`
	PreAckCount      int           `json:"pre_ack_count"`
	PostAckCount     int           `json:"post_ack_count"`
	VerifiedLostJobs int           `json:"verified_lost_jobs"`
	Valid            bool          `json:"valid"`
	InvalidReason    string        `json:"invalid_reason,omitempty"`
	Pass             bool          `json:"pass"`
}

// FailoverResult encapsulates B5 failover benchmark metrics.
type FailoverResult struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	Flags                string          `json:"flags"`
	TrialsCount          int             `json:"trials_count"`
	ValidTrialsCount     int             `json:"valid_trials_count"`
	Trials               []FailoverTrial `json:"trials"`
	InvalidTrials        []FailoverTrial `json:"invalid_trials,omitempty"`
	ExpectedLowerBound   time.Duration   `json:"expected_lower_bound"`
	LowerBoundDerivation string          `json:"lower_bound_derivation"`
	IsPrimaryHeadline    bool            `json:"is_primary_headline"`
	Suspicious           bool            `json:"suspicious"`
	MinDuration          time.Duration   `json:"min_duration"`
	MedianDuration       time.Duration   `json:"median_duration"`
	P95Duration          time.Duration   `json:"p95_duration"`
	P95Valid             bool            `json:"p95_valid"`
	MaxDuration          time.Duration   `json:"max_duration"`
	TotalLostJobs        int             `json:"total_lost_jobs"`
	OverallPass          bool            `json:"overall_pass"`
}

// SuiteReport contains all results across B0b through B5.
type SuiteReport struct {
	Env         EnvInfo            `json:"env"`
	B0b_Fsync   *BenchmarkResult   `json:"b0b_fsync,omitempty"`
	B1_Storage  []*BenchmarkResult `json:"b1_storage,omitempty"`
	B2_Cluster  []*BenchmarkResult `json:"b2_cluster,omitempty"`
	B3_OpenLoop []*BenchmarkResult `json:"b3_openloop,omitempty"`
	B4_E2E      []*BenchmarkResult `json:"b4_e2e,omitempty"`
	B5_Failover []*FailoverResult  `json:"b5_failover,omitempty"`
}
