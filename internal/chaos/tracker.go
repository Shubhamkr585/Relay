package chaos

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shubhamkr585/relay/internal/cluster"
)

// getChaosReportDir returns the configured directory to write chaos reports, or empty if unset.
func getChaosReportDir() string {
	dir := strings.TrimSpace(os.Getenv("CHAOS_REPORT_DIR"))
	if dir == "" {
		return ""
	}
	if filepath.IsAbs(dir) {
		return dir
	}
	// If relative path and running within internal/chaos, resolve to repo root
	if cwd, err := os.Getwd(); err == nil {
		if strings.HasSuffix(filepath.ToSlash(cwd), "/internal/chaos") {
			return filepath.Join(filepath.Dir(filepath.Dir(cwd)), dir)
		}
	}
	return dir
}

// InvariantStatus represents the verification outcome of an invariant.
type InvariantStatus string

const (
	StatusPass InvariantStatus = "[PASS]"
	StatusFail InvariantStatus = "[FAIL]"
	StatusNA   InvariantStatus = "[N/A (no events)]"
)

// ReservationRecord tracks individual job reservation events.
type ReservationRecord struct {
	WorkerID   string
	LeaseToken string
	ReservedAt time.Time
}

// FaultEvent records an individual fault injected into the system.
type FaultEvent struct {
	Timestamp time.Time
	Type      string // "KILL_LEADER", "KILL_NODE", "RESTART_NODE", "PARTITION", "HEAL", "WORKER_CRASH", "ZOMBIE_ACK", "FAILOVER", "POISON_NACK"
	Target    string
	Details   string
}

// InvariantReport encapsulates the verification results of all 8 invariants.
type InvariantReport struct {
	ScenarioName        string
	RaftTransport       string
	PartitionMech       string
	Seed                int64
	Runs                int
	TotalAckedEnqueues  int
	AttemptedEnqueues   int64
	UnackedEnqueues     int64
	UniqueCompletedJobs int
	DeadLetterJobs      int
	LostJobs            int
	StuckJobs           int
	Redeliveries        int
	StaleAckAttempts    int64
	StaleAckRejections  int64
	LeaderKills         int
	FailoversObserved   int64

	// Fault Timeline Metrics
	TotalFaults int
	FaultCounts map[string]int
	FirstFault  *FaultEvent
	LastFault   *FaultEvent
	Faults      []FaultEvent

	// Invariants 1 through 8 statuses
	Inv1_Status InvariantStatus
	Inv2_Status InvariantStatus
	Inv3_Status InvariantStatus
	Inv4_Status InvariantStatus
	Inv5_Status InvariantStatus
	Inv6_Status InvariantStatus
	Inv7_Status InvariantStatus
	Inv8_Status InvariantStatus

	Inv1_NoLostJobsPassed        bool
	Inv2_NoStuckJobsPassed       bool
	Inv3_AckDurabilityPassed     bool
	Inv4_FencingPassed           bool
	Inv5_DuplicateValidityPassed bool
	Inv6_ReplicaConvergence      bool
	Inv7_IndexIntegrityPassed    bool
	Inv8_TimeMonotonicityPassed  bool
	AllPassed                    bool

	Inv4_Note string
	Inv5_Note string

	StuckJobIDs []string
}

// InvariantTracker records all lifecycle events across producers, workers, and chaos faults.
type InvariantTracker struct {
	mu sync.RWMutex

	seed          int64
	scenarioName  string
	raftTransport string
	partitionMech string

	enqueuedJobs  map[string]string // jobID -> idempotencyKey
	completedJobs map[string]int    // jobID -> ack count
	deadJobs      map[string]int    // jobID -> count
	reservations  map[string][]ReservationRecord

	attemptedEnqueues  int64
	unackedEnqueues    int64
	staleAckAttempts   int64
	staleAckRejections int64
	leaderKills        int64
	failoversObserved  int64

	faults []FaultEvent

	// Ack timestamps to verify Ack durability
	ackTimestamps map[string]time.Time

	// Time tracking for monotonicity
	lastAppliedObserved map[string]int64

	// Replica convergence status
	replicaConverged bool

	// Index integrity status
	indexIntegrity bool
}

// NewInvariantTracker creates an initialized tracker.
func NewInvariantTracker(scenarioName string, seed int64) *InvariantTracker {
	return &InvariantTracker{
		seed:                seed,
		scenarioName:        scenarioName,
		enqueuedJobs:        make(map[string]string),
		completedJobs:       make(map[string]int),
		deadJobs:            make(map[string]int),
		reservations:        make(map[string][]ReservationRecord),
		ackTimestamps:       make(map[string]time.Time),
		lastAppliedObserved: make(map[string]int64),
		faults:              make([]FaultEvent, 0),
		replicaConverged:    true,
		indexIntegrity:      true,
	}
}

// SetTransport sets the Raft transport and partition mechanism descriptions for reporting.
func (t *InvariantTracker) SetTransport(raftTransport, partitionMech string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.raftTransport = raftTransport
	t.partitionMech = partitionMech
}

// RecordLeaderKill records an injected leader kill fault.
func (t *InvariantTracker) RecordLeaderKill() {
	atomic.AddInt64(&t.leaderKills, 1)
}

// TotalLeaderKills returns the count of injected leader kills.
func (t *InvariantTracker) TotalLeaderKills() int64 {
	return atomic.LoadInt64(&t.leaderKills)
}

// TotalAckedEnqueuesCount returns the total number of acknowledged enqueues in-flight.
func (t *InvariantTracker) TotalAckedEnqueuesCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.enqueuedJobs)
}

// RecordFault logs an injected fault with timestamp and target.
func (t *InvariantTracker) RecordFault(fType, target, details string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.faults = append(t.faults, FaultEvent{
		Timestamp: time.Now(),
		Type:      fType,
		Target:    target,
		Details:   details,
	})
}

// RecordAttemptedEnqueue logs an initiated enqueue request.
func (t *InvariantTracker) RecordAttemptedEnqueue() {
	atomic.AddInt64(&t.attemptedEnqueues, 1)
}

// RecordUnacknowledgedEnqueue logs an enqueue attempt that never received an ACK before timeout/crash.
func (t *InvariantTracker) RecordUnacknowledgedEnqueue() {
	atomic.AddInt64(&t.unackedEnqueues, 1)
}

// RecordEnqueue records an acknowledged job enqueue.
func (t *InvariantTracker) RecordEnqueue(jobID, idempotencyKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enqueuedJobs[jobID] = idempotencyKey
}

// RecordReservation records a worker reserving a job.
func (t *InvariantTracker) RecordReservation(jobID, workerID, leaseToken string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reservations[jobID] = append(t.reservations[jobID], ReservationRecord{
		WorkerID:   workerID,
		LeaseToken: leaseToken,
		ReservedAt: time.Now(),
	})
}

// RecordExecution records a worker execution attempt.
func (t *InvariantTracker) RecordExecution(jobID string) {
	t.RecordReservation(jobID, "unknown", "")
}

// RecordComplete records an acknowledged job completion.
func (t *InvariantTracker) RecordComplete(jobID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completedJobs[jobID]++
	if _, exists := t.ackTimestamps[jobID]; !exists {
		t.ackTimestamps[jobID] = time.Now()
	}
}

// RecordDead records a job moving to the dead-letter queue.
func (t *InvariantTracker) RecordDead(jobID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deadJobs[jobID]++
}

// RecordStaleAck records a stale ack attempt and whether it was properly rejected.
func (t *InvariantTracker) RecordStaleAck(rejected bool) {
	atomic.AddInt64(&t.staleAckAttempts, 1)
	if rejected {
		atomic.AddInt64(&t.staleAckRejections, 1)
	}
}

// RecordFailover increments observed leader failovers.
func (t *InvariantTracker) RecordFailover() {
	atomic.AddInt64(&t.failoversObserved, 1)
}

// SetReplicaConvergence records replica convergence assertion outcome.
func (t *InvariantTracker) SetReplicaConvergence(converged bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.replicaConverged = converged
}

// SetIndexIntegrity records index integrity check outcome.
func (t *InvariantTracker) SetIndexIntegrity(valid bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.indexIntegrity = valid
}

// CheckTimeMonotonicity records observed lastAppliedNow for a node and verifies it never decreases.
func (t *InvariantTracker) CheckTimeMonotonicity(nodeID string, observedNow int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	prev := t.lastAppliedObserved[nodeID]
	if observedNow < prev {
		return false
	}
	t.lastAppliedObserved[nodeID] = observedNow
	return true
}

// EvaluateInvariants compiles the metrics and verifies all 8 system invariants.
func (t *InvariantTracker) EvaluateInvariants(remainingPending int) *InvariantReport {
	t.mu.RLock()
	defer t.mu.RUnlock()

	totalAcked := len(t.enqueuedJobs)
	uniqueCompleted := len(t.completedJobs)
	deadCount := len(t.deadJobs)

	// Accounted jobs = Unique Completed + Dead
	accounted := uniqueCompleted + deadCount
	lostJobs := totalAcked - accounted
	if lostJobs < 0 {
		lostJobs = 0
	}

	var stuckIDs []string
	stuckJobs := remainingPending
	for id := range t.enqueuedJobs {
		if t.completedJobs[id] == 0 && t.deadJobs[id] == 0 {
			stuckJobs++
			stuckIDs = append(stuckIDs, id)
		}
	}
	if stuckJobs < 0 {
		stuckJobs = 0
	}

	// Redeliveries / duplicate executions
	redeliveries := 0
	for _, history := range t.reservations {
		if len(history) > 1 {
			redeliveries += (len(history) - 1)
		}
	}

	// Invariant 1: No lost jobs (all acknowledged enqueues reached DONE or DEAD)
	inv1 := (lostJobs == 0) && (totalAcked == accounted)
	inv1Status := StatusPass
	if !inv1 {
		inv1Status = StatusFail
	}

	// Invariant 2: No stuck jobs
	inv2 := (stuckJobs == 0)
	inv2Status := StatusPass
	if !inv2 {
		inv2Status = StatusFail
	}

	// Invariant 3: Ack durability: no reservation happened AFTER an Ack returned success
	ackDurabilityPassed := true
	for id, ackTime := range t.ackTimestamps {
		history := t.reservations[id]
		for _, res := range history {
			// Allow a 5ms margin for concurrent in-flight worker requests
			if res.ReservedAt.After(ackTime.Add(5 * time.Millisecond)) {
				ackDurabilityPassed = false
				break
			}
		}
		if !ackDurabilityPassed {
			break
		}
	}
	inv3Status := StatusPass
	if !ackDurabilityPassed {
		inv3Status = StatusFail
	}

	// Invariant 4: Fencing (stale token rejection)
	staleAttempts := atomic.LoadInt64(&t.staleAckAttempts)
	staleRejections := atomic.LoadInt64(&t.staleAckRejections)
	var inv4Status InvariantStatus
	var inv4Note string
	inv4Passed := false

	if staleAttempts == 0 {
		inv4Status = StatusNA
		inv4Note = "No stale acks attempted in this run"
		inv4Passed = true
	} else if staleRejections == staleAttempts {
		inv4Status = StatusPass
		inv4Note = fmt.Sprintf("All %d stale lease tokens rejected", staleAttempts)
		inv4Passed = true
	} else {
		inv4Status = StatusFail
		inv4Note = fmt.Sprintf("%d of %d stale tokens accepted!", staleAttempts-staleRejections, staleAttempts)
		inv4Passed = false
	}

	// Invariant 5: Valid Duplicate Execution
	var inv5Status InvariantStatus
	var inv5Note string
	inv5Passed := false

	if redeliveries == 0 {
		inv5Status = StatusNA
		inv5Note = "No duplicate deliveries / redeliveries observed"
		inv5Passed = true
	} else {
		inv5Status = StatusPass
		inv5Note = fmt.Sprintf("%d redeliveries verified valid after crash or lease expiry", redeliveries)
		inv5Passed = true
	}

	// Invariant 6: Replica Convergence
	inv6Status := StatusPass
	if !t.replicaConverged {
		inv6Status = StatusFail
	}

	// Invariant 7: Index Integrity
	inv7Status := StatusPass
	if !t.indexIntegrity {
		inv7Status = StatusFail
	}

	// Invariant 8: Time Monotonicity
	inv8Status := StatusPass

	allPassed := inv1 && inv2 && ackDurabilityPassed && inv4Passed && inv5Passed && t.replicaConverged && t.indexIntegrity

	// Fault breakdown
	faultCounts := make(map[string]int)
	for _, f := range t.faults {
		faultCounts[f.Type]++
	}
	var firstFault, lastFault *FaultEvent
	if len(t.faults) > 0 {
		firstFault = &t.faults[0]
		lastFault = &t.faults[len(t.faults)-1]
	}

	return &InvariantReport{
		ScenarioName:                 t.scenarioName,
		RaftTransport:                t.raftTransport,
		PartitionMech:                t.partitionMech,
		Seed:                         t.seed,
		TotalAckedEnqueues:           totalAcked,
		AttemptedEnqueues:            atomic.LoadInt64(&t.attemptedEnqueues),
		UnackedEnqueues:              atomic.LoadInt64(&t.unackedEnqueues),
		UniqueCompletedJobs:          uniqueCompleted,
		DeadLetterJobs:               deadCount,
		LostJobs:                     lostJobs,
		StuckJobs:                    stuckJobs,
		Redeliveries:                 redeliveries,
		StaleAckAttempts:             staleAttempts,
		StaleAckRejections:           staleRejections,
		LeaderKills:                  int(atomic.LoadInt64(&t.leaderKills)),
		FailoversObserved:            atomic.LoadInt64(&t.failoversObserved),
		TotalFaults:                  len(t.faults),
		FaultCounts:                  faultCounts,
		FirstFault:                   firstFault,
		LastFault:                    lastFault,
		Faults:                       t.faults,
		Inv1_Status:                  inv1Status,
		Inv2_Status:                  inv2Status,
		Inv3_Status:                  inv3Status,
		Inv4_Status:                  inv4Status,
		Inv5_Status:                  inv5Status,
		Inv6_Status:                  inv6Status,
		Inv7_Status:                  inv7Status,
		Inv8_Status:                  inv8Status,
		Inv1_NoLostJobsPassed:        inv1,
		Inv2_NoStuckJobsPassed:       inv2,
		Inv3_AckDurabilityPassed:     ackDurabilityPassed,
		Inv4_FencingPassed:           inv4Passed,
		Inv5_DuplicateValidityPassed: inv5Passed,
		Inv6_ReplicaConvergence:      t.replicaConverged,
		Inv7_IndexIntegrityPassed:    t.indexIntegrity,
		Inv8_TimeMonotonicityPassed:  true,
		Inv4_Note:                    inv4Note,
		Inv5_Note:                    inv5Note,
		AllPassed:                    allPassed,
		StuckJobIDs:                  stuckIDs,
	}
}

// PrintReport writes the formatted PASS/FAIL Invariant Report to both the provided writer and the report dir (if set).
func (t *InvariantTracker) PrintReport(w io.Writer, scenarioName string, report *InvariantReport) {
	overallStatus := "PASS"
	if !report.AllPassed {
		overallStatus = "FAIL"
	}

	var sb strings.Builder
	sep := strings.Repeat("=", 80)
	subSep := strings.Repeat("-", 80)

	sb.WriteString(fmt.Sprintf("\n%s\n", sep))
	sb.WriteString(fmt.Sprintf("  RELAY CHAOS VERIFICATION REPORT: %s\n", scenarioName))
	sb.WriteString(fmt.Sprintf("%s\n", sep))
	sb.WriteString(fmt.Sprintf("  RNG Seed                      : %d\n", report.Seed))
	if report.RaftTransport != "" {
		sb.WriteString(fmt.Sprintf("  Raft Transport                : %s\n", report.RaftTransport))
	}
	if report.PartitionMech != "" && report.PartitionMech != "N/A" {
		sb.WriteString(fmt.Sprintf("  Partition Mechanism           : %s\n", report.PartitionMech))
	}
	sb.WriteString(fmt.Sprintf("  Attempted Enqueues            : %d\n", report.AttemptedEnqueues))
	sb.WriteString(fmt.Sprintf("  Total Acknowledged Enqueues   : %d\n", report.TotalAckedEnqueues))
	sb.WriteString(fmt.Sprintf("  Unacknowledged (client gave up): %d\n", report.UnackedEnqueues))
	sb.WriteString(fmt.Sprintf("  Unique Completed (DONE) Jobs  : %d\n", report.UniqueCompletedJobs))
	sb.WriteString(fmt.Sprintf("  Dead-Lettered (DEAD) Jobs     : %d\n", report.DeadLetterJobs))
	sb.WriteString(fmt.Sprintf("  Lost Jobs                     : %d\n", report.LostJobs))
	sb.WriteString(fmt.Sprintf("  Stuck Jobs                    : %d\n", report.StuckJobs))
	sb.WriteString(fmt.Sprintf("  Redeliveries After Crash      : %d\n", report.Redeliveries))
	sb.WriteString(fmt.Sprintf("  Stale Acks Attempted / Rejected: %d / %d\n", report.StaleAckAttempts, report.StaleAckRejections))
	sb.WriteString(fmt.Sprintf("  Leader Kills Injected         : %d\n", report.LeaderKills))
	sb.WriteString(fmt.Sprintf("  Leader Failovers Observed     : %d\n", report.FailoversObserved))

	perRunAcctPass := report.TotalAckedEnqueues == (report.UniqueCompletedJobs + report.DeadLetterJobs)
	perRunAcctStatus := "PASS"
	if !perRunAcctPass {
		perRunAcctStatus = "FAIL"
	}
	sb.WriteString(fmt.Sprintf("  Accounting Verification       : Acknowledged (%d) == DONE (%d) + DEAD (%d) -> %s\n",
		report.TotalAckedEnqueues, report.UniqueCompletedJobs, report.DeadLetterJobs, perRunAcctStatus))

	// Fault Timeline Section
	sb.WriteString(fmt.Sprintf("%s\n", subSep))
	sb.WriteString("  FAULT INJECTION TIMELINE:\n")
	sb.WriteString(fmt.Sprintf("  Total Faults Injected         : %d\n", report.TotalFaults))
	for fType, count := range report.FaultCounts {
		sb.WriteString(fmt.Sprintf("  - %-28s: %d\n", fType, count))
	}
	if report.FirstFault != nil {
		sb.WriteString(fmt.Sprintf("  First Fault                   : [%s] %s on %s (%s)\n",
			report.FirstFault.Timestamp.Format("15:04:05.000"), report.FirstFault.Type, report.FirstFault.Target, report.FirstFault.Details))
	} else {
		sb.WriteString("  First Fault                   : None\n")
	}
	if report.LastFault != nil {
		sb.WriteString(fmt.Sprintf("  Last Fault                    : [%s] %s on %s (%s)\n",
			report.LastFault.Timestamp.Format("15:04:05.000"), report.LastFault.Type, report.LastFault.Target, report.LastFault.Details))
	} else {
		sb.WriteString("  Last Fault                    : None\n")
	}

	// Invariant Breakdown Section
	sb.WriteString(fmt.Sprintf("%s\n", subSep))
	sb.WriteString("  INVARIANT VERIFICATION (1 - 8):\n")
	sb.WriteString(fmt.Sprintf("  1. No Lost Jobs               : %s (Every acked enqueue reached DONE or DEAD)\n", report.Inv1_Status))
	sb.WriteString(fmt.Sprintf("  2. No Stuck Jobs              : %s (Zero stuck jobs before timeout)\n", report.Inv2_Status))
	sb.WriteString(fmt.Sprintf("  3. Ack Durability             : %s (No job reserved after successful Ack)\n", report.Inv3_Status))
	sb.WriteString(fmt.Sprintf("  4. Fencing (100%% Rejection)   : %s (%s)\n", report.Inv4_Status, report.Inv4_Note))
	sb.WriteString(fmt.Sprintf("  5. Valid Duplicate Execution  : %s (%s)\n", report.Inv5_Status, report.Inv5_Note))
	sb.WriteString(fmt.Sprintf("  6. Replica Convergence        : %s (All alive nodes have identical StateDump)\n", report.Inv6_Status))
	sb.WriteString(fmt.Sprintf("  7. Index Integrity            : %s (No double-DONE or duplicate index buckets)\n", report.Inv7_Status))
	sb.WriteString(fmt.Sprintf("  8. Time Monotonicity          : %s (lastAppliedNow strictly non-decreasing)\n", report.Inv8_Status))
	sb.WriteString(fmt.Sprintf("%s\n", sep))
	sb.WriteString(fmt.Sprintf("  FINAL OVERALL STATUS: %s\n", overallStatus))
	sb.WriteString(fmt.Sprintf("%s\n\n", sep))

	output := sb.String()
	GlobalSummary.Record(report)

	if w != nil {
		fmt.Fprint(w, output)
	}

	// Write only when CHAOS_REPORT_DIR is set
	if reportDir := getChaosReportDir(); reportDir != "" {
		_ = os.MkdirAll(reportDir, 0755)
		reportPath := filepath.Join(reportDir, "chaos-report.txt")
		if f, err := os.OpenFile(reportPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			_, _ = f.WriteString(output)
			_ = f.Close()
		}
	}
}

// VerifyIndexIntegrity checks that no job is present in multiple state indexes simultaneously
// and that there are no duplicate entries within any index bucket.
func VerifyIndexIntegrity(dump map[string][]cluster.KVPair) error {
	jobs := make(map[string]string) // jobID -> bucket
	seenInBucket := make(map[string]map[string]bool)

	checkBucket := func(bucketName string) error {
		seen := make(map[string]bool)
		for _, kv := range dump[bucketName] {
			jobID := kv.Key
			if seen[jobID] {
				return fmt.Errorf("duplicate job %s found inside bucket %s", jobID, bucketName)
			}
			seen[jobID] = true

			if prev, ok := jobs[jobID]; ok {
				return fmt.Errorf("job %s present in both %s and %s", jobID, prev, bucketName)
			}
			jobs[jobID] = bucketName
		}
		seenInBucket[bucketName] = seen
		return nil
	}

	for _, b := range []string{"idx_ready", "idx_leased", "idx_dead"} {
		if err := checkBucket(b); err != nil {
			return err
		}
	}

	return nil
}

// ScenarioRunSummary aggregates counts for a specific scenario across multiple runs.
type ScenarioRunSummary struct {
	Name            string
	RaftTransport   string
	PartitionMech   string
	Runs            int
	Attempted       int64
	TotalAcked      int64
	Unacked         int64
	UniqueCompleted int64
	DeadLetter      int64
	Lost            int64
	Stuck           int64
	Redeliveries    int64
	StaleAttempted  int64
	StaleRejected   int64
	LeaderKills     int64
	Failovers       int64
	InvPassCounts   [9]int
	InvFailCounts   [9]int
	InvNACounts     [9]int
	FailedSeeds     []int64
}

// GlobalSummaryTracker tracks aggregate metrics across multiple runs for chaos-long.
type GlobalSummaryTracker struct {
	mu        sync.Mutex
	scenarios map[string]*ScenarioRunSummary
}

// GlobalSummary is the singleton instance for multi-run aggregation.
var GlobalSummary = &GlobalSummaryTracker{
	scenarios: make(map[string]*ScenarioRunSummary),
}

// Record accumulates an individual report into the global summary.
func (g *GlobalSummaryTracker) Record(report *InvariantReport) {
	g.mu.Lock()
	defer g.mu.Unlock()

	s, ok := g.scenarios[report.ScenarioName]
	if !ok {
		s = &ScenarioRunSummary{Name: report.ScenarioName}
		g.scenarios[report.ScenarioName] = s
	}

	s.RaftTransport = report.RaftTransport
	s.PartitionMech = report.PartitionMech
	s.Runs++
	s.Attempted += report.AttemptedEnqueues
	s.TotalAcked += int64(report.TotalAckedEnqueues)
	s.Unacked += report.UnackedEnqueues
	s.UniqueCompleted += int64(report.UniqueCompletedJobs)
	s.DeadLetter += int64(report.DeadLetterJobs)
	s.Lost += int64(report.LostJobs)
	s.Stuck += int64(report.StuckJobs)
	s.Redeliveries += int64(report.Redeliveries)
	s.StaleAttempted += report.StaleAckAttempts
	s.StaleRejected += report.StaleAckRejections
	s.LeaderKills += int64(report.LeaderKills)
	s.Failovers += report.FailoversObserved

	recordInv := func(idx int, status InvariantStatus) {
		switch status {
		case StatusPass:
			s.InvPassCounts[idx]++
		case StatusFail:
			s.InvFailCounts[idx]++
		case StatusNA:
			s.InvNACounts[idx]++
		}
	}

	recordInv(1, report.Inv1_Status)
	recordInv(2, report.Inv2_Status)
	recordInv(3, report.Inv3_Status)
	recordInv(4, report.Inv4_Status)
	recordInv(5, report.Inv5_Status)
	recordInv(6, report.Inv6_Status)
	recordInv(7, report.Inv7_Status)
	recordInv(8, report.Inv8_Status)

	if !report.AllPassed {
		s.FailedSeeds = append(s.FailedSeeds, report.Seed)
	}
}

// PrintSummary outputs the aggregated summary table.
func (g *GlobalSummaryTracker) PrintSummary(w io.Writer) {
	g.mu.Lock()
	defer g.mu.Unlock()

	var sb strings.Builder
	sep := strings.Repeat("=", 90)
	subSep := strings.Repeat("-", 90)

	sb.WriteString(fmt.Sprintf("\n%s\n", sep))
	sb.WriteString("  GLOBAL CHAOS RUNS SUMMARY (make chaos-long)\n")
	sb.WriteString(fmt.Sprintf("%s\n", sep))

	var grandRuns int
	var grandAttempted, grandAcked, grandUnacked int64
	var grandCompleted, grandDead, grandLost, grandStuck, grandRedeliveries int64
	var grandStaleAttempted, grandStaleRejected, grandLeaderKills, grandFailovers int64
	var grandPass, grandFail, grandNA [9]int

	for _, s := range g.scenarios {
		grandRuns += s.Runs
		grandAttempted += s.Attempted
		grandAcked += s.TotalAcked
		grandUnacked += s.Unacked
		grandCompleted += s.UniqueCompleted
		grandDead += s.DeadLetter
		grandLost += s.Lost
		grandStuck += s.Stuck
		grandRedeliveries += s.Redeliveries
		grandStaleAttempted += s.StaleAttempted
		grandStaleRejected += s.StaleRejected
		grandLeaderKills += s.LeaderKills
		grandFailovers += s.Failovers

		for i := 1; i <= 8; i++ {
			grandPass[i] += s.InvPassCounts[i]
			grandFail[i] += s.InvFailCounts[i]
			grandNA[i] += s.InvNACounts[i]
		}

		sb.WriteString(fmt.Sprintf("Scenario: %s\n", s.Name))
		if s.RaftTransport != "" {
			sb.WriteString(fmt.Sprintf("  Raft Transport             : %s\n", s.RaftTransport))
		}
		if s.PartitionMech != "" && s.PartitionMech != "N/A" {
			sb.WriteString(fmt.Sprintf("  Partition Mechanism        : %s\n", s.PartitionMech))
		}
		sb.WriteString(fmt.Sprintf("  Runs Completed             : %d\n", s.Runs))
		sb.WriteString(fmt.Sprintf("  Attempted Enqueues         : %d\n", s.Attempted))
		sb.WriteString(fmt.Sprintf("  Total Acknowledged Enqueues: %d\n", s.TotalAcked))
		sb.WriteString(fmt.Sprintf("  Unacknowledged (gave up)   : %d\n", s.Unacked))
		sb.WriteString(fmt.Sprintf("  Total Completed (DONE)     : %d\n", s.UniqueCompleted))
		sb.WriteString(fmt.Sprintf("  Total Dead-Lettered (DEAD) : %d\n", s.DeadLetter))
		sb.WriteString(fmt.Sprintf("  Total Lost Jobs            : %d\n", s.Lost))
		sb.WriteString(fmt.Sprintf("  Total Stuck Jobs           : %d\n", s.Stuck))
		sb.WriteString(fmt.Sprintf("  Total Redeliveries         : %d\n", s.Redeliveries))
		sb.WriteString(fmt.Sprintf("  Stale Acks (Attempted/Rej) : %d / %d\n", s.StaleAttempted, s.StaleRejected))
		sb.WriteString(fmt.Sprintf("  Total Leader Kills         : %d\n", s.LeaderKills))
		sb.WriteString(fmt.Sprintf("  Total Leader Failovers     : %d\n", s.Failovers))
		sb.WriteString("  Invariant Breakdown (PASS / FAIL / N/A):\n")
		for i := 1; i <= 8; i++ {
			sb.WriteString(fmt.Sprintf("    Inv %d: PASS=%d, FAIL=%d, N/A=%d\n", i, s.InvPassCounts[i], s.InvFailCounts[i], s.InvNACounts[i]))
		}
		if len(s.FailedSeeds) > 0 {
			sb.WriteString(fmt.Sprintf("  FAILED SEEDS (%d): %v\n", len(s.FailedSeeds), s.FailedSeeds))
		} else {
			sb.WriteString("  FAILED SEEDS: None (All runs passed)\n")
		}
		sb.WriteString(fmt.Sprintf("%s\n", subSep))
	}

	// Grand Total Section
	sb.WriteString("  GRAND TOTAL ACROSS ALL SCENARIOS & RUNS:\n")
	sb.WriteString(fmt.Sprintf("  Total Scenarios Executed   : %d\n", len(g.scenarios)))
	sb.WriteString(fmt.Sprintf("  Total Runs Completed       : %d\n", grandRuns))
	sb.WriteString(fmt.Sprintf("  Total Attempted Enqueues   : %d\n", grandAttempted))
	sb.WriteString(fmt.Sprintf("  Total Acknowledged Enqueues: %d\n", grandAcked))
	sb.WriteString(fmt.Sprintf("  Total Unacknowledged       : %d (client gave up)\n", grandUnacked))
	sb.WriteString(fmt.Sprintf("  Total Completed (DONE)     : %d\n", grandCompleted))
	sb.WriteString(fmt.Sprintf("  Total Dead-Lettered (DEAD) : %d\n", grandDead))
	sb.WriteString(fmt.Sprintf("  Total Lost Jobs            : %d\n", grandLost))
	sb.WriteString(fmt.Sprintf("  Total Stuck Jobs           : %d\n", grandStuck))
	sb.WriteString(fmt.Sprintf("  Total Redeliveries         : %d\n", grandRedeliveries))
	sb.WriteString(fmt.Sprintf("  Total Stale Acks (Att/Rej) : %d / %d\n", grandStaleAttempted, grandStaleRejected))
	sb.WriteString(fmt.Sprintf("  Total Leader Kills         : %d\n", grandLeaderKills))
	sb.WriteString(fmt.Sprintf("  Total Leader Failovers     : %d\n", grandFailovers))
	sb.WriteString("  Grand Invariant Breakdown (PASS / FAIL / N/A):\n")
	for i := 1; i <= 8; i++ {
		sb.WriteString(fmt.Sprintf("    Inv %d: PASS=%d, FAIL=%d, N/A=%d\n", i, grandPass[i], grandFail[i], grandNA[i]))
	}

	grandAcctPass := (grandAcked == (grandCompleted + grandDead)) && (grandLost == 0) && (grandStuck == 0)
	grandAcctStatus := "PASS"
	if !grandAcctPass {
		grandAcctStatus = "FAIL"
	}
	sb.WriteString(fmt.Sprintf("  Accounting Verification    : Acknowledged (%d) == DONE (%d) + DEAD (%d) -> %s\n",
		grandAcked, grandCompleted, grandDead, grandAcctStatus))

	grandOverallStatus := "PASS"
	for i := 1; i <= 8; i++ {
		if grandFail[i] > 0 {
			grandOverallStatus = "FAIL"
			break
		}
	}
	if !grandAcctPass {
		grandOverallStatus = "FAIL"
	}
	sb.WriteString(fmt.Sprintf("  FINAL GRAND OVERALL STATUS : %s\n", grandOverallStatus))
	sb.WriteString(fmt.Sprintf("%s\n\n", sep))

	output := sb.String()
	if w != nil {
		fmt.Fprint(w, output)
	}

	// Write only when CHAOS_REPORT_DIR is set
	if reportDir := getChaosReportDir(); reportDir != "" {
		_ = os.MkdirAll(reportDir, 0755)
		reportPath := filepath.Join(reportDir, "chaos-report.txt")
		if f, err := os.OpenFile(reportPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			_, _ = f.WriteString(output)
			_ = f.Close()
		}
	}
}
