package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// getRunsCount returns the configured number of chaos iterations.
func getRunsCount() int {
	if val := os.Getenv("CHAOS_RUNS"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return 1
}

func TestMain(m *testing.M) {
	code := m.Run()
	if getRunsCount() > 1 || os.Getenv("CHAOS_REPORT_DIR") != "" {
		GlobalSummary.PrintSummary(os.Stdout)
	}
	os.Exit(code)
}

// getJobCount returns the number of jobs per test based on environment.
func getJobCount(defaultCount int) int {
	if val := os.Getenv("CHAOS_JOBS_PER_RUN"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	if val := os.Getenv("CHAOS_JOBS"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	if runs := getRunsCount(); runs > 1 {
		return 200 // at least 200 per run in chaos-long
	}
	return defaultCount
}

// getChaosSeed returns the configured or randomly initialized RNG seed.
func getChaosSeed() int64 {
	if val := os.Getenv("CHAOS_SEED"); val != "" {
		if s, err := strconv.ParseInt(val, 10, 64); err == nil {
			return s
		}
	}
	return time.Now().UnixNano()
}

// ---------------------------------------------------------------------------
// Scenario A: Continuous Leader Assassination
// ---------------------------------------------------------------------------
func TestChaos_ScenarioA_LeaderAssassination(t *testing.T) {
	runs := getRunsCount()
	numJobs := getJobCount(600)

	for run := 1; run <= runs; run++ {
		t.Run(fmt.Sprintf("Run_%d", run), func(t *testing.T) {
			seed := getChaosSeed()
			baseDir := t.TempDir()
			h, err := NewClusterHarness(baseDir)
			require.NoError(t, err)
			defer h.Shutdown()

			_, err = h.WaitForLeader(10 * time.Second)
			require.NoError(t, err)

			queue := fmt.Sprintf("chaos-a-q-%d", run)
			tracker := NewInvariantTracker("Scenario A: 3 leader kills per run", seed)
			tracker.SetTransport("in-memory (raft.InmemTransport) with real loopback TCP gRPC", "N/A")
			h.SetTracker(tracker)

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			var producerWg sync.WaitGroup
			numProducers := 4
			jobsPerProducer := numJobs / numProducers

			// 1. Concurrent Producers
			for p := 0; p < numProducers; p++ {
				producerWg.Add(1)
				go func(pID int) {
					defer producerWg.Done()
					for j := 0; j < jobsPerProducer; j++ {
						idempKey := fmt.Sprintf("sc-a-prod-%d-job-%d", pID, j)
						isPoison := (j%30 == 0) // inject poison jobs to test DLQ & retries under chaos
						payload := []byte(fmt.Sprintf("payload-%s", idempKey))
						if isPoison {
							payload = []byte(fmt.Sprintf("poison-%s", idempKey))
						}

						tracker.RecordAttemptedEnqueue()
						for {
							var resp *relaypb.EnqueueResponse
							err := executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
								reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
								defer reqCancel()
								var eErr error
								resp, eErr = c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
									Queue:          queue,
									Payload:        payload,
									Priority:       1,
									IdempotencyKey: idempKey,
									MaxAttempts:    3,
								})
								return eErr
							}, h)

							if err == nil && resp != nil {
								tracker.RecordEnqueue(resp.JobId, idempKey)
								break
							}
							if ctx.Err() != nil {
								tracker.RecordUnacknowledgedEnqueue()
								break
							}
							time.Sleep(30 * time.Millisecond)
						}
					}
				}(p)
			}

			// 2. Concurrent Workers
			var workerWg sync.WaitGroup
			numWorkers := 4
			stopWorkers := make(chan struct{})
			var stopWorkersOnce sync.Once
			stopWorkersFn := func() {
				stopWorkersOnce.Do(func() {
					close(stopWorkers)
				})
				workerWg.Wait()
			}
			defer stopWorkersFn()

			for w := 0; w < numWorkers; w++ {
				workerWg.Add(1)
				go func(wID int) {
					defer workerWg.Done()
					workerName := fmt.Sprintf("sc-a-worker-%d", wID)

					for {
						select {
						case <-stopWorkers:
							return
						case <-ctx.Done():
							return
						default:
						}

						client, _, cErr := h.GetRandomActiveClient()
						if cErr != nil {
							time.Sleep(50 * time.Millisecond)
							continue
						}

						resCtx, resCancel := context.WithTimeout(ctx, 3*time.Second)
						resResp, rErr := client.Reserve(resCtx, &relaypb.ReserveRequest{
							Queue:           queue,
							WorkerId:        workerName,
							LeaseDurationMs: 2500,
						})
						resCancel()

						if rErr != nil {
							time.Sleep(40 * time.Millisecond)
							continue
						}

						job := resResp.Job
						tracker.RecordReservation(job.Id, workerName, resResp.LeaseToken)

						// Failure-injecting worker for poison jobs: Nacks them until dead
						if strings.HasPrefix(string(job.Payload), "poison-") {
							tracker.RecordFault("POISON_NACK", workerName, fmt.Sprintf("nacking poison job %s attempt %d", job.Id, job.Attempts))
							_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
								nackCtx, nackCancel := context.WithTimeout(ctx, 3*time.Second)
								defer nackCancel()
								_, nErr := c.Nack(nackCtx, &relaypb.NackRequest{
									JobId:        job.Id,
									LeaseToken:   resResp.LeaseToken,
									ErrorMsg:     "simulated unrecoverable poison failure",
									RetryAfterMs: 0,
								})
								return nErr
							}, h)
							continue
						}

						// Ack the job with retry on Unavailable
						_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
							ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
							defer ackCancel()
							_, aErr := c.Ack(ackCtx, &relaypb.AckRequest{
								JobId:      job.Id,
								LeaseToken: resResp.LeaseToken,
							})
							if aErr == nil {
								tracker.RecordComplete(job.Id)
								return nil
							}
							st, _ := status.FromError(aErr)
							if st.Code() == codes.FailedPrecondition {
								if strings.Contains(aErr.Error(), "JOB_STATE_DONE") {
									tracker.RecordComplete(job.Id)
									return nil
								}
								tracker.RecordStaleAck(true)
								return nil
							}
							return aErr
						}, h)
					}
				}(w)
			}

			// 3. Fault Injector: Trigger 3 leader kills while load is in flight
			var killsCount int64
			var chaosErr error
			chaosDone := make(chan struct{})
			producersDone := make(chan struct{})

			go func() {
				defer close(chaosDone)
				milestones := []int{numJobs / 4, numJobs / 2, (numJobs * 3) / 4}
				for k, targetAck := range milestones {
					// Wait until target acknowledged enqueues reached or context cancelled
					for {
						if ctx.Err() != nil {
							chaosErr = fmt.Errorf("kill %d: context cancelled while waiting for traffic milestone %d: %w", k+1, targetAck, ctx.Err())
							return
						}
						if tracker.TotalAckedEnqueuesCount() >= targetAck {
							break
						}
						select {
						case <-producersDone:
							// Producers already finished, proceed with kill
							break
						default:
							time.Sleep(10 * time.Millisecond)
						}
						if tracker.TotalAckedEnqueuesCount() >= targetAck {
							break
						}
					}

					killedID, kErr := h.KillLeader()
					if kErr != nil {
						chaosErr = fmt.Errorf("kill %d failed to assassinate leader: %w", k+1, kErr)
						return
					}
					tracker.RecordLeaderKill()
					atomic.AddInt64(&killsCount, 1)

					// Wait for surviving quorum to elect new leader while killed leader is down
					_, wErr1 := h.WaitForLeader(15 * time.Second)
					if wErr1 != nil {
						_ = h.RestartNode(killedID)
						chaosErr = fmt.Errorf("kill %d: surviving quorum failed to elect leader within 15s: %w", k+1, wErr1)
						return
					}

					// Allow in-flight load traffic to hit new leader while old leader is down
					time.Sleep(150 * time.Millisecond)

					// Restart killed node to maintain quorum
					rErr := h.RestartNode(killedID)
					if rErr != nil {
						chaosErr = fmt.Errorf("kill %d: failed to restart killed node %s: %w", k+1, killedID, rErr)
						return
					}

					// Verify quorum is healthy with stable leader
					_, wErr2 := h.WaitForLeader(15 * time.Second)
					if wErr2 != nil {
						chaosErr = fmt.Errorf("kill %d: failed to establish leader after restart of %s: %w", k+1, killedID, wErr2)
						return
					}
				}
			}()

			// Wait for producers to finish enqueuing
			producerWg.Wait()
			close(producersDone)

			// Wait for all 3 assassinations to complete
			<-chaosDone
			require.NoError(t, chaosErr, "Leader assassination fault injection failed. Run: %d, Seed: %d", run, seed)
			time.Sleep(200 * time.Millisecond)
			_, err = h.WaitForLeader(15 * time.Second)
			require.NoError(t, err, "Cluster failed to maintain leader after chaos")

			// Drain remaining work
			stopWorkersFn()

			drainCtx, drainCancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer drainCancel()
			_ = h.DrainQueue(drainCtx, queue, tracker, 4)

			// Sync any jobs that reached DONE or DEAD in the leader's FSM
			if leaderDump, err := h.tc.StateDump(h.GetLeaderID()); err == nil {
				for _, kv := range leaderDump["jobs"] {
					var job relaypb.Job
					if err := proto.Unmarshal([]byte(kv.Value), &job); err == nil {
						if job.State == relaypb.JobState_JOB_STATE_DONE {
							tracker.RecordComplete(job.Id)
						} else if job.State == relaypb.JobState_JOB_STATE_DEAD {
							tracker.RecordDead(job.Id)
						}
					}
				}
			}

			// Check index integrity and replica convergence
			err = h.VerifyFSMConsistency()
			tracker.SetReplicaConvergence(err == nil)
			if leaderDump, dErr := h.tc.StateDump(h.GetLeaderID()); dErr == nil {
				iErr := VerifyIndexIntegrity(leaderDump)
				tracker.SetIndexIntegrity(iErr == nil)
			}

			// Invariant Verification (Assert all 8 invariants)
			report := tracker.EvaluateInvariants(0)
			tracker.PrintReport(os.Stdout, "Scenario A: 3 leader kills per run", report)

			require.GreaterOrEqual(t, atomic.LoadInt64(&killsCount), int64(3), "Every run must kill the leader at least 3 times. Run: %d, Seed: %d", run, seed)
			require.Greater(t, report.FailoversObserved, int64(0), "Run with 0 observed failovers must FAIL (fault not injected). Run: %d, Seed: %d", run, seed)
			require.Greater(t, report.DeadLetterJobs, 0, "Dead-Lettered jobs must be > 0 in Scenario A. Seed: %d", seed)
			require.True(t, report.Inv1_NoLostJobsPassed, "Inv 1 (Zero Lost Jobs) violated. Seed: %d", seed)
			require.True(t, report.Inv2_NoStuckJobsPassed, "Inv 2 (Zero Stuck Jobs) violated: %v. Seed: %d", report.StuckJobIDs, seed)
			require.True(t, report.Inv3_AckDurabilityPassed, "Inv 3 (Ack Durability) violated. Seed: %d", seed)
			require.True(t, report.Inv4_FencingPassed, "Inv 4 (Stale Lease Fencing) violated. Seed: %d", seed)
			require.True(t, report.Inv5_DuplicateValidityPassed, "Inv 5 (Valid Duplicates) violated. Seed: %d", seed)
			require.True(t, report.Inv6_ReplicaConvergence, "Inv 6 (Replica Convergence) violated. Seed: %d", seed)
			require.True(t, report.Inv7_IndexIntegrityPassed, "Inv 7 (Index Integrity) violated. Seed: %d", seed)
			require.True(t, report.Inv8_TimeMonotonicityPassed, "Inv 8 (Time Monotonicity) violated. Seed: %d", seed)
			require.True(t, report.AllPassed, "Overall chaos invariants failed. Seed: %d", seed)
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario B: Worker Crashes & Late Stale-Ack Attempts
// ---------------------------------------------------------------------------
func TestChaos_ScenarioB_WorkerCrashAndStaleAck(t *testing.T) {
	runs := getRunsCount()
	numJobs := getJobCount(300)

	for run := 1; run <= runs; run++ {
		t.Run(fmt.Sprintf("Run_%d", run), func(t *testing.T) {
			seed := getChaosSeed()
			baseDir := t.TempDir()
			h, err := NewClusterHarness(baseDir)
			require.NoError(t, err)
			defer h.Shutdown()

			_, err = h.WaitForLeader(10 * time.Second)
			require.NoError(t, err)

			queue := fmt.Sprintf("chaos-b-stale-q-%d", run)
			tracker := NewInvariantTracker("Scenario B: Worker Crashes & Late Stale-Ack Attempts", seed)
			tracker.SetTransport("in-memory (raft.InmemTransport) with real loopback TCP gRPC", "N/A")
			h.SetTracker(tracker)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			// 1. Enqueue all test jobs
			for i := 0; i < numJobs; i++ {
				idempKey := fmt.Sprintf("sc-b-job-%d", i)
				tracker.RecordAttemptedEnqueue()
				for {
					var resp *relaypb.EnqueueResponse
					err := executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
						reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
						defer reqCancel()
						var eErr error
						resp, eErr = c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
							Queue:          queue,
							Payload:        []byte("stale-test-payload"),
							Priority:       1,
							IdempotencyKey: idempKey,
							MaxAttempts:    3,
						})
						return eErr
					}, h)
					if err == nil && resp != nil {
						tracker.RecordEnqueue(resp.JobId, idempKey)
						break
					}
					if ctx.Err() != nil {
						tracker.RecordUnacknowledgedEnqueue()
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			// 2. Simulated Crashing Workers with Short Leases
			type LeasedJob struct {
				jobID      string
				leaseToken string
			}
			var crashedJobs []LeasedJob
			var crashedMu sync.Mutex

			var crashWg sync.WaitGroup
			numCrashWorkers := 4

			for w := 0; w < numCrashWorkers; w++ {
				crashWg.Add(1)
				go func(wID int) {
					defer crashWg.Done()
					workerName := fmt.Sprintf("crash-worker-%d", wID)
					for i := 0; i < 20; i++ {
						client, _, cErr := h.GetRandomActiveClient()
						if cErr != nil {
							continue
						}

						// Reserve with very short lease (150ms)
						resCtx, resCancel := context.WithTimeout(ctx, 3*time.Second)
						resResp, rErr := client.Reserve(resCtx, &relaypb.ReserveRequest{
							Queue:           queue,
							WorkerId:        workerName,
							LeaseDurationMs: 150,
						})
						resCancel()

						if rErr == nil && resResp != nil && resResp.Job != nil {
							tracker.RecordReservation(resResp.Job.Id, workerName, resResp.LeaseToken)
							tracker.RecordFault("WORKER_CRASH", workerName, fmt.Sprintf("worker dropped lease for job %s", resResp.Job.Id))
							crashedMu.Lock()
							crashedJobs = append(crashedJobs, LeasedJob{
								jobID:      resResp.Job.Id,
								leaseToken: resResp.LeaseToken,
							})
							crashedMu.Unlock()
							// "Crash": Worker abruptly drops lease without acking
						}
						time.Sleep(30 * time.Millisecond)
					}
				}(w)
			}
			crashWg.Wait()

			// Wait for leases to expire and leader lease reaper to sweep them back to PENDING
			time.Sleep(1200 * time.Millisecond)

			// 3. Healthy Workers drain and complete all jobs (including previously crashed ones)
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer drainCancel()
			_ = h.DrainQueue(drainCtx, queue, tracker, 4)

			// 4. "Zombie" recovery: crashed workers wake up late and attempt Ack with old tokens
			crashedMu.Lock()
			staleAttempts := make([]LeasedJob, len(crashedJobs))
			copy(staleAttempts, crashedJobs)
			crashedMu.Unlock()

			for _, attempt := range staleAttempts {
				client, _, cErr := h.GetRandomActiveClient()
				if cErr != nil {
					continue
				}

				tracker.RecordFault("ZOMBIE_ACK", "zombie-worker", fmt.Sprintf("stale ack attempt for job %s", attempt.jobID))
				ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
				_, aErr := client.Ack(ackCtx, &relaypb.AckRequest{
					JobId:      attempt.jobID,
					LeaseToken: attempt.leaseToken,
				})
				ackCancel()

				// MUST be rejected with FAILED_PRECONDITION
				if aErr != nil {
					st, _ := status.FromError(aErr)
					if st.Code() == codes.FailedPrecondition {
						tracker.RecordStaleAck(true)
						continue
					}
				}
				tracker.RecordStaleAck(false)
			}

			// Verify replica convergence & index integrity
			err = h.VerifyFSMConsistency()
			tracker.SetReplicaConvergence(err == nil)
			if leaderDump, dErr := h.tc.StateDump(h.GetLeaderID()); dErr == nil {
				iErr := VerifyIndexIntegrity(leaderDump)
				tracker.SetIndexIntegrity(iErr == nil)
			}

			// Invariant Verification (Assert all 8 invariants)
			report := tracker.EvaluateInvariants(0)
			tracker.PrintReport(os.Stdout, "Scenario B: Worker Crashes & Late Stale-Ack Attempts", report)

			require.True(t, report.Inv1_NoLostJobsPassed, "Inv 1 (Zero Lost Jobs) violated. Seed: %d", seed)
			require.True(t, report.Inv2_NoStuckJobsPassed, "Inv 2 (Zero Stuck Jobs) violated: %v. Seed: %d", report.StuckJobIDs, seed)
			require.True(t, report.Inv3_AckDurabilityPassed, "Inv 3 (Ack Durability) violated. Seed: %d", seed)
			require.True(t, report.Inv4_FencingPassed, "Inv 4 (Stale Lease Fencing) violated. Seed: %d", seed)
			require.True(t, report.Inv5_DuplicateValidityPassed, "Inv 5 (Valid Duplicates) violated. Seed: %d", seed)
			require.True(t, report.Inv6_ReplicaConvergence, "Inv 6 (Replica Convergence) violated. Seed: %d", seed)
			require.True(t, report.Inv7_IndexIntegrityPassed, "Inv 7 (Index Integrity) violated. Seed: %d", seed)
			require.True(t, report.Inv8_TimeMonotonicityPassed, "Inv 8 (Time Monotonicity) violated. Seed: %d", seed)
			require.True(t, report.AllPassed, "Overall chaos invariants failed. Seed: %d", seed)
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario C: Network Partition & Rolling Restart
// ---------------------------------------------------------------------------
func TestChaos_ScenarioC_NetworkPartitionAndRollingRestart(t *testing.T) {
	runs := getRunsCount()
	numJobs := getJobCount(400)

	for run := 1; run <= runs; run++ {
		t.Run(fmt.Sprintf("Run_%d", run), func(t *testing.T) {
			seed := getChaosSeed()
			baseDir := t.TempDir()
			h, err := NewClusterHarness(baseDir)
			require.NoError(t, err)
			defer h.Shutdown()

			leaderID, err := h.WaitForLeader(10 * time.Second)
			require.NoError(t, err)

			queue := fmt.Sprintf("chaos-c-partition-q-%d", run)
			tracker := NewInvariantTracker("Scenario C: Minority Partition & Rolling Restart", seed)
			tracker.SetTransport("in-memory (raft.InmemTransport) with real loopback TCP gRPC", "InmemTransport.Disconnect() isolating minority follower from cluster peers")
			h.SetTracker(tracker)

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			// Identify a follower to isolate
			var followerID string
			for _, id := range []string{"node-1", "node-2", "node-3"} {
				if id != leaderID {
					followerID = id
					break
				}
			}
			require.NotEmpty(t, followerID)

			// 1. Enqueue initial batch of jobs
			for i := 0; i < numJobs/2; i++ {
				idempKey := fmt.Sprintf("sc-c-pre-part-%d", i)
				tracker.RecordAttemptedEnqueue()
				for {
					var resp *relaypb.EnqueueResponse
					err := executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
						reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
						defer reqCancel()
						var eErr error
						resp, eErr = c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
							Queue:          queue,
							Payload:        []byte("partition-payload"),
							Priority:       1,
							IdempotencyKey: idempKey,
							MaxAttempts:    3,
						})
						return eErr
					}, h)
					if err == nil && resp != nil {
						tracker.RecordEnqueue(resp.JobId, idempKey)
						break
					}
					if ctx.Err() != nil {
						tracker.RecordUnacknowledgedEnqueue()
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			// 2. Network Partition: Isolate minority node (follower)
			err = h.Partition(followerID)
			require.NoError(t, err)

			// Traffic continues on remaining majority (2 nodes)
			for i := numJobs / 2; i < numJobs; i++ {
				idempKey := fmt.Sprintf("sc-c-during-part-%d", i)
				tracker.RecordAttemptedEnqueue()
				for {
					var resp *relaypb.EnqueueResponse
					err := executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
						reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
						defer reqCancel()
						var eErr error
						resp, eErr = c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
							Queue:          queue,
							Payload:        []byte("partition-payload-during"),
							Priority:       1,
							IdempotencyKey: idempKey,
							MaxAttempts:    3,
						})
						return eErr
					}, h)
					if err == nil && resp != nil {
						tracker.RecordEnqueue(resp.JobId, idempKey)
						break
					}
					if ctx.Err() != nil {
						tracker.RecordUnacknowledgedEnqueue()
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			// 3. Heal Partition: Reconnect isolated follower
			time.Sleep(500 * time.Millisecond)
			err = h.HealPartition()
			require.NoError(t, err)

			// Allow follower to catch up with Raft log
			time.Sleep(1 * time.Second)

			// 4. Rolling Restart: Sequentially restart all 3 nodes (restarting current leader first)
			err = h.RollingRestart(800 * time.Millisecond)
			require.NoError(t, err)

			// 5. Drain remaining jobs
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer drainCancel()
			_ = h.DrainQueue(drainCtx, queue, tracker, 4)

			// 6. Verify State Consistency across nodes
			err = h.VerifyFSMConsistency()
			tracker.SetReplicaConvergence(err == nil)
			require.NoError(t, err, "FSM state dump must match across all nodes after heal and rolling restart")
			if leaderDump, dErr := h.tc.StateDump(h.GetLeaderID()); dErr == nil {
				iErr := VerifyIndexIntegrity(leaderDump)
				tracker.SetIndexIntegrity(iErr == nil)
			}

			// Invariant Verification (Assert all 8 invariants)
			report := tracker.EvaluateInvariants(0)
			tracker.PrintReport(os.Stdout, "Scenario C: Minority Partition & Rolling Restart", report)

			require.Greater(t, report.FailoversObserved, int64(0), "Scenario C must record at least 1 leader failover during rolling restart. Seed: %d", seed)

			require.True(t, report.Inv1_NoLostJobsPassed, "Inv 1 (Zero Lost Jobs) violated. Seed: %d", seed)
			require.True(t, report.Inv2_NoStuckJobsPassed, "Inv 2 (Zero Stuck Jobs) violated: %v. Seed: %d", report.StuckJobIDs, seed)
			require.True(t, report.Inv3_AckDurabilityPassed, "Inv 3 (Ack Durability) violated. Seed: %d", seed)
			require.True(t, report.Inv4_FencingPassed, "Inv 4 (Stale Lease Fencing) violated. Seed: %d", seed)
			require.True(t, report.Inv5_DuplicateValidityPassed, "Inv 5 (Valid Duplicates) violated. Seed: %d", seed)
			require.True(t, report.Inv6_ReplicaConvergence, "Inv 6 (Replica Convergence) violated. Seed: %d", seed)
			require.True(t, report.Inv7_IndexIntegrityPassed, "Inv 7 (Index Integrity) violated. Seed: %d", seed)
			require.True(t, report.Inv8_TimeMonotonicityPassed, "Inv 8 (Time Monotonicity) violated. Seed: %d", seed)
			require.True(t, report.AllPassed, "Overall chaos invariants failed. Seed: %d", seed)
		})
	}
}

// ---------------------------------------------------------------------------
// High Volume Test: 10,000+ Jobs Total Across Runs / Heavy Workload
// ---------------------------------------------------------------------------
func TestChaos_HighVolumeWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high volume workload in short mode")
	}

	runs := getRunsCount()
	totalJobs := getJobCount(1000)

	for run := 1; run <= runs; run++ {
		t.Run(fmt.Sprintf("Run_%d", run), func(t *testing.T) {
			seed := getChaosSeed()
			baseDir := t.TempDir()
			h, err := NewClusterHarness(baseDir)
			require.NoError(t, err)
			defer h.Shutdown()

			_, err = h.WaitForLeader(10 * time.Second)
			require.NoError(t, err)

			queue := fmt.Sprintf("chaos-high-volume-q-%d", run)
			tracker := NewInvariantTracker("High Volume Workload (Concurrent Producers & Workers)", seed)
			tracker.SetTransport("in-memory (raft.InmemTransport) with real loopback TCP gRPC", "N/A")
			h.SetTracker(tracker)

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			var wg sync.WaitGroup
			producers := 8
			jobsPerProducer := totalJobs / producers

			// Concurrent producers
			for p := 0; p < producers; p++ {
				wg.Add(1)
				go func(pID int) {
					defer wg.Done()
					for j := 0; j < jobsPerProducer; j++ {
						idempKey := fmt.Sprintf("hv-p-%d-j-%d", pID, j)
						tracker.RecordAttemptedEnqueue()
						for {
							var resp *relaypb.EnqueueResponse
							err := executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
								reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
								defer reqCancel()
								var eErr error
								resp, eErr = c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
									Queue:          queue,
									Payload:        []byte("high-volume-payload"),
									Priority:       uint32(rand.Intn(3)),
									IdempotencyKey: idempKey,
									MaxAttempts:    3,
								})
								return eErr
							}, h)
							if err == nil && resp != nil {
								tracker.RecordEnqueue(resp.JobId, idempKey)
								break
							}
							if ctx.Err() != nil {
								tracker.RecordUnacknowledgedEnqueue()
								break
							}
							time.Sleep(20 * time.Millisecond)
						}
					}
				}(p)
			}

			// Concurrent workers processing concurrently
			var workerWg sync.WaitGroup
			workers := 8
			stopWorkers := make(chan struct{})
			var stopWorkersOnce sync.Once
			stopWorkersFn := func() {
				stopWorkersOnce.Do(func() {
					close(stopWorkers)
				})
				workerWg.Wait()
			}
			defer stopWorkersFn()

			for w := 0; w < workers; w++ {
				workerWg.Add(1)
				go func(wID int) {
					defer workerWg.Done()
					workerName := fmt.Sprintf("hv-worker-%d", wID)

					for {
						select {
						case <-stopWorkers:
							return
						case <-ctx.Done():
							return
						default:
						}

						client, _, cErr := h.GetRandomActiveClient()
						if cErr != nil {
							time.Sleep(30 * time.Millisecond)
							continue
						}

						resCtx, resCancel := context.WithTimeout(ctx, 3*time.Second)
						resResp, rErr := client.Reserve(resCtx, &relaypb.ReserveRequest{
							Queue:           queue,
							WorkerId:        workerName,
							LeaseDurationMs: 2500,
						})
						resCancel()

						if rErr != nil {
							time.Sleep(30 * time.Millisecond)
							continue
						}

						job := resResp.Job
						tracker.RecordReservation(job.Id, workerName, resResp.LeaseToken)

						_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
							ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
							defer ackCancel()
							_, aErr := c.Ack(ackCtx, &relaypb.AckRequest{
								JobId:      job.Id,
								LeaseToken: resResp.LeaseToken,
							})
							if aErr == nil {
								tracker.RecordComplete(job.Id)
								return nil
							}
							st, _ := status.FromError(aErr)
							if st.Code() == codes.FailedPrecondition {
								if strings.Contains(aErr.Error(), "JOB_STATE_DONE") {
									tracker.RecordComplete(job.Id)
									return nil
								}
								tracker.RecordStaleAck(true)
								return nil
							}
							return aErr
						}, h)
					}
				}(w)
			}

			// Intermittent chaos: 2 leader assassinations during load
			stopAssassination := make(chan struct{})
			go func() {
				ticker := time.NewTicker(3 * time.Second)
				defer ticker.Stop()

				assassinationCount := 0
				for {
					select {
					case <-stopAssassination:
						return
					case <-ticker.C:
						if assassinationCount < 2 {
							killedID, kErr := h.KillLeader()
							if kErr == nil && killedID != "" {
								assassinationCount++
								time.Sleep(300 * time.Millisecond)
								_ = h.RestartNode(killedID)
								_, _ = h.WaitForLeader(8 * time.Second)
							}
						}
					}
				}
			}()

			wg.Wait()
			close(stopAssassination)
			time.Sleep(1 * time.Second)

			stopWorkersFn()

			// Drain all remaining
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 35*time.Second)
			defer drainCancel()
			_ = h.DrainQueue(drainCtx, queue, tracker, 8)

			// Sync any jobs that reached DONE or DEAD in the leader's FSM
			if leaderDump, err := h.tc.StateDump(h.GetLeaderID()); err == nil {
				for _, kv := range leaderDump["jobs"] {
					var job relaypb.Job
					if err := proto.Unmarshal([]byte(kv.Value), &job); err == nil {
						if job.State == relaypb.JobState_JOB_STATE_DONE {
							tracker.RecordComplete(job.Id)
						} else if job.State == relaypb.JobState_JOB_STATE_DEAD {
							tracker.RecordDead(job.Id)
						}
					}
				}
			}

			// Check index integrity and replica convergence
			err = h.VerifyFSMConsistency()
			tracker.SetReplicaConvergence(err == nil)
			if leaderDump, dErr := h.tc.StateDump(h.GetLeaderID()); dErr == nil {
				iErr := VerifyIndexIntegrity(leaderDump)
				tracker.SetIndexIntegrity(iErr == nil)
			}

			// Final Invariant Report (Assert all 8 invariants)
			report := tracker.EvaluateInvariants(0)
			tracker.PrintReport(os.Stdout, "High Volume Workload (Concurrent Producers & Workers)", report)

			require.True(t, report.Inv1_NoLostJobsPassed, "Inv 1 (Zero Lost Jobs) violated. Seed: %d", seed)
			require.True(t, report.Inv2_NoStuckJobsPassed, "Inv 2 (Zero Stuck Jobs) violated: %v. Seed: %d", report.StuckJobIDs, seed)
			require.True(t, report.Inv3_AckDurabilityPassed, "Inv 3 (Ack Durability) violated. Seed: %d", seed)
			require.True(t, report.Inv4_FencingPassed, "Inv 4 (Stale Lease Fencing) violated. Seed: %d", seed)
			require.True(t, report.Inv5_DuplicateValidityPassed, "Inv 5 (Valid Duplicates) violated. Seed: %d", seed)
			require.True(t, report.Inv6_ReplicaConvergence, "Inv 6 (Replica Convergence) violated. Seed: %d", seed)
			require.True(t, report.Inv7_IndexIntegrityPassed, "Inv 7 (Index Integrity) violated. Seed: %d", seed)
			require.True(t, report.Inv8_TimeMonotonicityPassed, "Inv 8 (Time Monotonicity) violated. Seed: %d", seed)
			require.True(t, report.AllPassed, "Overall chaos invariants failed. Seed: %d", seed)
		})
	}
}
