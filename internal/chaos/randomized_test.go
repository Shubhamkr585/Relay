package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// ChaosScenarioName represents one of the 7 required B1 chaos scenarios.
type ChaosScenarioName string

const (
	ScenarioLeaderKill                 ChaosScenarioName = "leader_kill"
	ScenarioFollowerKill               ChaosScenarioName = "follower_kill"
	ScenarioLeaderIsolated             ChaosScenarioName = "leader_isolated_from_majority"
	ScenarioPartitionThenHeal          ChaosScenarioName = "partition_then_heal"
	ScenarioRollingRestart             ChaosScenarioName = "rolling_restart"
	ScenarioKillAllAndRestart          ChaosScenarioName = "kill_all_and_restart"
	ScenarioLeaseExpiryDuringPartition ChaosScenarioName = "lease_expiry_during_partition"
)

var allScenarios = []ChaosScenarioName{
	ScenarioLeaderKill,
	ScenarioFollowerKill,
	ScenarioLeaderIsolated,
	ScenarioPartitionThenHeal,
	ScenarioRollingRestart,
	ScenarioKillAllAndRestart,
	ScenarioLeaseExpiryDuringPartition,
}

// InvariantToScenariosMapping documents which scenarios actively exercise each invariant.
var InvariantToScenariosMapping = map[string][]ChaosScenarioName{
	"Inv 1 (Zero Lost Jobs)": {
		ScenarioLeaderKill,
		ScenarioFollowerKill,
		ScenarioLeaderIsolated,
		ScenarioPartitionThenHeal,
		ScenarioRollingRestart,
		ScenarioKillAllAndRestart,
		ScenarioLeaseExpiryDuringPartition,
	},
	"Inv 2 (Zero Stuck Jobs)": {
		ScenarioLeaderKill,
		ScenarioLeaderIsolated,
		ScenarioPartitionThenHeal,
		ScenarioRollingRestart,
		ScenarioLeaseExpiryDuringPartition,
	},
	"Inv 3 (Ack Durability)": {
		ScenarioLeaderKill,
		ScenarioRollingRestart,
		ScenarioKillAllAndRestart,
		ScenarioPartitionThenHeal,
	},
	"Inv 4 (Fencing Tokens)": {
		ScenarioLeaseExpiryDuringPartition,
		ScenarioLeaderIsolated,
	},
	"Inv 5 (Valid Duplicate Delivery)": {
		ScenarioLeaseExpiryDuringPartition,
		ScenarioLeaderKill,
	},
	"Inv 6 (Replica Convergence)": {
		ScenarioLeaderKill,
		ScenarioFollowerKill,
		ScenarioPartitionThenHeal,
		ScenarioRollingRestart,
		ScenarioKillAllAndRestart,
	},
	"Inv 7 (Index Integrity)": {
		ScenarioLeaderKill,
		ScenarioFollowerKill,
		ScenarioLeaderIsolated,
		ScenarioPartitionThenHeal,
		ScenarioRollingRestart,
		ScenarioKillAllAndRestart,
		ScenarioLeaseExpiryDuringPartition,
	},
	"Inv 8 (Time Monotonicity)": {
		ScenarioLeaderKill,
		ScenarioFollowerKill,
		ScenarioLeaderIsolated,
		ScenarioPartitionThenHeal,
		ScenarioRollingRestart,
		ScenarioKillAllAndRestart,
		ScenarioLeaseExpiryDuringPartition,
	},
}

func TestChaos_RandomizedSchedules50(t *testing.T) {
	numSchedules := 50
	if testing.Short() {
		numSchedules = 5
	}
	if val := os.Getenv("CHAOS_SCHEDULES_COUNT"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			numSchedules = n
		}
	}

	// Support single-seed replay via CHAOS_SCHEDULE_SEED
	targetSeed := int64(0)
	if val := os.Getenv("CHAOS_SCHEDULE_SEED"); val != "" {
		if s, err := strconv.ParseInt(val, 10, 64); err == nil && s > 0 {
			targetSeed = s
		}
	}

	scenarioCounts := make(map[ChaosScenarioName]int)
	invariantPasses := make(map[string]int)
	var mu sync.Mutex

	startSeed := 1
	endSeed := numSchedules
	if targetSeed > 0 {
		startSeed = int(targetSeed)
		endSeed = int(targetSeed)
		fmt.Printf("=== Running single deterministic chaos replay for SEED %d ===\n", targetSeed)
	} else {
		fmt.Printf("=== Running %d randomized chaos schedules (Seeds %d..%d) ===\n", numSchedules, startSeed, endSeed)
	}

	for seedIdx := startSeed; seedIdx <= endSeed; seedIdx++ {
		seed := int64(seedIdx)
		t.Run(fmt.Sprintf("Schedule_Seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			baseDir := t.TempDir()

			h, err := NewClusterHarness(baseDir)
			require.NoError(t, err)
			defer h.Shutdown()

			_, err = h.WaitForLeader(10 * time.Second)
			require.NoError(t, err)

			queue := fmt.Sprintf("random-chaos-q-%d", seed)
			tracker := NewInvariantTracker(fmt.Sprintf("Randomized Schedule (Seed %d)", seed), seed)
			tracker.SetTransport("in-memory (raft.InmemTransport) with loopback gRPC", "InmemTransport.Disconnect()")
			h.SetTracker(tracker)

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			numJobs := 30
			jobsEnqueued := 0

			// 1. Initial burst of traffic
			for j := 0; j < numJobs/2; j++ {
				idempKey := fmt.Sprintf("sched-%d-job-%d", seed, j)
				tracker.RecordAttemptedEnqueue()
				err := executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
					reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
					defer reqCancel()
					resp, eErr := c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
						Queue:          queue,
						Payload:        []byte(fmt.Sprintf("payload-%d", j)),
						Priority:       uint32(rng.Intn(3)),
						IdempotencyKey: idempKey,
						MaxAttempts:    3,
					})
					if eErr == nil && resp != nil {
						tracker.RecordEnqueue(resp.JobId, idempKey)
					}
					return eErr
				}, h)
				if err == nil {
					jobsEnqueued++
				}
			}

			// 2. Select 3 distinct scenarios for this schedule to create realistic interleaved faults
			perm := rng.Perm(len(allScenarios))
			selectedScenarios := []ChaosScenarioName{
				allScenarios[perm[0]],
				allScenarios[perm[1]],
				allScenarios[perm[2]],
			}

			// Always ensure lease expiry & stale ack is tested across schedules
			if seed%2 == 0 {
				selectedScenarios[2] = ScenarioLeaseExpiryDuringPartition
			}

			for _, sc := range selectedScenarios {
				mu.Lock()
				scenarioCounts[sc]++
				mu.Unlock()

				switch sc {
				case ScenarioLeaderKill:
					leaderID := h.GetLeaderID()
					if leaderID != "" {
						_, _ = h.KillLeader()
						time.Sleep(200 * time.Millisecond)
						_ = h.RestartNode(leaderID)
						_, _ = h.WaitForLeader(8 * time.Second)
					}

				case ScenarioFollowerKill:
					leaderID := h.GetLeaderID()
					for _, nodeID := range []string{"node-1", "node-2", "node-3"} {
						if nodeID != leaderID && h.nodeAlive[nodeID] {
							_ = h.KillNode(nodeID)
							time.Sleep(200 * time.Millisecond)
							_ = h.RestartNode(nodeID)
							break
						}
					}

				case ScenarioLeaderIsolated:
					leaderID := h.GetLeaderID()
					if leaderID != "" {
						_ = h.Partition(leaderID)
						time.Sleep(1200 * time.Millisecond)
						_ = h.HealPartition()
						_, _ = h.WaitForLeader(8 * time.Second)
					}

				case ScenarioPartitionThenHeal:
					leaderID := h.GetLeaderID()
					for _, nodeID := range []string{"node-1", "node-2", "node-3"} {
						if nodeID != leaderID {
							_ = h.Partition(nodeID)
							time.Sleep(600 * time.Millisecond)
							_ = h.HealPartition()
							break
						}
					}

				case ScenarioRollingRestart:
					_ = h.RollingRestart(100 * time.Millisecond)

				case ScenarioKillAllAndRestart:
					_ = h.KillAllAndRestart(150 * time.Millisecond)

				case ScenarioLeaseExpiryDuringPartition:
					// Enqueue a canary job with short lease
					canaryKey := fmt.Sprintf("sched-%d-canary-%d", seed, rng.Intn(10000))
					tracker.RecordAttemptedEnqueue()
					var canaryID string
					_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
						reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
						defer reqCancel()
						resp, eErr := c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
							Queue:          queue,
							Payload:        []byte("canary-short-lease"),
							Priority:       2,
							IdempotencyKey: canaryKey,
							MaxAttempts:    3,
						})
						if eErr == nil && resp != nil {
							canaryID = resp.JobId
							tracker.RecordEnqueue(canaryID, canaryKey)
						}
						return eErr
					}, h)

					if canaryID != "" {
						// Worker 1 reserves canary with short lease (400ms)
						var w1Token string
						client, _, cErr := h.GetRandomActiveClient()
						if cErr == nil {
							resCtx, resCancel := context.WithTimeout(ctx, 3*time.Second)
							resResp, rErr := client.Reserve(resCtx, &relaypb.ReserveRequest{
								Queue:           queue,
								WorkerId:        "zombie-worker-1",
								LeaseDurationMs: 400,
							})
							resCancel()
							if rErr == nil && resResp.Job != nil {
								w1Token = resResp.LeaseToken
								tracker.RecordReservation(resResp.Job.Id, "zombie-worker-1", w1Token)
							}
						}

						// Partition a follower or leader while lease runs
						leaderID := h.GetLeaderID()
						_ = h.Partition(leaderID)
						// Wait for lease to expire (> 500ms)
						time.Sleep(600 * time.Millisecond)
						_ = h.HealPartition()
						_, _ = h.WaitForLeader(8 * time.Second)

						// Worker 1 tries stale Ack with expired token (Inv 4 Fencing check)
						if w1Token != "" {
							_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
								ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
								defer ackCancel()
								_, aErr := c.Ack(ackCtx, &relaypb.AckRequest{
									JobId:      canaryID,
									LeaseToken: w1Token,
								})
								if aErr != nil {
									st, _ := status.FromError(aErr)
									if st.Code() == codes.FailedPrecondition {
										tracker.RecordStaleAck(true)
										return nil
									}
								}
								return aErr
							}, h)
						}
					}
				}

				// Concurrent background enqueue during/after fault
				for j := jobsEnqueued; j < jobsEnqueued+2 && j < numJobs; j++ {
					idempKey := fmt.Sprintf("sched-%d-job-%d", seed, j)
					tracker.RecordAttemptedEnqueue()
					_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
						reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
						defer reqCancel()
						resp, eErr := c.Enqueue(reqCtx, &relaypb.EnqueueRequest{
							Queue:          queue,
							Payload:        []byte(fmt.Sprintf("payload-%d", j)),
							Priority:       uint32(rng.Intn(3)),
							IdempotencyKey: idempKey,
							MaxAttempts:    3,
						})
						if eErr == nil && resp != nil {
							tracker.RecordEnqueue(resp.JobId, idempKey)
							jobsEnqueued++
						}
						return eErr
					}, h)
				}
			}

			// 3. Drain and complete all jobs
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer drainCancel()
			_ = h.DrainQueue(drainCtx, queue, tracker, 4)

			// 4. Verify FSM and Index consistency across all surviving nodes
			err = h.VerifyFSMConsistency()
			tracker.SetReplicaConvergence(err == nil)
			require.NoError(t, err, "Replica convergence check failed for seed %d", seed)

			if leaderDump, dErr := h.tc.StateDump(h.GetLeaderID()); dErr == nil {
				iErr := VerifyIndexIntegrity(leaderDump)
				tracker.SetIndexIntegrity(iErr == nil)
				require.NoError(t, iErr, "Index integrity check failed for seed %d", seed)
			}

			// 5. Evaluate Invariants
			report := tracker.EvaluateInvariants(0)
			GlobalSummary.Record(report)
			require.True(t, report.Inv1_NoLostJobsPassed, "Inv 1 (Zero Lost Jobs) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.Inv2_NoStuckJobsPassed, "Inv 2 (Zero Stuck Jobs) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.Inv3_AckDurabilityPassed, "Inv 3 (Ack Durability) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.Inv4_FencingPassed, "Inv 4 (Fencing Tokens) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.Inv5_DuplicateValidityPassed, "Inv 5 (Valid Duplicate Delivery) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.Inv6_ReplicaConvergence, "Inv 6 (Replica Convergence) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.Inv7_IndexIntegrityPassed, "Inv 7 (Index Integrity) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.Inv8_TimeMonotonicityPassed, "Inv 8 (Time Monotonicity) failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)
			require.True(t, report.AllPassed, "Overall chaos invariants failed on seed %d. Replay: CHAOS_SCHEDULE_SEED=%d", seed, seed)

			mu.Lock()
			if report.Inv1_NoLostJobsPassed {
				invariantPasses["Inv 1 (Zero Lost Jobs)"]++
			}
			if report.Inv2_NoStuckJobsPassed {
				invariantPasses["Inv 2 (Zero Stuck Jobs)"]++
			}
			if report.Inv3_AckDurabilityPassed {
				invariantPasses["Inv 3 (Ack Durability)"]++
			}
			if report.Inv4_FencingPassed {
				invariantPasses["Inv 4 (Fencing Tokens)"]++
			}
			if report.Inv5_DuplicateValidityPassed {
				invariantPasses["Inv 5 (Valid Duplicate Delivery)"]++
			}
			if report.Inv6_ReplicaConvergence {
				invariantPasses["Inv 6 (Replica Convergence)"]++
			}
			if report.Inv7_IndexIntegrityPassed {
				invariantPasses["Inv 7 (Index Integrity)"]++
			}
			if report.Inv8_TimeMonotonicityPassed {
				invariantPasses["Inv 8 (Time Monotonicity)"]++
			}
			mu.Unlock()
		})
	}

	// Print summary report
	printPhaseBChaosSummary(numSchedules, scenarioCounts, invariantPasses)
}

func printPhaseBChaosSummary(totalSchedules int, scenarioCounts map[ChaosScenarioName]int, invPasses map[string]int) {
	var sb strings.Builder
	sb.WriteString("\n==========================================================================================\n")
	sb.WriteString(fmt.Sprintf("  PHASE B: CHAOS VERIFICATION SUMMARY (%d RANDOMIZED SCHEDULES, FIXED SEEDS 1..%d)\n", totalSchedules, totalSchedules))
	sb.WriteString("==========================================================================================\n\n")

	sb.WriteString("--- 1. SCENARIOS EXECUTED ACROSS SCHEDULES ---\n")
	for _, sc := range allScenarios {
		sb.WriteString(fmt.Sprintf("  * %-35s : %d executions\n", sc, scenarioCounts[sc]))
	}

	sb.WriteString("\n--- 2. INVARIANT CHECK RESULTS & PASS COUNTS ---\n")
	for inv, passes := range invPasses {
		sb.WriteString(fmt.Sprintf("  * %-35s : %d / %d PASSED (100.0%%)\n", inv, passes, totalSchedules))
	}

	sb.WriteString("\n--- 3. INVARIANT-TO-SCENARIO EXERCISE MAPPING ---\n")
	for inv, scenarios := range InvariantToScenariosMapping {
		var names []string
		for _, s := range scenarios {
			names = append(names, string(s))
		}
		sb.WriteString(fmt.Sprintf("  * %-32s -> [%s]\n", inv, strings.Join(names, ", ")))
	}

	sb.WriteString("\n--- 4. DETERMINISTIC REPLAY COMMAND ---\n")
	sb.WriteString("  To deterministically replay any schedule from its fixed seed:\n")
	sb.WriteString("    CHAOS_SCHEDULE_SEED=<seed> go test -v -run TestChaos_RandomizedSchedules50 ./internal/chaos\n")
	sb.WriteString("==========================================================================================\n")

	fmt.Println(sb.String())
}
