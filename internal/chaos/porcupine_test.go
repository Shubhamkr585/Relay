package chaos

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// TestChaos_PorcupineLinearizability runs concurrent Enqueue, Reserve, and Ack operations
// under leader kill faults with long leases (300s so expiry does not occur) and verifies
// that the execution history is strictly linearizable using Porcupine.
func TestChaos_PorcupineLinearizability(t *testing.T) {
	seeds := []int64{1001, 2002, 3003}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("Seed_%d", seed), func(t *testing.T) {
			baseDir := t.TempDir()
			h, err := NewClusterHarness(baseDir)
			require.NoError(t, err)
			defer h.Shutdown()

			_, err = h.WaitForLeader(10 * time.Second)
			require.NoError(t, err)

			recorder := NewLinearizabilityRecorder()
			q := fmt.Sprintf("porcupine-q-%d", seed)
			totalJobs := 16
			leaseDurationMs := int64(300_000) // 300 seconds - guarantees no expiry occurs

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			var wg sync.WaitGroup

			// 1. Concurrent Producers
			numProducers := 2
			jobsPerProducer := totalJobs / numProducers

			for p := 0; p < numProducers; p++ {
				wg.Add(1)
				go func(pID int) {
					defer wg.Done()
					for j := 0; j < jobsPerProducer; j++ {
						key := fmt.Sprintf("porc-job-%d-%d", pID, j)
						in := QueueInput{
							Op:    OpEnqueue,
							Queue: q,
							Key:   key,
						}
						opID := recorder.RecordInvocation(pID, in)

						// Attempt enqueue with retry on failover
						var enqErr error
						var resJobId string
						for attempt := 0; attempt < 10; attempt++ {
							client, _, cErr := h.GetRandomActiveClient()
							if cErr != nil {
								time.Sleep(100 * time.Millisecond)
								continue
							}
							enqCtx, enqCancel := context.WithTimeout(ctx, 3*time.Second)
							enqResp, err := client.Enqueue(enqCtx, &relaypb.EnqueueRequest{
								Queue:          q,
								Payload:        []byte(fmt.Sprintf("porcupine-job-%d-%d", pID, j)),
								IdempotencyKey: key,
								MaxAttempts:    3,
							})
							enqCancel()
							enqErr = err
							if enqErr == nil {
								resJobId = enqResp.JobId
								break
							}
							time.Sleep(100 * time.Millisecond)
						}

						out := QueueOutput{
							Ok:       enqErr == nil,
							Key:      key,
							JobID:    resJobId,
							TimedOut: enqErr != nil,
						}
						recorder.RecordReturn(opID, out)
					}
				}(p)
			}

			// 2. Concurrent Workers
			numWorkers := 2
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(wID int) {
					defer wg.Done()
					workerName := fmt.Sprintf("porcupine-worker-%d", wID)
					clientID := 10 + wID

					for {
						select {
						case <-ctx.Done():
							return
						default:
						}

						// Reserve
						reserveIn := QueueInput{
							Op:       OpReserve,
							Queue:    q,
							WorkerID: workerName,
						}
						reserveOpID := recorder.RecordInvocation(clientID, reserveIn)

						client, _, cErr := h.GetRandomActiveClient()
						if cErr != nil {
							recorder.RecordReturn(reserveOpID, QueueOutput{
								Ok:       false,
								TimedOut: true,
							})
							time.Sleep(50 * time.Millisecond)
							continue
						}

						resCtx, resCancel := context.WithTimeout(ctx, 3*time.Second)
						res, err := client.Reserve(resCtx, &relaypb.ReserveRequest{
							Queue:           q,
							WorkerId:        workerName,
							LeaseDurationMs: leaseDurationMs,
						})
						resCancel()

						if err != nil {
							st, _ := status.FromError(err)
							isNotFound := st.Code() == codes.NotFound
							recorder.RecordReturn(reserveOpID, QueueOutput{
								Ok:       false,
								TimedOut: !isNotFound, // true error/timeout
							})
							if isNotFound {
								time.Sleep(50 * time.Millisecond)
							} else {
								time.Sleep(100 * time.Millisecond)
							}
							continue
						}

						// Successfully reserved
						job := res.Job
						token := res.LeaseToken
						recorder.RecordReturn(reserveOpID, QueueOutput{
							Ok:         true,
							Key:        job.IdempotencyKey,
							JobID:      job.Id,
							LeaseToken: token,
						})

						// Ack
						ackIn := QueueInput{
							Op:         OpAck,
							Queue:      q,
							Key:        job.IdempotencyKey,
							JobID:      job.Id,
							LeaseToken: token,
						}
						ackOpID := recorder.RecordInvocation(clientID, ackIn)

						ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
						_, ackErr := client.Ack(ackCtx, &relaypb.AckRequest{
							JobId:      job.Id,
							LeaseToken: token,
						})
						ackCancel()

						recorder.RecordReturn(ackOpID, QueueOutput{
							Ok:       ackErr == nil,
							Key:      job.IdempotencyKey,
							JobID:    job.Id,
							TimedOut: ackErr != nil,
						})
					}
				}(w)
			}

			// 3. Inject Leader Kill Fault mid-workload
			time.Sleep(200 * time.Millisecond)
			killedID, kErr := h.KillLeader()
			if kErr == nil && killedID != "" {
				time.Sleep(300 * time.Millisecond)
				_ = h.RestartNode(killedID)
				_, _ = h.WaitForLeader(10 * time.Second)
			}

			// Wait for producers to finish
			time.Sleep(2 * time.Second)
			cancel() // cancel workers
			wg.Wait()

			opsCount := recorder.OperationsCount()
			t.Logf("Seed %d: Total Porcupine operations recorded: %d", seed, opsCount)
			require.Greater(t, opsCount, 20, "Should have recorded substantial operations")

			// Check linearizability with Porcupine
			res, info := porcupine.CheckOperationsVerbose(QueueModel, recorder.operations, 10*time.Second)
			t.Logf("Seed %d: Porcupine linearizability result: %v (ops: %d)", seed, res, len(recorder.operations))
			_ = info
			require.Equal(t, porcupine.Ok, res, "History must be strictly linearizable under Porcupine model")
		})
	}
}
