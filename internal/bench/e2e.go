package bench

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// E2EBenchConfig defines parameters for B4 end-to-end flow benchmark.
type E2EBenchConfig struct {
	Producers    int
	Workers      int
	RateLimitOps float64 // Optional producer rate limit in jobs/sec; 0 for unthrottled
	PayloadSize  int
	Duration     time.Duration
	Warmup       time.Duration
	Queue        string
}

// RunE2EBenchmark runs concurrent producers and workers executing the full Enqueue -> Reserve -> Ack lifecycle.
func RunE2EBenchmark(target ClusterTarget, cfg E2EBenchConfig) (*RepetitionResult, error) {
	if cfg.Producers <= 0 {
		cfg.Producers = 8
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.PayloadSize < 8 {
		cfg.PayloadSize = 256
	}
	if cfg.Queue == "" {
		cfg.Queue = fmt.Sprintf("bench-e2e-q-%d", time.Now().UnixNano())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 0. Warmup phase: pre-populate queue with initial work so workers have jobs to reserve
	if cfg.Warmup > 0 {
		warmupCtx, warmupCancel := context.WithTimeout(ctx, cfg.Warmup)
		var warmupWg sync.WaitGroup
		for p := 0; p < cfg.Producers; p++ {
			warmupWg.Add(1)
			go func(prodID int) {
				defer warmupWg.Done()
				counter := 0
				payload := make([]byte, cfg.PayloadSize)
				for {
					if warmupCtx.Err() != nil {
						return
					}
					counter++
					client, err := target.GetClient()
					if err != nil {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					idempKey := fmt.Sprintf("warmup-e2e-p%d-%d", prodID, counter)
					reqCtx, reqCancel := context.WithTimeout(warmupCtx, 2*time.Second)
					_, _ = client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
						Queue:          cfg.Queue,
						Payload:        payload,
						Priority:       1,
						IdempotencyKey: idempKey,
						MaxAttempts:    3,
					})
					reqCancel()
					time.Sleep(15 * time.Millisecond)
				}
			}(p)
		}
		warmupWg.Wait()
		warmupCancel()
	}

	var totalEnqueued uint64
	var totalCompleted uint64
	var errCount int64
	var enqCalls, enqErrors uint64
	var resCalls, resErrors uint64
	var ackCalls, ackErrors uint64
	var leaseTimeouts uint64

	var latenciesMu sync.Mutex
	e2eLatencies := make([]time.Duration, 0, 50000)

	measCtx, measCancel := context.WithTimeout(ctx, cfg.Duration)
	defer measCancel()

	startTime := time.Now()

	// Thermal slice tracking
	sliceWindow := 5 * time.Second
	sliceTracker := &sliceAggregator{
		window:   sliceWindow,
		start:    startTime,
		deadline: startTime.Add(cfg.Duration),
	}

	// In-flight timestamps map: job_id -> enqueue time
	var inflightMu sync.RWMutex
	inflight := make(map[string]time.Time)

	// 1. Concurrent Producers
	var prodWg sync.WaitGroup
	for p := 0; p < cfg.Producers; p++ {
		prodWg.Add(1)
		go func(prodID int) {
			defer prodWg.Done()
			counter := 0
			payload := make([]byte, cfg.PayloadSize)
			_, _ = rand.Read(payload)

			var interval time.Duration
			if cfg.RateLimitOps > 0 && cfg.Producers > 0 {
				interval = time.Duration(float64(time.Second) / (cfg.RateLimitOps / float64(cfg.Producers)))
			}

			for {
				if measCtx.Err() != nil {
					return
				}
				if interval > 0 {
					time.Sleep(interval)
				}
				counter++
				client, err := target.GetClient()
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				idempKey := fmt.Sprintf("e2e-p%d-%d-%d", prodID, counter, time.Now().UnixNano())

				now := time.Now()
				binary.BigEndian.PutUint64(payload[:8], uint64(now.UnixNano()))

				atomic.AddUint64(&enqCalls, 1)
				reqCtx, reqCancel := context.WithTimeout(measCtx, 4*time.Second)
				resp, eErr := client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
					Queue:          cfg.Queue,
					Payload:        payload,
					Priority:       1,
					IdempotencyKey: idempKey,
					MaxAttempts:    3,
				})
				reqCancel()

				if eErr != nil {
					atomic.AddUint64(&enqErrors, 1)
					st, _ := status.FromError(eErr)
					if measCtx.Err() != nil || st.Code() == codes.DeadlineExceeded || st.Code() == codes.Canceled {
						return
					}
					atomic.AddInt64(&errCount, 1)
					continue
				}

				if resp != nil && resp.JobId != "" {
					inflightMu.Lock()
					inflight[resp.JobId] = now
					inflightMu.Unlock()
					atomic.AddUint64(&totalEnqueued, 1)
				}
			}
		}(p)
	}

	// 2. Concurrent Workers
	var workWg sync.WaitGroup
	stopWorkers := make(chan struct{})

	for w := 0; w < cfg.Workers; w++ {
		workWg.Add(1)
		go func(workerID int) {
			defer workWg.Done()
			wName := fmt.Sprintf("e2e-worker-%d", workerID)
			localLatencies := make([]time.Duration, 0, 5000)

			for {
				select {
				case <-stopWorkers:
					latenciesMu.Lock()
					e2eLatencies = append(e2eLatencies, localLatencies...)
					latenciesMu.Unlock()
					return
				default:
				}

				client, err := target.GetClient()
				if err != nil {
					time.Sleep(20 * time.Millisecond)
					continue
				}

				atomic.AddUint64(&resCalls, 1)
				resCtx, resCancel := context.WithTimeout(ctx, 3*time.Second)
				resResp, rErr := client.Reserve(resCtx, &relaypb.ReserveRequest{
					Queue:           cfg.Queue,
					WorkerId:        wName,
					LeaseDurationMs: 5000,
				})
				resCancel()

				if rErr != nil {
					st, _ := status.FromError(rErr)
					if st.Code() == codes.DeadlineExceeded {
						atomic.AddUint64(&leaseTimeouts, 1)
					}
					if st.Code() != codes.NotFound {
						atomic.AddUint64(&resErrors, 1)
					}
					time.Sleep(50 * time.Millisecond)
					continue
				}

				if resResp == nil || resResp.Job == nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}

				job := resResp.Job
				leaseToken := resResp.LeaseToken

				// Ack the job
				atomic.AddUint64(&ackCalls, 1)
				ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
				_, aErr := client.Ack(ackCtx, &relaypb.AckRequest{
					JobId:      job.Id,
					LeaseToken: leaseToken,
				})
				ackDone := time.Now()
				ackCancel()

				if aErr != nil {
					st, _ := status.FromError(aErr)
					if st.Code() == codes.DeadlineExceeded {
						atomic.AddUint64(&leaseTimeouts, 1)
					}
					atomic.AddUint64(&ackErrors, 1)
					continue
				}

				atomic.AddUint64(&totalCompleted, 1)
				sliceTracker.recordOp()

				inflightMu.Lock()
				enqTime, ok := inflight[job.Id]
				if ok {
					delete(inflight, job.Id)
				}
				inflightMu.Unlock()

				if ok {
					localLatencies = append(localLatencies, ackDone.Sub(enqTime))
				} else if len(job.Payload) >= 8 {
					nano := int64(binary.BigEndian.Uint64(job.Payload[:8]))
					if nano > 0 {
						localLatencies = append(localLatencies, ackDone.Sub(time.Unix(0, nano)))
					}
				}
			}
		}(w)
	}

	// Wait for measurement duration
	prodWg.Wait()

	// Drain remaining in-flight jobs for a brief window
	drainDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(drainDeadline) {
		if atomic.LoadUint64(&totalCompleted) >= atomic.LoadUint64(&totalEnqueued) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	close(stopWorkers)
	workWg.Wait()

	totalDuration := time.Since(startTime)
	throughput := 0.0
	if totalDuration.Seconds() > 0 {
		throughput = float64(totalCompleted) / totalDuration.Seconds()
	}

	latStats := ComputeLatencyStats(e2eLatencies)
	thermalSlices := sliceTracker.getSlices()

	backlog := int64(totalEnqueued) - int64(totalCompleted)
	if backlog < 0 {
		backlog = 0
	}

	valid := (totalCompleted > 0 && latStats.P50 > 0)
	var invalidReason string
	if !valid {
		invalidReason = fmt.Sprintf("0 jobs completed (enqueued: %d, enq_calls: %d [err: %d], reserve_calls: %d [err: %d], ack_calls: %d [err: %d])",
			totalEnqueued, enqCalls, enqErrors, resCalls, resErrors, ackCalls, ackErrors)
	}

	sanityPass := valid
	sanityNote := fmt.Sprintf("E2E flow: %d completed out of %d enqueued (backlog: %d)", totalCompleted, totalEnqueued, backlog)
	if !valid {
		sanityNote = fmt.Sprintf("E2E flow FAILED: %s", invalidReason)
	}

	return &RepetitionResult{
		Duration:         totalDuration,
		TotalOps:         totalCompleted,
		Throughput:       throughput,
		Latency:          latStats,
		Errors:           int(errCount),
		ThermalSlices:    thermalSlices,
		Backlog:          backlog,
		Producers:        cfg.Producers,
		Workers:          cfg.Workers,
		ReserveCallsMade: resCalls,
		ReserveErrors:    resErrors,
		AckCallsMade:     ackCalls,
		AckErrors:        ackErrors,
		EnqueueCallsMade: enqCalls,
		EnqueueErrors:    enqErrors,
		LeaseTimeouts:    leaseTimeouts,
		Valid:            valid,
		InvalidReason:    invalidReason,
		SanityPass:       sanityPass,
		SanityNote:       sanityNote,
	}, nil
}
