package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// ClusterBenchConfig defines parameters for a B2 cluster benchmark repetition.
type ClusterBenchConfig struct {
	Concurrency int
	PayloadSize int
	Duration    time.Duration
	Warmup      time.Duration
	Queue       string
}

// RunClusterBenchmark executes closed-loop Enqueue load against a 3-node cluster target.
func RunClusterBenchmark(target ClusterTarget, cfg ClusterBenchConfig) (*RepetitionResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.PayloadSize <= 0 {
		cfg.PayloadSize = 256
	}
	if cfg.Queue == "" {
		cfg.Queue = fmt.Sprintf("bench-cluster-q-%d", time.Now().UnixNano())
	}

	payload := make([]byte, cfg.PayloadSize)
	_, _ = rand.Read(payload)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Warm-up phase
	if cfg.Warmup > 0 {
		warmupCtx, warmupCancel := context.WithTimeout(ctx, cfg.Warmup)
		var warmupWg sync.WaitGroup
		for w := 0; w < cfg.Concurrency; w++ {
			warmupWg.Add(1)
			go func(workerID int) {
				defer warmupWg.Done()
				counter := 0
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
					idempKey := fmt.Sprintf("warmup-c-w%d-%d", workerID, counter)
					reqCtx, reqCancel := context.WithTimeout(warmupCtx, 3*time.Second)
					_, _ = client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
						Queue:          cfg.Queue,
						Payload:        payload,
						Priority:       1,
						IdempotencyKey: idempKey,
						MaxAttempts:    3,
					})
					reqCancel()
				}
			}(w)
		}
		warmupWg.Wait()
		warmupCancel()
	}

	// 2. Measurement phase
	var totalOps uint64
	var errCount int64
	var timeoutCount int64

	var samplesMu sync.Mutex
	allSamples := make([]time.Duration, 0, 100000)

	measCtx, measCancel := context.WithTimeout(ctx, cfg.Duration)
	defer measCancel()

	startTime := time.Now()

	// Thermal slicing
	sliceWindow := 5 * time.Second
	sliceTracker := &sliceAggregator{
		window:   sliceWindow,
		start:    startTime,
		deadline: startTime.Add(cfg.Duration),
	}

	var errBreakdownMu sync.Mutex
	errBreakdown := make(map[string]int)

	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSamples := make([]time.Duration, 0, 10000)
			opCounter := 0

			for {
				if measCtx.Err() != nil {
					break
				}
				opCounter++
				idempKey := fmt.Sprintf("meas-c-w%d-%d-%d", workerID, opCounter, time.Now().UnixNano())

				client, err := target.GetClient()
				if err != nil {
					errBreakdownMu.Lock()
					errBreakdown["ClientUnavailable"]++
					errBreakdownMu.Unlock()
					atomic.AddInt64(&errCount, 1)
					time.Sleep(10 * time.Millisecond)
					continue
				}

				reqCtx, reqCancel := context.WithTimeout(measCtx, 5*time.Second)
				opStart := NowHighRes()
				_, eErr := client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
					Queue:          cfg.Queue,
					Payload:        payload,
					Priority:       1,
					IdempotencyKey: idempKey,
					MaxAttempts:    3,
				})
				opDuration := SinceHighRes(opStart)
				reqCancel()

				if eErr != nil {
					if measCtx.Err() != nil {
						// Measurement context completed, normal termination
						break
					}
					st, _ := status.FromError(eErr)
					codeStr := st.Code().String()
					if codeStr == "Unknown" && reqCtx.Err() == context.DeadlineExceeded {
						codeStr = "DeadlineExceeded"
					}

					errBreakdownMu.Lock()
					errBreakdown[codeStr]++
					errBreakdownMu.Unlock()

					if st.Code() == codes.DeadlineExceeded || reqCtx.Err() == context.DeadlineExceeded {
						atomic.AddInt64(&timeoutCount, 1)
					} else {
						atomic.AddInt64(&errCount, 1)
					}
					continue
				}

				localSamples = append(localSamples, opDuration)
				atomic.AddUint64(&totalOps, 1)
				sliceTracker.recordOp()
			}

			samplesMu.Lock()
			allSamples = append(allSamples, localSamples...)
			samplesMu.Unlock()
		}(w)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// 3. Sanity check: verified acknowledged enqueues == jobs present in cluster
	statsCtx, statsCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer statsCancel()
	stats, sErr := target.GetStats(statsCtx, cfg.Queue)

	sanityPass := (sErr == nil)
	var sanityNote string
	if sanityPass && stats != nil {
		totalClusterJobs := stats.PendingCount + stats.LeasedCount + stats.DoneCount + stats.DeadCount
		if totalClusterJobs >= int64(totalOps) {
			sanityNote = fmt.Sprintf("Sanity check passed: %d jobs in cluster >= %d acknowledged enqueues", totalClusterJobs, totalOps)
		} else {
			sanityPass = false
			sanityNote = fmt.Sprintf("Sanity check FAILED: %d cluster jobs < %d acknowledged enqueues", totalClusterJobs, totalOps)
		}
	} else {
		sanityPass = false
		sanityNote = fmt.Sprintf("Sanity check failed to retrieve stats from cluster: %v", sErr)
	}

	throughput := 0.0
	if totalDuration.Seconds() > 0 {
		throughput = float64(totalOps) / totalDuration.Seconds()
	}

	latStats := ComputeLatencyStats(allSamples)
	thermalSlices := sliceTracker.getSlices()

	var batchMean time.Duration
	if totalOps > 0 && totalDuration > 0 {
		batchMean = totalDuration / time.Duration(totalOps)
	}
	clockRes := GetMeasuredClockResolution()
	unreliable := (latStats.P50 < 10*clockRes)

	valid := (totalOps > 0)
	var invalidReason string
	if !valid {
		invalidReason = fmt.Sprintf("0 successful enqueues completed (errors: %d, timeouts: %d)", errCount, timeoutCount)
	}

	return &RepetitionResult{
		Duration:       totalDuration,
		TotalOps:       totalOps,
		Throughput:     throughput,
		Latency:        latStats,
		Unreliable:     unreliable,
		BatchMean:      batchMean,
		Errors:         int(errCount),
		Timeouts:       int(timeoutCount),
		ErrorBreakdown: errBreakdown,
		Valid:          valid,
		InvalidReason:  invalidReason,
		ThermalSlices:  thermalSlices,
		SanityPass:     sanityPass,
		SanityNote:     sanityNote,
	}, nil
}
