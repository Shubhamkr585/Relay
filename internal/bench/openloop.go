package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// OpenLoopConfig defines parameters for B3 open-loop benchmark.
type OpenLoopConfig struct {
	TargetRate  float64 // requests per second
	Duration    time.Duration
	Warmup      time.Duration
	PayloadSize int
	Queue       string
}

// ScheduledTask represents an individual open-loop task on a fixed timeline.
type ScheduledTask struct {
	Index         int
	ScheduledTime time.Time
}

// RunOpenLoopBenchmark schedules requests on a strict fixed timeline and measures latency from the scheduled send time.
func RunOpenLoopBenchmark(target ClusterTarget, cfg OpenLoopConfig) (*RepetitionResult, error) {
	if cfg.TargetRate <= 0 {
		return nil, fmt.Errorf("target rate must be positive, got %f", cfg.TargetRate)
	}
	if cfg.PayloadSize <= 0 {
		cfg.PayloadSize = 256
	}
	if cfg.Queue == "" {
		cfg.Queue = fmt.Sprintf("bench-openloop-q-%d", time.Now().UnixNano())
	}

	payload := make([]byte, cfg.PayloadSize)
	_, _ = rand.Read(payload)

	interval := time.Duration(float64(time.Second) / cfg.TargetRate)
	if interval <= 0 {
		interval = time.Microsecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Warm-up
	if cfg.Warmup > 0 {
		warmupCtx, warmupCancel := context.WithTimeout(ctx, cfg.Warmup)
		taskCh := make(chan ScheduledTask, 1000)
		var warmupWg sync.WaitGroup
		poolSize := 64
		for i := 0; i < poolSize; i++ {
			warmupWg.Add(1)
			go func(workerID int) {
				defer warmupWg.Done()
				for task := range taskCh {
					client, err := target.GetClient()
					if err != nil {
						continue
					}
					idempKey := fmt.Sprintf("warmup-ol-%d-%d", workerID, task.Index)
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
			}(i)
		}

		wStart := time.Now()
		wIdx := 0
		for time.Now().Before(wStart.Add(cfg.Warmup)) {
			wIdx++
			select {
			case taskCh <- ScheduledTask{Index: wIdx, ScheduledTime: time.Now()}:
			default:
			}
			time.Sleep(interval)
		}
		close(taskCh)
		warmupWg.Wait()
		warmupCancel()
	}

	// 2. Measurement phase: scheduled on a strict fixed timeline
	measCtx, measCancel := context.WithTimeout(ctx, cfg.Duration)
	defer measCancel()

	taskCh := make(chan ScheduledTask, 50000)
	var samplesMu sync.Mutex
	allSamples := make([]time.Duration, 0, 50000)

	var totalOps uint64
	var errCount int64

	sliceTracker := &sliceAggregator{
		window:   5 * time.Second,
		start:    time.Now(),
		deadline: time.Now().Add(cfg.Duration),
	}

	// Worker pool to handle in-flight requests concurrently without stalling scheduler
	workerPoolSize := 128
	var workerWg sync.WaitGroup

	for w := 0; w < workerPoolSize; w++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			localSamples := make([]time.Duration, 0, 2000)

			for task := range taskCh {
				client, err := target.GetClient()
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}

				idempKey := fmt.Sprintf("meas-ol-%d-%d-%d", workerID, task.Index, time.Now().UnixNano())
				reqCtx, reqCancel := context.WithTimeout(measCtx, 5*time.Second)
				_, eErr := client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
					Queue:          cfg.Queue,
					Payload:        payload,
					Priority:       1,
					IdempotencyKey: idempKey,
					MaxAttempts:    3,
				})
				doneTime := time.Now()
				reqCancel()

				if eErr != nil {
					if measCtx.Err() != nil {
						break
					}
					atomic.AddInt64(&errCount, 1)
					continue
				}

				// CRITICAL CO-ORDINATED OMISSION RULE:
				// Latency is measured strictly from task.ScheduledTime (NOT actual send time)!
				latencyFromScheduled := doneTime.Sub(task.ScheduledTime)
				localSamples = append(localSamples, latencyFromScheduled)
				atomic.AddUint64(&totalOps, 1)
				sliceTracker.recordOp()
			}

			samplesMu.Lock()
			allSamples = append(allSamples, localSamples...)
			samplesMu.Unlock()
		}(w)
	}

	// Dispatcher loop: follows fixed timeline
	startTime := time.Now()
	deadline := startTime.Add(cfg.Duration)
	opIndex := 0

DispatchLoop:
	for {
		opIndex++
		schedTime := startTime.Add(time.Duration(opIndex) * interval)
		now := time.Now()
		if now.After(deadline) || measCtx.Err() != nil {
			break
		}

		if schedTime.After(now) {
			time.Sleep(schedTime.Sub(now))
		}

		select {
		case taskCh <- ScheduledTask{Index: opIndex, ScheduledTime: schedTime}:
		case <-measCtx.Done():
			break DispatchLoop
		default:
			// Buffer full means backpressure; record dropped scheduled task as error
			atomic.AddInt64(&errCount, 1)
		}
	}

	close(taskCh)
	workerWg.Wait()
	totalDuration := time.Since(startTime)

	throughput := 0.0
	if totalDuration.Seconds() > 0 {
		throughput = float64(totalOps) / totalDuration.Seconds()
	}

	latStats := ComputeLatencyStats(allSamples)

	return &RepetitionResult{
		Duration:      totalDuration,
		TotalOps:      totalOps,
		Throughput:    throughput,
		Latency:       latStats,
		Errors:        int(errCount),
		ThermalSlices: sliceTracker.getSlices(),
		SanityPass:    totalOps > 0,
		SanityNote:    fmt.Sprintf("Open-loop rate %.0f req/s achieved %.0f ops/sec (0 coordinated omission)", cfg.TargetRate, throughput),
	}, nil
}
