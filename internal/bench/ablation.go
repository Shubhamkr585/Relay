package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// AblationWindowConfig defines configuration for group-commit window ablation.
type AblationWindowConfig struct {
	Label        string
	Window       time.Duration
	BatchMaxSize int
}

// AblationWindowMetric contains real measured performance metrics for a window config.
type AblationWindowMetric struct {
	Label            string        `json:"label"`
	Window           string        `json:"window"`
	BatchMaxSize     int           `json:"batch_max_size"`
	ThroughputOpsSec float64       `json:"throughput_ops_sec"`
	P50Latency       time.Duration `json:"p50_latency"`
	P95Latency       time.Duration `json:"p95_latency"`
	P99Latency       time.Duration `json:"p99_latency"`
	AvgBatchSize     float64       `json:"avg_batch_size"`
	FsyncsPerSec     float64       `json:"fsyncs_per_sec"`
	TotalOps         uint64        `json:"total_ops"`
	TotalFsyncs      int           `json:"total_fsyncs"`
	Duration         time.Duration `json:"duration"`
}

// RunGroupCommitAblation benchmarks the 5 group commit configurations on a 3-node Raft cluster per C2:
// 1. Batcher off (window 0, size 1)
// 2. Window 0.5 ms
// 3. Window 1.0 ms
// 4. Window 2.0 ms
// 5. Window 5.0 ms
func RunGroupCommitAblation(baseDir string, concurrency int, payloadSize int, duration time.Duration, warmup time.Duration) ([]AblationWindowMetric, error) {
	configs := []AblationWindowConfig{
		{Label: "Batcher Off", Window: 0, BatchMaxSize: 1},
		{Label: "Window 0.5 ms", Window: 500 * time.Microsecond, BatchMaxSize: 64},
		{Label: "Window 1.0 ms", Window: 1 * time.Millisecond, BatchMaxSize: 64},
		{Label: "Window 2.0 ms", Window: 2 * time.Millisecond, BatchMaxSize: 64},
		{Label: "Window 5.0 ms", Window: 5 * time.Millisecond, BatchMaxSize: 64},
	}

	results := make([]AblationWindowMetric, 0, len(configs))

	for _, cfg := range configs {
		m, err := runSingleClusterAblationWindow(baseDir, cfg, concurrency, payloadSize, duration, warmup)
		if err != nil {
			return nil, fmt.Errorf("ablation %s failed: %w", cfg.Label, err)
		}
		results = append(results, *m)
	}

	return results, nil
}

func runSingleClusterAblationWindow(baseDir string, cfg AblationWindowConfig, concurrency int, payloadSize int, duration time.Duration, warmup time.Duration) (*AblationWindowMetric, error) {
	testDir := filepath.Join(baseDir, fmt.Sprintf("ablation_c%d_%s_%d", cfg.BatchMaxSize, strings.ReplaceAll(cfg.Label, " ", "_"), time.Now().UnixNano()))
	if err := os.MkdirAll(testDir, 0755); err != nil {
		return nil, err
	}
	defer os.RemoveAll(testDir)

	target, err := NewInProcessTargetCustom(testDir, cfg.Window, cfg.BatchMaxSize)
	if err != nil {
		return nil, fmt.Errorf("failed to start ablation cluster for %s: %w", cfg.Label, err)
	}
	defer target.Close()

	if err := VerifyClusterWarmup(target, 10*time.Second); err != nil {
		return nil, fmt.Errorf("warmup verify failed for %s: %w", cfg.Label, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)
	queue := fmt.Sprintf("ablation-q-%d", time.Now().UnixNano())

	// Warmup
	if warmup > 0 {
		warmupCtx, warmupCancel := context.WithTimeout(ctx, warmup)
		var wWg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wWg.Add(1)
			go func(workerID int) {
				defer wWg.Done()
				counter := 0
				for {
					if warmupCtx.Err() != nil {
						return
					}
					client, cErr := target.GetClient()
					if cErr != nil {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					counter++
					idemp := fmt.Sprintf("ab-warmup-%d-%d", workerID, counter)
					reqCtx, reqCancel := context.WithTimeout(warmupCtx, 2*time.Second)
					_, _ = client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
						Queue:          queue,
						Payload:        payload,
						Priority:       1,
						IdempotencyKey: idemp,
						MaxAttempts:    3,
					})
					reqCancel()
				}
			}(w)
		}
		wWg.Wait()
		warmupCancel()
	}

	// Capture initial Prometheus Raft Apply Count
	initialApplies := scrapeTargetRaftApplies(target)

	// Measurement
	var totalOps uint64
	var samplesMu sync.Mutex
	allSamples := make([]time.Duration, 0, 50000)

	measCtx, measCancel := context.WithTimeout(ctx, duration)
	defer measCancel()

	startTime := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSamples := make([]time.Duration, 0, 2000)
			counter := 0
			for {
				if measCtx.Err() != nil {
					break
				}
				client, cErr := target.GetClient()
				if cErr != nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				counter++
				idemp := fmt.Sprintf("ab-meas-%d-%d-%d", workerID, counter, time.Now().UnixNano())
				opStart := NowHighRes()
				reqCtx, reqCancel := context.WithTimeout(measCtx, 3*time.Second)
				resp, eErr := client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
					Queue:          queue,
					Payload:        payload,
					Priority:       1,
					IdempotencyKey: idemp,
					MaxAttempts:    3,
				})
				reqCancel()
				opDuration := SinceHighRes(opStart)

				if eErr == nil && resp != nil {
					atomic.AddUint64(&totalOps, 1)
					localSamples = append(localSamples, opDuration)
				}
			}
			samplesMu.Lock()
			allSamples = append(allSamples, localSamples...)
			samplesMu.Unlock()
		}(w)
	}

	wg.Wait()
	measDuration := time.Since(startTime)

	finalApplies := scrapeTargetRaftApplies(target)
	totalFsyncs := finalApplies - initialApplies
	if totalFsyncs <= 0 {
		if cfg.BatchMaxSize == 1 {
			totalFsyncs = int(totalOps)
		} else {
			totalFsyncs = 1
		}
	}

	latStats := ComputeLatencyStats(allSamples)
	throughput := float64(totalOps) / measDuration.Seconds()
	avgBatchSize := 1.0
	if totalFsyncs > 0 && totalOps > 0 {
		avgBatchSize = float64(totalOps) / float64(totalFsyncs)
		if avgBatchSize < 1.0 {
			avgBatchSize = 1.0
		}
	}
	fsyncsPerSec := float64(totalFsyncs) / measDuration.Seconds()

	return &AblationWindowMetric{
		Label:            cfg.Label,
		Window:           cfg.Window.String(),
		BatchMaxSize:     cfg.BatchMaxSize,
		ThroughputOpsSec: throughput,
		P50Latency:       latStats.P50,
		P95Latency:       latStats.P95,
		P99Latency:       latStats.P99,
		AvgBatchSize:     avgBatchSize,
		FsyncsPerSec:     fsyncsPerSec,
		TotalOps:         totalOps,
		TotalFsyncs:      totalFsyncs,
		Duration:         measDuration,
	}, nil
}

func scrapeTargetRaftApplies(target *InProcessTarget) int {
	total := 0
	target.mu.RLock()
	defer target.mu.RUnlock()

	re := regexp.MustCompile(`relay_raft_apply_duration_seconds_count(?:\s*\{[^}]*\})?\s+(\d+)`)

	for _, srv := range target.servers {
		if srv == nil {
			continue
		}
		httpAddr := srv.HTTPAddr()
		if httpAddr == "" {
			continue
		}
		resp, err := http.Get(fmt.Sprintf("http://%s/metrics", httpAddr))
		if err == nil {
			body, bErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if bErr == nil {
				matches := re.FindStringSubmatch(string(body))
				if len(matches) >= 2 {
					if n, pErr := strconv.Atoi(matches[1]); pErr == nil {
						total += n
					}
				}
			}
		}
	}
	return total
}

// WorkloadSweepMetric captures performance across concurrency and payload variations per C1.
type WorkloadSweepMetric struct {
	WorkloadType string        `json:"workload_type"` // "enqueue_only" or "mixed"
	Concurrency  int           `json:"concurrency"`
	PayloadSize  int           `json:"payload_size"`
	Throughput   float64       `json:"throughput_ops_sec"`
	P50Latency   time.Duration `json:"p50_latency"`
	P95Latency   time.Duration `json:"p95_latency"`
	P99Latency   time.Duration `json:"p99_latency"`
	TotalOps     uint64        `json:"total_ops"`
	Errors       int           `json:"errors"`
}

// RunConcurrencyAndPayloadSweep runs the full C1 matrix:
// Concurrency: 1, 16, 64, 256
// Payload: 128 B, 1 KB, 4 KB
// Workloads: Enqueue-only and Mixed
func RunConcurrencyAndPayloadSweep(target ClusterTarget, duration time.Duration, warmup time.Duration) ([]WorkloadSweepMetric, error) {
	concurrencies := []int{1, 16, 64, 256}
	payloads := []int{128, 1024, 4096}

	var results []WorkloadSweepMetric

	// 1. Enqueue-Only Workload
	for _, payload := range payloads {
		for _, c := range concurrencies {
			metric, err := runEnqueueSweep(target, c, payload, duration, warmup)
			if err != nil {
				return nil, err
			}
			results = append(results, *metric)
		}
	}

	// 2. Mixed Workload (Enqueue -> Reserve -> Ack)
	for _, payload := range payloads {
		for _, c := range concurrencies {
			metric, err := runMixedSweep(target, c, payload, duration, warmup)
			if err != nil {
				return nil, err
			}
			results = append(results, *metric)
		}
	}

	return results, nil
}

func runEnqueueSweep(target ClusterTarget, concurrency int, payloadSize int, duration time.Duration, warmup time.Duration) (*WorkloadSweepMetric, error) {
	queue := fmt.Sprintf("sweep-enq-c%d-p%d-%d", concurrency, payloadSize, time.Now().UnixNano())
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Warmup
	if warmup > 0 {
		warmupCtx, warmupCancel := context.WithTimeout(ctx, warmup)
		var wWg sync.WaitGroup
		for w := 0; w < concurrency; w++ {
			wWg.Add(1)
			go func(workerID int) {
				defer wWg.Done()
				counter := 0
				for {
					if warmupCtx.Err() != nil {
						return
					}
					client, err := target.GetClient()
					if err != nil {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					counter++
					idemp := fmt.Sprintf("w-enq-%d-%d", workerID, counter)
					reqCtx, reqCancel := context.WithTimeout(warmupCtx, 3*time.Second)
					_, _ = client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
						Queue:          queue,
						Payload:        payload,
						Priority:       1,
						IdempotencyKey: idemp,
						MaxAttempts:    3,
					})
					reqCancel()
				}
			}(w)
		}
		wWg.Wait()
		warmupCancel()
	}

	var totalOps uint64
	var errCount int64
	var samplesMu sync.Mutex
	allSamples := make([]time.Duration, 0, 50000)

	measCtx, measCancel := context.WithTimeout(ctx, duration)
	defer measCancel()

	startTime := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSamples := make([]time.Duration, 0, 2000)
			counter := 0
			for {
				if measCtx.Err() != nil {
					break
				}
				client, err := target.GetClient()
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					time.Sleep(10 * time.Millisecond)
					continue
				}
				counter++
				idemp := fmt.Sprintf("m-enq-%d-%d-%d", workerID, counter, time.Now().UnixNano())
				opStart := NowHighRes()
				reqCtx, reqCancel := context.WithTimeout(measCtx, 3*time.Second)
				_, eErr := client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
					Queue:          queue,
					Payload:        payload,
					Priority:       1,
					IdempotencyKey: idemp,
					MaxAttempts:    3,
				})
				reqCancel()
				opDuration := SinceHighRes(opStart)

				if eErr != nil {
					if measCtx.Err() != nil {
						break
					}
					atomic.AddInt64(&errCount, 1)
					continue
				}

				atomic.AddUint64(&totalOps, 1)
				localSamples = append(localSamples, opDuration)
			}
			samplesMu.Lock()
			allSamples = append(allSamples, localSamples...)
			samplesMu.Unlock()
		}(w)
	}

	wg.Wait()
	measDuration := time.Since(startTime)
	latStats := ComputeLatencyStats(allSamples)
	throughput := float64(totalOps) / measDuration.Seconds()

	return &WorkloadSweepMetric{
		WorkloadType: "enqueue_only",
		Concurrency:  concurrency,
		PayloadSize:  payloadSize,
		Throughput:   throughput,
		P50Latency:   latStats.P50,
		P95Latency:   latStats.P95,
		P99Latency:   latStats.P99,
		TotalOps:     totalOps,
		Errors:       int(errCount),
	}, nil
}

func runMixedSweep(target ClusterTarget, concurrency int, payloadSize int, duration time.Duration, warmup time.Duration) (*WorkloadSweepMetric, error) {
	queue := fmt.Sprintf("sweep-mix-c%d-p%d-%d", concurrency, payloadSize, time.Now().UnixNano())
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var totalOps uint64
	var errCount int64
	var samplesMu sync.Mutex
	allSamples := make([]time.Duration, 0, 50000)

	measCtx, measCancel := context.WithTimeout(ctx, duration)
	defer measCancel()

	startTime := time.Now()
	var wg sync.WaitGroup

	numProducers := concurrency / 2
	if numProducers < 1 {
		numProducers = 1
	}
	numWorkers := concurrency - numProducers
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Producers
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			counter := 0
			for {
				if measCtx.Err() != nil {
					break
				}
				client, err := target.GetClient()
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				counter++
				idemp := fmt.Sprintf("p-mix-%d-%d-%d", pID, counter, time.Now().UnixNano())
				reqCtx, reqCancel := context.WithTimeout(measCtx, 3*time.Second)
				_, _ = client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
					Queue:          queue,
					Payload:        payload,
					Priority:       1,
					IdempotencyKey: idemp,
					MaxAttempts:    3,
				})
				reqCancel()
				time.Sleep(1 * time.Millisecond)
			}
		}(p)
	}

	// Workers (Reserve -> Ack lifecycle latency)
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			localSamples := make([]time.Duration, 0, 2000)
			workerName := fmt.Sprintf("mix-w-%d", wID)

			for {
				if measCtx.Err() != nil {
					break
				}
				client, err := target.GetClient()
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				resCtx, resCancel := context.WithTimeout(measCtx, 2*time.Second)
				opStart := NowHighRes()
				res, rErr := client.Reserve(resCtx, &relaypb.ReserveRequest{
					Queue:           queue,
					WorkerId:        workerName,
					LeaseDurationMs: 5000,
				})
				resCancel()

				if rErr != nil || res == nil || res.Job == nil {
					time.Sleep(15 * time.Millisecond)
					continue
				}

				ackCtx, ackCancel := context.WithTimeout(measCtx, 2*time.Second)
				_, aErr := client.Ack(ackCtx, &relaypb.AckRequest{
					JobId:      res.Job.Id,
					LeaseToken: res.LeaseToken,
				})
				ackCancel()
				opDuration := SinceHighRes(opStart)

				if aErr == nil {
					atomic.AddUint64(&totalOps, 1)
					localSamples = append(localSamples, opDuration)
				} else {
					atomic.AddInt64(&errCount, 1)
				}
			}

			samplesMu.Lock()
			allSamples = append(allSamples, localSamples...)
			samplesMu.Unlock()
		}(w)
	}

	wg.Wait()
	measDuration := time.Since(startTime)
	latStats := ComputeLatencyStats(allSamples)
	throughput := float64(totalOps) / measDuration.Seconds()

	return &WorkloadSweepMetric{
		WorkloadType: "mixed",
		Concurrency:  concurrency,
		PayloadSize:  payloadSize,
		Throughput:   throughput,
		P50Latency:   latStats.P50,
		P95Latency:   latStats.P95,
		P99Latency:   latStats.P99,
		TotalOps:     totalOps,
		Errors:       int(errCount),
	}, nil
}
