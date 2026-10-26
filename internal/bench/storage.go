package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shubhamkr585/relay/internal/store"
)

// StorageBenchConfig configures a B1 storage engine benchmark run.
type StorageBenchConfig struct {
	Concurrency  int
	BatchWindow  time.Duration
	BatchMaxSize int
	NoSync       bool
	PayloadSize  int
	Duration     time.Duration
	Warmup       time.Duration
}

// RunStorageBenchmark benchmarks the single-node storage engine (BBoltEngine) directly without Raft.
func RunStorageBenchmark(dataDir string, cfg StorageBenchConfig) (*RepetitionResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.PayloadSize <= 0 {
		cfg.PayloadSize = 256
	}

	testDir := filepath.Join(dataDir, fmt.Sprintf("storage_c%d_w%v_m%d_ns%t", cfg.Concurrency, cfg.BatchWindow, cfg.BatchMaxSize, cfg.NoSync))
	if err := os.MkdirAll(testDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage bench dir: %w", err)
	}
	defer os.RemoveAll(testDir)

	dbPath := filepath.Join(testDir, "bench_storage.db")
	storeCfg := store.DefaultConfig(dbPath)
	storeCfg.BatchWindow = cfg.BatchWindow
	storeCfg.BatchMaxSize = cfg.BatchMaxSize
	storeCfg.NoSync = cfg.NoSync

	engine, err := store.NewBBoltEngine(storeCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage engine: %w", err)
	}
	defer engine.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := make([]byte, cfg.PayloadSize)
	_, _ = rand.Read(payload)

	queue := "bench-storage-queue"

	// 1. Warmup phase
	if cfg.Warmup > 0 {
		warmupCtx, warmupCancel := context.WithTimeout(ctx, cfg.Warmup)
		var warmupWg sync.WaitGroup
		for w := 0; w < cfg.Concurrency; w++ {
			warmupWg.Add(1)
			go func(workerID int) {
				defer warmupWg.Done()
				i := 0
				for {
					if warmupCtx.Err() != nil {
						return
					}
					i++
					idempKey := fmt.Sprintf("warmup-w%d-%d", workerID, i)
					_, _, _ = engine.Enqueue(warmupCtx, &store.EnqueueParams{
						Queue:          queue,
						Payload:        payload,
						Priority:       1,
						IdempotencyKey: idempKey,
						MaxAttempts:    3,
					})
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
				idempKey := fmt.Sprintf("meas-w%d-%d-%d", workerID, opCounter, time.Now().UnixNano())

				opStart := NowHighRes()
				_, _, eErr := engine.Enqueue(measCtx, &store.EnqueueParams{
					Queue:          queue,
					Payload:        payload,
					Priority:       1,
					IdempotencyKey: idempKey,
					MaxAttempts:    3,
				})
				opDuration := SinceHighRes(opStart)

				if eErr != nil {
					if measCtx.Err() != nil {
						break
					}
					atomic.AddInt64(&errCount, 1)
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

	// 3. Sanity check: verify all acknowledged enqueues exist in the engine
	stats, sErr := engine.Stats(context.Background(), queue)
	sanityPass := (sErr == nil)
	var sanityNote string
	if sanityPass && stats != nil {
		// Total jobs in queue should be at least totalOps
		if stats.PendingCount >= int64(totalOps) {
			sanityNote = fmt.Sprintf("Sanity check passed: %d jobs pending in storage >= %d acknowledged enqueues", stats.PendingCount, totalOps)
		} else {
			sanityPass = false
			sanityNote = fmt.Sprintf("Sanity check FAILED: %d pending < %d acknowledged", stats.PendingCount, totalOps)
		}
	} else {
		sanityPass = false
		sanityNote = fmt.Sprintf("Sanity check failed to retrieve stats: %v", sErr)
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

	return &RepetitionResult{
		Duration:      totalDuration,
		TotalOps:      totalOps,
		Throughput:    throughput,
		Latency:       latStats,
		Unreliable:    unreliable,
		BatchMean:     batchMean,
		Errors:        int(errCount),
		Timeouts:      int(timeoutCount),
		ThermalSlices: thermalSlices,
		SanityPass:    sanityPass,
		SanityNote:    sanityNote,
	}, nil
}

type sliceAggregator struct {
	mu       sync.Mutex
	window   time.Duration
	start    time.Time
	deadline time.Time
	curStart time.Time
	curOps   int
	slices   []float64
}

func (s *sliceAggregator) recordOp() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curStart.IsZero() {
		s.curStart = time.Now()
	}
	s.curOps++
	if time.Since(s.curStart) >= s.window {
		elapsed := time.Since(s.curStart).Seconds()
		if elapsed > 0 {
			s.slices = append(s.slices, float64(s.curOps)/elapsed)
		}
		s.curStart = time.Now()
		s.curOps = 0
	}
}

func (s *sliceAggregator) getSlices() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curOps > 0 && !s.curStart.IsZero() {
		elapsed := time.Since(s.curStart).Seconds()
		if elapsed > 0 {
			s.slices = append(s.slices, float64(s.curOps)/elapsed)
		}
		s.curOps = 0
	}
	res := make([]float64, len(s.slices))
	copy(res, s.slices)
	return res
}
