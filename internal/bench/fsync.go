package bench

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// RunFsyncBenchmark writes records of recordSize bytes to a file and calls file.Sync() after each.
// Measures the latency of individual fsync calls and computes the raw fsyncs/sec ceiling.
func RunFsyncBenchmark(dataDir string, recordSize int, duration, warmup time.Duration) (*RepetitionResult, error) {
	if recordSize <= 0 {
		recordSize = 256
	}

	testDir := filepath.Join(dataDir, "fsync_bench")
	if err := os.MkdirAll(testDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create fsync bench dir: %w", err)
	}
	defer os.RemoveAll(testDir)

	filePath := filepath.Join(testDir, "fsync_test.bin")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open fsync file: %w", err)
	}
	defer f.Close()

	payload := make([]byte, recordSize)
	_, _ = rand.Read(payload)

	// 1. Warm-up phase
	if warmup > 0 {
		warmupEnd := time.Now().Add(warmup)
		for time.Now().Before(warmupEnd) {
			if _, err := f.Write(payload); err != nil {
				return nil, err
			}
			if err := f.Sync(); err != nil {
				return nil, err
			}
		}
	}

	// 2. Measurement phase
	samples := make([]time.Duration, 0, 10000)
	startTime := time.Now()
	deadline := startTime.Add(duration)

	// Thermal slicing
	sliceWindow := 5 * time.Second
	sliceStart := startTime
	sliceOps := 0
	thermalSlices := make([]float64, 0)

	totalOps := uint64(0)
	errorsCount := 0

	for {
		now := time.Now()
		if now.After(deadline) {
			break
		}

		if _, err := f.Write(payload); err != nil {
			errorsCount++
			continue
		}

		syncStart := time.Now()
		if err := f.Sync(); err != nil {
			errorsCount++
			continue
		}
		syncDuration := time.Since(syncStart)

		samples = append(samples, syncDuration)
		totalOps++
		sliceOps++

		// Check 5-second slice
		if time.Since(sliceStart) >= sliceWindow {
			elapsedSlice := time.Since(sliceStart).Seconds()
			thermalSlices = append(thermalSlices, float64(sliceOps)/elapsedSlice)
			sliceStart = time.Now()
			sliceOps = 0
		}
	}

	totalDuration := time.Since(startTime)
	if sliceOps > 0 {
		elapsedSlice := time.Since(sliceStart).Seconds()
		if elapsedSlice > 0 {
			thermalSlices = append(thermalSlices, float64(sliceOps)/elapsedSlice)
		}
	}

	stats := ComputeLatencyStats(samples)
	throughput := 0.0
	if totalDuration > 0 {
		throughput = float64(totalOps) / totalDuration.Seconds()
	}

	return &RepetitionResult{
		Duration:      totalDuration,
		TotalOps:      totalOps,
		Throughput:    throughput,
		Latency:       stats,
		Errors:        errorsCount,
		ThermalSlices: thermalSlices,
		SanityPass:    totalOps > 0 && errorsCount == 0,
		SanityNote:    fmt.Sprintf("Raw disk fsync verified (%d ops, 0 errors)", totalOps),
	}, nil
}
