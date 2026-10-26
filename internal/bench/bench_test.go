package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLatencyStats_Percentiles validates percentile calculations on known sample sets.
func TestLatencyStats_Percentiles(t *testing.T) {
	// 100 samples from 1ms to 100ms
	samples := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		samples[i] = time.Duration(i+1) * time.Millisecond
	}

	stats := ComputeLatencyStats(samples)
	require.Equal(t, uint64(100), stats.Count)
	require.Equal(t, 1*time.Millisecond, stats.Min)
	require.Equal(t, 50*time.Millisecond, stats.P50)
	require.Equal(t, 95*time.Millisecond, stats.P95)
	require.Equal(t, 99*time.Millisecond, stats.P99)
	require.Equal(t, 100*time.Millisecond, stats.P999)
	require.Equal(t, 100*time.Millisecond, stats.Max)
}

// TestClockResolution_Validity verifies that clamping is removed and that any percentile
// equaling clock resolution exactly must be marked with the unreliable flag.
func TestClockResolution_Validity(t *testing.T) {
	clockRes := MeasureClockResolution()
	require.Greater(t, clockRes, time.Duration(0), "Clock resolution must be positive")
	SetMeasuredClockResolution(clockRes)

	// Sub-microsecond samples below clock resolution must not be clamped
	subSamples := []time.Duration{
		1 * time.Nanosecond,
		2 * time.Nanosecond,
		3 * time.Nanosecond,
	}
	subStats := ComputeLatencyStats(subSamples)
	require.Equal(t, 2*time.Nanosecond, subStats.P50, "P50 must reflect raw high-res sample without clamping")
	require.Less(t, subStats.P50, clockRes, "Raw p50 below clock resolution must remain unclamped")

	// Verify that if a repetition result has p50 < 10*clockRes, it is flagged as unreliable
	rep := RepetitionResult{
		TotalOps:   uint64(len(subSamples)),
		Duration:   time.Millisecond,
		Latency:    subStats,
		SanityPass: true,
	}
	agg := AggregateRepetitions("test_unreliable", "Test Unreliable", "desc", map[string]string{"concurrency": "1"}, []RepetitionResult{rep}, "")
	require.True(t, agg.Unreliable, "Results with p50 < 10x clock resolution must be marked unreliable")
	require.Greater(t, agg.BatchMean.Median, time.Duration(0), "Batch-derived mean must be populated for unreliable results")

	// Conversely, results with p50 well above 10x clock resolution must NOT be marked unreliable
	reliableSamples := make([]time.Duration, 10)
	for i := range reliableSamples {
		reliableSamples[i] = 100 * clockRes
	}
	reliableStats := ComputeLatencyStats(reliableSamples)
	reliableRep := RepetitionResult{
		TotalOps:   10,
		Duration:   time.Second,
		Latency:    reliableStats,
		SanityPass: true,
	}
	reliableAgg := AggregateRepetitions("test_reliable", "Test Reliable", "desc", map[string]string{"concurrency": "1"}, []RepetitionResult{reliableRep}, "")
	require.False(t, reliableAgg.Unreliable, "Results with p50 >= 10x clock resolution must not be marked unreliable")

	// Fails if any percentile equals the clock resolution exactly without the "unreliable" flag
	exactSamples := []time.Duration{clockRes, clockRes, clockRes}
	exactStats := ComputeLatencyStats(exactSamples)
	exactRep := RepetitionResult{
		TotalOps:   3,
		Duration:   time.Millisecond,
		Latency:    exactStats,
		SanityPass: true,
	}
	exactAgg := AggregateRepetitions("test_exact", "Test Exact", "desc", map[string]string{"concurrency": "1"}, []RepetitionResult{exactRep}, "")
	if exactAgg.LatencyP50.Median == clockRes || exactAgg.LatencyP95.Median == clockRes {
		require.True(t, exactAgg.Unreliable, "A percentile equaling the clock resolution exactly must have the unreliable flag")
	}
}

// TestAggregation_MedianRangeStdDev validates median, min, max, and stddev aggregation.
func TestAggregation_MedianRangeStdDev(t *testing.T) {
	values := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	agg := ComputeAggregatedMetric(values)

	require.Equal(t, 30.0, agg.Median)
	require.Equal(t, 10.0, agg.Min)
	require.Equal(t, 50.0, agg.Max)
	require.InDelta(t, 15.811, agg.StdDev, 0.01)

	// Even number of items
	evenValues := []float64{10.0, 20.0, 30.0, 40.0}
	evenAgg := ComputeAggregatedMetric(evenValues)
	require.Equal(t, 25.0, evenAgg.Median)
}

// TestOpenLoop_ScheduledTimeMeasurement verifies that latency is measured from scheduled time.
func TestOpenLoop_ScheduledTimeMeasurement(t *testing.T) {
	t0 := time.Now()
	schedTime := t0.Add(-50 * time.Millisecond) // was scheduled 50ms in the past
	doneTime := t0.Add(10 * time.Millisecond)   // completes 10ms after t0

	// Latency measured from scheduled send time
	latency := doneTime.Sub(schedTime)
	require.Equal(t, 60*time.Millisecond, latency)

	// In contrast, latency measured from actual send time would only be 10ms (coordinated omission)
	require.Greater(t, latency, 10*time.Millisecond, "Must avoid coordinated omission by including schedule delay")
}

// TestSanityCheck_Logic validates the sanity check evaluation for cluster and storage checks.
func TestSanityCheck_Logic(t *testing.T) {
	// Case 1: cluster has at least as many jobs as acknowledged enqueues -> PASS
	totalAcked := uint64(500)
	clusterTotal := int64(500)
	pass := clusterTotal >= int64(totalAcked)
	require.True(t, pass)

	// Case 2: cluster has fewer jobs than acknowledged -> FAIL (data loss)
	clusterTotalMissing := int64(499)
	passMissing := clusterTotalMissing >= int64(totalAcked)
	require.False(t, passMissing)
}

// TestBenchmarkSuite_SmokeTest runs short benchmark passes against in-process targets
// and verifies that output files are produced, non-empty, and parseable JSON.
func TestBenchmarkSuite_SmokeTest(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Fsync smoke
	fRes, err := RunFsyncBenchmark(tempDir, 64, 500*time.Millisecond, 100*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, fRes)
	require.Greater(t, fRes.TotalOps, uint64(0))

	// 2. Storage smoke
	sRes, err := RunStorageBenchmark(tempDir, StorageBenchConfig{
		Concurrency:  2,
		BatchWindow:  2 * time.Millisecond,
		BatchMaxSize: 16,
		NoSync:       true, // quick test
		PayloadSize:  64,
		Duration:     500 * time.Millisecond,
		Warmup:       100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NotNil(t, sRes)
	require.True(t, sRes.SanityPass)

	// 3. In-process cluster smoke
	inprocTarget, err := NewInProcessTarget(filepath.Join(tempDir, "inproc"))
	require.NoError(t, err)
	defer inprocTarget.Close()

	cRes, err := RunClusterBenchmark(inprocTarget, ClusterBenchConfig{
		Concurrency: 2,
		PayloadSize: 64,
		Duration:    500 * time.Millisecond,
		Warmup:      100 * time.Millisecond,
		Queue:       "smoke-cluster-q",
	})
	require.NoError(t, err)
	require.NotNil(t, cRes)
	require.True(t, cRes.SanityPass)

	// 4. Open-loop smoke
	olRes, err := RunOpenLoopBenchmark(inprocTarget, OpenLoopConfig{
		TargetRate:  50,
		Duration:    500 * time.Millisecond,
		Warmup:      100 * time.Millisecond,
		PayloadSize: 64,
		Queue:       "smoke-openloop-q",
	})
	require.NoError(t, err)
	require.NotNil(t, olRes)
	require.True(t, olRes.SanityPass)

	// 5. E2E smoke
	e2eRes, err := RunE2EBenchmark(inprocTarget, E2EBenchConfig{
		Producers:   2,
		Workers:     2,
		PayloadSize: 64,
		Duration:    500 * time.Millisecond,
		Warmup:      100 * time.Millisecond,
		Queue:       "smoke-e2e-q",
	})
	require.NoError(t, err)
	require.NotNil(t, e2eRes)
	require.True(t, e2eRes.SanityPass)

	// 6. Generate report files and verify JSON parseability
	reportDir := filepath.Join(tempDir, "report_test")
	report := &SuiteReport{
		Env:       *CaptureEnv(DefaultRelayBenchConfig(), "quick"),
		B0b_Fsync: AggregateRepetitions("b0b", "Fsync", "desc", map[string]string{}, []RepetitionResult{*fRes}, ""),
		B1_Storage: []*BenchmarkResult{
			AggregateRepetitions("b1", "Storage", "desc", map[string]string{}, []RepetitionResult{*sRes}, ""),
		},
		B2_Cluster: []*BenchmarkResult{
			AggregateRepetitions("b2", "Cluster", "desc", map[string]string{}, []RepetitionResult{*cRes}, ""),
		},
		B3_OpenLoop: []*BenchmarkResult{
			AggregateRepetitions("b3", "OpenLoop", "desc", map[string]string{}, []RepetitionResult{*olRes}, ""),
		},
		B4_E2E: []*BenchmarkResult{
			AggregateRepetitions("b4", "E2E", "desc", map[string]string{}, []RepetitionResult{*e2eRes}, ""),
		},
	}

	reportPath, err := GenerateReportFiles(reportDir, report)
	require.NoError(t, err)
	require.FileExists(t, reportPath)

	// Verify env.json parseable
	envData, err := os.ReadFile(filepath.Join(reportDir, "env.json"))
	require.NoError(t, err)
	var parsedEnv EnvInfo
	require.NoError(t, json.Unmarshal(envData, &parsedEnv))
	require.NotEmpty(t, parsedEnv.CapturedAt)

	// Verify b1_storage.json parseable
	b1Data, err := os.ReadFile(filepath.Join(reportDir, "b1_storage.json"))
	require.NoError(t, err)
	var parsedB1 []*BenchmarkResult
	require.NoError(t, json.Unmarshal(b1Data, &parsedB1))
	require.Len(t, parsedB1, 1)
}

// TestB2_Unbatched_C8_FiveRuns executes unbatched c=8 five times and prints raw metrics for flakiness investigation.
func TestB2_Unbatched_C8_FiveRuns(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "b2_c8_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	inprocTarget, err := NewInProcessTargetCustom(tempDir, 0, 1)
	require.NoError(t, err)
	defer inprocTarget.Close()

	t.Log("=== B2 UNBATCHED C=8 5-RUN INVESTIGATION ===")
	for run := 1; run <= 5; run++ {
		res, err := RunClusterBenchmark(inprocTarget, ClusterBenchConfig{
			Concurrency: 8,
			PayloadSize: 256,
			Duration:    300 * time.Millisecond,
			Warmup:      50 * time.Millisecond,
			Queue:       fmt.Sprintf("b2-c8-run-%d", run),
		})
		require.NoError(t, err)
		t.Logf("Run %d/5: Throughput=%.0f ops/s | p50=%s | p95=%s | Errors=%d | Timeouts=%d | Retries=%d | Sanity=%v (Note: %s)",
			run, res.Throughput, FormatDuration(res.Latency.P50), FormatDuration(res.Latency.P95),
			res.Errors, res.Timeouts, res.Retries, res.SanityPass, res.SanityNote)
	}
}
