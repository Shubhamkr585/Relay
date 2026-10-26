package bench

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

var (
	clockResMu            sync.RWMutex
	globalClockResolution time.Duration
)

// SetMeasuredClockResolution sets the active clock resolution floor.
func SetMeasuredClockResolution(d time.Duration) {
	clockResMu.Lock()
	defer clockResMu.Unlock()
	globalClockResolution = d
}

// GetMeasuredClockResolution gets the measured clock resolution floor, initializing if necessary.
func GetMeasuredClockResolution() time.Duration {
	clockResMu.RLock()
	res := globalClockResolution
	clockResMu.RUnlock()
	if res > 0 {
		return res
	}
	res = MeasureClockResolution()
	SetMeasuredClockResolution(res)
	return res
}

// ComputeLatencyStats calculates percentiles (min, p50, p95, p99, p99.9, max, mean)
// from raw duration samples without clamping.
// Sorts the samples slice in-place.
func ComputeLatencyStats(samples []time.Duration) LatencyStats {
	n := len(samples)
	if n == 0 {
		return LatencyStats{}
	}

	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})

	var sum time.Duration
	for _, s := range samples {
		sum += s
	}

	percentile := func(p float64) time.Duration {
		if n == 0 {
			return 0
		}
		idx := int(math.Ceil(p*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return samples[idx]
	}

	return LatencyStats{
		Count: uint64(n),
		Min:   samples[0],
		P50:   percentile(0.50),
		P95:   percentile(0.95),
		P99:   percentile(0.99),
		P999:  percentile(0.999),
		Max:   samples[n-1],
		Mean:  sum / time.Duration(n),
	}
}

// ComputeAggregatedMetric aggregates float64 samples across repetitions into median, min, max, stddev.
func ComputeAggregatedMetric(values []float64) AggregatedMetric {
	n := len(values)
	if n == 0 {
		return AggregatedMetric{}
	}

	sorted := make([]float64, n)
	copy(sorted, values)
	sort.Float64s(sorted)

	var median float64
	if n%2 == 1 {
		median = sorted[n/2]
	} else {
		median = (sorted[n/2-1] + sorted[n/2]) / 2.0
	}

	minVal := sorted[0]
	maxVal := sorted[n-1]

	var sum, sumSq float64
	for _, v := range sorted {
		sum += v
		sumSq += v * v
	}
	mean := sum / float64(n)
	variance := 0.0
	if n > 1 {
		variance = (sumSq - float64(n)*mean*mean) / float64(n-1)
		if variance < 0 {
			variance = 0
		}
	}

	return AggregatedMetric{
		Median: median,
		Min:    minVal,
		Max:    maxVal,
		StdDev: math.Sqrt(variance),
	}
}

// ComputeAggregatedDuration aggregates time.Duration values across repetitions into median, min, max, stddev.
func ComputeAggregatedDuration(durations []time.Duration) AggregatedDuration {
	n := len(durations)
	if n == 0 {
		return AggregatedDuration{}
	}

	sorted := make([]time.Duration, n)
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var median time.Duration
	if n%2 == 1 {
		median = sorted[n/2]
	} else {
		median = (sorted[n/2-1] + sorted[n/2]) / 2
	}

	minDur := sorted[0]
	maxDur := sorted[n-1]

	floatVals := make([]float64, n)
	for i, d := range sorted {
		floatVals[i] = float64(d)
	}
	agg := ComputeAggregatedMetric(floatVals)

	return AggregatedDuration{
		Median: median,
		Min:    minDur,
		Max:    maxDur,
		StdDev: time.Duration(agg.StdDev),
	}
}

// AggregateRepetitions combines repetition results into a single BenchmarkResult.
func AggregateRepetitions(id, name, desc string, config map[string]string, reps []RepetitionResult, notes string) *BenchmarkResult {
	if len(reps) == 0 {
		return &BenchmarkResult{
			ID:          id,
			Name:        name,
			Description: desc,
			Config:      config,
			OverallPass: false,
			Notes:       notes,
		}
	}

	throughputs := make([]float64, len(reps))
	p50s := make([]time.Duration, len(reps))
	p95s := make([]time.Duration, len(reps))
	p99s := make([]time.Duration, len(reps))
	p999s := make([]time.Duration, len(reps))
	maxs := make([]time.Duration, len(reps))
	batchMeans := make([]time.Duration, len(reps))

	var totalErrors, totalTimeouts, totalRetries int
	var sumMeanDuration time.Duration

	allPassed := true
	for i, r := range reps {
		throughputs[i] = r.Throughput
		p50s[i] = r.Latency.P50
		p95s[i] = r.Latency.P95
		p99s[i] = r.Latency.P99
		p999s[i] = r.Latency.P999
		maxs[i] = r.Latency.Max
		sumMeanDuration += r.Latency.Mean

		if r.BatchMean > 0 {
			batchMeans[i] = r.BatchMean
		} else if r.TotalOps > 0 && r.Duration > 0 {
			batchMeans[i] = r.Duration / time.Duration(r.TotalOps)
		}

		totalErrors += r.Errors
		totalTimeouts += r.Timeouts
		totalRetries += r.Retries

		if !r.SanityPass || r.Errors > 0 {
			allPassed = false
		}
	}

	aggThroughput := ComputeAggregatedMetric(throughputs)
	aggP50 := ComputeAggregatedDuration(p50s)
	aggP95 := ComputeAggregatedDuration(p95s)
	aggP99 := ComputeAggregatedDuration(p99s)
	aggP999 := ComputeAggregatedDuration(p999s)
	aggMax := ComputeAggregatedDuration(maxs)
	aggBatchMean := ComputeAggregatedDuration(batchMeans)

	clockRes := GetMeasuredClockResolution()
	unreliable := (aggP50.Median < 10*clockRes)

	// Coefficient of variation (CV) stability gate across repetitions
	var cv float64
	var unstable bool
	stabilityStatus := "N/A (1 rep)"
	if len(throughputs) > 1 {
		var sum float64
		for _, v := range throughputs {
			sum += v
		}
		mean := sum / float64(len(throughputs))
		if mean > 0 {
			var sumSqDiff float64
			for _, v := range throughputs {
				d := v - mean
				sumSqDiff += d * d
			}
			stdDev := math.Sqrt(sumSqDiff / float64(len(throughputs)-1))
			cv = (stdDev / mean) * 100.0
		}
		if cv > 25.0 {
			unstable = true
			stabilityStatus = fmt.Sprintf("UNSTABLE (CV: %.1f%%)", cv)
		} else {
			stabilityStatus = fmt.Sprintf("STABLE (CV: %.1f%%)", cv)
		}
	}

	// Aggregate error breakdowns across repetitions
	aggErrorBreakdown := make(map[string]int)
	for _, r := range reps {
		for code, count := range r.ErrorBreakdown {
			aggErrorBreakdown[code] += count
		}
	}

	// Aggregate B4 / pipeline counters across repetitions
	var offeredJobs, achievedJobs uint64
	var resCalls, resErrors, ackCalls, ackErrors, enqCalls, enqErrors, leaseTimeouts uint64
	var lastBacklog int64
	for _, r := range reps {
		if r.EnqueueCallsMade > r.EnqueueErrors {
			offeredJobs += r.EnqueueCallsMade - r.EnqueueErrors
		}
		achievedJobs += r.TotalOps
		resCalls += r.ReserveCallsMade
		resErrors += r.ReserveErrors
		ackCalls += r.AckCallsMade
		ackErrors += r.AckErrors
		enqCalls += r.EnqueueCallsMade
		enqErrors += r.EnqueueErrors
		leaseTimeouts += r.LeaseTimeouts
		lastBacklog = r.Backlog
	}

	// Validation status
	valid := true
	var invalidReason string
	for _, r := range reps {
		if !r.Valid && r.InvalidReason != "" {
			valid = false
			invalidReason = r.InvalidReason
			break
		}
	}
	if aggThroughput.Median == 0 && valid {
		valid = false
		invalidReason = fmt.Sprintf("0 jobs completed across all repetitions (enq_calls: %d [err: %d], reserve_calls: %d [err: %d], ack_calls: %d [err: %d])",
			enqCalls, enqErrors, resCalls, resErrors, ackCalls, ackErrors)
	}
	if aggP50.Median == 0 && valid {
		valid = false
		invalidReason = "p50 latency is exactly 0.0µs (no completed operations)"
	}

	// Little's Law consistency check for closed-loop rows: mean_latency ~= concurrency / throughput
	var concurrency int
	if cStr, ok := config["concurrency"]; ok {
		fmt.Sscanf(cStr, "%d", &concurrency)
	} else if wStr, ok := config["workers"]; ok {
		fmt.Sscanf(wStr, "%d", &concurrency)
	}

	hasErrors := (totalErrors > 0 || totalTimeouts > 0)
	littlesLawConsistent := true
	var littlesLawNote string

	if hasErrors {
		// When errors occur, successful latency exhibits survivor bias
		littlesLawConsistent = false
		littlesLawNote = "INCONSISTENT: survivor bias (errors present)"
	} else if concurrency > 0 && aggThroughput.Median > 0 {
		expectedMeanSec := float64(concurrency) / aggThroughput.Median
		expectedMean := time.Duration(expectedMeanSec * 1e9)
		measuredMean := sumMeanDuration / time.Duration(len(reps))

		if expectedMean > 0 && measuredMean > 0 {
			ratio := float64(measuredMean) / float64(expectedMean)
			if ratio < 0.5 || ratio > 2.0 {
				littlesLawConsistent = false
				littlesLawNote = fmt.Sprintf("INCONSISTENT (measured mean %s vs expected %s, ratio %.2fx)",
					FormatDuration(measuredMean), FormatDuration(expectedMean), ratio)
			} else {
				littlesLawNote = fmt.Sprintf("Consistent (measured mean %s vs expected %s, ratio %.2fx)",
					FormatDuration(measuredMean), FormatDuration(expectedMean), ratio)
			}
		}
	}

	return &BenchmarkResult{
		ID:                   id,
		Name:                 name,
		Description:          desc,
		Config:               config,
		Repetitions:          reps,
		Throughput:           aggThroughput,
		LatencyP50:           aggP50,
		LatencyP95:           aggP95,
		LatencyP99:           aggP99,
		LatencyP999:          aggP999,
		LatencyMax:           aggMax,
		Unreliable:           unreliable,
		BatchMean:            aggBatchMean,
		TotalErrors:          totalErrors,
		TotalTimeouts:        totalTimeouts,
		TotalRetries:         totalRetries,
		ErrorBreakdown:       aggErrorBreakdown,
		HasErrors:            hasErrors,
		CV:                   cv,
		Unstable:             unstable,
		StabilityStatus:      stabilityStatus,
		Valid:                valid,
		InvalidReason:        invalidReason,
		LittlesLawConsistent: littlesLawConsistent,
		LittlesLawNote:       littlesLawNote,
		OfferedJobs:          offeredJobs,
		AchievedJobs:         achievedJobs,
		Backlog:              lastBacklog,
		ReserveCallsMade:     resCalls,
		ReserveErrors:        resErrors,
		AckCallsMade:         ackCalls,
		AckErrors:            ackErrors,
		EnqueueCallsMade:     enqCalls,
		EnqueueErrors:        enqErrors,
		LeaseTimeouts:        leaseTimeouts,
		OverallPass:          allPassed && valid,
		Notes:                notes,
	}
}

// FormatOpsRange formats throughput as "median [min - max] ops/sec".
func FormatOpsRange(m AggregatedMetric) string {
	if m.Min == m.Max {
		return fmt.Sprintf("%.0f", m.Median)
	}
	return fmt.Sprintf("%.0f [%.0f - %.0f]", m.Median, m.Min, m.Max)
}

// FormatDuration formats duration nicely (µs, ms, or s).
func FormatDuration(d time.Duration) string {
	if d <= 0 {
		return "0.0µs"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// FormatDurationRange formats duration range as "median [min - max]".
func FormatDurationRange(d AggregatedDuration) string {
	if d.Min == d.Max {
		return FormatDuration(d.Median)
	}
	return fmt.Sprintf("%s [%s - %s]", FormatDuration(d.Median), FormatDuration(d.Min), FormatDuration(d.Max))
}
