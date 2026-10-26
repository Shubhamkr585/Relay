package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getDirSize(path string) int64 {
	var total int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func getClusterStats(target ClusterTarget, queue string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := target.GetStats(ctx, queue)
	if err != nil {
		return 0, err
	}
	return resp.PendingCount + resp.LeasedCount + resp.DoneCount + resp.DeadCount, nil
}

// TestB2_Instability_Investigation directly answers 1a:
// Compares 5 runs of unbatched c=8 with fresh clusters vs 5 runs reusing one cluster.
func TestB2_Instability_Investigation(t *testing.T) {
	t.Log("\n================================================================================")
	t.Log("  B2 INSTABILITY INVESTIGATION (Requirement 1a)")
	t.Log("================================================================================")

	// SET A: 5 runs with FRESH cluster and FRESH data dirs
	t.Log("\n--- SET A: FRESH CLUSTER & FRESH DATA DIRS PER RUN ---")
	for i := 1; i <= 5; i++ {
		tempDir, err := os.MkdirTemp("", fmt.Sprintf("b2_fresh_%d_*", i))
		require.NoError(t, err)

		goroutinesBefore := runtime.NumGoroutine()
		dirSizeBefore := getDirSize(tempDir)

		target, err := NewInProcessTargetCustom(tempDir, 0, 1)
		require.NoError(t, err)

		qName := fmt.Sprintf("fresh-q-%d", i)
		qDepthBefore, _ := getClusterStats(target, qName)

		fmt.Printf("SET A Run %d BEFORE: Goroutines=%d | QueueDepth=%d | DataDirBytes=%d\n",
			i, goroutinesBefore, qDepthBefore, dirSizeBefore)

		res, err := RunClusterBenchmark(target, ClusterBenchConfig{
			Concurrency: 8,
			PayloadSize: 256,
			Duration:    3 * time.Second,
			Warmup:      1 * time.Second,
			Queue:       qName,
		})
		require.NoError(t, err)

		goroutinesAfter := runtime.NumGoroutine()
		qDepthAfter, _ := getClusterStats(target, qName)
		dirSizeAfter := getDirSize(tempDir)

		fmt.Printf("SET A Run %d RESULT: Throughput=%.0f ops/s | p50=%s | p95=%s | Errors=%d | Timeouts=%d | Retries=%d\n",
			i, res.Throughput, FormatDuration(res.Latency.P50), FormatDuration(res.Latency.P95),
			res.Errors, res.Timeouts, res.Retries)
		fmt.Printf("SET A Run %d AFTER : Goroutines=%d | QueueDepth=%d | DataDirBytes=%d\n\n",
			i, goroutinesAfter, qDepthAfter, dirSizeAfter)

		_ = target.Close()
		_ = os.RemoveAll(tempDir)
		time.Sleep(200 * time.Millisecond)
	}

	// SET B: 5 runs REUSING one cluster
	t.Log("--- SET B: REUSED CLUSTER (SAME INSTANCE & DATA DIR ACCUMULATING) ---")
	reusedDir, err := os.MkdirTemp("", "b2_reused_*")
	require.NoError(t, err)
	defer os.RemoveAll(reusedDir)

	reusedTarget, err := NewInProcessTargetCustom(reusedDir, 0, 1)
	require.NoError(t, err)
	defer reusedTarget.Close()

	for i := 1; i <= 5; i++ {
		goroutinesBefore := runtime.NumGoroutine()
		dirSizeBefore := getDirSize(reusedDir)
		qName := "reused-q"
		qDepthBefore, _ := getClusterStats(reusedTarget, qName)

		fmt.Printf("SET B Run %d BEFORE: Goroutines=%d | QueueDepth=%d | DataDirBytes=%d\n",
			i, goroutinesBefore, qDepthBefore, dirSizeBefore)

		res, err := RunClusterBenchmark(reusedTarget, ClusterBenchConfig{
			Concurrency: 8,
			PayloadSize: 256,
			Duration:    3 * time.Second,
			Warmup:      1 * time.Second,
			Queue:       qName,
		})
		require.NoError(t, err)

		goroutinesAfter := runtime.NumGoroutine()
		qDepthAfter, _ := getClusterStats(reusedTarget, qName)
		dirSizeAfter := getDirSize(reusedDir)

		fmt.Printf("SET B Run %d RESULT: Throughput=%.0f ops/s | p50=%s | p95=%s | Errors=%d | Timeouts=%d | Retries=%d\n",
			i, res.Throughput, FormatDuration(res.Latency.P50), FormatDuration(res.Latency.P95),
			res.Errors, res.Timeouts, res.Retries)
		fmt.Printf("SET B Run %d AFTER : Goroutines=%d | QueueDepth=%d | DataDirBytes=%d\n\n",
			i, goroutinesAfter, qDepthAfter, dirSizeAfter)

		time.Sleep(200 * time.Millisecond)
	}
}
