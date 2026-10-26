package store_test

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shubhamkr585/relay/internal/domain"
	"github.com/shubhamkr585/relay/internal/store"
)

// TestCrashHelperProcess is invoked as a separate subprocess by TestCrashRecovery_ZeroLostAcknowledgedJobs.
// It actively commits jobs to disk with strict fsync enabled and records acknowledged jobs to a file.
func TestCrashHelperProcess(t *testing.T) {
	if os.Getenv("RELAY_CRASH_HELPER") != "1" {
		return
	}

	args := os.Args
	sepIdx := -1
	for i, arg := range args {
		if arg == "--" {
			sepIdx = i
			break
		}
	}
	if sepIdx == -1 || len(args) < sepIdx+3 {
		fmt.Fprintf(os.Stderr, "Missing arguments: expected dbPath and ackedPath\n")
		os.Exit(1)
	}

	dbPath := args[sepIdx+1]
	ackedPath := args[sepIdx+2]

	ackFile, err := os.OpenFile(ackedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open ackFile: %v\n", err)
		os.Exit(1)
	}
	defer ackFile.Close()

	cfg := store.DefaultConfig(dbPath)
	cfg.NoSync = false // Strict durability: fsync on commit!
	cfg.BatchWindow = 2 * time.Millisecond
	cfg.BatchMaxSize = 32

	engine, err := store.NewBBoltEngine(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open engine: %v\n", err)
		os.Exit(1)
	}
	defer engine.Close()

	ctx := context.Background()
	var fileMu sync.Mutex

	// Launch 8 concurrent workers to keep disk IO and group-commit write transactions actively saturated
	numWorkers := 8
	for w := 0; w < numWorkers; w++ {
		workerID := w
		go func() {
			counter := 0
			for {
				counter++
				payload := fmt.Sprintf("worker-%d-job-%d-payload-data", workerID, counter)
				job, _, err := engine.Enqueue(ctx, &store.EnqueueParams{
					Queue:   "crash-durability-queue",
					Payload: []byte(payload),
				})
				if err == nil && job != nil {
					// Acknowledged and fsynced to disk! Record to ackFile
					fileMu.Lock()
					_, _ = ackFile.WriteString(job.Id + "\n")
					_ = ackFile.Sync()
					fileMu.Unlock()
				}
			}
		}()
	}

	// Keep running until killed abruptly by the parent test process
	select {}
}

// TestCrashRecovery_ZeroLostAcknowledgedJobs verifies that ungraceful termination (SIGKILL)
// mid-load across multiple successive crash cycles results in ZERO loss of acknowledged jobs.
func TestCrashRecovery_ZeroLostAcknowledgedJobs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "relay-crash-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "crash.db")
	ackedPath := filepath.Join(tmpDir, "acked.txt")

	// Perform 3 successive crash-and-recovery cycles on the EXACT same database file
	// Each cycle commits jobs, gets killed mid-flight at different jitter timing, and verifies integrity.
	cycles := 3
	cumulativeAcked := make(map[string]struct{})

	for cycle := 1; cycle <= cycles; cycle++ {
		// Spawn helper subprocess
		cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelperProcess", "--", dbPath, ackedPath)
		cmd.Env = append(os.Environ(), "RELAY_CRASH_HELPER=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err = cmd.Start()
		require.NoError(t, err, "failed to spawn crash helper in cycle %d", cycle)

		// Wait until at least 200 additional jobs are committed in this cycle
		targetNewJobs := 200
		startCount := len(cumulativeAcked)

		require.Eventually(t, func() bool {
			data, err := os.ReadFile(ackedPath)
			if err != nil {
				return false
			}
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			validCount := 0
			for _, l := range lines {
				if strings.TrimSpace(l) != "" {
					validCount++
				}
			}
			return validCount >= startCount+targetNewJobs
		}, 10*time.Second, 20*time.Millisecond, "timed out waiting for helper writes in cycle %d", cycle)

		// Introduce random microsecond jitter to kill at arbitrary points during active fsync writes
		jitterMicros := rand.Intn(10000)
		time.Sleep(time.Duration(jitterMicros) * time.Microsecond)

		// Kill the subprocess abruptly while heavy concurrent writes and fsyncs are actively in flight
		err = cmd.Process.Kill()
		require.NoError(t, err, "failed to kill helper in cycle %d", cycle)
		_ = cmd.Wait()
		time.Sleep(50 * time.Millisecond)

		// Read all acknowledged job IDs up to this kill
		ackFile, err := os.Open(ackedPath)
		require.NoError(t, err)
		scanner := bufio.NewScanner(ackFile)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				cumulativeAcked[line] = struct{}{}
			}
		}
		require.NoError(t, scanner.Err())
		require.NoError(t, ackFile.Close())

		t.Logf("Cycle %d: Helper killed mid-write. Total acknowledged jobs to verify: %d", cycle, len(cumulativeAcked))

		// Recover the storage engine on the same database file
		cfg := store.DefaultConfig(dbPath)
		cfg.NoSync = false // Strict fsync

		recoveredEngine, err := store.NewBBoltEngine(cfg)
		require.NoError(t, err, "failed to recover bbolt engine on db in cycle %d", cycle)

		ctx := context.Background()

		// Verify 100% of acknowledged jobs are present and intact
		lostJobs := 0
		for jobID := range cumulativeAcked {
			job, err := recoveredEngine.GetJob(ctx, jobID)
			if err != nil || job == nil {
				t.Errorf("Cycle %d DATA LOSS: Acknowledged job %s missing from storage! err: %v", cycle, jobID, err)
				lostJobs++
			} else {
				if job.State != domain.StatePending {
					t.Errorf("Cycle %d unexpected state %s for job %s", cycle, job.State, jobID)
				}
			}
		}

		require.Equal(t, 0, lostJobs, "Invariant violated: found lost acknowledged jobs in cycle %d", cycle)

		// Verify queue stats reflect at least the acknowledged count
		stats, err := recoveredEngine.Stats(ctx, "crash-durability-queue")
		require.NoError(t, err)
		require.GreaterOrEqual(t, stats.PendingCount, int64(len(cumulativeAcked)))

		_ = recoveredEngine.Close()
	}

	t.Logf("PASS: Successfully executed %d crash cycles under active load with 0 lost jobs (Total verified: %d)!", cycles, len(cumulativeAcked))
}
