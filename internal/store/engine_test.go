package store_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shubhamkr585/relay/internal/domain"
	"github.com/shubhamkr585/relay/internal/store"
)

func setupTestEngine(t *testing.T, noSync bool) (*store.BBoltEngine, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "relay-store-test-*")
	require.NoError(t, err)

	dbPath := filepath.Join(tmpDir, "test_relay.db")
	cfg := store.DefaultConfig(dbPath)
	cfg.NoSync = noSync
	cfg.BatchWindow = 2 * time.Millisecond
	cfg.BatchMaxSize = 32
	cfg.ReaperInterval = 20 * time.Millisecond
	cfg.BaseBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 100 * time.Millisecond

	engine, err := store.NewBBoltEngine(cfg)
	require.NoError(t, err)

	cleanup := func() {
		_ = engine.Close()
		_ = os.RemoveAll(tmpDir)
	}

	return engine, cleanup
}

func TestEnqueueAndPriorityReserve(t *testing.T) {
	engine, cleanup := setupTestEngine(t, true)
	defer cleanup()

	ctx := context.Background()

	// Enqueue jobs with different priorities: 5 (low), 0 (highest), 2 (medium)
	j1, dedup1, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:    "emails",
		Payload:  []byte("job-priority-5"),
		Priority: 5,
	})
	require.NoError(t, err)
	require.False(t, dedup1)

	j2, dedup2, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:    "emails",
		Payload:  []byte("job-priority-0"),
		Priority: 0,
	})
	require.NoError(t, err)
	require.False(t, dedup2)

	j3, dedup3, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:    "emails",
		Payload:  []byte("job-priority-2"),
		Priority: 2,
	})
	require.NoError(t, err)
	require.False(t, dedup3)

	// First Reserve MUST return Priority 0 (j2)
	res1, tok1, err := engine.Reserve(ctx, "emails", "worker-1", 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, j2.Id, res1.Id)
	require.NotEmpty(t, tok1)
	require.Equal(t, uint32(1), res1.Attempts)
	require.Equal(t, domain.StateLeased, res1.State)

	// Second Reserve MUST return Priority 2 (j3)
	res2, tok2, err := engine.Reserve(ctx, "emails", "worker-1", 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, j3.Id, res2.Id)
	require.NotEmpty(t, tok2)

	// Third Reserve MUST return Priority 5 (j1)
	res3, tok3, err := engine.Reserve(ctx, "emails", "worker-1", 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, j1.Id, res3.Id)
	require.NotEmpty(t, tok3)

	// Fourth Reserve MUST return ErrQueueEmpty
	_, _, err = engine.Reserve(ctx, "emails", "worker-1", 5*time.Second)
	require.ErrorIs(t, err, domain.ErrQueueEmpty)
}

func TestIdempotencyDeduplication(t *testing.T) {
	engine, cleanup := setupTestEngine(t, true)
	defer cleanup()

	ctx := context.Background()

	idempKey := "invoice-txn-98765"

	// First Enqueue
	j1, dedup1, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:          "billing",
		Payload:        []byte("charge $50"),
		IdempotencyKey: idempKey,
	})
	require.NoError(t, err)
	require.False(t, dedup1)
	require.NotEmpty(t, j1.Id)

	// Duplicate Enqueue with same idempotency key and queue
	j2, dedup2, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:          "billing",
		Payload:        []byte("charge $50 duplicate"),
		IdempotencyKey: idempKey,
	})
	require.NoError(t, err)
	require.True(t, dedup2)
	require.Equal(t, j1.Id, j2.Id)
	require.Equal(t, j1.Payload, j2.Payload)

	// Ensure only ONE job exists in the queue
	res, _, err := engine.Reserve(ctx, "billing", "worker-1", time.Minute)
	require.NoError(t, err)
	require.Equal(t, j1.Id, res.Id)

	// Queue should now be empty
	_, _, err = engine.Reserve(ctx, "billing", "worker-1", time.Minute)
	require.ErrorIs(t, err, domain.ErrQueueEmpty)
}

func TestAckAndFencingRules(t *testing.T) {
	engine, cleanup := setupTestEngine(t, true)
	defer cleanup()

	ctx := context.Background()

	j, _, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:   "tasks",
		Payload: []byte("task-1"),
	})
	require.NoError(t, err)

	leasedJob, validToken, err := engine.Reserve(ctx, "tasks", "worker-A", time.Minute)
	require.NoError(t, err)

	// 1. Attempt Ack with stale token -> ErrStaleLease
	errStale := engine.Ack(ctx, j.Id, "wrong-stale-token")
	require.ErrorIs(t, errStale, domain.ErrStaleLease)

	// 2. Ack with correct token -> Success
	err = engine.Ack(ctx, j.Id, validToken)
	require.NoError(t, err)

	// Verify state is DONE
	doneJob, err := engine.GetJob(ctx, j.Id)
	require.NoError(t, err)
	require.Equal(t, domain.StateDone, doneJob.State)

	// 3. Repeated Ack on completed job -> ErrStaleLease
	errRepeat := engine.Ack(ctx, j.Id, validToken)
	require.ErrorIs(t, errRepeat, domain.ErrStaleLease)

	// 4. ExtendLease on DONE job -> ErrStaleLease
	_, errExt := engine.ExtendLease(ctx, j.Id, validToken, time.Minute)
	require.ErrorIs(t, errExt, domain.ErrStaleLease)

	_ = leasedJob
}

func TestNackAndDeadLetterQueue(t *testing.T) {
	engine, cleanup := setupTestEngine(t, true)
	defer cleanup()

	ctx := context.Background()

	// Job with MaxAttempts = 2
	j, _, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:       "orders",
		Payload:     []byte("order-101"),
		MaxAttempts: 2,
	})
	require.NoError(t, err)

	// First attempt
	res1, tok1, err := engine.Reserve(ctx, "orders", "worker-1", time.Minute)
	require.NoError(t, err)
	require.Equal(t, uint32(1), res1.Attempts)

	// Nack attempt 1 -> should return to PENDING immediately (0 delay for test)
	err = engine.Nack(ctx, j.Id, tok1, "transient error", 0)
	require.NoError(t, err)

	// Second attempt
	res2, tok2, err := engine.Reserve(ctx, "orders", "worker-2", time.Minute)
	require.NoError(t, err)
	require.Equal(t, uint32(2), res2.Attempts)

	// Nack attempt 2 -> attempts (2) >= MaxAttempts (2) -> must transition to DEAD (DLQ)
	err = engine.Nack(ctx, j.Id, tok2, "permanent error", 0)
	require.NoError(t, err)

	// Queue should have no ready jobs
	_, _, err = engine.Reserve(ctx, "orders", "worker-3", time.Minute)
	require.ErrorIs(t, err, domain.ErrQueueEmpty)

	// Verify job is DEAD
	deadJob, err := engine.GetJob(ctx, j.Id)
	require.NoError(t, err)
	require.Equal(t, domain.StateDead, deadJob.State)
	require.Equal(t, "permanent error", deadJob.LastError)

	// ListDead
	deadList, err := engine.ListDead(ctx, "orders", 10)
	require.NoError(t, err)
	require.Len(t, deadList, 1)
	require.Equal(t, j.Id, deadList[0].Id)

	// RequeueDead
	requeued, err := engine.RequeueDead(ctx, j.Id)
	require.NoError(t, err)
	require.Equal(t, domain.StatePending, requeued.State)
	require.Equal(t, uint32(0), requeued.Attempts)

	// Can now be reserved again!
	res3, tok3, err := engine.Reserve(ctx, "orders", "worker-4", time.Minute)
	require.NoError(t, err)
	require.Equal(t, j.Id, res3.Id)
	require.Equal(t, uint32(1), res3.Attempts)
	require.NoError(t, engine.Ack(ctx, j.Id, tok3))
}

func TestExtendLease(t *testing.T) {
	engine, cleanup := setupTestEngine(t, true)
	defer cleanup()

	ctx := context.Background()

	j, _, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:   "long-running",
		Payload: []byte("render-video"),
	})
	require.NoError(t, err)

	res, token, err := engine.Reserve(ctx, "long-running", "worker-1", 10*time.Second)
	require.NoError(t, err)
	initialExpiresAt := res.LeaseExpiresAt

	// Stale token rejected
	_, errStale := engine.ExtendLease(ctx, j.Id, "bad-token", 30*time.Second)
	require.ErrorIs(t, errStale, domain.ErrStaleLease)

	// Valid extension
	newExpiresAt, err := engine.ExtendLease(ctx, j.Id, token, 30*time.Second)
	require.NoError(t, err)
	require.Greater(t, newExpiresAt, initialExpiresAt)

	updated, err := engine.GetJob(ctx, j.Id)
	require.NoError(t, err)
	require.Equal(t, newExpiresAt, updated.LeaseExpiresAt)

	// Ack succeeds with same token
	require.NoError(t, engine.Ack(ctx, j.Id, token))
}

func TestScheduledJobsAndLeaseReaper(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "relay-clock-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "clock_test.db")
	initialTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	clock := store.NewControllableClock(initialTime)

	cfg := store.DefaultConfig(dbPath)
	cfg.NoSync = true
	cfg.Clock = clock
	cfg.BaseBackoff = 500 * time.Millisecond
	cfg.MaxBackoff = 10 * time.Second
	// Set ReaperInterval to 1 hour so background ticker doesn't introduce non-deterministic race
	cfg.ReaperInterval = time.Hour

	engine, err := store.NewBBoltEngine(cfg)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// 1. Enqueue a job scheduled 10 minutes into the future
	j, _, err := engine.Enqueue(ctx, &store.EnqueueParams{
		Queue:   "scheduled-q",
		Payload: []byte("scheduled-job"),
		Delay:   10 * time.Minute,
	})
	require.NoError(t, err)

	// Immediately at initial time T0: job is NOT ready
	_, _, err = engine.Reserve(ctx, "scheduled-q", "worker-1", time.Minute)
	require.ErrorIs(t, err, domain.ErrQueueEmpty)

	// 2. Advance clock by 11 minutes (past the 10m delay) without any wall-clock sleeps
	clock.Advance(11 * time.Minute)

	// Trigger promotion at current simulated time
	_, promoted, err := engine.TriggerReaperAt(ctx, clock.Now())
	require.NoError(t, err)
	require.Equal(t, 1, promoted)

	// 3. Now job is READY and can be reserved with a 30s lease
	res1, tok1, err := engine.Reserve(ctx, "scheduled-q", "worker-1", 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, j.Id, res1.Id)
	require.NotEmpty(t, tok1)
	require.Equal(t, uint32(1), res1.Attempts)

	// 4. Advance clock by 31 seconds (lease has expired!)
	clock.Advance(31 * time.Second)

	// Trigger reaper at current simulated time
	reaped, _, err := engine.TriggerReaperAt(ctx, clock.Now())
	require.NoError(t, err)
	require.Equal(t, 1, reaped)

	// 5. Advance clock past the retry backoff delay and jitter
	clock.Advance(10 * time.Second)

	// Promote rescheduled job
	_, promoted2, err := engine.TriggerReaperAt(ctx, clock.Now())
	require.NoError(t, err)
	require.Equal(t, 1, promoted2)

	// 6. Job can now be reserved with attempt 2 and a new monotonic token!
	res2, tok2, err := engine.Reserve(ctx, "scheduled-q", "worker-2", time.Minute)
	require.NoError(t, err)
	require.Equal(t, j.Id, res2.Id)
	require.NotEqual(t, tok1, tok2)
	require.Equal(t, uint32(2), res2.Attempts)

	// 7. Ack succeeds
	require.NoError(t, engine.Ack(ctx, j.Id, tok2))
}

func TestConcurrentMicroBatcher(t *testing.T) {
	// Test high concurrency under group commit with fsync (NoSync = false)
	engine, cleanup := setupTestEngine(t, false)
	defer cleanup()

	ctx := context.Background()
	numProducers := 10
	jobsPerProducer := 20
	totalJobs := numProducers * jobsPerProducer

	var wg sync.WaitGroup
	wg.Add(numProducers)

	jobIDs := make([]string, totalJobs)
	var mu sync.Mutex

	for p := 0; p < numProducers; p++ {
		producerID := p
		go func() {
			defer wg.Done()
			for i := 0; i < jobsPerProducer; i++ {
				payload := fmt.Sprintf("producer-%d-job-%d", producerID, i)
				j, dedup, err := engine.Enqueue(ctx, &store.EnqueueParams{
					Queue:   "high-traffic",
					Payload: []byte(payload),
				})
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
					return
				}
				if dedup {
					t.Errorf("unexpected dedup for distinct jobs")
				}
				mu.Lock()
				jobIDs[producerID*jobsPerProducer+i] = j.Id
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Verify all jobs are present and can be reserved and acked concurrently
	var workerWg sync.WaitGroup
	numWorkers := 5
	jobsPerWorker := totalJobs / numWorkers
	workerWg.Add(numWorkers)

	processedJobs := make(map[string]int)
	var procMu sync.Mutex

	for w := 0; w < numWorkers; w++ {
		workerID := fmt.Sprintf("worker-%d", w)
		go func() {
			defer workerWg.Done()
			for i := 0; i < jobsPerWorker; i++ {
				j, token, err := engine.Reserve(ctx, "high-traffic", workerID, 5*time.Second)
				if err != nil {
					t.Errorf("reserve error on %s: %v", workerID, err)
					return
				}
				procMu.Lock()
				processedJobs[j.Id]++
				procMu.Unlock()

				if err := engine.Ack(ctx, j.Id, token); err != nil {
					t.Errorf("ack error on %s: %v", workerID, err)
					return
				}
			}
		}()
	}

	workerWg.Wait()

	// Strict Correctness Invariant:
	// Every single enqueued job MUST have been reserved and processed EXACTLY ONCE. 0 lost, 0 duplicates.
	require.Equal(t, totalJobs, len(processedJobs), "all enqueued jobs must be processed")
	for _, id := range jobIDs {
		require.Equal(t, 1, processedJobs[id], "job %s was not processed exactly once", id)
	}

	// Queue should now have 0 pending and 0 leased, totalJobs done
	stats, err := engine.Stats(ctx, "high-traffic")
	require.NoError(t, err)
	require.Equal(t, int64(0), stats.PendingCount)
	require.Equal(t, int64(0), stats.LeasedCount)
	require.Equal(t, int64(totalJobs), stats.DoneCount)
}
