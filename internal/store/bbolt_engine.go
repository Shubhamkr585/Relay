package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	bbolt "go.etcd.io/bbolt"

	"github.com/shubhamkr585/relay/internal/domain"
)

// BBoltEngine implements store.Engine backed by BBolt with secondary composite indexing,
// group-commit micro-batching, and an automated lease reaper.
type BBoltEngine struct {
	db      *bbolt.DB
	cfg     Config
	batcher *Batcher
	reaper  *Reaper
	mu      sync.RWMutex
	closed  bool
}

// NewBBoltEngine opens or creates a BBolt database at cfg.Path, initializes all buckets,
// and starts the group-commit batcher and background reaper.
func NewBBoltEngine(cfg Config) (*BBoltEngine, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: db path cannot be empty", domain.ErrInvalidArguments)
	}

	bboltOpts := bbolt.DefaultOptions
	bboltOpts.NoSync = cfg.NoSync

	db, err := bbolt.Open(cfg.Path, 0600, bboltOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt db at %s: %w", cfg.Path, err)
	}

	// Initialize all required buckets in an atomic write transaction
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, bName := range AllBuckets {
			if _, err := tx.CreateBucketIfNotExists(bName); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", string(bName), err)
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	defaultBackoff := func(attempt uint32) time.Duration {
		return domain.CalculateBackoffWithRandom(attempt, cfg.BaseBackoff, cfg.MaxBackoff)
	}

	batcher := newBatcher(db, cfg, defaultBackoff)
	reaper := newReaper(batcher, cfg.ReaperInterval)

	return &BBoltEngine{
		db:      db,
		cfg:     cfg,
		batcher: batcher,
		reaper:  reaper,
	}, nil
}

// Enqueue submits a job through the group-commit micro-batcher.
func (e *BBoltEngine) Enqueue(ctx context.Context, params *EnqueueParams) (*domain.Job, bool, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, false, domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	op := &writeOp{
		kind:          kindEnqueue,
		enqueueParams: params,
	}

	res, err := e.batcher.submit(ctx, op)
	if err != nil {
		return nil, false, err
	}
	return res.job, res.deduplicated, res.err
}

// Reserve leases the highest priority ready job for a queue.
func (e *BBoltEngine) Reserve(ctx context.Context, queue, workerID string, leaseDuration time.Duration) (*domain.Job, string, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, "", domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	op := &writeOp{
		kind:          kindReserve,
		queue:         queue,
		workerID:      workerID,
		leaseDuration: leaseDuration,
	}

	res, err := e.batcher.submit(ctx, op)
	if err != nil {
		return nil, "", err
	}
	return res.job, res.leaseToken, res.err
}

// Ack marks a leased job as DONE, fenced by leaseToken.
func (e *BBoltEngine) Ack(ctx context.Context, jobID, leaseToken string) error {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	op := &writeOp{
		kind:       kindAck,
		jobID:      jobID,
		leaseToken: leaseToken,
	}

	res, err := e.batcher.submit(ctx, op)
	if err != nil {
		return err
	}
	return res.err
}

// Nack returns a leased job to PENDING or moves it to DEAD.
func (e *BBoltEngine) Nack(ctx context.Context, jobID, leaseToken, errorMsg string, retryAfter time.Duration) error {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	op := &writeOp{
		kind:       kindNack,
		jobID:      jobID,
		leaseToken: leaseToken,
		errorMsg:   errorMsg,
		retryAfter: retryAfter,
	}

	res, err := e.batcher.submit(ctx, op)
	if err != nil {
		return err
	}
	return res.err
}

// ExtendLease increases the lease expiration for a currently leased job.
func (e *BBoltEngine) ExtendLease(ctx context.Context, jobID, leaseToken string, extraDuration time.Duration) (int64, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return 0, domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	op := &writeOp{
		kind:          kindExtendLease,
		jobID:         jobID,
		leaseToken:    leaseToken,
		extraDuration: extraDuration,
	}

	res, err := e.batcher.submit(ctx, op)
	if err != nil {
		return 0, err
	}
	return res.newExpiresAt, res.err
}

// Stats returns aggregate queue statistics.
func (e *BBoltEngine) Stats(ctx context.Context, queue string) (*domain.QueueStats, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	var stats *domain.QueueStats
	err := e.db.View(func(tx *bbolt.Tx) error {
		var txErr error
		stats, txErr = txStats(tx, queue)
		return txErr
	})

	return stats, err
}

// ListDead returns jobs currently in DEAD state (DLQ) for the queue.
func (e *BBoltEngine) ListDead(ctx context.Context, queue string, limit uint32) ([]*domain.Job, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	var deadJobs []*domain.Job
	err := e.db.View(func(tx *bbolt.Tx) error {
		var txErr error
		deadJobs, txErr = txListDead(tx, queue, limit)
		return txErr
	})

	return deadJobs, err
}

// RequeueDead moves a job from DEAD back to PENDING.
func (e *BBoltEngine) RequeueDead(ctx context.Context, jobID string) (*domain.Job, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	op := &writeOp{
		kind:  kindRequeueDead,
		jobID: jobID,
	}

	res, err := e.batcher.submit(ctx, op)
	if err != nil {
		return nil, err
	}
	return res.job, res.err
}

// GetJob retrieves a job by ID.
func (e *BBoltEngine) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, domain.ErrEngineClosed
	}
	e.mu.RUnlock()

	var job *domain.Job
	err := e.db.View(func(tx *bbolt.Tx) error {
		var txErr error
		job, txErr = txGetJob(tx, jobID)
		return txErr
	})

	return job, err
}

// TriggerReaperManual forces a sweep of expired leases and scheduled jobs (used in tests).
func (e *BBoltEngine) TriggerReaperManual(ctx context.Context) (int, int, error) {
	return e.reaper.ReapAndPromoteOnce(ctx)
}

// TriggerReaperAt forces a sweep evaluated at timestamp now without relying on wall-clock sleeps.
func (e *BBoltEngine) TriggerReaperAt(ctx context.Context, now time.Time) (int, int, error) {
	return e.reaper.ReapAndPromoteAt(ctx, now)
}

// Close gracefully flushes all pending operations and closes the database.
func (e *BBoltEngine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	e.reaper.stop()
	e.batcher.stop()
	return e.db.Close()
}
