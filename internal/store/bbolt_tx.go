package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/google/uuid"
	bbolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	"github.com/shubhamkr585/relay/internal/domain"
)

// txEnqueue executes an Enqueue operation inside an atomic bbolt write transaction.
func txEnqueue(tx *bbolt.Tx, params *EnqueueParams, nowNano int64) (*domain.Job, bool, error) {
	if params.Queue == "" {
		return nil, false, fmt.Errorf("%w: queue cannot be empty", domain.ErrInvalidArguments)
	}

	bJobs := tx.Bucket(BucketJobs)
	bReady := tx.Bucket(BucketIdxReady)
	bSched := tx.Bucket(BucketIdxSched)
	bIdemp := tx.Bucket(BucketIdxIdemp)

	// Idempotency deduplication check
	if params.IdempotencyKey != "" {
		idempK := idempotencyKey(params.Queue, params.IdempotencyKey)
		existingJobID := bIdemp.Get(idempK)
		if existingJobID != nil {
			jobBytes := bJobs.Get(existingJobID)
			if jobBytes != nil {
				var existingJob domain.Job
				if err := proto.Unmarshal(jobBytes, &existingJob); err == nil {
					return &existingJob, true, nil
				}
			}
		}
	}

	maxAttempts := params.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	priority := params.Priority
	if priority > 255 {
		priority = 255
	}

	runAt := nowNano
	if params.RunAt > 0 {
		runAt = params.RunAt
	} else if params.Delay > 0 {
		runAt = nowNano + params.Delay.Nanoseconds()
	}

	jobID := params.JobID
	if jobID == "" {
		jobID = uuid.NewString()
	}

	job := &domain.Job{
		Id:             jobID,
		Queue:          params.Queue,
		Payload:        params.Payload,
		Priority:       priority,
		State:          domain.StatePending,
		Attempts:       0,
		MaxAttempts:    maxAttempts,
		RunAt:          runAt,
		LeaseExpiresAt: 0,
		LeaseToken:     "",
		WorkerId:       "",
		IdempotencyKey: params.IdempotencyKey,
		CreatedAt:      nowNano,
		LastError:      "",
	}

	jobBytes, err := proto.Marshal(job)
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal job: %w", err)
	}

	if err := bJobs.Put([]byte(job.Id), jobBytes); err != nil {
		return nil, false, fmt.Errorf("failed to put job: %w", err)
	}

	if params.IdempotencyKey != "" {
		idempK := idempotencyKey(params.Queue, params.IdempotencyKey)
		if err := bIdemp.Put(idempK, []byte(job.Id)); err != nil {
			return nil, false, fmt.Errorf("failed to put idempotency index: %w", err)
		}
	}

	if runAt <= nowNano {
		rKey := readyKey(job.Queue, uint8(job.Priority), job.CreatedAt, job.Id)
		if err := bReady.Put(rKey, nil); err != nil {
			return nil, false, fmt.Errorf("failed to put ready index: %w", err)
		}
	} else {
		sKey := scheduledKey(job.RunAt, job.Id)
		if err := bSched.Put(sKey, []byte(job.Queue)); err != nil {
			return nil, false, fmt.Errorf("failed to put scheduled index: %w", err)
		}
	}

	return job, false, nil
}

// txReserve leases the highest priority ready job for a queue inside an atomic transaction.
func txReserve(tx *bbolt.Tx, queue, workerID string, leaseDuration time.Duration, nowNano int64, leaseToken string) (*domain.Job, string, error) {
	bJobs := tx.Bucket(BucketJobs)
	bReady := tx.Bucket(BucketIdxReady)
	bLeased := tx.Bucket(BucketIdxLeased)

	prefix := readyPrefix(queue)
	c := bReady.Cursor()
	k, _ := c.Seek(prefix)

	if k == nil || !bytes.HasPrefix(k, prefix) {
		return nil, "", domain.ErrQueueEmpty
	}

	jobID := extractJobIDFromReadyKey(len(queue), k)

	// Invariant: ALL validations and reads MUST happen before any write!
	jobBytes := bJobs.Get([]byte(jobID))
	if jobBytes == nil {
		return nil, "", domain.ErrNotFound
	}

	var job domain.Job
	if err := proto.Unmarshal(jobBytes, &job); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal job: %w", err)
	}

	if job.State != domain.StatePending {
		return nil, "", fmt.Errorf("%w: job in invalid state %s", domain.ErrInvalidState, job.State)
	}

	// Writes begin ONLY after all validations pass
	if err := c.Delete(); err != nil {
		return nil, "", fmt.Errorf("failed to remove from ready index: %w", err)
	}

	job.Attempts++
	job.WorkerId = workerID
	job.State = domain.StateLeased
	if leaseToken == "" {
		leaseToken = domain.FormatLeaseToken("node", job.Attempts, nowNano)
	}
	job.LeaseToken = leaseToken
	job.LeaseExpiresAt = nowNano + leaseDuration.Nanoseconds()

	updatedBytes, err := proto.Marshal(&job)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal leased job: %w", err)
	}

	if err := bJobs.Put([]byte(job.Id), updatedBytes); err != nil {
		return nil, "", fmt.Errorf("failed to update leased job in storage: %w", err)
	}

	lKey := leasedKey(job.LeaseExpiresAt, job.Id)
	if err := bLeased.Put(lKey, []byte(queue)); err != nil {
		return nil, "", fmt.Errorf("failed to put leased index: %w", err)
	}

	return &job, leaseToken, nil
}

// txAck marks a leased job as DONE inside an atomic transaction, enforcing lease fencing.
func txAck(tx *bbolt.Tx, jobID, leaseToken string) error {
	bJobs := tx.Bucket(BucketJobs)
	bLeased := tx.Bucket(BucketIdxLeased)

	jobBytes := bJobs.Get([]byte(jobID))
	if jobBytes == nil {
		return domain.ErrNotFound
	}

	var job domain.Job
	if err := proto.Unmarshal(jobBytes, &job); err != nil {
		return fmt.Errorf("failed to unmarshal job: %w", err)
	}

	if err := domain.ValidateLeaseFencing(&job, leaseToken); err != nil {
		return err
	}

	// Remove from idx_leased
	lKey := leasedKey(job.LeaseExpiresAt, job.Id)
	_ = bLeased.Delete(lKey)

	job.State = domain.StateDone
	job.LeaseToken = ""

	updatedBytes, err := proto.Marshal(&job)
	if err != nil {
		return fmt.Errorf("failed to marshal acked job: %w", err)
	}

	return bJobs.Put([]byte(job.Id), updatedBytes)
}

// txNack releases a leased job back to PENDING (with backoff) or DEAD inside an atomic transaction.
func txNack(tx *bbolt.Tx, jobID, leaseToken, errorMsg string, retryAfter time.Duration, nowNano int64) (*domain.Job, error) {
	bJobs := tx.Bucket(BucketJobs)
	bReady := tx.Bucket(BucketIdxReady)
	bSched := tx.Bucket(BucketIdxSched)
	bLeased := tx.Bucket(BucketIdxLeased)
	bDead := tx.Bucket(BucketIdxDead)

	jobBytes := bJobs.Get([]byte(jobID))
	if jobBytes == nil {
		return nil, domain.ErrNotFound
	}

	var job domain.Job
	if err := proto.Unmarshal(jobBytes, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	if err := domain.ValidateLeaseFencing(&job, leaseToken); err != nil {
		return nil, err
	}

	lKey := leasedKey(job.LeaseExpiresAt, job.Id)
	_ = bLeased.Delete(lKey)

	job.LastError = errorMsg
	job.LeaseToken = ""
	job.WorkerId = ""

	if job.Attempts < job.MaxAttempts {
		job.State = domain.StatePending
		job.RunAt = nowNano + retryAfter.Nanoseconds()

		updatedBytes, err := proto.Marshal(&job)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal nacked job: %w", err)
		}
		if err := bJobs.Put([]byte(job.Id), updatedBytes); err != nil {
			return nil, err
		}

		if job.RunAt <= nowNano {
			rKey := readyKey(job.Queue, uint8(job.Priority), job.CreatedAt, job.Id)
			if err := bReady.Put(rKey, nil); err != nil {
				return nil, err
			}
		} else {
			sKey := scheduledKey(job.RunAt, job.Id)
			if err := bSched.Put(sKey, []byte(job.Queue)); err != nil {
				return nil, err
			}
		}
		return &job, nil
	}

	// Max attempts reached -> DLQ
	job.State = domain.StateDead
	updatedBytes, err := proto.Marshal(&job)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dead job: %w", err)
	}
	if err := bJobs.Put([]byte(job.Id), updatedBytes); err != nil {
		return nil, err
	}

	dKey := deadKey(job.Queue, job.CreatedAt, job.Id)
	if err := bDead.Put(dKey, nil); err != nil {
		return nil, err
	}
	return &job, nil
}

// txExtendLease updates the lease expiration inside an atomic transaction.
func txExtendLease(tx *bbolt.Tx, jobID, leaseToken string, extraDuration time.Duration, nowNano int64) (int64, error) {
	bJobs := tx.Bucket(BucketJobs)
	bLeased := tx.Bucket(BucketIdxLeased)

	jobBytes := bJobs.Get([]byte(jobID))
	if jobBytes == nil {
		return 0, domain.ErrNotFound
	}

	var job domain.Job
	if err := proto.Unmarshal(jobBytes, &job); err != nil {
		return 0, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	if err := domain.ValidateLeaseFencing(&job, leaseToken); err != nil {
		return 0, err
	}

	oldLKey := leasedKey(job.LeaseExpiresAt, job.Id)
	_ = bLeased.Delete(oldLKey)

	newExpiresAt := job.LeaseExpiresAt + extraDuration.Nanoseconds()
	if newExpiresAt < nowNano {
		newExpiresAt = nowNano + extraDuration.Nanoseconds()
	}
	job.LeaseExpiresAt = newExpiresAt

	newLKey := leasedKey(job.LeaseExpiresAt, job.Id)
	if err := bLeased.Put(newLKey, []byte(job.Queue)); err != nil {
		return 0, err
	}

	updatedBytes, err := proto.Marshal(&job)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal job with extended lease: %w", err)
	}

	if err := bJobs.Put([]byte(job.Id), updatedBytes); err != nil {
		return 0, err
	}

	return newExpiresAt, nil
}

// txRequeueDead moves a job from DEAD back to PENDING inside an atomic transaction.
func txRequeueDead(tx *bbolt.Tx, jobID string, nowNano int64) (*domain.Job, error) {
	bJobs := tx.Bucket(BucketJobs)
	bReady := tx.Bucket(BucketIdxReady)
	bDead := tx.Bucket(BucketIdxDead)

	jobBytes := bJobs.Get([]byte(jobID))
	if jobBytes == nil {
		return nil, domain.ErrNotFound
	}

	var job domain.Job
	if err := proto.Unmarshal(jobBytes, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	if err := domain.CanRequeueDead(&job); err != nil {
		return nil, err
	}

	dKey := deadKey(job.Queue, job.CreatedAt, job.Id)
	_ = bDead.Delete(dKey)

	job.State = domain.StatePending
	job.Attempts = 0
	job.RunAt = nowNano
	job.LastError = ""
	job.LeaseToken = ""
	job.WorkerId = ""

	updatedBytes, err := proto.Marshal(&job)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal requeued job: %w", err)
	}

	if err := bJobs.Put([]byte(job.Id), updatedBytes); err != nil {
		return nil, err
	}

	rKey := readyKey(job.Queue, uint8(job.Priority), job.CreatedAt, job.Id)
	if err := bReady.Put(rKey, nil); err != nil {
		return nil, err
	}

	return &job, nil
}

type expiredLeaseItem struct {
	key       []byte
	expiresAt int64
	jobID     string
}

const (
	// MaxReapPerCommand bounds the number of expired leases reaped in one command.
	MaxReapPerCommand = 1000
	// MaxPromotePerCommand bounds the number of scheduled jobs promoted in one command.
	MaxPromotePerCommand = 1000
)

// txReapExpiredLeases sweeps expired leases in O(K) time without full bucket scans.
// Bound to at most MaxReapPerCommand (1000) jobs per invocation in deterministic key order.
func txReapExpiredLeases(tx *bbolt.Tx, nowNano int64, defaultBackoff func(attempts uint32) time.Duration) (int, error) {
	bJobs := tx.Bucket(BucketJobs)
	bReady := tx.Bucket(BucketIdxReady)
	bSched := tx.Bucket(BucketIdxSched)
	bLeased := tx.Bucket(BucketIdxLeased)
	bDead := tx.Bucket(BucketIdxDead)

	var expired []expiredLeaseItem
	c := bLeased.Cursor()

	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if len(expired) >= MaxReapPerCommand {
			break
		}
		expiresAt, jobID := extractLeaseExpiresAtAndJobID(k)
		if expiresAt > nowNano {
			// Big-endian index: all subsequent keys are in the future
			break
		}
		kCopy := make([]byte, len(k))
		copy(kCopy, k)
		expired = append(expired, expiredLeaseItem{
			key:       kCopy,
			expiresAt: expiresAt,
			jobID:     jobID,
		})
	}

	reaped := 0
	for _, item := range expired {
		jobBytes := bJobs.Get([]byte(item.jobID))
		if jobBytes == nil {
			continue
		}

		var job domain.Job
		if err := proto.Unmarshal(jobBytes, &job); err != nil {
			continue
		}

		if job.State != domain.StateLeased {
			continue
		}

		// Writes begin only after all reads and validations pass
		_ = bLeased.Delete(item.key)

		reaped++
		job.LeaseToken = ""
		job.WorkerId = ""
		job.LastError = "lease expired"

		if job.Attempts < job.MaxAttempts {
			job.State = domain.StatePending
			delay := defaultBackoff(job.Attempts)
			job.RunAt = nowNano + delay.Nanoseconds()

			updatedBytes, _ := proto.Marshal(&job)
			_ = bJobs.Put([]byte(job.Id), updatedBytes)

			if job.RunAt <= nowNano {
				rKey := readyKey(job.Queue, uint8(job.Priority), job.CreatedAt, job.Id)
				_ = bReady.Put(rKey, nil)
			} else {
				sKey := scheduledKey(job.RunAt, job.Id)
				_ = bSched.Put(sKey, []byte(job.Queue))
			}
		} else {
			job.State = domain.StateDead
			updatedBytes, _ := proto.Marshal(&job)
			_ = bJobs.Put([]byte(job.Id), updatedBytes)

			dKey := deadKey(job.Queue, job.CreatedAt, job.Id)
			_ = bDead.Put(dKey, nil)
		}
	}

	return reaped, nil
}

type scheduledItem struct {
	key   []byte
	runAt int64
	jobID string
	queue string
}

// txPromoteScheduled promotes scheduled jobs whose run_at <= nowNano to idx_ready.
func txPromoteScheduled(tx *bbolt.Tx, nowNano int64) (int, error) {
	bJobs := tx.Bucket(BucketJobs)
	bReady := tx.Bucket(BucketIdxReady)
	bSched := tx.Bucket(BucketIdxSched)

	var due []scheduledItem
	c := bSched.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(due) >= MaxPromotePerCommand {
			break
		}
		runAt, jobID := extractRunAtAndJobIDFromScheduledKey(k)
		if runAt > nowNano {
			// Big-endian index: all subsequent keys are in the future
			break
		}
		kCopy := make([]byte, len(k))
		copy(kCopy, k)
		due = append(due, scheduledItem{
			key:   kCopy,
			runAt: runAt,
			jobID: jobID,
			queue: string(v),
		})
	}

	promoted := 0
	for _, item := range due {
		jobBytes := bJobs.Get([]byte(item.jobID))
		if jobBytes == nil {
			continue
		}

		var job domain.Job
		if err := proto.Unmarshal(jobBytes, &job); err != nil {
			continue
		}

		if job.State != domain.StatePending {
			continue
		}

		// Writes begin only after all reads and validations pass
		_ = bSched.Delete(item.key)

		promoted++
		rKey := readyKey(job.Queue, uint8(job.Priority), job.CreatedAt, job.Id)
		_ = bReady.Put(rKey, nil)
	}

	return promoted, nil
}

// txStats gathers queue depth statistics.
func txStats(tx *bbolt.Tx, queue string) (*domain.QueueStats, error) {
	bReady := tx.Bucket(BucketIdxReady)
	bSched := tx.Bucket(BucketIdxSched)
	bLeased := tx.Bucket(BucketIdxLeased)
	bDead := tx.Bucket(BucketIdxDead)
	bJobs := tx.Bucket(BucketJobs)

	stats := &domain.QueueStats{Queue: queue}

	// Count ready pending jobs
	readyPref := readyPrefix(queue)
	cReady := bReady.Cursor()
	for k, _ := cReady.Seek(readyPref); k != nil && bytes.HasPrefix(k, readyPref); k, _ = cReady.Next() {
		stats.PendingCount++
	}

	// Count scheduled pending jobs for this queue
	cSched := bSched.Cursor()
	for k, v := cSched.First(); k != nil; k, v = cSched.Next() {
		if string(v) == queue {
			stats.PendingCount++
		}
	}

	// Count leased jobs
	cLeased := bLeased.Cursor()
	for k, v := cLeased.First(); k != nil; k, v = cLeased.Next() {
		if string(v) == queue {
			stats.LeasedCount++
		}
	}

	// Count dead jobs
	deadPref := deadPrefix(queue)
	cDead := bDead.Cursor()
	for k, _ := cDead.Seek(deadPref); k != nil && bytes.HasPrefix(k, deadPref); k, _ = cDead.Next() {
		stats.DeadCount++
	}

	// Count done jobs from jobs bucket
	cJobs := bJobs.Cursor()
	for k, v := cJobs.First(); k != nil; k, v = cJobs.Next() {
		var j domain.Job
		if err := proto.Unmarshal(v, &j); err == nil {
			if j.Queue == queue && j.State == domain.StateDone {
				stats.DoneCount++
			}
		}
	}

	return stats, nil
}

// txListDead returns up to limit jobs in DEAD state for the queue.
func txListDead(tx *bbolt.Tx, queue string, limit uint32) ([]*domain.Job, error) {
	if limit == 0 {
		limit = 50
	}
	bJobs := tx.Bucket(BucketJobs)
	bDead := tx.Bucket(BucketIdxDead)

	var deadJobs []*domain.Job
	prefix := deadPrefix(queue)
	c := bDead.Cursor()

	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix) && uint32(len(deadJobs)) < limit; k, _ = c.Next() {
		jobID := extractJobIDFromDeadKey(len(queue), k)
		jobBytes := bJobs.Get([]byte(jobID))
		if jobBytes != nil {
			var job domain.Job
			if err := proto.Unmarshal(jobBytes, &job); err == nil {
				deadJobs = append(deadJobs, &job)
			}
		}
	}

	return deadJobs, nil
}

// txGetJob returns a job by ID from the jobs bucket.
func txGetJob(tx *bbolt.Tx, jobID string) (*domain.Job, error) {
	bJobs := tx.Bucket(BucketJobs)
	jobBytes := bJobs.Get([]byte(jobID))
	if jobBytes == nil {
		return nil, domain.ErrNotFound
	}
	var job domain.Job
	if err := proto.Unmarshal(jobBytes, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}
	return &job, nil
}

// SetLastAppliedNow records the highest applied timestamp in the meta bucket.
func SetLastAppliedNow(tx *bbolt.Tx, nowNano int64) error {
	b := tx.Bucket(BucketMeta)
	if b == nil {
		return fmt.Errorf("meta bucket does not exist")
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(nowNano))
	return b.Put(KeyLastAppliedNow, buf[:])
}

// GetLastAppliedNow retrieves the highest applied timestamp from the meta bucket.
func GetLastAppliedNow(tx *bbolt.Tx) (int64, error) {
	b := tx.Bucket(BucketMeta)
	if b == nil {
		return 0, nil
	}
	val := b.Get(KeyLastAppliedNow)
	if len(val) < 8 {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(val)), nil
}

// Exported transaction helpers for Raft FSM execution
func TxEnqueue(tx *bbolt.Tx, params *EnqueueParams, nowNano int64) (*domain.Job, bool, error) {
	return txEnqueue(tx, params, nowNano)
}

func TxReserve(tx *bbolt.Tx, queue, workerID string, leaseDuration time.Duration, nowNano int64, leaseToken string) (*domain.Job, string, error) {
	return txReserve(tx, queue, workerID, leaseDuration, nowNano, leaseToken)
}

func TxAck(tx *bbolt.Tx, jobID, leaseToken string) error {
	return txAck(tx, jobID, leaseToken)
}

func TxNack(tx *bbolt.Tx, jobID, leaseToken, errorMsg string, retryAfter time.Duration, nowNano int64) (*domain.Job, error) {
	return txNack(tx, jobID, leaseToken, errorMsg, retryAfter, nowNano)
}

func TxExtendLease(tx *bbolt.Tx, jobID, leaseToken string, extraDuration time.Duration, nowNano int64) (int64, error) {
	return txExtendLease(tx, jobID, leaseToken, extraDuration, nowNano)
}

func TxRequeueDead(tx *bbolt.Tx, jobID string, nowNano int64) (*domain.Job, error) {
	return txRequeueDead(tx, jobID, nowNano)
}

func TxReapExpiredLeases(tx *bbolt.Tx, nowNano int64, defaultBackoff func(attempts uint32) time.Duration) (int, error) {
	if defaultBackoff == nil {
		defaultBackoff = func(attempts uint32) time.Duration {
			return domain.CalculateDeterministicBackoff("default", attempts, domain.DefaultBaseDelay, domain.DefaultMaxDelay)
		}
	}
	return txReapExpiredLeases(tx, nowNano, defaultBackoff)
}

func TxPromoteScheduled(tx *bbolt.Tx, nowNano int64) (int, error) {
	return txPromoteScheduled(tx, nowNano)
}

func TxGetJob(tx *bbolt.Tx, jobID string) (*domain.Job, error) {
	return txGetJob(tx, jobID)
}

func TxStats(tx *bbolt.Tx, queue string) (*domain.QueueStats, error) {
	return txStats(tx, queue)
}

func TxListDead(tx *bbolt.Tx, queue string, limit uint32) ([]*domain.Job, error) {
	return txListDead(tx, queue, limit)
}

func txStatsAll(tx *bbolt.Tx) (map[string]*domain.QueueStats, error) {
	queues := make(map[string]bool)

	bReady := tx.Bucket(BucketIdxReady)
	cReady := bReady.Cursor()
	for k, _ := cReady.First(); k != nil; k, _ = cReady.Next() {
		idx := bytes.IndexByte(k, 0x00)
		if idx > 0 {
			queues[string(k[:idx])] = true
		}
	}

	bSched := tx.Bucket(BucketIdxSched)
	cSched := bSched.Cursor()
	for k, v := cSched.First(); k != nil; k, v = cSched.Next() {
		if len(v) > 0 {
			queues[string(v)] = true
		}
	}

	bLeased := tx.Bucket(BucketIdxLeased)
	cLeased := bLeased.Cursor()
	for k, v := cLeased.First(); k != nil; k, v = cLeased.Next() {
		if len(v) > 0 {
			queues[string(v)] = true
		}
	}

	bDead := tx.Bucket(BucketIdxDead)
	cDead := bDead.Cursor()
	for k, _ := cDead.First(); k != nil; k, _ = cDead.Next() {
		idx := bytes.IndexByte(k, 0x00)
		if idx > 0 {
			queues[string(k[:idx])] = true
		}
	}

	bJobs := tx.Bucket(BucketJobs)
	cJobs := bJobs.Cursor()
	for k, v := cJobs.First(); k != nil; k, v = cJobs.Next() {
		var job domain.Job
		if err := proto.Unmarshal(v, &job); err == nil && job.Queue != "" {
			queues[job.Queue] = true
		}
	}

	allStats := make(map[string]*domain.QueueStats)
	for q := range queues {
		s, err := txStats(tx, q)
		if err == nil {
			allStats[q] = s
		}
	}
	return allStats, nil
}

func TxStatsAll(tx *bbolt.Tx) (map[string]*domain.QueueStats, error) {
	return txStatsAll(tx)
}
