package cluster

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	bbolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/domain"
	"github.com/shubhamkr585/relay/internal/store"
)

// SubCommandResult captures the outcome of a single sub-command inside a CommandBatch.
type SubCommandResult struct {
	Job          *domain.Job
	Deduplicated bool
	LeaseToken   string
	NewExpiresAt int64
	Reaped       int
	Promoted     int
	Err          error
}

// BatchApplyResult is returned by FSM.Apply when processing a CommandBatch.
type BatchApplyResult struct {
	Results []SubCommandResult
}

// FSM wraps the bbolt-backed job state store and executes deterministic state transitions.
type FSM struct {
	mu             sync.RWMutex
	dbPath         string
	db             *bbolt.DB
	lastAppliedNow int64
}

// NewFSM creates a fresh FSM instance.
// Per B4 rules: On every node startup, the FSM db file is deleted and recreated fresh.
// State is rebuilt exclusively from snapshot restore + Raft log replay.
func NewFSM(dbPath string) (*FSM, error) {
	// Wipe any existing stale FSM file per specification
	_ = os.Remove(dbPath)

	bboltOpts := bbolt.DefaultOptions
	bboltOpts.NoSync = true // Raft WAL already fsyncs; FSM uses NoSync=true

	db, err := bbolt.Open(dbPath, 0600, bboltOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open fsm bbolt db at %s: %w", dbPath, err)
	}

	// Initialize all buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, bName := range store.AllBuckets {
			if _, err := tx.CreateBucketIfNotExists(bName); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &FSM{
		dbPath: dbPath,
		db:     db,
	}, nil
}

// DB returns the underlying bbolt database (used for peek read transactions).
func (f *FSM) DB() *bbolt.DB {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.db
}

// LastAppliedNow returns the highest timestamp processed by the FSM.
func (f *FSM) LastAppliedNow() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastAppliedNow
}

// Apply applies a committed Raft log entry deterministically.
// B1 Rule: ZERO wall-clock time.Now() or math/rand inside Apply.
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd relaypb.RaftCommand
	if err := proto.Unmarshal(log.Data, &cmd); err != nil {
		return &BatchApplyResult{
			Results: []SubCommandResult{{Err: fmt.Errorf("failed to unmarshal raft command: %w", err)}},
		}
	}

	// Compute local now := max(f.lastAppliedNow, batch leader time)
	now := f.lastAppliedNow
	if cmd.TimestampNano > now {
		now = cmd.TimestampNano
	}

	var subCmds []*relaypb.RaftCommand
	if cmd.Type == relaypb.CommandType_COMMAND_TYPE_BATCH {
		subCmds = cmd.SubCommands
	} else {
		subCmds = []*relaypb.RaftCommand{&cmd}
	}

	results := make([]SubCommandResult, len(subCmds))

	err := f.db.Update(func(tx *bbolt.Tx) error {
		// Update lastAppliedNow in the persistent meta bucket
		if err := store.SetLastAppliedNow(tx, now); err != nil {
			return err
		}

		for i, sub := range subCmds {
			var res SubCommandResult
			batchPosition := i

			if sub == nil {
				res.Err = fmt.Errorf("%w: nil sub-command in batch", domain.ErrInvalidArguments)
				results[i] = res
				continue
			}

			switch sub.Type {
			case relaypb.CommandType_COMMAND_TYPE_ENQUEUE:
				if sub.Job == nil {
					res.Err = fmt.Errorf("%w: enqueue requires non-nil Job", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				if sub.Job.Queue == "" {
					res.Err = fmt.Errorf("%w: enqueue requires non-empty queue", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				if sub.Job.Id == "" {
					res.Err = fmt.Errorf("%w: enqueue requires non-empty job ID", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				enqueueParams := &store.EnqueueParams{
					Queue:          sub.Job.Queue,
					Payload:        sub.Job.Payload,
					Priority:       sub.Job.Priority,
					IdempotencyKey: sub.Job.IdempotencyKey,
					MaxAttempts:    sub.Job.MaxAttempts,
					JobID:          sub.Job.Id,
					RunAt:          sub.Job.RunAt,
				}
				job, dedup, err := store.TxEnqueue(tx, enqueueParams, now)
				res.Job = job
				res.Deduplicated = dedup
				res.Err = err

			case relaypb.CommandType_COMMAND_TYPE_RESERVE:
				if sub.Queue == "" {
					res.Err = fmt.Errorf("%w: reserve requires non-empty queue", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				if sub.LeaseDurationMs <= 0 {
					res.Err = fmt.Errorf("%w: reserve requires positive lease duration", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				// B1 Rule: The leader cannot know the Raft log index at proposal time,
				// so the leader does NOT generate lease tokens. Inside Apply:
				leaseToken := fmt.Sprintf("%d-%d", log.Index, batchPosition)
				duration := time.Duration(sub.LeaseDurationMs) * time.Millisecond
				job, token, err := store.TxReserve(tx, sub.Queue, sub.WorkerId, duration, now, leaseToken)
				res.Job = job
				res.LeaseToken = token
				res.Err = err

			case relaypb.CommandType_COMMAND_TYPE_ACK:
				var jobID string
				if sub.Job != nil {
					jobID = sub.Job.Id
				}
				if jobID == "" {
					res.Err = fmt.Errorf("%w: ack requires non-empty job ID", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				if sub.LeaseToken == "" {
					res.Err = fmt.Errorf("%w: ack requires non-empty lease token", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				res.Err = store.TxAck(tx, jobID, sub.LeaseToken)

			case relaypb.CommandType_COMMAND_TYPE_NACK:
				var jobID string
				if sub.Job != nil {
					jobID = sub.Job.Id
				}
				if jobID == "" {
					res.Err = fmt.Errorf("%w: nack requires non-empty job ID", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				if sub.LeaseToken == "" {
					res.Err = fmt.Errorf("%w: nack requires non-empty lease token", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				retryAfter := time.Duration(sub.RetryAfterNano)
				job, err := store.TxNack(tx, jobID, sub.LeaseToken, sub.LastError, retryAfter, now)
				res.Job = job
				res.Err = err

			case relaypb.CommandType_COMMAND_TYPE_EXTEND_LEASE:
				var jobID string
				if sub.Job != nil {
					jobID = sub.Job.Id
				}
				if jobID == "" {
					res.Err = fmt.Errorf("%w: extend_lease requires non-empty job ID", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				if sub.LeaseToken == "" {
					res.Err = fmt.Errorf("%w: extend_lease requires non-empty lease token", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				if sub.ExtraDurationMs <= 0 {
					res.Err = fmt.Errorf("%w: extend_lease requires positive extra duration", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				extra := time.Duration(sub.ExtraDurationMs) * time.Millisecond
				expiresAt, err := store.TxExtendLease(tx, jobID, sub.LeaseToken, extra, now)
				res.NewExpiresAt = expiresAt
				res.Err = err

			case relaypb.CommandType_COMMAND_TYPE_REQUEUE_DEAD:
				var jobID string
				if sub.Job != nil {
					jobID = sub.Job.Id
				}
				if jobID == "" {
					res.Err = fmt.Errorf("%w: requeue_dead requires non-empty job ID", domain.ErrInvalidArguments)
					results[i] = res
					continue
				}
				job, err := store.TxRequeueDead(tx, jobID, now)
				res.Job = job
				res.Err = err

			case relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
				relaypb.CommandType_COMMAND_TYPE_EXPIRE_LEASES,
				relaypb.CommandType_COMMAND_TYPE_PROMOTE_SCHEDULED:
				promoted, errPromote := store.TxPromoteScheduled(tx, now)
				reaped, errReap := store.TxReapExpiredLeases(tx, now, nil)
				res.Promoted = promoted
				res.Reaped = reaped
				if errPromote != nil {
					res.Err = errPromote
				} else {
					res.Err = errReap
				}

			default:
				res.Err = fmt.Errorf("%w: unknown command type: %v", domain.ErrInvalidArguments, sub.Type)
			}

			// B3 Rule: A failed sub-command (stale lease, not found, etc.)
			// must NOT abort or roll back the other sub-commands in the transaction.
			results[i] = res
		}
		return nil
	})

	if err != nil {
		// Rule 1: Fail-stop panic. State is rebuilt from Raft log on restart.
		panic(fmt.Sprintf("FATAL: FSM.Apply failed to commit bbolt transaction: %v (fail-stop for crash-recovery)", err))
	}

	// Update in-memory lastAppliedNow ONLY after the transaction succeeds!
	f.lastAppliedNow = now

	return &BatchApplyResult{Results: results}
}

// PeekReady checks if there is any ready job for a queue without mutating state.
// B2 Rule: Used by the Leader to avoid empty Raft Reserve proposals.
func (f *FSM) PeekReady(queue string) bool {
	f.mu.RLock()
	db := f.db
	f.mu.RUnlock()

	ready := false
	_ = db.View(func(tx *bbolt.Tx) error {
		bReady := tx.Bucket(store.BucketIdxReady)
		if bReady == nil {
			return nil
		}
		c := bReady.Cursor()
		prefix := make([]byte, len(queue)+1)
		copy(prefix, []byte(queue))
		prefix[len(queue)] = 0x00

		k, _ := c.Seek(prefix)
		if k != nil && bytes.HasPrefix(k, prefix) {
			ready = true
		}
		return nil
	})
	return ready
}

// PeekRipeWork checks if there are expired leases or scheduled jobs due for promotion.
// B2 Rule: Used by the Leader ticker to avoid proposing empty promote-and-expire commands.
func (f *FSM) PeekRipeWork(nowNano int64) bool {
	f.mu.RLock()
	db := f.db
	f.mu.RUnlock()

	hasWork := false
	_ = db.View(func(tx *bbolt.Tx) error {
		// Check scheduled jobs
		bSched := tx.Bucket(store.BucketIdxSched)
		if bSched != nil {
			c := bSched.Cursor()
			k, _ := c.First()
			if k != nil && len(k) >= 8 {
				runAt, _ := store.ExtractRunAtAndJobIDFromScheduledKey(k)
				if runAt <= nowNano {
					hasWork = true
					return nil
				}
			}
		}

		// Check expired leases
		bLeased := tx.Bucket(store.BucketIdxLeased)
		if bLeased != nil {
			c := bLeased.Cursor()
			k, _ := c.First()
			if k != nil && len(k) >= 8 {
				expiresAt, _ := store.ExtractLeaseExpiresAtAndJobID(k)
				if expiresAt <= nowNano {
					hasWork = true
					return nil
				}
			}
		}
		return nil
	})
	return hasWork
}

// Snapshot implements raft.FSM.
// B4 Rule: Opens a cheap bbolt read transaction; Persist() streams with tx.WriteTo.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	tx, err := f.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to open read tx for snapshot: %w", err)
	}

	return &fsmSnapshot{tx: tx}, nil
}

type fsmSnapshot struct {
	tx *bbolt.Tx
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer s.tx.Rollback()

	if _, err := s.tx.WriteTo(sink); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("failed to stream bbolt snapshot to sink: %w", err)
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {
	_ = s.tx.Rollback()
}

// Restore implements raft.FSM.
// B4 Rule: Close and delete the current FSM db, stream new db from rc, restore lastAppliedNow.
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rc.Close()

	if f.db != nil {
		_ = f.db.Close()
	}
	_ = os.Remove(f.dbPath)

	tempPath := f.dbPath + ".restore.tmp"
	_ = os.Remove(tempPath)

	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create temp restore db file: %w", err)
	}

	if _, err := io.Copy(file, rc); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to stream snapshot into db file: %w", err)
	}
	_ = file.Close()

	if err := os.Rename(tempPath, f.dbPath); err != nil {
		return fmt.Errorf("failed to rename restored db file: %w", err)
	}

	bboltOpts := bbolt.DefaultOptions
	bboltOpts.NoSync = true

	db, err := bbolt.Open(f.dbPath, 0600, bboltOpts)
	if err != nil {
		return fmt.Errorf("failed to reopen restored bbolt db: %w", err)
	}
	f.db = db

	// Verify buckets and recover lastAppliedNow
	var lastNow int64
	err = db.View(func(tx *bbolt.Tx) error {
		var txErr error
		lastNow, txErr = store.GetLastAppliedNow(tx)
		return txErr
	})
	if err != nil {
		return fmt.Errorf("failed to read last applied now from restored db: %w", err)
	}

	f.lastAppliedNow = lastNow
	return nil
}

// Close closes the underlying bbolt database.
func (f *FSM) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.db != nil {
		err := f.db.Close()
		f.db = nil
		return err
	}
	return nil
}

// KVPair represents an exact key-value entry in a bucket.
type KVPair struct {
	Key   string
	Value string
}

// DumpState returns all buckets as sorted (key, value) pairs for byte-for-byte state equality testing.
func (f *FSM) DumpState() (map[string][]KVPair, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	state := make(map[string][]KVPair)

	err := f.db.View(func(tx *bbolt.Tx) error {
		for _, bName := range store.AllBuckets {
			b := tx.Bucket(bName)
			if b == nil {
				continue
			}
			var pairs []KVPair
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				pairs = append(pairs, KVPair{
					Key:   string(k),
					Value: string(v),
				})
			}
			state[string(bName)] = pairs
		}
		return nil
	})

	return state, err
}

// Stats returns queue depth statistics from local FSM state.
// May be slightly stale on followers.
func (f *FSM) Stats(queue string) (*domain.QueueStats, error) {
	f.mu.RLock()
	db := f.db
	f.mu.RUnlock()

	var stats *domain.QueueStats
	err := db.View(func(tx *bbolt.Tx) error {
		var err error
		stats, err = store.TxStats(tx, queue)
		return err
	})
	return stats, err
}

// StatsAll returns queue depth statistics for all known queues from local FSM state.
func (f *FSM) StatsAll() (map[string]*domain.QueueStats, error) {
	f.mu.RLock()
	db := f.db
	f.mu.RUnlock()

	var stats map[string]*domain.QueueStats
	err := db.View(func(tx *bbolt.Tx) error {
		var err error
		stats, err = store.TxStatsAll(tx)
		return err
	})
	return stats, err
}

// ListDead returns up to limit dead jobs for a queue from local FSM state.
// May be slightly stale on followers.
func (f *FSM) ListDead(queue string, limit uint32) ([]*domain.Job, error) {
	f.mu.RLock()
	db := f.db
	f.mu.RUnlock()

	var deadJobs []*domain.Job
	err := db.View(func(tx *bbolt.Tx) error {
		var err error
		deadJobs, err = store.TxListDead(tx, queue, limit)
		return err
	})
	return deadJobs, err
}

// GetJob returns a job by ID from local FSM state.
func (f *FSM) GetJob(jobID string) (*domain.Job, error) {
	f.mu.RLock()
	db := f.db
	f.mu.RUnlock()

	var job *domain.Job
	err := db.View(func(tx *bbolt.Tx) error {
		var err error
		job, err = store.TxGetJob(tx, jobID)
		return err
	})
	return job, err
}
