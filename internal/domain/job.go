package domain

import (
	"fmt"
	"time"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// Alias protobuf types to keep domain models clean and idiomatic
type Job = relaypb.Job
type JobState = relaypb.JobState

const (
	StateUnspecified = relaypb.JobState_JOB_STATE_UNSPECIFIED
	StatePending     = relaypb.JobState_JOB_STATE_PENDING
	StateLeased      = relaypb.JobState_JOB_STATE_LEASED
	StateDone        = relaypb.JobState_JOB_STATE_DONE
	StateDead        = relaypb.JobState_JOB_STATE_DEAD
)

// QueueStats holds aggregate counts for a queue across its states.
type QueueStats struct {
	Queue        string `json:"queue"`
	PendingCount int64  `json:"pending_count"`
	LeasedCount  int64  `json:"leased_count"`
	DoneCount    int64  `json:"done_count"`
	DeadCount    int64  `json:"dead_count"`
}

// ValidateNewJob checks that a newly submitted job has valid initial fields.
func ValidateNewJob(job *Job) error {
	if job == nil {
		return fmt.Errorf("%w: job cannot be nil", ErrInvalidArguments)
	}
	if job.Queue == "" {
		return fmt.Errorf("%w: queue name cannot be empty", ErrInvalidArguments)
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = 3 // default to 3 attempts if unspecified
	}
	if job.Priority > 255 {
		job.Priority = 255
	}
	return nil
}

// CanReserve checks if the job is eligible to be leased at time nowNano.
func CanReserve(job *Job, nowNano int64) error {
	if job.State != StatePending {
		return fmt.Errorf("%w: job %s is in state %s, expected PENDING", ErrInvalidState, job.Id, job.State)
	}
	if nowNano < job.RunAt {
		return fmt.Errorf("%w: job %s is scheduled for future run_at %d (current %d)", ErrInvalidState, job.Id, job.RunAt, nowNano)
	}
	return nil
}

// ValidateLeaseFencing checks that the job is currently LEASED and matches the leaseToken.
// Stale tokens or non-leased states MUST return ErrStaleLease.
func ValidateLeaseFencing(job *Job, leaseToken string) error {
	if job.State != StateLeased {
		return fmt.Errorf("%w: job %s is in state %s, expected LEASED", ErrStaleLease, job.Id, job.State)
	}
	if leaseToken == "" || job.LeaseToken != leaseToken {
		return fmt.Errorf("%w: token mismatch for job %s (expected %s, got %s)", ErrStaleLease, job.Id, job.LeaseToken, leaseToken)
	}
	return nil
}

// CanRequeueDead checks if a job in DLQ can be transitioned back to PENDING.
func CanRequeueDead(job *Job) error {
	if job.State != StateDead {
		return fmt.Errorf("%w: job %s is in state %s, expected DEAD", ErrInvalidState, job.Id, job.State)
	}
	return nil
}

// Helper to format a monotonic lease token.
// In Phase 1 (single node), it includes nano timestamp and attempt count.
// In Phase 2 (Raft), leader stamps log index: fmt.Sprintf("%d-%d", logIndex, attempts).
func FormatLeaseToken(nodeOrIndex string, attempts uint32, extraNano int64) string {
	if nodeOrIndex == "" {
		nodeOrIndex = "node"
	}
	return fmt.Sprintf("%s-%d-%d", nodeOrIndex, attempts, extraNano)
}

// CloneJob returns a deep copy of a protobuf Job to prevent race conditions across concurrent readers.
func CloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	payloadCopy := make([]byte, len(job.Payload))
	copy(payloadCopy, job.Payload)

	return &Job{
		Id:             job.Id,
		Queue:          job.Queue,
		Payload:        payloadCopy,
		Priority:       job.Priority,
		State:          job.State,
		Attempts:       job.Attempts,
		MaxAttempts:    job.MaxAttempts,
		RunAt:          job.RunAt,
		LeaseExpiresAt: job.LeaseExpiresAt,
		LeaseToken:     job.LeaseToken,
		WorkerId:       job.WorkerId,
		IdempotencyKey: job.IdempotencyKey,
		CreatedAt:      job.CreatedAt,
		LastError:      job.LastError,
	}
}

// FormatTime converts UnixNano to time.Time safely.
func NanoToTime(nano int64) time.Time {
	return time.Unix(0, nano)
}
