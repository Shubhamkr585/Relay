package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/shubhamkr585/relay/internal/domain"
)

func TestJobValidation(t *testing.T) {
	t.Run("nil job returns error", func(t *testing.T) {
		err := domain.ValidateNewJob(nil)
		require.ErrorIs(t, err, domain.ErrInvalidArguments)
	})

	t.Run("empty queue returns error", func(t *testing.T) {
		err := domain.ValidateNewJob(&domain.Job{Id: "1", Queue: ""})
		require.ErrorIs(t, err, domain.ErrInvalidArguments)
	})

	t.Run("valid job sets defaults", func(t *testing.T) {
		job := &domain.Job{Id: "1", Queue: "emails", Priority: 300}
		err := domain.ValidateNewJob(job)
		require.NoError(t, err)
		require.Equal(t, uint32(3), job.MaxAttempts)
		require.Equal(t, uint32(255), job.Priority) // clamped
	})
}

func TestStateTransitionsAndFencing(t *testing.T) {
	now := time.Now().UnixNano()

	tests := []struct {
		name          string
		job           *domain.Job
		token         string
		action        string
		expectedError error
	}{
		{
			name: "Reserve succeeds when job is pending and run_at <= now",
			job: &domain.Job{
				Id:    "job-1",
				Queue: "default",
				State: domain.StatePending,
				RunAt: now - 100,
			},
			action:        "reserve",
			expectedError: nil,
		},
		{
			name: "Reserve fails when job is scheduled in future",
			job: &domain.Job{
				Id:    "job-2",
				Queue: "default",
				State: domain.StatePending,
				RunAt: now + int64(10*time.Second),
			},
			action:        "reserve",
			expectedError: domain.ErrInvalidState,
		},
		{
			name: "Reserve fails when job is already LEASED",
			job: &domain.Job{
				Id:    "job-3",
				Queue: "default",
				State: domain.StateLeased,
				RunAt: now - 100,
			},
			action:        "reserve",
			expectedError: domain.ErrInvalidState,
		},
		{
			name: "Ack succeeds when token matches and state is LEASED",
			job: &domain.Job{
				Id:         "job-4",
				State:      domain.StateLeased,
				LeaseToken: "token-abc",
			},
			token:         "token-abc",
			action:        "ack",
			expectedError: nil,
		},
		{
			name: "Ack fails with ErrStaleLease when token mismatches",
			job: &domain.Job{
				Id:         "job-5",
				State:      domain.StateLeased,
				LeaseToken: "token-fresh",
			},
			token:         "token-stale",
			action:        "ack",
			expectedError: domain.ErrStaleLease,
		},
		{
			name: "Ack fails with ErrStaleLease when job is not LEASED",
			job: &domain.Job{
				Id:         "job-6",
				State:      domain.StatePending,
				LeaseToken: "token-abc",
			},
			token:         "token-abc",
			action:        "ack",
			expectedError: domain.ErrStaleLease,
		},
		{
			name: "RequeueDead succeeds when state is DEAD",
			job: &domain.Job{
				Id:    "job-7",
				State: domain.StateDead,
			},
			action:        "requeue_dead",
			expectedError: nil,
		},
		{
			name: "RequeueDead fails when state is not DEAD",
			job: &domain.Job{
				Id:    "job-8",
				State: domain.StatePending,
			},
			action:        "requeue_dead",
			expectedError: domain.ErrInvalidState,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			switch tc.action {
			case "reserve":
				err = domain.CanReserve(tc.job, now)
			case "ack":
				err = domain.ValidateLeaseFencing(tc.job, tc.token)
			case "requeue_dead":
				err = domain.CanRequeueDead(tc.job)
			}

			if tc.expectedError != nil {
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expectedError), "expected %v, got %v", tc.expectedError, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackoffMath(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := 10 * time.Second
	jitter := 25 * time.Millisecond

	// attempt 0: 100ms * 2^0 + 25ms = 125ms
	d0 := domain.CalculateBackoff(0, base, maxDelay, jitter)
	require.Equal(t, 125*time.Millisecond, d0)

	// attempt 1: 100ms * 2^1 + 25ms = 225ms
	d1 := domain.CalculateBackoff(1, base, maxDelay, jitter)
	require.Equal(t, 225*time.Millisecond, d1)

	// attempt 2: 100ms * 2^2 + 25ms = 425ms
	d2 := domain.CalculateBackoff(2, base, maxDelay, jitter)
	require.Equal(t, 425*time.Millisecond, d2)

	// attempt 3: 100ms * 2^3 + 25ms = 825ms
	d3 := domain.CalculateBackoff(3, base, maxDelay, jitter)
	require.Equal(t, 825*time.Millisecond, d3)

	// Large attempt clamped to maxDelay
	dLarge := domain.CalculateBackoff(50, base, maxDelay, jitter)
	require.Equal(t, maxDelay, dLarge)
}

func TestToGRPCErrorMapping(t *testing.T) {
	err := domain.ToGRPCError(domain.ErrStaleLease)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())

	errNotFound := domain.ToGRPCError(domain.ErrNotFound)
	require.Error(t, errNotFound)
	stNotFound, ok := status.FromError(errNotFound)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, stNotFound.Code())
}
