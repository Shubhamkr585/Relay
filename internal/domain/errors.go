package domain

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrNotFound is returned when the target job does not exist in storage.
	ErrNotFound = errors.New("job not found")

	// ErrStaleLease is returned when an Ack, Nack, or ExtendLease operation presents
	// an invalid/mismatched lease token or targets a job not currently in LEASED state.
	// Per spec, this maps strictly to gRPC FAILED_PRECONDITION.
	ErrStaleLease = errors.New("stale lease token or job not in leased state")

	// ErrInvalidState is returned when an illegal state transition is attempted.
	ErrInvalidState = errors.New("invalid job state transition")

	// ErrInvalidArguments is returned when required parameters are missing or out of bounds.
	ErrInvalidArguments = errors.New("invalid arguments")

	// ErrQueueEmpty is returned when no ready job is available for reservation.
	ErrQueueEmpty = errors.New("no ready jobs in queue")

	// ErrEngineClosed is returned when the storage engine is closed.
	ErrEngineClosed = errors.New("storage engine is closed")
)

// ToGRPCError maps domain errors to standard gRPC status errors.
func ToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrStaleLease):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrNotFound) || errors.Is(err, ErrQueueEmpty):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrInvalidArguments) || errors.Is(err, ErrInvalidState):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
