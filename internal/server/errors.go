package server

import (
	"context"
	"errors"
	"strings"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/shubhamkr585/relay/internal/cluster"
	"github.com/shubhamkr585/relay/internal/domain"
)

// MapError converts internal domain and cluster errors into standard gRPC status errors.
func MapError(err error) error {
	if err == nil {
		return nil
	}

	// Check if already a gRPC status error
	if _, ok := status.FromError(err); ok {
		return err
	}

	switch {
	// Stale lease or invalid state -> FAILED_PRECONDITION
	case errors.Is(err, domain.ErrStaleLease):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrInvalidState):
		return status.Error(codes.FailedPrecondition, err.Error())
	case strings.Contains(err.Error(), "stale lease token") || strings.Contains(err.Error(), "lease has expired"):
		return status.Error(codes.FailedPrecondition, err.Error())

	// No job ready or not found -> NOT_FOUND
	case errors.Is(err, domain.ErrQueueEmpty):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())

	// Not leader, leadership lost, or batcher closed -> UNAVAILABLE (retryable by client)
	case errors.Is(err, cluster.ErrNotLeader):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, cluster.ErrBatcherClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.Unavailable, "request deadline exceeded")
	case strings.Contains(err.Error(), "leadership lost") || strings.Contains(err.Error(), "not leader"):
		return status.Error(codes.Unavailable, err.Error())

	// Validation errors -> INVALID_ARGUMENT
	case errors.Is(err, domain.ErrInvalidArguments):
		return status.Error(codes.InvalidArgument, err.Error())

	default:
		return status.Error(codes.Internal, err.Error())
	}
}
