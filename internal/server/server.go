package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/cluster"
	"github.com/shubhamkr585/relay/internal/domain"
)

// ServerConfig contains configuration options for a Relay server node.
type ServerConfig struct {
	NodeID       string
	GRPCAddr     string
	HTTPAddr     string
	PeerResolver PeerResolver
}

// Server implements the Relay gRPC service, transparent leader forwarding,
// HTTP health/metrics endpoints, and lifecycle management.
type Server struct {
	relaypb.UnimplementedRelayServiceServer

	nodeID    string
	raft      *raft.Raft
	fsm       *cluster.FSM
	batcher   *cluster.LeaderBatcher
	forwarder *Forwarder
	metrics   *Metrics
	logger    *slog.Logger

	grpcServer *grpc.Server
	httpServer *http.Server

	grpcListener net.Listener
	httpListener net.Listener

	mu     sync.Mutex
	closed bool
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewServer initializes a new Relay Server.
func NewServer(
	cfg ServerConfig,
	r *raft.Raft,
	fsm *cluster.FSM,
	batcher *cluster.LeaderBatcher,
	metrics *Metrics,
	logger *slog.Logger,
) *Server {
	if metrics == nil {
		metrics = NewMetrics()
	}
	if logger == nil {
		logger = slog.Default()
	}

	forwarder := NewForwarder(cfg.NodeID, r, cfg.PeerResolver)

	s := &Server{
		nodeID:    cfg.NodeID,
		raft:      r,
		fsm:       fsm,
		batcher:   batcher,
		forwarder: forwarder,
		metrics:   metrics,
		logger:    logger,
		stopCh:    make(chan struct{}),
	}

	// Create gRPC server with unary logging and metrics interceptor
	s.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(UnaryLoggingAndMetricsInterceptor(logger, metrics)),
	)
	relaypb.RegisterRelayServiceServer(s.grpcServer, s)

	if s.fsm != nil {
		s.metrics.RegisterFSM(s.fsm)
	}
	if s.batcher != nil {
		s.batcher.SetOnLeasesReaped(func(count int) {
			s.metrics.IncLeaseExpiries("", count)
		})
	}

	// Create HTTP server for /metrics, /healthz, /readyz, /join
	s.httpServer = &http.Server{
		Handler: s.SetupHTTPHandler(),
	}

	// Start Raft leadership observer for metrics
	if s.raft != nil {
		s.wg.Add(1)
		go s.observeRaftLeadership()
	}

	return s
}

// Start binds listeners and starts the gRPC and HTTP servers.
func (s *Server) Start(grpcAddr, httpAddr string) error {
	var err error
	s.grpcListener, err = net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC addr %s: %w", grpcAddr, err)
	}

	s.httpListener, err = net.Listen("tcp", httpAddr)
	if err != nil {
		_ = s.grpcListener.Close()
		return fmt.Errorf("failed to listen on HTTP addr %s: %w", httpAddr, err)
	}

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		if err := s.grpcServer.Serve(s.grpcListener); err != nil && err != grpc.ErrServerStopped {
			s.logger.Error("gRPC server terminated with error", slog.String("error", err.Error()))
		}
	}()

	go func() {
		defer s.wg.Done()
		if err := s.httpServer.Serve(s.httpListener); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server terminated with error", slog.String("error", err.Error()))
		}
	}()

	s.logger.Info("Relay node server started",
		slog.String("node_id", s.nodeID),
		slog.String("grpc_addr", s.grpcListener.Addr().String()),
		slog.String("http_addr", s.httpListener.Addr().String()),
	)

	return nil
}

// GRPCAddr returns the local address the gRPC server is listening on.
func (s *Server) GRPCAddr() string {
	if s.grpcListener != nil {
		return s.grpcListener.Addr().String()
	}
	return ""
}

// HTTPAddr returns the local address the HTTP server is listening on.
func (s *Server) HTTPAddr() string {
	if s.httpListener != nil {
		return s.httpListener.Addr().String()
	}
	return ""
}

// Metrics returns the server's metrics instance.
func (s *Server) Metrics() *Metrics {
	return s.metrics
}

// observeRaftLeadership monitors Raft leader transition events.
func (s *Server) observeRaftLeadership() {
	defer s.wg.Done()
	leaderCh := s.raft.LeaderCh()

	for {
		select {
		case <-s.stopCh:
			return
		case isLeader, ok := <-leaderCh:
			if !ok {
				return
			}
			s.metrics.SetIsLeader(isLeader)
			s.metrics.RaftLeaderChanges.Inc()
			s.logger.Info("Raft leadership changed", slog.Bool("is_leader", isLeader))
		}
	}
}

// Enqueue enqueues a new job through the Raft leader.
func (s *Server) Enqueue(ctx context.Context, req *relaypb.EnqueueRequest) (*relaypb.EnqueueResponse, error) {
	if s.raft.State() != raft.Leader {
		return s.forwarder.ForwardEnqueue(ctx, req)
	}

	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue cannot be empty")
	}
	if len(req.Payload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "payload cannot be empty")
	}

	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}

	delay := time.Duration(req.DelayMs) * time.Millisecond

	job, dedup, err := s.batcher.Enqueue(ctx, req.Queue, req.Payload, req.Priority, delay, req.IdempotencyKey, maxAttempts)
	if err != nil {
		return nil, MapError(err)
	}

	s.metrics.IncEnqueued(req.Queue, dedup)

	return &relaypb.EnqueueResponse{
		JobId:        job.Id,
		Deduplicated: dedup,
	}, nil
}

// Reserve leases a ready job through the Raft leader.
func (s *Server) Reserve(ctx context.Context, req *relaypb.ReserveRequest) (*relaypb.ReserveResponse, error) {
	if s.raft.State() != raft.Leader {
		return s.forwarder.ForwardReserve(ctx, req)
	}

	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue cannot be empty")
	}

	// Phase 2 Rule: Peek local state first! Return NOT_FOUND without proposing if empty.
	if !s.fsm.PeekReady(req.Queue) {
		return nil, status.Error(codes.NotFound, "queue is empty")
	}

	leaseDuration := time.Duration(req.LeaseDurationMs) * time.Millisecond
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	job, token, err := s.batcher.Reserve(ctx, req.Queue, req.WorkerId, leaseDuration)
	if err != nil {
		return nil, MapError(err)
	}

	s.metrics.IncReserved(req.Queue)

	return &relaypb.ReserveResponse{
		Job:        domainJobToProto(job),
		LeaseToken: token,
	}, nil
}

// Ack acknowledges completion of a leased job.
func (s *Server) Ack(ctx context.Context, req *relaypb.AckRequest) (*relaypb.AckResponse, error) {
	if s.raft.State() != raft.Leader {
		return s.forwarder.ForwardAck(ctx, req)
	}

	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id cannot be empty")
	}
	if req.LeaseToken == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_token cannot be empty")
	}

	err := s.batcher.Ack(ctx, req.JobId, req.LeaseToken)
	if err != nil {
		mapped := MapError(err)
		if status.Code(mapped) == codes.FailedPrecondition {
			s.metrics.IncStaleLeaseRejections()
		}
		return nil, mapped
	}

	s.metrics.IncAcked()

	return &relaypb.AckResponse{}, nil
}

// Nack returns a leased job to pending (with backoff) or moves it to dead.
func (s *Server) Nack(ctx context.Context, req *relaypb.NackRequest) (*relaypb.NackResponse, error) {
	if s.raft.State() != raft.Leader {
		return s.forwarder.ForwardNack(ctx, req)
	}

	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id cannot be empty")
	}
	if req.LeaseToken == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_token cannot be empty")
	}

	retryAfter := time.Duration(req.RetryAfterMs) * time.Millisecond

	job, err := s.batcher.Nack(ctx, req.JobId, req.LeaseToken, req.ErrorMsg, retryAfter)
	if err != nil {
		mapped := MapError(err)
		if status.Code(mapped) == codes.FailedPrecondition {
			s.metrics.IncStaleLeaseRejections()
		}
		return nil, mapped
	}

	s.metrics.IncNacked()
	if job != nil {
		if job.State == domain.StateDead {
			s.metrics.IncDead(job.Queue)
		} else if job.State == domain.StatePending {
			s.metrics.IncRetried(job.Queue)
		}
	}

	return &relaypb.NackResponse{}, nil
}

// ExtendLease increases lease expiration for an active lease.
func (s *Server) ExtendLease(ctx context.Context, req *relaypb.ExtendLeaseRequest) (*relaypb.ExtendLeaseResponse, error) {
	if s.raft.State() != raft.Leader {
		return s.forwarder.ForwardExtendLease(ctx, req)
	}

	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id cannot be empty")
	}
	if req.LeaseToken == "" {
		return nil, status.Error(codes.InvalidArgument, "lease_token cannot be empty")
	}
	if req.ExtraDurationMs <= 0 {
		return nil, status.Error(codes.InvalidArgument, "extra_duration_ms must be positive")
	}

	extra := time.Duration(req.ExtraDurationMs) * time.Millisecond

	newExpiresAt, err := s.batcher.ExtendLease(ctx, req.JobId, req.LeaseToken, extra)
	if err != nil {
		mapped := MapError(err)
		if status.Code(mapped) == codes.FailedPrecondition {
			s.metrics.IncStaleLeaseRejections()
		}
		return nil, mapped
	}

	return &relaypb.ExtendLeaseResponse{
		NewLeaseExpiresAt: newExpiresAt,
	}, nil
}

// RequeueDead moves a job from DEAD back to PENDING.
func (s *Server) RequeueDead(ctx context.Context, req *relaypb.RequeueDeadRequest) (*relaypb.RequeueDeadResponse, error) {
	if s.raft.State() != raft.Leader {
		return s.forwarder.ForwardRequeueDead(ctx, req)
	}

	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id cannot be empty")
	}

	job, err := s.batcher.RequeueDead(ctx, req.JobId)
	if err != nil {
		return nil, MapError(err)
	}

	return &relaypb.RequeueDeadResponse{
		Job: domainJobToProto(job),
	}, nil
}

// Stats returns queue statistics directly from local FSM state.
// Note: Stats are served locally and may be slightly stale on followers.
func (s *Server) Stats(ctx context.Context, req *relaypb.StatsRequest) (*relaypb.StatsResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue cannot be empty")
	}

	stats, err := s.fsm.Stats(req.Queue)
	if err != nil {
		return nil, MapError(err)
	}

	return &relaypb.StatsResponse{
		PendingCount: stats.PendingCount,
		LeasedCount:  stats.LeasedCount,
		DoneCount:    stats.DoneCount,
		DeadCount:    stats.DeadCount,
	}, nil
}

// ListDead returns dead-lettered jobs directly from local FSM state.
// Note: ListDead is served locally and may be slightly stale on followers.
func (s *Server) ListDead(ctx context.Context, req *relaypb.ListDeadRequest) (*relaypb.ListDeadResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue cannot be empty")
	}

	limit := req.Limit
	if limit == 0 {
		limit = 100
	}

	deadJobs, err := s.fsm.ListDead(req.Queue, limit)
	if err != nil {
		return nil, MapError(err)
	}

	protoJobs := make([]*relaypb.Job, len(deadJobs))
	for i, j := range deadJobs {
		protoJobs[i] = domainJobToProto(j)
	}

	return &relaypb.ListDeadResponse{
		Jobs: protoJobs,
	}, nil
}

// Join adds a new voter node to the Raft cluster idempotently.
func (s *Server) Join(ctx context.Context, req *relaypb.JoinRequest) (*relaypb.JoinResponse, error) {
	if s.raft.State() != raft.Leader {
		return s.forwarder.ForwardJoin(ctx, req)
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id cannot be empty")
	}
	if req.RaftAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "raft_address cannot be empty")
	}

	cfgFuture := s.raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return nil, MapError(err)
	}

	// Idempotency: verify if already part of configuration
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(req.NodeId) {
			if srv.Address == raft.ServerAddress(req.RaftAddress) {
				s.logger.Info("Node already part of Raft cluster (idempotent join)",
					slog.String("node_id", req.NodeId),
					slog.String("raft_address", req.RaftAddress),
				)
				return &relaypb.JoinResponse{Success: true}, nil
			}
			// Address changed, remove first
			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return nil, MapError(err)
			}
			break
		}
	}

	addFuture := s.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddress), 0, 0)
	if err := addFuture.Error(); err != nil {
		return nil, MapError(err)
	}

	s.logger.Info("Successfully added voter to Raft cluster",
		slog.String("node_id", req.NodeId),
		slog.String("raft_address", req.RaftAddress),
	)

	return &relaypb.JoinResponse{Success: true}, nil
}

// Stop gracefully shuts down gRPC and HTTP servers and closes cached forwarder connections.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.stopCh)
	s.mu.Unlock()

	// Stop accepting new gRPC calls and wait for in-flight requests
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	// Stop HTTP server
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(ctx)
	}

	// Close forwarder client connections
	if s.forwarder != nil {
		_ = s.forwarder.Close()
	}

	s.wg.Wait()
	s.logger.Info("Relay node server stopped gracefully", slog.String("node_id", s.nodeID))
}

// domainJobToProto converts a domain.Job model to its protobuf representation.
func domainJobToProto(j *domain.Job) *relaypb.Job {
	if j == nil {
		return nil
	}

	var state relaypb.JobState
	switch j.State {
	case domain.StatePending:
		state = relaypb.JobState_JOB_STATE_PENDING
	case domain.StateLeased:
		state = relaypb.JobState_JOB_STATE_LEASED
	case domain.StateDone:
		state = relaypb.JobState_JOB_STATE_DONE
	case domain.StateDead:
		state = relaypb.JobState_JOB_STATE_DEAD
	default:
		state = relaypb.JobState_JOB_STATE_UNSPECIFIED
	}

	return &relaypb.Job{
		Id:             j.Id,
		Queue:          j.Queue,
		Payload:        j.Payload,
		Priority:       uint32(j.Priority),
		State:          state,
		Attempts:       uint32(j.Attempts),
		MaxAttempts:    uint32(j.MaxAttempts),
		RunAt:          j.RunAt,
		LeaseExpiresAt: j.LeaseExpiresAt,
		LeaseToken:     j.LeaseToken,
		WorkerId:       j.WorkerId,
		IdempotencyKey: j.IdempotencyKey,
		CreatedAt:      j.CreatedAt,
		LastError:      j.LastError,
	}
}
