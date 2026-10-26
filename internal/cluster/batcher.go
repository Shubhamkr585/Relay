package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/domain"
)

var (
	// ErrNotLeader is returned when proposing to a node that is not the active Raft leader.
	ErrNotLeader = errors.New("node is not the raft leader")

	// ErrBatcherClosed is returned when the leader batcher has been stopped.
	ErrBatcherClosed = errors.New("leader batcher is closed")
)

/*
Distributed Systems Failure Ambiguity Note (per specification B3):
When a proposal is in flight and the Raft leader loses leadership mid-flight (ErrLeadershipLost / context deadline),
the state of the proposal is strictly AMBIGUOUS from the client's perspective: the proposal may or may not have reached
quorum before leadership was transferred.

How Relay guarantees correctness on client retry:
1. Enqueue: 100% safe to retry. The producer supplies an `idempotency_key`; if the command committed before leadership loss,
   the retry returns the existing job with `deduplicated = true` without creating duplicate work.
2. Reserve: If the lost proposal committed, a job was leased. If the client retries against the new leader, it may receive
   a different job. The previously leased job is NOT lost; if the worker never touches it, its lease expires and the
   new leader's Lease Reaper reclaims it back to PENDING or DLQ.
3. Ack / Nack / ExtendLease: If the lost proposal committed, the job transitioned (e.g. to DONE). On client retry against
   the new leader, the presented lease_token is now stale, returning `ErrStaleLease` (FAILED_PRECONDITION). Clients treating
   ErrStaleLease as terminal know the work was already settled or reassigned.
*/

// Proposal represents a single client operation submitted to the leader batcher.
type Proposal struct {
	cmd   *relaypb.RaftCommand
	resCh chan SubCommandResult
}

// LeaderBatcherConfig configures leader-side micro-batching and reaper tick parameters.
type LeaderBatcherConfig struct {
	BatchWindow    time.Duration // default 2ms
	BatchMaxSize   int           // default 64
	TickerInterval time.Duration // default 100ms
	ProposeTimeout time.Duration // default 5s
}

// DefaultLeaderBatcherConfig provides production-grade defaults.
func DefaultLeaderBatcherConfig() LeaderBatcherConfig {
	return LeaderBatcherConfig{
		BatchWindow:    2 * time.Millisecond,
		BatchMaxSize:   64,
		TickerInterval: 100 * time.Millisecond,
		ProposeTimeout: 5 * time.Second,
	}
}

// LeaderBatcher coalesces client mutating requests on the Raft Leader into single CommandBatch entries.
type LeaderBatcher struct {
	raft           *raft.Raft
	fsm            *FSM
	cfg            LeaderBatcherConfig
	proposalCh     chan *Proposal
	stopCh         chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	closed         bool
	onLeasesReaped func(count int)
}

// NewLeaderBatcher creates and starts the LeaderBatcher and its background maintenance ticker.
func NewLeaderBatcher(r *raft.Raft, fsm *FSM, cfg LeaderBatcherConfig) *LeaderBatcher {
	if cfg.BatchWindow <= 0 {
		cfg.BatchWindow = 2 * time.Millisecond
	}
	if cfg.BatchMaxSize <= 0 {
		cfg.BatchMaxSize = 64
	}
	if cfg.TickerInterval <= 0 {
		cfg.TickerInterval = 100 * time.Millisecond
	}
	if cfg.ProposeTimeout <= 0 {
		cfg.ProposeTimeout = 5 * time.Second
	}

	lb := &LeaderBatcher{
		raft:       r,
		fsm:        fsm,
		cfg:        cfg,
		proposalCh: make(chan *Proposal, 4096),
		stopCh:     make(chan struct{}),
	}

	lb.wg.Add(2)
	go lb.runBatcher()
	go lb.runTicker()

	return lb
}

// Enqueue submits an Enqueue proposal to the leader batcher.
func (lb *LeaderBatcher) Enqueue(ctx context.Context, queue string, payload []byte, priority uint32, delay time.Duration, idempotencyKey string, maxAttempts uint32) (*domain.Job, bool, error) {
	if lb.raft.State() != raft.Leader {
		return nil, false, ErrNotLeader
	}

	now := time.Now().UnixNano()
	runAt := now
	if delay > 0 {
		runAt = now + delay.Nanoseconds()
	}

	cmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: now,
		CommandId:     uuid.NewString(),
		Job: &relaypb.Job{
			Id:             uuid.NewString(),
			Queue:          queue,
			Payload:        payload,
			Priority:       priority,
			State:          relaypb.JobState_JOB_STATE_PENDING,
			Attempts:       0,
			MaxAttempts:    maxAttempts,
			RunAt:          runAt,
			IdempotencyKey: idempotencyKey,
			CreatedAt:      now,
		},
	}

	res, err := lb.submit(ctx, cmd)
	if err != nil {
		return nil, false, err
	}
	return res.Job, res.Deduplicated, res.Err
}

// Reserve leases the highest priority ready job.
// B2 Rule: Peeks local FSM state first. If nothing is ready, returns domain.ErrQueueEmpty without proposing.
func (lb *LeaderBatcher) Reserve(ctx context.Context, queue, workerID string, leaseDuration time.Duration) (*domain.Job, string, error) {
	if lb.raft.State() != raft.Leader {
		return nil, "", ErrNotLeader
	}

	// B2: Peek local state first to prevent empty proposals in Raft log
	if !lb.fsm.PeekReady(queue) {
		return nil, "", domain.ErrQueueEmpty
	}

	now := time.Now().UnixNano()
	cmd := &relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
		TimestampNano:   now,
		CommandId:       uuid.NewString(),
		Queue:           queue,
		WorkerId:        workerID,
		LeaseDurationMs: leaseDuration.Milliseconds(),
	}

	res, err := lb.submit(ctx, cmd)
	if err != nil {
		return nil, "", err
	}
	return res.Job, res.LeaseToken, res.Err
}

// Ack acknowledges completion of a leased job.
func (lb *LeaderBatcher) Ack(ctx context.Context, jobID, leaseToken string) error {
	if lb.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	cmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
		TimestampNano: time.Now().UnixNano(),
		CommandId:     uuid.NewString(),
		Job:           &relaypb.Job{Id: jobID},
		LeaseToken:    leaseToken,
	}

	res, err := lb.submit(ctx, cmd)
	if err != nil {
		return err
	}
	return res.Err
}

// SetOnLeasesReaped registers a callback invoked when the ticker reaps expired leases.
func (lb *LeaderBatcher) SetOnLeasesReaped(fn func(count int)) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.onLeasesReaped = fn
}

// Nack returns a job to PENDING (with backoff) or moves to DEAD.
func (lb *LeaderBatcher) Nack(ctx context.Context, jobID, leaseToken, lastError string, retryAfter time.Duration) (*domain.Job, error) {
	if lb.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	cmd := &relaypb.RaftCommand{
		Type:           relaypb.CommandType_COMMAND_TYPE_NACK,
		TimestampNano:  time.Now().UnixNano(),
		CommandId:      uuid.NewString(),
		Job:            &relaypb.Job{Id: jobID},
		LeaseToken:     leaseToken,
		LastError:      lastError,
		RetryAfterNano: retryAfter.Nanoseconds(),
	}

	res, err := lb.submit(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.Job, res.Err
}

// ExtendLease increases lease expiration for an active lease.
func (lb *LeaderBatcher) ExtendLease(ctx context.Context, jobID, leaseToken string, extraDuration time.Duration) (int64, error) {
	if lb.raft.State() != raft.Leader {
		return 0, ErrNotLeader
	}

	cmd := &relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_EXTEND_LEASE,
		TimestampNano:   time.Now().UnixNano(),
		CommandId:       uuid.NewString(),
		Job:             &relaypb.Job{Id: jobID},
		LeaseToken:      leaseToken,
		ExtraDurationMs: extraDuration.Milliseconds(),
	}

	res, err := lb.submit(ctx, cmd)
	if err != nil {
		return 0, err
	}
	return res.NewExpiresAt, res.Err
}

// RequeueDead moves a job from DEAD back to PENDING.
func (lb *LeaderBatcher) RequeueDead(ctx context.Context, jobID string) (*domain.Job, error) {
	if lb.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	cmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_REQUEUE_DEAD,
		TimestampNano: time.Now().UnixNano(),
		CommandId:     uuid.NewString(),
		Job:           &relaypb.Job{Id: jobID},
	}

	res, err := lb.submit(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return res.Job, res.Err
}

// submit sends a command to the batcher channel and blocks until committed.
func (lb *LeaderBatcher) submit(ctx context.Context, cmd *relaypb.RaftCommand) (SubCommandResult, error) {
	p := &Proposal{
		cmd:   cmd,
		resCh: make(chan SubCommandResult, 1),
	}

	select {
	case <-lb.stopCh:
		return SubCommandResult{}, ErrBatcherClosed
	case <-ctx.Done():
		return SubCommandResult{}, ctx.Err()
	case lb.proposalCh <- p:
	}

	select {
	case <-lb.stopCh:
		select {
		case res := <-p.resCh:
			return res, nil
		default:
			return SubCommandResult{}, ErrBatcherClosed
		}
	case <-ctx.Done():
		return SubCommandResult{}, ctx.Err()
	case res := <-p.resCh:
		return res, nil
	}
}

// runBatcher coalesces proposals into CommandBatch Raft log entries.
func (lb *LeaderBatcher) runBatcher() {
	defer lb.wg.Done()

	batch := make([]*Proposal, 0, lb.cfg.BatchMaxSize)
	timer := time.NewTimer(lb.cfg.BatchWindow)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timerActive := false

	flush := func() {
		if len(batch) == 0 {
			return
		}
		lb.proposeBatch(batch)
		batch = make([]*Proposal, 0, lb.cfg.BatchMaxSize)
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}
	}

	for {
		select {
		case <-lb.stopCh:
			// Drain any in-flight batch on shutdown and fail pending items
			for _, p := range batch {
				p.resCh <- SubCommandResult{Err: ErrBatcherClosed}
				close(p.resCh)
			}
			batch = nil

			// Drain queue on shutdown and fail pending items
			for {
				select {
				case p := <-lb.proposalCh:
					p.resCh <- SubCommandResult{Err: ErrBatcherClosed}
					close(p.resCh)
				default:
					return
				}
			}

		case p := <-lb.proposalCh:
			batch = append(batch, p)
			if len(batch) == 1 {
				timer.Reset(lb.cfg.BatchWindow)
				timerActive = true
			}
			if len(batch) >= lb.cfg.BatchMaxSize {
				flush()
			}

		case <-timer.C:
			timerActive = false
			flush()
		}
	}
}

// proposeBatch wraps the batch into a CommandBatch and applies it via Raft.
func (lb *LeaderBatcher) proposeBatch(batch []*Proposal) {
	if lb.raft.State() != raft.Leader {
		for _, p := range batch {
			p.resCh <- SubCommandResult{Err: ErrNotLeader}
			close(p.resCh)
		}
		return
	}

	subCmds := make([]*relaypb.RaftCommand, len(batch))
	for i, p := range batch {
		subCmds[i] = p.cmd
	}

	batchCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: time.Now().UnixNano(),
		CommandId:     uuid.NewString(),
		SubCommands:   subCmds,
	}

	data, err := proto.Marshal(batchCmd)
	if err != nil {
		for _, p := range batch {
			p.resCh <- SubCommandResult{Err: err}
			close(p.resCh)
		}
		return
	}

	applyFuture := lb.raft.Apply(data, lb.cfg.ProposeTimeout)
	err = applyFuture.Error()

	if err != nil {
		for _, p := range batch {
			p.resCh <- SubCommandResult{Err: err}
			close(p.resCh)
		}
		return
	}

	response := applyFuture.Response()
	batchRes, ok := response.(*BatchApplyResult)
	if !ok || len(batchRes.Results) != len(batch) {
		for _, p := range batch {
			p.resCh <- SubCommandResult{Err: errors.New("invalid batch apply response from FSM")}
			close(p.resCh)
		}
		return
	}

	for i, p := range batch {
		p.resCh <- batchRes.Results[i]
		close(p.resCh)
	}
}

// runTicker periodically sweeps expired leases and scheduled jobs if ripe work exists.
// B2 Rule: Peeks local FSM state first. Only proposes if there is ripe work.
func (lb *LeaderBatcher) runTicker() {
	defer lb.wg.Done()
	ticker := time.NewTicker(lb.cfg.TickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-lb.stopCh:
			return
		case <-ticker.C:
			if lb.raft.State() != raft.Leader {
				continue
			}

			// Re-propose while ripe work remains (e.g. if bounded sweep reaped/promoted 1000 and more remain)
			for {
				if lb.raft.State() != raft.Leader {
					break
				}

				nowNano := time.Now().UnixNano()
				// B2: Peek local state first! If no ripe work, DO NOT propose empty command to Raft!
				if !lb.fsm.PeekRipeWork(nowNano) {
					break
				}

				cmd := &relaypb.RaftCommand{
					Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
					TimestampNano: nowNano,
					CommandId:     uuid.NewString(),
				}

				data, err := proto.Marshal(cmd)
				if err != nil {
					break
				}

				applyFuture := lb.raft.Apply(data, 2*time.Second)
				if err := applyFuture.Error(); err != nil {
					break
				}

				resp := applyFuture.Response()
				batchRes, ok := resp.(*BatchApplyResult)
				if !ok || len(batchRes.Results) == 0 {
					break
				}
				res := batchRes.Results[0]
				if res.Reaped > 0 {
					lb.mu.RLock()
					callback := lb.onLeasesReaped
					lb.mu.RUnlock()
					if callback != nil {
						callback(res.Reaped)
					}
				}
				// Stop looping when a command processed 0 items to avoid spinning on unconsumable work
				if res.Promoted == 0 && res.Reaped == 0 {
					break
				}

				select {
				case <-lb.stopCh:
					return
				default:
				}

				// If no more ripe work remains, exit loop and await next tick
				if !lb.fsm.PeekRipeWork(time.Now().UnixNano()) {
					break
				}
			}
		}
	}
}

// Stop gracefully stops the leader batcher and ticker.
func (lb *LeaderBatcher) Stop() {
	lb.mu.Lock()
	if lb.closed {
		lb.mu.Unlock()
		return
	}
	lb.closed = true
	lb.mu.Unlock()

	close(lb.stopCh)
	lb.wg.Wait()
}
