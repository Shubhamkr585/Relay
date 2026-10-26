package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/cluster"
	"github.com/shubhamkr585/relay/internal/server"
)

// ClusterHarness coordinates a 3-node in-process cluster with running gRPC servers.
type ClusterHarness struct {
	tc              *cluster.TestCluster
	baseDir         string
	mu              sync.RWMutex
	servers         map[string]*server.Server
	clients         map[string]relaypb.RelayServiceClient
	conns           map[string]*grpc.ClientConn
	grpcAddrs       map[string]string
	httpAddrs       map[string]string
	nodeAlive       map[string]bool
	tracker         *InvariantTracker
	lastKnownLeader string
}

// NewClusterHarness initializes and starts a 3-node cluster with gRPC servers.
func NewClusterHarness(baseDir string) (*ClusterHarness, error) {
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create test cluster: %w", err)
	}

	h := &ClusterHarness{
		tc:        tc,
		baseDir:   baseDir,
		servers:   make(map[string]*server.Server),
		clients:   make(map[string]relaypb.RelayServiceClient),
		conns:     make(map[string]*grpc.ClientConn),
		grpcAddrs: make(map[string]string),
		httpAddrs: make(map[string]string),
		nodeAlive: make(map[string]bool),
	}

	peerResolver := func(leaderID raft.ServerID, leaderRaftAddr raft.ServerAddress) (string, error) {
		h.mu.RLock()
		defer h.mu.RUnlock()
		if addr, ok := h.grpcAddrs[string(leaderID)]; ok {
			return addr, nil
		}
		return "", fmt.Errorf("leader %s not found in grpcAddrs", leaderID)
	}

	for id, node := range tc.Nodes {
		cfg := server.ServerConfig{
			NodeID:       id,
			PeerResolver: peerResolver,
		}
		srv := server.NewServer(cfg, node.Raft, node.FSM, node.Batcher, nil, nil)
		if err := srv.Start("127.0.0.1:0", "127.0.0.1:0"); err != nil {
			h.Shutdown()
			return nil, fmt.Errorf("failed to start server on %s: %w", id, err)
		}

		h.servers[id] = srv
		h.grpcAddrs[id] = srv.GRPCAddr()
		h.httpAddrs[id] = srv.HTTPAddr()
		h.nodeAlive[id] = true

		conn, err := grpc.NewClient(srv.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			h.Shutdown()
			return nil, fmt.Errorf("failed to connect gRPC client to %s: %w", id, err)
		}
		h.conns[id] = conn
		h.clients[id] = relaypb.NewRelayServiceClient(conn)
	}

	return h, nil
}

// SetTracker binds the InvariantTracker to this harness for fault and failover logging.
func (h *ClusterHarness) SetTracker(t *InvariantTracker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tracker = t
}

// RecordFault logs a fault to the bound tracker if present.
func (h *ClusterHarness) RecordFault(fType, target, details string) {
	t := h.tracker
	if t != nil {
		t.RecordFault(fType, target, details)
	}
}

// WaitForLeader waits for a stable Raft leader to be elected.
func (h *ClusterHarness) WaitForLeader(timeout time.Duration) (string, error) {
	node, err := h.tc.WaitForLeader(timeout)
	if err != nil {
		return "", err
	}
	h.mu.Lock()
	newLeader := node.ID
	oldLeader := h.lastKnownLeader
	if oldLeader != "" && oldLeader != newLeader {
		if h.tracker != nil {
			h.tracker.RecordFailover()
			h.tracker.RecordFault("FAILOVER", newLeader, fmt.Sprintf("leader changed from %s to %s", oldLeader, newLeader))
		}
	}
	h.lastKnownLeader = newLeader
	h.mu.Unlock()
	return node.ID, nil
}

// GetLeaderID returns the ID of the current leader, or empty string if none.
func (h *ClusterHarness) GetLeaderID() string {
	leader := h.tc.GetLeader()
	if leader != nil {
		return leader.ID
	}
	return ""
}

// GetClient returns the gRPC client for a specific node.
func (h *ClusterHarness) GetClient(id string) (relaypb.RelayServiceClient, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.nodeAlive[id] {
		return nil, fmt.Errorf("node %s is dead", id)
	}
	client, ok := h.clients[id]
	if !ok {
		return nil, fmt.Errorf("no client for node %s", id)
	}
	return client, nil
}

// GetRandomActiveClient returns a random client among all currently running nodes.
// This ensures traffic hits followers as well as leaders to thoroughly exercise forwarding.
func (h *ClusterHarness) GetRandomActiveClient() (relaypb.RelayServiceClient, string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var activeIDs []string
	for id, alive := range h.nodeAlive {
		if alive {
			activeIDs = append(activeIDs, id)
		}
	}
	if len(activeIDs) == 0 {
		return nil, "", fmt.Errorf("no active nodes available")
	}

	chosen := activeIDs[rand.Intn(len(activeIDs))]
	return h.clients[chosen], chosen, nil
}

// GetLeaderClient returns the client and ID for the active Raft leader.
func (h *ClusterHarness) GetLeaderClient() (relaypb.RelayServiceClient, string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	leaderNode := h.tc.GetLeader()
	if leaderNode == nil {
		return nil, "", fmt.Errorf("no active leader found")
	}
	id := leaderNode.ID
	if !h.nodeAlive[id] {
		return nil, "", fmt.Errorf("leader %s is marked dead", id)
	}
	client, ok := h.clients[id]
	if !ok {
		return nil, "", fmt.Errorf("client not found for leader %s", id)
	}
	return client, id, nil
}

// KillLeader kills the current Raft leader.
func (h *ClusterHarness) KillLeader() (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	leaderNode := h.tc.GetLeader()
	if leaderNode == nil {
		return "", fmt.Errorf("no active leader found to kill")
	}
	id := leaderNode.ID

	if !h.nodeAlive[id] {
		return "", fmt.Errorf("node %s is already dead", id)
	}

	// Close client connection and stop server
	if conn, ok := h.conns[id]; ok {
		_ = conn.Close()
		delete(h.conns, id)
		delete(h.clients, id)
	}
	if srv, ok := h.servers[id]; ok {
		srv.Stop()
		delete(h.servers, id)
	}

	h.nodeAlive[id] = false
	if err := h.tc.KillNode(id); err != nil {
		return "", err
	}
	h.RecordFault("KILL_LEADER", id, fmt.Sprintf("killed active leader %s", id))

	return id, nil
}

// KillNode stops a specific node.
func (h *ClusterHarness) KillNode(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.nodeAlive[id] {
		return nil
	}

	if conn, ok := h.conns[id]; ok {
		_ = conn.Close()
		delete(h.conns, id)
		delete(h.clients, id)
	}
	if srv, ok := h.servers[id]; ok {
		srv.Stop()
		delete(h.servers, id)
	}

	h.nodeAlive[id] = false
	err := h.tc.KillNode(id)
	h.RecordFault("KILL_NODE", id, fmt.Sprintf("killed node %s", id))
	return err
}

// RestartNode restarts a killed node and resumes its gRPC server.
func (h *ClusterHarness) RestartNode(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.nodeAlive[id] {
		return fmt.Errorf("node %s is already running", id)
	}

	if err := h.tc.RestartNode(id); err != nil {
		return fmt.Errorf("cluster RestartNode failed: %w", err)
	}

	node := h.tc.Nodes[id]
	peerResolver := func(leaderID raft.ServerID, leaderRaftAddr raft.ServerAddress) (string, error) {
		h.mu.RLock()
		defer h.mu.RUnlock()
		if addr, ok := h.grpcAddrs[string(leaderID)]; ok {
			return addr, nil
		}
		return "", fmt.Errorf("leader %s not found in grpcAddrs", leaderID)
	}

	cfg := server.ServerConfig{
		NodeID:       id,
		PeerResolver: peerResolver,
	}
	srv := server.NewServer(cfg, node.Raft, node.FSM, node.Batcher, nil, nil)
	if err := srv.Start("127.0.0.1:0", "127.0.0.1:0"); err != nil {
		return fmt.Errorf("failed to restart server on %s: %w", id, err)
	}

	h.servers[id] = srv
	h.grpcAddrs[id] = srv.GRPCAddr()
	h.httpAddrs[id] = srv.HTTPAddr()
	h.nodeAlive[id] = true

	conn, err := grpc.NewClient(srv.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to reconnect client to %s: %w", id, err)
	}
	h.conns[id] = conn
	h.clients[id] = relaypb.NewRelayServiceClient(conn)

	h.RecordFault("RESTART_NODE", id, fmt.Sprintf("restarted node %s", id))
	return nil
}

// Partition isolates a single node from all peers.
func (h *ClusterHarness) Partition(id string) error {
	err := h.tc.Partition(id)
	if err == nil {
		h.RecordFault("PARTITION", id, fmt.Sprintf("isolated minority node %s", id))
	}
	return err
}

// HealPartition restores full connectivity between all active nodes.
func (h *ClusterHarness) HealPartition() error {
	err := h.tc.HealPartition()
	if err == nil {
		h.RecordFault("HEAL", "cluster", "reconnected all partitioned nodes")
	}
	return err
}

// RollingRestart sequentially restarts all cluster nodes one by one.
// Ensures the current leader is restarted first to trigger real failover.
func (h *ClusterHarness) RollingRestart(interWait time.Duration) error {
	leaderID := h.GetLeaderID()
	var nodeIDs []string
	if leaderID != "" {
		nodeIDs = append(nodeIDs, leaderID)
		for _, id := range []string{"node-1", "node-2", "node-3"} {
			if id != leaderID {
				nodeIDs = append(nodeIDs, id)
			}
		}
	} else {
		nodeIDs = []string{"node-1", "node-2", "node-3"}
	}

	for _, id := range nodeIDs {
		isLeader := (id == leaderID)
		if err := h.KillNode(id); err != nil {
			return fmt.Errorf("failed killing node %s during rolling restart: %w", id, err)
		}

		if isLeader {
			// Wait for surviving majority to elect a new leader while old leader is dead
			if _, err := h.WaitForLeader(8 * time.Second); err != nil {
				return fmt.Errorf("cluster failed to elect new leader while old leader %s was dead: %w", id, err)
			}
		} else {
			time.Sleep(150 * time.Millisecond)
		}

		if err := h.RestartNode(id); err != nil {
			return fmt.Errorf("failed restarting node %s during rolling restart: %w", id, err)
		}

		// Wait for cluster stabilization
		if _, err := h.WaitForLeader(10 * time.Second); err != nil {
			return fmt.Errorf("cluster failed to re-elect leader after restarting %s: %w", id, err)
		}
		time.Sleep(interWait)
	}
	return nil
}

// DrainQueue runs worker loops until all jobs in the given queue reach DONE or DEAD state.
func (h *ClusterHarness) DrainQueue(ctx context.Context, queue string, tracker *InvariantTracker, numWorkers int) error {
	stopDrain := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("drain-worker-%d", i)
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stopDrain:
					return
				case <-ctx.Done():
					return
				default:
				}

				client, _, err := h.GetRandomActiveClient()
				if err != nil {
					time.Sleep(30 * time.Millisecond)
					continue
				}

				reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				resResp, err := client.Reserve(reqCtx, &relaypb.ReserveRequest{
					Queue:           queue,
					WorkerId:        workerID,
					LeaseDurationMs: 2000,
				})
				cancel()

				if err != nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}

				job := resResp.Job
				tracker.RecordReservation(job.Id, workerID, resResp.LeaseToken)

				// Failure-injecting drainer for poison jobs: Nacks them until dead
				if strings.HasPrefix(string(job.Payload), "poison-") {
					tracker.RecordFault("POISON_NACK", workerID, fmt.Sprintf("nacking poison job %s attempt %d", job.Id, job.Attempts))
					_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
						nackCtx, nackCancel := context.WithTimeout(ctx, 3*time.Second)
						defer nackCancel()
						_, nErr := c.Nack(nackCtx, &relaypb.NackRequest{
							JobId:        job.Id,
							LeaseToken:   resResp.LeaseToken,
							ErrorMsg:     "poison failure in drain",
							RetryAfterMs: 0,
						})
						return nErr
					}, h)
					continue
				}

				// Acknowledge completion with retry on Unavailable
				_ = executeClientWithRetry(ctx, func(c relaypb.RelayServiceClient) error {
					ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
					defer ackCancel()
					_, aErr := c.Ack(ackCtx, &relaypb.AckRequest{
						JobId:      job.Id,
						LeaseToken: resResp.LeaseToken,
					})
					if aErr == nil {
						tracker.RecordComplete(job.Id)
						return nil
					}
					st, _ := status.FromError(aErr)
					if st.Code() == codes.FailedPrecondition {
						tracker.RecordStaleAck(true)
						return nil
					}
					return aErr
				}, h)
			}
		}()
	}

	// Coordinator: monitor queue until empty on the leader
	consecutiveClean := 0
	for {
		select {
		case <-ctx.Done():
			close(stopDrain)
			wg.Wait()
			return ctx.Err()
		default:
		}

		leaderClient, _, err := h.GetLeaderClient()
		if err == nil {
			statsCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			resp, sErr := leaderClient.Stats(statsCtx, &relaypb.StatsRequest{Queue: queue})
			cancel()

			if sErr == nil && resp != nil {
				if resp.PendingCount == 0 && resp.LeasedCount == 0 {
					consecutiveClean++
					if consecutiveClean >= 5 {
						// Truly drained on the leader!
						close(stopDrain)
						wg.Wait()

						// Sync dead jobs from cluster DLQ into tracker
						listDeadCtx, listDeadCancel := context.WithTimeout(context.Background(), 5*time.Second)
						deadResp, dErr := leaderClient.ListDead(listDeadCtx, &relaypb.ListDeadRequest{
							Queue: queue,
							Limit: 50000,
						})
						listDeadCancel()
						if dErr == nil && deadResp != nil {
							for _, dj := range deadResp.Jobs {
								tracker.RecordDead(dj.Id)
							}
						}
						return nil
					}
				} else {
					consecutiveClean = 0
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// VerifyFSMConsistency checks that all living nodes have identical FSM dumps.
func (h *ClusterHarness) VerifyFSMConsistency() error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var firstDump map[string][]cluster.KVPair
	var firstID string

	for id, alive := range h.nodeAlive {
		if !alive {
			continue
		}
		dump, err := h.tc.StateDump(id)
		if err != nil {
			return fmt.Errorf("failed to dump state for %s: %w", id, err)
		}

		if firstDump == nil {
			firstDump = dump
			firstID = id
			continue
		}

		// Compare bucket lengths
		for bucket, kv1 := range firstDump {
			kv2, ok := dump[bucket]
			if !ok {
				return fmt.Errorf("node %s missing bucket %s present in %s", id, bucket, firstID)
			}
			if len(kv1) != len(kv2) {
				return fmt.Errorf("node %s bucket %s count mismatch: %d vs %s: %d", id, bucket, len(kv2), firstID, len(kv1))
			}
		}
	}
	return nil
}

// Shutdown stops all servers and the underlying Raft cluster.
func (h *ClusterHarness) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, conn := range h.conns {
		_ = conn.Close()
	}
	for _, srv := range h.servers {
		srv.Stop()
	}
	_ = h.tc.Shutdown()
}

// executeClientWithRetry executes an RPC operation against a random active cluster node with exponential backoff on retryable errors.
func executeClientWithRetry(ctx context.Context, fn func(client relaypb.RelayServiceClient) error, h *ClusterHarness) error {
	deadline := time.Now().Add(30 * time.Second)
	backoff := 50 * time.Millisecond

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		client, _, err := h.GetRandomActiveClient()
		if err != nil {
			time.Sleep(backoff)
			continue
		}

		opErr := fn(client)
		if opErr == nil {
			return nil
		}

		st, _ := status.FromError(opErr)
		code := st.Code()
		errMsg := opErr.Error()
		if code == codes.Unavailable ||
			code == codes.DeadlineExceeded ||
			code == codes.Canceled ||
			code == codes.ResourceExhausted ||
			strings.Contains(errMsg, "context canceled") ||
			strings.Contains(errMsg, "connection reset") ||
			strings.Contains(errMsg, "no Raft leader") ||
			strings.Contains(errMsg, "connection refused") ||
			strings.Contains(errMsg, "transport is closing") ||
			strings.Contains(errMsg, "broken pipe") {
			time.Sleep(backoff)
			if backoff < 500*time.Millisecond {
				backoff *= 2
			}
			continue
		}

		// Non-retryable error
		return opErr
	}
	return fmt.Errorf("operation timed out across retries: %w", ctx.Err())
}
