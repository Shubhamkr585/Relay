package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/cluster"
	"github.com/shubhamkr585/relay/internal/server"
)

type TestClusterServers struct {
	tc        *cluster.TestCluster
	servers   map[string]*server.Server
	clients   map[string]relaypb.RelayServiceClient
	conns     map[string]*grpc.ClientConn
	grpcAddrs map[string]string
	httpAddrs map[string]string
}

func newTestClusterServers(t *testing.T) *TestClusterServers {
	t.Helper()
	baseDir := t.TempDir()

	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)

	grpcAddrs := make(map[string]string)
	httpAddrs := make(map[string]string)
	servers := make(map[string]*server.Server)
	clients := make(map[string]relaypb.RelayServiceClient)
	conns := make(map[string]*grpc.ClientConn)

	peerResolver := func(leaderID raft.ServerID, leaderRaftAddr raft.ServerAddress) (string, error) {
		if addr, ok := grpcAddrs[string(leaderID)]; ok {
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
		err := srv.Start("127.0.0.1:0", "127.0.0.1:0")
		require.NoError(t, err)

		servers[id] = srv
		grpcAddrs[id] = srv.GRPCAddr()
		httpAddrs[id] = srv.HTTPAddr()

		conn, err := grpc.NewClient(srv.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		conns[id] = conn
		clients[id] = relaypb.NewRelayServiceClient(conn)
	}

	return &TestClusterServers{
		tc:        tc,
		servers:   servers,
		clients:   clients,
		conns:     conns,
		grpcAddrs: grpcAddrs,
		httpAddrs: httpAddrs,
	}
}

func (tcs *TestClusterServers) Shutdown() {
	for _, conn := range tcs.conns {
		_ = conn.Close()
	}
	for _, srv := range tcs.servers {
		srv.Stop()
	}
	_ = tcs.tc.Shutdown()
}

// a. Enqueue/Reserve/Ack end to end through a FOLLOWER (forwarding works).
func TestFollowerForwarding_EnqueueReserveAck(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)
	require.NotNil(t, leaderNode)

	// Identify a follower node
	var followerID string
	for id := range tcs.servers {
		if id != leaderNode.ID {
			followerID = id
			break
		}
	}
	require.NotEmpty(t, followerID)

	// Ensure the follower has received the leader heartbeat and knows the leader address
	require.Eventually(t, func() bool {
		leaderAddr := tcs.tc.Nodes[followerID].Raft.Leader()
		return leaderAddr != ""
	}, 5*time.Second, 20*time.Millisecond, "follower did not learn leader address in time")

	followerClient := tcs.clients[followerID]
	ctx := context.Background()

	// 1. Enqueue through follower -> transparently forwarded to leader
	enqResp, err := followerClient.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:          "tasks-forwarding",
		Payload:        []byte("payload-forwarded"),
		Priority:       1,
		IdempotencyKey: "fwd-idemp-1",
		MaxAttempts:    3,
	})
	require.NoError(t, err)
	require.NotEmpty(t, enqResp.JobId)
	require.False(t, enqResp.Deduplicated)

	// Wait for replication
	time.Sleep(200 * time.Millisecond)

	// 2. Reserve through follower -> transparently forwarded to leader
	resResp, err := followerClient.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           "tasks-forwarding",
		WorkerId:        "worker-follower",
		LeaseDurationMs: 10000,
	})
	require.NoError(t, err)
	require.Equal(t, enqResp.JobId, resResp.Job.Id)
	require.NotEmpty(t, resResp.LeaseToken)

	// 3. Ack through follower -> transparently forwarded to leader
	_, err = followerClient.Ack(ctx, &relaypb.AckRequest{
		JobId:      resResp.Job.Id,
		LeaseToken: resResp.LeaseToken,
	})
	require.NoError(t, err)

	// 4. Verify local stats on follower (eventually consistent as follower catches up Raft log)
	require.Eventually(t, func() bool {
		statsResp, err := followerClient.Stats(ctx, &relaypb.StatsRequest{
			Queue: "tasks-forwarding",
		})
		return err == nil && statsResp.PendingCount == 0 && statsResp.LeasedCount == 0 && statsResp.DoneCount == 1
	}, 5*time.Second, 50*time.Millisecond, "follower local stats must eventually reflect committed Ack")
}

// b. Leader killed mid-stream: client retries on UNAVAILABLE and eventually succeeds;
// every enqueue that returned success exists on the new leader (0 missing).
func TestLeaderFailoverMidStream_ClientRetry(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	numEnqueues := 30
	successJobIDs := make(map[string]bool)
	var mu sync.Mutex

	// Find a client to issue requests against
	client := tcs.clients[leaderNode.ID]

	killTriggered := false
	for i := 0; i < numEnqueues; i++ {
		if i == 10 && !killTriggered {
			killTriggered = true
			go func() {
				time.Sleep(10 * time.Millisecond)
				_, _ = tcs.tc.KillLeader()
			}()
		}

		key := fmt.Sprintf("failover-key-%d", i)
		req := &relaypb.EnqueueRequest{
			Queue:          "failover-q",
			Payload:        []byte(fmt.Sprintf("data-%d", i)),
			Priority:       1,
			IdempotencyKey: key,
			MaxAttempts:    3,
		}

		// Client retry loop with exponential backoff on UNAVAILABLE
		deadline := time.Now().Add(12 * time.Second)
		var resp *relaypb.EnqueueResponse
		var enqErr error

		for time.Now().Before(deadline) {
			// Find current leader or follower
			currLeader := tcs.tc.GetLeader()
			if currLeader != nil {
				client = tcs.clients[currLeader.ID]
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, enqErr = client.Enqueue(ctx, req)
			cancel()

			if enqErr == nil {
				break
			}
			st, _ := status.FromError(enqErr)
			if st.Code() == codes.Unavailable {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}

		require.NoError(t, enqErr, "enqueue %d must eventually succeed across failover", i)
		require.NotNil(t, resp)
		mu.Lock()
		successJobIDs[resp.JobId] = true
		mu.Unlock()
	}

	// Wait for stable new leader
	newLeader, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	// Verify that every acknowledged enqueue exists on the new leader
	dump, err := tcs.tc.StateDump(newLeader.ID)
	require.NoError(t, err)

	presentJobs := make(map[string]bool)
	for _, kv := range dump["jobs"] {
		presentJobs[kv.Key] = true
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, numEnqueues, len(successJobIDs))
	for jobID := range successJobIDs {
		require.True(t, presentJobs[jobID], "Acknowledged job %s must exist on new leader (0 lost)", jobID)
	}
}

// c. Stale lease Ack returns FAILED_PRECONDITION; empty queue Reserve returns
// NOT_FOUND without growing the raft log (assert log index unchanged).
func TestStaleLeaseAndEmptyQueue_Errors(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leaderNode.ID]
	ctx := context.Background()

	// 1. Enqueue and Reserve
	_, err = client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       "stale-test-q",
		Payload:     []byte("test"),
		Priority:    1,
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	resResp, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           "stale-test-q",
		WorkerId:        "w1",
		LeaseDurationMs: 10000,
	})
	require.NoError(t, err)

	// Ack with stale lease token -> FAILED_PRECONDITION
	_, err = client.Ack(ctx, &relaypb.AckRequest{
		JobId:      resResp.Job.Id,
		LeaseToken: "completely-stale-invalid-token",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code(), "Stale lease Ack must return FAILED_PRECONDITION")

	// Ack with valid token
	_, err = client.Ack(ctx, &relaypb.AckRequest{
		JobId:      resResp.Job.Id,
		LeaseToken: resResp.LeaseToken,
	})
	require.NoError(t, err)

	// Ack again after job is DONE -> FAILED_PRECONDITION
	_, err = client.Ack(ctx, &relaypb.AckRequest{
		JobId:      resResp.Job.Id,
		LeaseToken: resResp.LeaseToken,
	})
	require.Error(t, err)
	st, _ = status.FromError(err)
	require.Equal(t, codes.FailedPrecondition, st.Code(), "Late Ack on DONE job must return FAILED_PRECONDITION")

	// 2. Empty queue Reserve: assert log index unchanged!
	time.Sleep(100 * time.Millisecond)
	initialLogIndex := leaderNode.Raft.AppliedIndex()

	_, err = client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           "completely-empty-queue",
		WorkerId:        "w2",
		LeaseDurationMs: 10000,
	})
	require.Error(t, err)
	st, _ = status.FromError(err)
	require.Equal(t, codes.NotFound, st.Code(), "Empty queue Reserve must return NOT_FOUND")

	// Assert Raft log index is completely unchanged!
	postReserveLogIndex := leaderNode.Raft.AppliedIndex()
	require.Equal(t, initialLogIndex, postReserveLogIndex, "Empty queue Reserve must NOT grow the Raft log")
}

// d. Idempotent Enqueue retry returns the same job_id with deduplicated=true.
func TestIdempotentEnqueueRetry(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leaderNode.ID]
	ctx := context.Background()

	idempKey := "unique-idemp-key-999"

	req := &relaypb.EnqueueRequest{
		Queue:          "dedup-q",
		Payload:        []byte("dedup-payload"),
		Priority:       1,
		IdempotencyKey: idempKey,
		MaxAttempts:    3,
	}

	// First call: new job created
	resp1, err := client.Enqueue(ctx, req)
	require.NoError(t, err)
	require.NotEmpty(t, resp1.JobId)
	require.False(t, resp1.Deduplicated)

	// Second call: deduplicated retry returns identical job_id
	resp2, err := client.Enqueue(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resp1.JobId, resp2.JobId, "Deduplicated enqueue must return identical job_id")
	require.True(t, resp2.Deduplicated, "Second enqueue must have Deduplicated=true")

	// Third call: still deduplicated
	resp3, err := client.Enqueue(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resp1.JobId, resp3.JobId)
	require.True(t, resp3.Deduplicated)
}

// e. Forwarding loop guard: two nodes pointing at each other do not loop.
func TestForwardingLoopGuard(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	var followerID string
	for id := range tcs.servers {
		if id != leaderNode.ID {
			followerID = id
			break
		}
	}
	followerClient := tcs.clients[followerID]

	// Simulate a request that has already been forwarded once
	md := metadata.Pairs(server.HeaderForwardedBy, "node-upstream")
	ctxWithLoopGuard := metadata.NewOutgoingContext(context.Background(), md)

	_, err = followerClient.Enqueue(ctxWithLoopGuard, &relaypb.EnqueueRequest{
		Queue:       "loop-q",
		Payload:     []byte("loop-payload"),
		MaxAttempts: 3,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code(), "Forwarding loop must return UNAVAILABLE")
	require.Contains(t, st.Message(), "forwarding loop detected")
}

// f. /metrics, /healthz and /readyz respond correctly during a job lifecycle and after a leader change.
func TestObservability_MetricsHealthzReadyz(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	leaderHTTPAddr := tcs.httpAddrs[leaderNode.ID]

	// 1. Check /healthz
	resp, err := http.Get(fmt.Sprintf("http://%s/healthz", leaderHTTPAddr))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "ok")

	// 2. Check /readyz
	respReady, err := http.Get(fmt.Sprintf("http://%s/readyz", leaderHTTPAddr))
	require.NoError(t, err)
	defer respReady.Body.Close()
	require.Equal(t, http.StatusOK, respReady.StatusCode)
	bodyReady, _ := io.ReadAll(respReady.Body)
	var readyMap map[string]interface{}
	require.NoError(t, json.Unmarshal(bodyReady, &readyMap))
	require.Equal(t, true, readyMap["ready"])

	// 3. Check /metrics during job lifecycle
	client := tcs.clients[leaderNode.ID]
	ctx := context.Background()

	enqResp, err := client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       "obs-metrics-q",
		Payload:     []byte("obs-data"),
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	resResp, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           "obs-metrics-q",
		WorkerId:        "worker-obs",
		LeaseDurationMs: 10000,
	})
	require.NoError(t, err)

	_, err = client.Ack(ctx, &relaypb.AckRequest{
		JobId:      resResp.Job.Id,
		LeaseToken: resResp.LeaseToken,
	})
	require.NoError(t, err)

	// Fetch /metrics
	respMetrics, err := http.Get(fmt.Sprintf("http://%s/metrics", leaderHTTPAddr))
	require.NoError(t, err)
	defer respMetrics.Body.Close()
	require.Equal(t, http.StatusOK, respMetrics.StatusCode)
	metricsBody, _ := io.ReadAll(respMetrics.Body)
	metricsStr := string(metricsBody)

	require.True(t, strings.Contains(metricsStr, "relay_jobs_enqueued_total"), "Metrics must export relay_jobs_enqueued_total")
	require.True(t, strings.Contains(metricsStr, "relay_jobs_reserved_total"), "Metrics must export relay_jobs_reserved_total")
	require.True(t, strings.Contains(metricsStr, "relay_jobs_acked_total"), "Metrics must export relay_jobs_acked_total")
	require.True(t, strings.Contains(metricsStr, "relay_rpc_duration_seconds"), "Metrics must export relay_rpc_duration_seconds")
	require.True(t, strings.Contains(metricsStr, "relay_raft_is_leader 1"), "Leader must report relay_raft_is_leader 1")
	_ = enqResp
}

// g. Goroutine leak check (goleak) after full server shutdown.
func TestServerShutdown_NoGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("testing.(*M).Run"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*loopyWriter).run"),
	)

	tcs := newTestClusterServers(t)

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leaderNode.ID]
	ctx := context.Background()

	_, err = client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       "leak-check-q",
		Payload:     []byte("data"),
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	tcs.Shutdown()
}

// 0.1: TestMetrics_QueueDepth_ScrapeAfterLifecycle tests that scraping /metrics
// after enqueue, reserve, and ack correctly dynamically reports relay_queue_depth gauge.
func TestMetrics_QueueDepth_ScrapeAfterLifecycle(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leaderNode.ID]
	leaderHTTPAddr := tcs.httpAddrs[leaderNode.ID]
	ctx := context.Background()
	q := "metric-depth-q"

	scrapeMetrics := func() string {
		resp, err := http.Get(fmt.Sprintf("http://%s/metrics", leaderHTTPAddr))
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	// 1. After Enqueue -> pending=1, leased=0, done=0
	_, err = client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       q,
		Payload:     []byte("payload-1"),
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	metrics := scrapeMetrics()
	require.Contains(t, metrics, fmt.Sprintf(`relay_queue_depth{queue="%s",state="pending"} 1`, q))
	require.Contains(t, metrics, fmt.Sprintf(`relay_queue_depth{queue="%s",state="leased"} 0`, q))

	// 2. After Reserve -> pending=0, leased=1
	res, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "worker-1",
		LeaseDurationMs: 5000,
	})
	require.NoError(t, err)

	metrics = scrapeMetrics()
	require.Contains(t, metrics, fmt.Sprintf(`relay_queue_depth{queue="%s",state="pending"} 0`, q))
	require.Contains(t, metrics, fmt.Sprintf(`relay_queue_depth{queue="%s",state="leased"} 1`, q))

	// 3. After Ack -> pending=0, leased=0, done=1
	_, err = client.Ack(ctx, &relaypb.AckRequest{
		JobId:      res.Job.Id,
		LeaseToken: res.LeaseToken,
	})
	require.NoError(t, err)

	metrics = scrapeMetrics()
	require.Contains(t, metrics, fmt.Sprintf(`relay_queue_depth{queue="%s",state="pending"} 0`, q))
	require.Contains(t, metrics, fmt.Sprintf(`relay_queue_depth{queue="%s",state="leased"} 0`, q))
	require.Contains(t, metrics, fmt.Sprintf(`relay_queue_depth{queue="%s",state="done"} 1`, q))
}

// 0.2: TestMetrics_RetriedDeadLeaseExpiries verifies that retried, dead, and lease_expiries
// counters really increment.
func TestMetrics_RetriedDeadLeaseExpiries(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leaderNode.ID]
	leaderHTTPAddr := tcs.httpAddrs[leaderNode.ID]
	ctx := context.Background()
	q := "retry-dead-q"

	scrapeMetrics := func() string {
		resp, err := http.Get(fmt.Sprintf("http://%s/metrics", leaderHTTPAddr))
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	// 1. Enqueue job with max_attempts=2
	_, err = client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       q,
		Payload:     []byte("retry-data"),
		MaxAttempts: 2,
	})
	require.NoError(t, err)

	// Reserve and Nack 1st time -> should increment retried
	res1, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "w1",
		LeaseDurationMs: 5000,
	})
	require.NoError(t, err)

	_, err = client.Nack(ctx, &relaypb.NackRequest{
		JobId:        res1.Job.Id,
		LeaseToken:   res1.LeaseToken,
		ErrorMsg:     "transient error",
		RetryAfterMs: 0,
	})
	require.NoError(t, err)

	metrics := scrapeMetrics()
	require.Contains(t, metrics, fmt.Sprintf(`relay_jobs_retried_total{queue="%s"} 1`, q))

	// Reserve and Nack 2nd time (attempt 2 reached max) -> should increment dead
	res2, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "w1",
		LeaseDurationMs: 5000,
	})
	require.NoError(t, err)

	_, err = client.Nack(ctx, &relaypb.NackRequest{
		JobId:        res2.Job.Id,
		LeaseToken:   res2.LeaseToken,
		ErrorMsg:     "final error",
		RetryAfterMs: 0,
	})
	require.NoError(t, err)

	metrics = scrapeMetrics()
	require.Contains(t, metrics, fmt.Sprintf(`relay_jobs_dead_total{queue="%s"} 1`, q))

	// 3. Lease expiry check: Enqueue and reserve with 50ms lease, wait for reaper tick
	_, err = client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       q,
		Payload:     []byte("reap-data"),
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	_, err = client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "w2",
		LeaseDurationMs: 50,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		m := scrapeMetrics()
		return strings.Contains(m, "relay_lease_expiries_total")
	}, 5*time.Second, 100*time.Millisecond, "Expected lease_expiries counter to increment after reaper sweep")
}

// 0.3: TestStaleLease_WrongTokenOnLeasedJob_ReturnsFailedPreconditionAndLeavesJobLeased asserts
// that Ack with wrong token on a currently leased job returns FAILED_PRECONDITION and leaves it LEASED.
func TestStaleLease_WrongTokenOnLeasedJob_ReturnsFailedPreconditionAndLeavesJobLeased(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leaderNode, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leaderNode.ID]
	ctx := context.Background()
	q := "fencing-test-q"

	_, err = client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       q,
		Payload:     []byte("fencing-data"),
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	res, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "worker-fencing",
		LeaseDurationMs: 10000,
	})
	require.NoError(t, err)

	// Call Ack with invalid/wrong lease token
	_, err = client.Ack(ctx, &relaypb.AckRequest{
		JobId:      res.Job.Id,
		LeaseToken: "completely-wrong-token-abc-123",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code(), "Ack with wrong token must return FAILED_PRECONDITION")

	// Verify job is still LEASED (not pending, not done, not deleted)
	stats, err := client.Stats(ctx, &relaypb.StatsRequest{Queue: q})
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.LeasedCount, "Job must remain in LEASED state")
	require.Equal(t, int64(0), stats.PendingCount)
	require.Equal(t, int64(0), stats.DoneCount)

	// Now Ack with correct lease token must succeed
	_, err = client.Ack(ctx, &relaypb.AckRequest{
		JobId:      res.Job.Id,
		LeaseToken: res.LeaseToken,
	})
	require.NoError(t, err)

	statsAfter, err := client.Stats(ctx, &relaypb.StatsRequest{Queue: q})
	require.NoError(t, err)
	require.Equal(t, int64(0), statsAfter.LeasedCount)
	require.Equal(t, int64(1), statsAfter.DoneCount)
}

// 0.4: TestFollowerForwarding_LeaderFailover_RedialsNewLeader verifies that after killing the leader,
// a follower transparently re-dials the newly elected leader without using a stale connection.
func TestFollowerForwarding_LeaderFailover_RedialsNewLeader(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	oldLeader, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	var followerID string
	for id := range tcs.servers {
		if id != oldLeader.ID {
			followerID = id
			break
		}
	}
	followerClient := tcs.clients[followerID]
	ctx := context.Background()
	q := "forwarder-failover-q"

	// 1. Initial forward through follower to current leader
	require.Eventually(t, func() bool {
		_, err := followerClient.Enqueue(ctx, &relaypb.EnqueueRequest{
			Queue:       q,
			Payload:     []byte("before-failover"),
			MaxAttempts: 3,
		})
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "Initial forward through follower must succeed")

	// 2. Kill the current leader
	tcs.servers[oldLeader.ID].Stop()
	_ = tcs.tc.KillNode(oldLeader.ID)

	// 3. Wait for a new leader to emerge among the remaining 2 nodes
	require.Eventually(t, func() bool {
		for id, node := range tcs.tc.Nodes {
			if id != oldLeader.ID && node.Raft.State() == raft.Leader {
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "New leader must be elected")

	// 4. Send Enqueue through the surviving follower (or follower client)
	// Even if follower had cached the old leader's gRPC connection, it must detect unavailability and dial the new leader.
	require.Eventually(t, func() bool {
		_, err := followerClient.Enqueue(ctx, &relaypb.EnqueueRequest{
			Queue:       q,
			Payload:     []byte("after-failover"),
			MaxAttempts: 3,
		})
		return err == nil
	}, 10*time.Second, 200*time.Millisecond, "Follower forwarding must succeed after failover")
}

// TestLinearizableReads_GetJobAndStats verifies that linearizable reads route through the leader
// and verify leadership before returning FSM state.
func TestLinearizableReads_GetJobAndStats(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leader, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	var followerID string
	for id := range tcs.servers {
		if id != leader.ID {
			followerID = id
			break
		}
	}
	followerClient := tcs.clients[followerID]
	ctx := context.Background()
	q := "linearizable-read-q"

	// Enqueue a job via follower (forwarded to leader)
	enqRes, err := followerClient.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       q,
		Payload:     []byte("test-linearizable-payload"),
		Priority:    10,
		MaxAttempts: 3,
	})
	require.NoError(t, err)
	require.NotEmpty(t, enqRes.JobId)

	// Call GetJob on follower -> transparently forwarded to leader, verifies leadership, returns job
	getRes, err := followerClient.GetJob(ctx, &relaypb.GetJobRequest{JobId: enqRes.JobId})
	require.NoError(t, err)
	require.Equal(t, enqRes.JobId, getRes.Job.Id)
	require.Equal(t, q, getRes.Job.Queue)
	require.Equal(t, []byte("test-linearizable-payload"), getRes.Job.Payload)
	require.Equal(t, uint32(10), getRes.Job.Priority)
	require.Equal(t, relaypb.JobState_JOB_STATE_PENDING, getRes.Job.State)

	// Call Stats on follower -> forwarded to leader, verifies leadership
	statsRes, err := followerClient.Stats(ctx, &relaypb.StatsRequest{Queue: q})
	require.NoError(t, err)
	require.Equal(t, int64(1), statsRes.PendingCount)
}

// TestQueueSemantics_ExtendLease_DLQ_RequeueDead verifies lease extension, max attempt DLQ movement,
// list-dead, and requeue-dead end-to-end through the cluster.
func TestQueueSemantics_ExtendLease_DLQ_RequeueDead(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leader, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leader.ID]
	ctx := context.Background()
	q := "dlq-requeue-q"

	// 1. Enqueue with MaxAttempts = 2
	enqRes, err := client.Enqueue(ctx, &relaypb.EnqueueRequest{
		Queue:       q,
		Payload:     []byte("dlq-payload"),
		MaxAttempts: 2,
	})
	require.NoError(t, err)
	jobID := enqRes.JobId

	// 2. Reserve the job
	res1, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "worker-1",
		LeaseDurationMs: 5000,
	})
	require.NoError(t, err)
	require.Equal(t, jobID, res1.Job.Id)
	token1 := res1.LeaseToken

	// 3. Extend Lease by 10s
	extRes, err := client.ExtendLease(ctx, &relaypb.ExtendLeaseRequest{
		JobId:           jobID,
		LeaseToken:      token1,
		ExtraDurationMs: 10000,
	})
	require.NoError(t, err)
	require.Greater(t, extRes.NewLeaseExpiresAt, res1.Job.LeaseExpiresAt)

	// Verify via GetJob that lease was extended
	getJob1, err := client.GetJob(ctx, &relaypb.GetJobRequest{JobId: jobID})
	require.NoError(t, err)
	require.Equal(t, extRes.NewLeaseExpiresAt, getJob1.Job.LeaseExpiresAt)

	// 4. Attempt 1 failure: Nack
	_, err = client.Nack(ctx, &relaypb.NackRequest{
		JobId:        jobID,
		LeaseToken:   token1,
		ErrorMsg:     "attempt 1 failed",
		RetryAfterMs: 0,
	})
	require.NoError(t, err)

	// 5. Reserve attempt 2
	res2, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "worker-2",
		LeaseDurationMs: 5000,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(2), res2.Job.Attempts)
	token2 := res2.LeaseToken

	// 6. Attempt 2 failure: Nack (reaches MaxAttempts = 2 -> must move to DLQ)
	_, err = client.Nack(ctx, &relaypb.NackRequest{
		JobId:      jobID,
		LeaseToken: token2,
		ErrorMsg:   "fatal attempt 2 failed",
	})
	require.NoError(t, err)

	// Verify via GetJob that job is DEAD
	deadJob, err := client.GetJob(ctx, &relaypb.GetJobRequest{JobId: jobID})
	require.NoError(t, err)
	require.Equal(t, relaypb.JobState_JOB_STATE_DEAD, deadJob.Job.State)

	// 7. ListDead must return the job
	deadList, err := client.ListDead(ctx, &relaypb.ListDeadRequest{Queue: q, Limit: 10})
	require.NoError(t, err)
	require.Len(t, deadList.Jobs, 1)
	require.Equal(t, jobID, deadList.Jobs[0].Id)

	// 8. RequeueDead moves job back to PENDING and resets attempts
	requeueRes, err := client.RequeueDead(ctx, &relaypb.RequeueDeadRequest{JobId: jobID})
	require.NoError(t, err)
	require.Equal(t, relaypb.JobState_JOB_STATE_PENDING, requeueRes.Job.State)
	require.Equal(t, uint32(0), requeueRes.Job.Attempts)

	// 9. Can now be reserved again!
	res3, err := client.Reserve(ctx, &relaypb.ReserveRequest{
		Queue:           q,
		WorkerId:        "worker-3",
		LeaseDurationMs: 5000,
	})
	require.NoError(t, err)
	require.Equal(t, jobID, res3.Job.Id)
	require.Equal(t, uint32(1), res3.Job.Attempts)
}

// TestClusterMembership_DynamicJoinNode verifies dynamically adding a 4th voter node
// to an active 3-node cluster via client.Join, and asserting that the new node converges.
func TestClusterMembership_DynamicJoinNode(t *testing.T) {
	tcs := newTestClusterServers(t)
	defer tcs.Shutdown()

	leader, err := tcs.tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	client := tcs.clients[leader.ID]
	ctx := context.Background()
	q := "dynamic-join-q"

	// 1. Enqueue some jobs before join
	for i := 0; i < 5; i++ {
		_, err := client.Enqueue(ctx, &relaypb.EnqueueRequest{
			Queue:       q,
			Payload:     []byte(fmt.Sprintf("before-join-%d", i)),
			MaxAttempts: 3,
		})
		require.NoError(t, err)
	}

	// 2. Create node-4 dynamically (non-bootstrapped)
	node4, err := tcs.tc.CreateNonBootstrapNode("node-4")
	require.NoError(t, err)

	// Start server on node-4
	peerResolver := func(leaderID raft.ServerID, leaderRaftAddr raft.ServerAddress) (string, error) {
		if addr, ok := tcs.grpcAddrs[string(leaderID)]; ok {
			return addr, nil
		}
		return "", fmt.Errorf("leader %s not found in grpcAddrs", leaderID)
	}
	srv4 := server.NewServer(server.ServerConfig{
		NodeID:       "node-4",
		PeerResolver: peerResolver,
	}, node4.Raft, node4.FSM, node4.Batcher, nil, nil)
	require.NoError(t, srv4.Start("127.0.0.1:0", "127.0.0.1:0"))
	tcs.servers["node-4"] = srv4
	tcs.grpcAddrs["node-4"] = srv4.GRPCAddr()

	conn4, err := grpc.NewClient(srv4.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	tcs.conns["node-4"] = conn4
	tcs.clients["node-4"] = relaypb.NewRelayServiceClient(conn4)

	// 3. Request Join through leader client
	joinRes, err := client.Join(ctx, &relaypb.JoinRequest{
		NodeId:      "node-4",
		RaftAddress: string(node4.Transport.LocalAddr()),
	})
	require.NoError(t, err)
	require.True(t, joinRes.Success)

	// 4. Assert node-4 is now in the Raft configuration as a voter
	cfgFuture := leader.Raft.GetConfiguration()
	require.NoError(t, cfgFuture.Error())
	var foundNode4 bool
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == "node-4" {
			foundNode4 = true
			break
		}
	}
	require.True(t, foundNode4, "node-4 must be registered in Raft configuration")

	// 5. Enqueue more jobs to exercise replication with the newly added node
	for i := 5; i < 10; i++ {
		_, err := client.Enqueue(ctx, &relaypb.EnqueueRequest{
			Queue:       q,
			Payload:     []byte(fmt.Sprintf("after-join-%d", i)),
			MaxAttempts: 3,
		})
		require.NoError(t, err)
	}

	// 6. Assert node-4 converges to exact same state as leader
	require.Eventually(t, func() bool {
		dumpLeader, err1 := tcs.tc.StateDump(leader.ID)
		dump4, err2 := tcs.tc.StateDump("node-4")
		return err1 == nil && err2 == nil && reflect.DeepEqual(dumpLeader, dump4)
	}, 10*time.Second, 100*time.Millisecond, "node-4 must replicate state and converge with leader")
}
