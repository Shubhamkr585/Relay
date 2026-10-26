package bench

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/cluster"
	"github.com/shubhamkr585/relay/internal/server"
)

// ClusterTarget defines an operational interface against a 3-node Relay cluster.
type ClusterTarget interface {
	// GetClient returns a gRPC client to an available cluster node.
	GetClient() (relaypb.RelayServiceClient, error)

	// GetLeaderClient returns a gRPC client directly to the active Raft leader.
	GetLeaderClient() (relaypb.RelayServiceClient, error)

	// GetStats retrieves queue depth statistics.
	GetStats(ctx context.Context, queue string) (*relaypb.StatsResponse, error)

	// Close cleans up connections and stops cluster instances if locally managed.
	Close() error
}

// InProcessTarget wraps an in-process 3-node cluster with real gRPC servers.
type InProcessTarget struct {
	tc        *cluster.TestCluster
	servers   map[string]*server.Server
	clients   map[string]relaypb.RelayServiceClient
	conns     map[string]*grpc.ClientConn
	grpcAddrs map[string]string
	batchers  map[string]*cluster.LeaderBatcher
	mu        sync.RWMutex
}

// NewInProcessTarget creates a 3-node in-process cluster with default micro-batcher (2ms/64).
func NewInProcessTarget(baseDir string) (*InProcessTarget, error) {
	return NewInProcessTargetCustom(baseDir, 2*time.Millisecond, 64)
}

// NewInProcessTargetCustom creates a 3-node in-process cluster with custom batcher settings.
func NewInProcessTargetCustom(baseDir string, batchWindow time.Duration, batchMaxSize int) (*InProcessTarget, error) {
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create test cluster: %w", err)
	}

	target := &InProcessTarget{
		tc:        tc,
		servers:   make(map[string]*server.Server),
		clients:   make(map[string]relaypb.RelayServiceClient),
		conns:     make(map[string]*grpc.ClientConn),
		grpcAddrs: make(map[string]string),
		batchers:  make(map[string]*cluster.LeaderBatcher),
	}

	peerResolver := func(leaderID raft.ServerID, leaderRaftAddr raft.ServerAddress) (string, error) {
		target.mu.RLock()
		defer target.mu.RUnlock()
		if addr, ok := target.grpcAddrs[string(leaderID)]; ok {
			return addr, nil
		}
		return "", fmt.Errorf("leader %s not found in grpcAddrs", leaderID)
	}

	for id, node := range tc.Nodes {
		var b *cluster.LeaderBatcher
		if batchMaxSize == 1 {
			// Unbatched (0ms/1-op): stop default batcher, create 1-op batcher
			node.Batcher.Stop()
			b = cluster.NewLeaderBatcher(node.Raft, node.FSM, cluster.LeaderBatcherConfig{
				BatchWindow:  time.Nanosecond,
				BatchMaxSize: 1,
			})
			node.Batcher = b
		} else {
			b = node.Batcher
		}
		target.batchers[id] = b

		cfg := server.ServerConfig{
			NodeID:       id,
			PeerResolver: peerResolver,
		}
		srv := server.NewServer(cfg, node.Raft, node.FSM, b, nil, nil)
		if err := srv.Start("127.0.0.1:0", "127.0.0.1:0"); err != nil {
			target.Close()
			return nil, fmt.Errorf("failed to start server on %s: %w", id, err)
		}

		target.servers[id] = srv
		target.grpcAddrs[id] = srv.GRPCAddr()

		conn, err := grpc.NewClient(srv.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			target.Close()
			return nil, fmt.Errorf("failed to connect gRPC client to %s: %w", id, err)
		}
		target.conns[id] = conn
		target.clients[id] = relaypb.NewRelayServiceClient(conn)
	}

	if _, err := tc.WaitForLeader(10 * time.Second); err != nil {
		target.Close()
		return nil, fmt.Errorf("failed to elect initial leader: %w", err)
	}

	return target, nil
}

func (t *InProcessTarget) GetClient() (relaypb.RelayServiceClient, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.clients) == 0 {
		return nil, fmt.Errorf("no active clients")
	}
	var clientList []relaypb.RelayServiceClient
	for _, c := range t.clients {
		clientList = append(clientList, c)
	}
	return clientList[rand.Intn(len(clientList))], nil
}

func (t *InProcessTarget) GetLeaderClient() (relaypb.RelayServiceClient, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.tc != nil {
		leader := t.tc.GetLeader()
		if leader != nil {
			if c, ok := t.clients[leader.ID]; ok {
				return c, nil
			}
		}
	}
	return t.GetClient()
}

func (t *InProcessTarget) GetStats(ctx context.Context, queue string) (*relaypb.StatsResponse, error) {
	client, err := t.GetLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.Stats(ctx, &relaypb.StatsRequest{Queue: queue})
}

func (t *InProcessTarget) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, conn := range t.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	for _, srv := range t.servers {
		if srv != nil {
			srv.Stop()
		}
	}
	for _, b := range t.batchers {
		if b != nil {
			b.Stop()
		}
	}
	if t.tc != nil {
		_ = t.tc.Shutdown()
	}
	return nil
}

// RemoteTarget connects to an existing Relay cluster via --servers host:port,host:port,...
type RemoteTarget struct {
	addrs   []string
	conns   []*grpc.ClientConn
	clients []relaypb.RelayServiceClient
	mu      sync.RWMutex
}

// NewRemoteTarget connects to pre-existing servers.
func NewRemoteTarget(serversCsv string) (*RemoteTarget, error) {
	parts := strings.Split(serversCsv, ",")
	var addrs []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			addrs = append(addrs, trimmed)
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no valid server addresses provided")
	}

	conns := make([]*grpc.ClientConn, len(addrs))
	clients := make([]relaypb.RelayServiceClient, len(addrs))

	for i, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// Close any already opened
			for j := 0; j < i; j++ {
				_ = conns[j].Close()
			}
			return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
		}
		conns[i] = conn
		clients[i] = relaypb.NewRelayServiceClient(conn)
	}

	return &RemoteTarget{
		addrs:   addrs,
		conns:   conns,
		clients: clients,
	}, nil
}

func (r *RemoteTarget) GetClient() (relaypb.RelayServiceClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.clients) == 0 {
		return nil, fmt.Errorf("no active clients")
	}
	idx := rand.Intn(len(r.clients))
	return r.clients[idx], nil
}

func (r *RemoteTarget) GetLeaderClient() (relaypb.RelayServiceClient, error) {
	// With transparent forwarding, any node will forward mutating calls to the leader.
	return r.GetClient()
}

func (r *RemoteTarget) GetStats(ctx context.Context, queue string) (*relaypb.StatsResponse, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, client := range r.clients {
		resp, err := client.Stats(ctx, &relaypb.StatsRequest{Queue: queue})
		if err == nil {
			return resp, nil
		}
	}
	return nil, fmt.Errorf("failed to get stats from any cluster node")
}

func (r *RemoteTarget) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conn := range r.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	r.conns = nil
	r.clients = nil
	return nil
}

// GetFreePort returns an available local TCP port.
func GetFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// VerifyClusterWarmup ensures a cluster target has an elected leader that is stable
// and recognized by all followers before measurement begins.
func VerifyClusterWarmup(target ClusterTarget, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if inproc, ok := target.(*InProcessTarget); ok {
			inproc.mu.RLock()
			tc := inproc.tc
			inproc.mu.RUnlock()
			if tc != nil {
				leader := tc.GetLeader()
				if leader != nil && leader.Raft != nil {
					leaderAddr := leader.Transport.LocalAddr()
					allFollowersKnow := true
					for _, node := range tc.Nodes {
						if !node.IsDead() && node.Raft != nil {
							if node.Raft.Leader() != leaderAddr {
								allFollowersKnow = false
								break
							}
						}
					}
					if allFollowersKnow {
						// Issue a probe stats call to confirm responsiveness
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
						_, err := inproc.GetStats(ctx, "warmup-probe-queue")
						cancel()
						if err == nil {
							time.Sleep(100 * time.Millisecond) // stabilize
							return nil
						}
					}
				}
			}
		} else {
			// Remote target probe
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err := target.GetStats(ctx, "warmup-probe-queue")
			cancel()
			if err == nil {
				time.Sleep(100 * time.Millisecond)
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("cluster warmup verification timed out after %v", timeout)
}
