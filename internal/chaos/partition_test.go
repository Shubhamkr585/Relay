package chaos

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/shubhamkr585/relay/internal/cluster"
)

// TestTransport_PartitionDropAndHeal verifies that PartitionableTransport around real TCP transports
// blocks Raft traffic when isolated (cannot become leader, cannot receive entries), and healing restores replication.
func TestTransport_PartitionDropAndHeal(t *testing.T) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	transports := make(map[string]*PartitionableTransport)
	servers := make([]raft.Server, 3)

	for i, id := range nodeIDs {
		// Create real TCP transport
		pt, err := NewRealTCPTransport(raft.ServerID(id), 3, 2*time.Second)
		require.NoError(t, err)
		defer pt.Close()

		transports[id] = pt
		servers[i] = raft.Server{
			ID:      raft.ServerID(id),
			Address: pt.LocalAddr(),
		}
	}

	nodes := make(map[string]*raft.Raft)
	fsms := make(map[string]*cluster.FSM)

	for _, id := range nodeIDs {
		dir := t.TempDir()
		fsm, err := cluster.NewFSM(filepath.Join(dir, "fsm.db"))
		require.NoError(t, err)
		defer fsm.Close()
		fsms[id] = fsm

		cfg := raft.DefaultConfig()
		cfg.LocalID = raft.ServerID(id)
		cfg.HeartbeatTimeout = 100 * time.Millisecond
		cfg.ElectionTimeout = 250 * time.Millisecond
		cfg.LeaderLeaseTimeout = 100 * time.Millisecond

		logStore := raft.NewInmemStore()
		stableStore := raft.NewInmemStore()
		snapStore := raft.NewInmemSnapshotStore()

		r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, transports[id])
		require.NoError(t, err)
		defer r.Shutdown()
		nodes[id] = r
	}

	// Bootstrap cluster on node-1
	bootFuture := nodes["node-1"].BootstrapCluster(raft.Configuration{Servers: servers})
	require.NoError(t, bootFuture.Error())

	// 1. Wait for stable leader
	var leaderID string
	require.Eventually(t, func() bool {
		for id, r := range nodes {
			if r.State() == raft.Leader {
				leaderID = id
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "Leader must be elected")

	// Pick a follower to isolate
	var followerID string
	for _, id := range nodeIDs {
		if id != leaderID {
			followerID = id
			break
		}
	}

	// 2. Partition follower: isolate it
	transports[followerID].SetIsolated(true)
	require.True(t, transports[followerID].IsIsolated())

	// Apply an entry on the leader
	applyFuture := nodes[leaderID].Apply([]byte("test-entry-1"), 2*time.Second)
	require.NoError(t, applyFuture.Error())

	// The remaining majority commits and advances applied index
	leaderIndex := nodes[leaderID].AppliedIndex()
	require.Greater(t, leaderIndex, uint64(0))

	// Wait 500ms: isolated node MUST NOT receive this entry
	time.Sleep(500 * time.Millisecond)
	isolatedIndex := nodes[followerID].AppliedIndex()
	require.Less(t, isolatedIndex, leaderIndex, "Isolated node must NOT receive replicated log entries")

	// 3. Isolated node cannot become leader even if election timeout fires
	require.NotEqual(t, raft.Leader, nodes[followerID].State(), "Isolated node cannot become leader")

	// 4. Heal partition: reconnect isolated node
	transports[followerID].SetIsolated(false)
	require.False(t, transports[followerID].IsIsolated())

	// Wait for follower to catch up with leader and indices to converge
	require.Eventually(t, func() bool {
		return nodes[followerID].AppliedIndex() == nodes[leaderID].AppliedIndex()
	}, 10*time.Second, 100*time.Millisecond, "Log applied indices must converge after healing")
}
