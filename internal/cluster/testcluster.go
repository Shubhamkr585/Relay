package cluster

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ClusterNode represents a single isolated Raft node running inside the test process.
type ClusterNode struct {
	ID        string
	Dir       string
	FSMPath   string
	FSM       *FSM
	Raft      *raft.Raft
	Transport *raft.InmemTransport
	LogStore  *raftboltdb.BoltStore
	SnapStore *raft.FileSnapshotStore
	Batcher   *LeaderBatcher
	mu        sync.Mutex
	isDead    bool
}

// IsDead returns true if the node is killed/stopped.
func (n *ClusterNode) IsDead() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.isDead
}

// AppliedIndex returns the Raft applied index safely.
func (n *ClusterNode) AppliedIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.isDead || n.Raft == nil {
		return 0
	}
	return n.Raft.AppliedIndex()
}

// TestCluster coordinates a 3-node in-process Raft cluster for deterministic testing without Docker.
type TestCluster struct {
	Nodes      map[string]*ClusterNode
	transports map[string]*raft.InmemTransport
	mu         sync.RWMutex
	baseDir    string
}

// NewTestCluster initializes and bootstraps a 3-node Raft cluster.
func NewTestCluster(baseDir string, snapshotThreshold uint64, trailingLogs uint64) (*TestCluster, error) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	transports := make(map[string]*raft.InmemTransport)

	for _, id := range nodeIDs {
		_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[id] = trans
	}

	// Connect all in-memory transports together
	for _, id1 := range nodeIDs {
		for _, id2 := range nodeIDs {
			if id1 != id2 {
				transports[id1].Connect(transports[id2].LocalAddr(), transports[id2])
			}
		}
	}

	tc := &TestCluster{
		Nodes:      make(map[string]*ClusterNode),
		transports: transports,
		baseDir:    baseDir,
	}

	// Prepare bootstrap configuration
	var servers []raft.Server
	for _, id := range nodeIDs {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(id),
			Address: transports[id].LocalAddr(),
		})
	}
	bootstrapConfig := raft.Configuration{Servers: servers}

	// Initialize each node
	for i, id := range nodeIDs {
		nodeDir := filepath.Join(baseDir, id)
		if err := os.MkdirAll(nodeDir, 0755); err != nil {
			return nil, err
		}

		node, err := tc.createAndStartNode(id, nodeDir, transports[id], snapshotThreshold, trailingLogs, i == 0, bootstrapConfig)
		if err != nil {
			_ = tc.Shutdown()
			return nil, err
		}
		tc.Nodes[id] = node
	}

	return tc, nil
}

// createAndStartNode configures and starts a single node.
func (tc *TestCluster) createAndStartNode(
	id string,
	nodeDir string,
	transport *raft.InmemTransport,
	snapshotThreshold uint64,
	trailingLogs uint64,
	bootstrap bool,
	bootstrapConfig raft.Configuration,
) (*ClusterNode, error) {
	fsmPath := filepath.Join(nodeDir, "fsm.db")
	fsm, err := NewFSM(fsmPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create FSM for %s: %w", id, err)
	}

	logDbPath := filepath.Join(nodeDir, "raft-log.db")
	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path: logDbPath,
	})
	if err != nil {
		_ = fsm.Close()
		return nil, fmt.Errorf("failed to create bolt store for %s: %w", id, err)
	}

	snapDir := filepath.Join(nodeDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		_ = fsm.Close()
		_ = boltStore.Close()
		return nil, err
	}

	snapStore, err := raft.NewFileSnapshotStore(snapDir, 2, io.Discard)
	if err != nil {
		_ = fsm.Close()
		_ = boltStore.Close()
		return nil, err
	}

	// B5 Config rules
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(id)
	raftCfg.HeartbeatTimeout = 1000 * time.Millisecond
	raftCfg.ElectionTimeout = 1000 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 500 * time.Millisecond
	raftCfg.LogOutput = io.Discard // Clean test logs

	if snapshotThreshold > 0 {
		raftCfg.SnapshotThreshold = snapshotThreshold
		raftCfg.SnapshotInterval = 200 * time.Millisecond
	}
	if trailingLogs > 0 {
		raftCfg.TrailingLogs = trailingLogs
	}

	if bootstrap {
		if err := raft.BootstrapCluster(raftCfg, boltStore, boltStore, snapStore, transport, bootstrapConfig); err != nil {
			_ = fsm.Close()
			_ = boltStore.Close()
			return nil, fmt.Errorf("failed to bootstrap cluster on %s: %w", id, err)
		}
	}

	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		_ = fsm.Close()
		_ = boltStore.Close()
		return nil, fmt.Errorf("failed to start raft on %s: %w", id, err)
	}

	batcher := NewLeaderBatcher(r, fsm, DefaultLeaderBatcherConfig())

	return &ClusterNode{
		ID:        id,
		Dir:       nodeDir,
		FSMPath:   fsmPath,
		FSM:       fsm,
		Raft:      r,
		Transport: transport,
		LogStore:  boltStore,
		SnapStore: snapStore,
		Batcher:   batcher,
	}, nil
}

// GetLeader returns the current leader node, or nil if no leader is established.
func (tc *TestCluster) GetLeader() *ClusterNode {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	for _, node := range tc.Nodes {
		node.mu.Lock()
		isDead := node.isDead
		r := node.Raft
		node.mu.Unlock()

		if !isDead && r != nil && r.State() == raft.Leader {
			return node
		}
	}
	return nil
}

// WaitForLeader polls until a stable leader is elected or timeout expires.
func (tc *TestCluster) WaitForLeader(timeout time.Duration) (*ClusterNode, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := tc.GetLeader()
		if leader != nil {
			return leader, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("timed out waiting for leader after %v", timeout)
}

// KillLeader terminates the current active leader.
func (tc *TestCluster) KillLeader() (*ClusterNode, error) {
	leader := tc.GetLeader()
	if leader == nil {
		return nil, fmt.Errorf("no active leader found to kill")
	}
	if err := tc.KillNode(leader.ID); err != nil {
		return nil, err
	}
	return leader, nil
}

// KillNode gracefully shuts down Raft, batcher, transport, and storage on the given node.
func (tc *TestCluster) KillNode(id string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	node, ok := tc.Nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isDead {
		return nil
	}
	node.isDead = true

	node.Batcher.Stop()
	future := node.Raft.Shutdown()
	_ = future.Error()
	_ = node.Transport.Close()
	_ = node.FSM.Close()
	_ = node.LogStore.Close()

	// Disconnect from peer transports
	for peerID, trans := range tc.transports {
		if peerID != id {
			trans.Disconnect(node.Transport.LocalAddr())
		}
	}

	return nil
}

// Partition isolates the specified node from all other nodes by disconnecting InmemTransport.
func (tc *TestCluster) Partition(id string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	node, ok := tc.Nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}

	for peerID, trans := range tc.transports {
		if peerID != id {
			trans.Disconnect(node.Transport.LocalAddr())
			node.Transport.Disconnect(trans.LocalAddr())
		}
	}
	return nil
}

// HealPartition restores all transport connections between all alive nodes.
func (tc *TestCluster) HealPartition() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for id1, node1 := range tc.Nodes {
		node1.mu.Lock()
		dead1 := node1.isDead
		node1.mu.Unlock()
		if dead1 {
			continue
		}

		for id2, node2 := range tc.Nodes {
			if id1 == id2 {
				continue
			}
			node2.mu.Lock()
			dead2 := node2.isDead
			node2.mu.Unlock()
			if dead2 {
				continue
			}

			tc.transports[id1].Connect(tc.transports[id2].LocalAddr(), tc.transports[id2])
		}
	}
	return nil
}

// RestartNode restarts a killed node.
// B4 & Part C Rule: Reuses the same Raft log/stable/snapshot dirs, but WIPES ONLY the FSM bbolt file.
func (tc *TestCluster) RestartNode(id string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	node, ok := tc.Nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.isDead {
		return fmt.Errorf("node %s is already running", id)
	}

	// Wipe ONLY the FSM bbolt file per specification!
	_ = os.Remove(node.FSMPath)

	// Recreate in-memory transport
	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	tc.transports[id] = trans

	// Reconnect with running peers
	for peerID, peerTrans := range tc.transports {
		if peerID != id {
			peerNode := tc.Nodes[peerID]
			peerNode.mu.Lock()
			peerDead := peerNode.isDead
			peerNode.mu.Unlock()

			if !peerDead {
				trans.Connect(peerTrans.LocalAddr(), peerTrans)
				peerTrans.Connect(trans.LocalAddr(), trans)
			}
		}
	}

	// Create fresh FSM
	fsm, err := NewFSM(node.FSMPath)
	if err != nil {
		return fmt.Errorf("failed to recreate FSM on restart %s: %w", id, err)
	}

	// Reopen existing Raft bolt log store
	logDbPath := filepath.Join(node.Dir, "raft-log.db")
	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path: logDbPath,
	})
	if err != nil {
		_ = fsm.Close()
		return fmt.Errorf("failed to reopen bolt log store on restart %s: %w", id, err)
	}

	snapDir := filepath.Join(node.Dir, "snapshots")
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 2, io.Discard)
	if err != nil {
		_ = fsm.Close()
		_ = boltStore.Close()
		return err
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(id)
	raftCfg.HeartbeatTimeout = 1000 * time.Millisecond
	raftCfg.ElectionTimeout = 1000 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 500 * time.Millisecond
	raftCfg.LogOutput = io.Discard

	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapStore, trans)
	if err != nil {
		_ = fsm.Close()
		_ = boltStore.Close()
		return fmt.Errorf("failed to restart raft on %s: %w", id, err)
	}

	batcher := NewLeaderBatcher(r, fsm, DefaultLeaderBatcherConfig())

	node.FSM = fsm
	node.Raft = r
	node.Transport = trans
	node.LogStore = boltStore
	node.SnapStore = snapStore
	node.Batcher = batcher
	node.isDead = false

	return nil
}

// CreateNonBootstrapNode initializes a new unbootstrapped node and interconnects its transport to all existing nodes.
func (tc *TestCluster) CreateNonBootstrapNode(id string) (*ClusterNode, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	tc.transports[id] = trans

	// Connect new transport with all existing nodes
	for existingID, existingTrans := range tc.transports {
		if existingID != id {
			trans.Connect(existingTrans.LocalAddr(), existingTrans)
			existingTrans.Connect(trans.LocalAddr(), trans)
		}
	}

	nodeDir := filepath.Join(tc.baseDir, id)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, err
	}

	node, err := tc.createAndStartNode(id, nodeDir, trans, 0, 0, false, raft.Configuration{})
	if err != nil {
		return nil, err
	}
	tc.Nodes[id] = node
	return node, nil
}

// StateDump dumps all buckets as sorted (key, value) pairs for byte-for-byte cluster consistency testing.
func (tc *TestCluster) StateDump(id string) (map[string][]KVPair, error) {
	tc.mu.RLock()
	node, ok := tc.Nodes[id]
	tc.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("node %s not found", id)
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.isDead {
		return nil, fmt.Errorf("node %s is dead", id)
	}

	return node.FSM.DumpState()
}

// Shutdown cleanly stops all cluster nodes and transports.
func (tc *TestCluster) Shutdown() error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for id, node := range tc.Nodes {
		node.mu.Lock()
		if !node.isDead {
			node.isDead = true
			node.Batcher.Stop()
			future := node.Raft.Shutdown()
			_ = future.Error()
			_ = node.Transport.Close()
			_ = node.FSM.Close()
			_ = node.LogStore.Close()
		}
		node.mu.Unlock()
		delete(tc.Nodes, id)
	}
	return nil
}
