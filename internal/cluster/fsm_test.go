package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/cluster"
	"github.com/shubhamkr585/relay/internal/domain"
	"github.com/shubhamkr585/relay/internal/store"
)

// Helper to wait until all live nodes have applied the highest committed index in the cluster.
func waitForReplication(t *testing.T, tc *cluster.TestCluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		leader := tc.GetLeader()
		if leader == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		var maxIndex uint64
		for _, node := range tc.Nodes {
			if !node.IsDead() {
				idx := node.AppliedIndex()
				if idx > maxIndex {
					maxIndex = idx
				}
			}
		}

		if maxIndex == 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		allMatch := true
		for _, node := range tc.Nodes {
			if !node.IsDead() {
				if node.AppliedIndex() < maxIndex {
					allMatch = false
					break
				}
			}
		}

		if allMatch {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for cluster replication after %v", timeout)
}

func TestThreeNodeReplicationAndStateEquality(t *testing.T) {
	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)
	defer tc.Shutdown()

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)
	require.NotNil(t, leader)

	ctx := context.Background()

	// 1. Enqueue multiple jobs
	j1, dedup1, err := leader.Batcher.Enqueue(ctx, "tasks", []byte("payload-1"), 1, 0, "idemp-1", 3)
	require.NoError(t, err)
	require.False(t, dedup1)
	require.NotEmpty(t, j1.Id)

	j2, dedup2, err := leader.Batcher.Enqueue(ctx, "tasks", []byte("payload-2"), 0, 0, "idemp-2", 3)
	require.NoError(t, err)
	require.False(t, dedup2)

	// 2. Reserve a job (must pick Priority 0 j2)
	resJob, token, err := leader.Batcher.Reserve(ctx, "tasks", "worker-1", 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, j2.Id, resJob.Id)
	require.NotEmpty(t, token)

	// 3. Ack the job
	err = leader.Batcher.Ack(ctx, resJob.Id, token)
	require.NoError(t, err)

	// Wait for replication across all 3 nodes
	waitForReplication(t, tc, 5*time.Second)

	// Verify byte-for-byte identical state across all 3 nodes
	dump1, err := tc.StateDump("node-1")
	require.NoError(t, err)
	dump2, err := tc.StateDump("node-2")
	require.NoError(t, err)
	dump3, err := tc.StateDump("node-3")
	require.NoError(t, err)

	require.Equal(t, dump1, dump2, "node-1 and node-2 state must be identical")
	require.Equal(t, dump1, dump3, "node-1 and node-3 state must be identical")
}

func TestLeaderKillAndAutomaticFailover(t *testing.T) {
	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)
	defer tc.Shutdown()

	oldLeader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx := context.Background()

	// Write initial job
	j, _, err := oldLeader.Batcher.Enqueue(ctx, "failover-q", []byte("data-1"), 0, 0, "key-1", 3)
	require.NoError(t, err)

	// Kill the leader abruptly
	oldLeaderID := oldLeader.ID
	err = tc.KillNode(oldLeaderID)
	require.NoError(t, err)

	// Wait for failover to elect a new leader from the remaining 2 nodes
	var newLeader *cluster.ClusterNode
	require.Eventually(t, func() bool {
		l := tc.GetLeader()
		if l != nil && l.ID != oldLeaderID {
			newLeader = l
			return true
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "new leader was not elected after failover")

	// Writes MUST succeed on the new leader
	j2, _, err := newLeader.Batcher.Enqueue(ctx, "failover-q", []byte("data-2"), 0, 0, "key-2", 3)
	require.NoError(t, err)
	require.NotEmpty(t, j2.Id)

	// Can reserve the previous job on the new leader
	res, token, err := newLeader.Batcher.Reserve(ctx, "failover-q", "worker-new", time.Minute)
	require.NoError(t, err)
	require.Equal(t, j.Id, res.Id)
	require.NotEmpty(t, token)
	require.NoError(t, newLeader.Batcher.Ack(ctx, res.Id, token))
}

func TestSnapshotCreationAndLaggingFollowerRestore(t *testing.T) {
	baseDir := t.TempDir()
	// Set low snapshot threshold (5 logs) and trailing logs (2 logs) to force snapshots
	tc, err := cluster.NewTestCluster(baseDir, 5, 2)
	require.NoError(t, err)
	defer tc.Shutdown()

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Isolate / Kill node-3 so it misses log entries
	err = tc.KillNode("node-3")
	require.NoError(t, err)

	// 2. Commit 15 operations on the 2 surviving nodes (exceeding snapshot threshold 5)
	for i := 0; i < 15; i++ {
		_, _, err := leader.Batcher.Enqueue(ctx, "snap-q", []byte(fmt.Sprintf("item-%d", i)), 0, 0, fmt.Sprintf("k-%d", i), 3)
		require.NoError(t, err)
	}

	// Force snapshot on leader to compact logs past TrailingLogs
	snapFuture := leader.Raft.Snapshot()
	require.NoError(t, snapFuture.Error())

	snaps, err := leader.SnapStore.List()
	require.NoError(t, err)
	require.NotEmpty(t, snaps, "snapshot must exist on leader snapshot store")

	// 3. Restart node-3 with a wiped FSM db
	err = tc.RestartNode("node-3")
	require.NoError(t, err)

	// 4. Wait for node-3 to restore snapshot and catch up
	waitForReplication(t, tc, 6*time.Second)

	dumpLeader, err := tc.StateDump(leader.ID)
	require.NoError(t, err)
	dump3, err := tc.StateDump("node-3")
	require.NoError(t, err)

	require.Equal(t, dumpLeader, dump3, "node-3 state after snapshot restore must match leader exactly")
}

func TestRestartNodeConvergesViaSnapshotAndLogReplay(t *testing.T) {
	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 10, 5)
	require.NoError(t, err)
	defer tc.Shutdown()

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue some initial jobs
	for i := 0; i < 5; i++ {
		_, _, err := leader.Batcher.Enqueue(ctx, "restart-q", []byte(fmt.Sprintf("job-%d", i)), 0, 0, fmt.Sprintf("k-%d", i), 3)
		require.NoError(t, err)
	}
	waitForReplication(t, tc, 5*time.Second)

	// Pick a follower node to kill and restart
	followerID := "node-2"
	if leader.ID == "node-2" {
		followerID = "node-3"
	}

	err = tc.KillNode(followerID)
	require.NoError(t, err)

	// Commit more writes while follower is dead
	for i := 5; i < 10; i++ {
		_, _, err := leader.Batcher.Enqueue(ctx, "restart-q", []byte(fmt.Sprintf("job-%d", i)), 0, 0, fmt.Sprintf("k-%d", i), 3)
		require.NoError(t, err)
	}

	// Restart the node (wiping its FSM db per spec)
	err = tc.RestartNode(followerID)
	require.NoError(t, err)

	// Wait for full convergence
	waitForReplication(t, tc, 5*time.Second)

	require.Eventually(t, func() bool {
		dumpLeader, err1 := tc.StateDump(leader.ID)
		dumpFollower, err2 := tc.StateDump(followerID)
		return err1 == nil && err2 == nil && reflect.DeepEqual(dumpLeader, dumpFollower)
	}, 5*time.Second, 50*time.Millisecond, "restarted node with wiped db must converge to identical state")

	// Full 3-node rolling restart
	require.NoError(t, tc.KillNode("node-1"))
	require.NoError(t, tc.KillNode("node-2"))
	require.NoError(t, tc.KillNode("node-3"))

	require.NoError(t, tc.RestartNode("node-1"))
	require.NoError(t, tc.RestartNode("node-2"))
	require.NoError(t, tc.RestartNode("node-3"))

	newLeader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	// Writes continue seamlessly after full cluster restart
	_, _, err = newLeader.Batcher.Enqueue(ctx, "restart-q", []byte("after-full-restart"), 0, 0, "k-post", 3)
	require.NoError(t, err)
}

func TestFSMDeterminism_IdenticalCommandLog(t *testing.T) {
	// Standalone FSM determinism verification:
	// Applying the exact same committed log sequence to two separate fresh FSMs
	// must result in 100% byte-for-byte identical database state.
	dirA := t.TempDir()
	dirB := t.TempDir()

	fsmA, err := cluster.NewFSM(filepath.Join(dirA, "fsm.db"))
	require.NoError(t, err)
	defer fsmA.Close()

	fsmB, err := cluster.NewFSM(filepath.Join(dirB, "fsm.db"))
	require.NoError(t, err)
	defer fsmB.Close()

	// Prepare a rich sequence of log entries
	nowNano := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

	// 1. Enqueue Log
	cmd1 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: nowNano,
		Job: &relaypb.Job{
			Id:             "job-det-1",
			Queue:          "det-q",
			Payload:        []byte("det-payload-1"),
			Priority:       0,
			State:          relaypb.JobState_JOB_STATE_PENDING,
			Attempts:       0,
			MaxAttempts:    3,
			RunAt:          nowNano,
			CreatedAt:      nowNano,
			IdempotencyKey: "idemp-det-1",
		},
	}
	data1, _ := proto.Marshal(cmd1)
	log1 := &raft.Log{Index: 1, Term: 1, Data: data1}

	// 2. Reserve Log
	cmd2 := &relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
		TimestampNano:   nowNano + 1000,
		Queue:           "det-q",
		WorkerId:        "worker-det",
		LeaseDurationMs: 5000,
	}
	data2, _ := proto.Marshal(cmd2)
	log2 := &raft.Log{Index: 2, Term: 1, Data: data2}

	// 3. Nack Log (triggers deterministic backoff jitter)
	cmd3 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_NACK,
		TimestampNano: nowNano + 2000,
		Job:           &relaypb.Job{Id: "job-det-1"},
		LeaseToken:    "2-0", // Formatted deterministically by FSM
		LastError:     "temporary failure",
	}
	data3, _ := proto.Marshal(cmd3)
	log3 := &raft.Log{Index: 3, Term: 1, Data: data3}

	// 4. Batch Log with multiple mixed operations
	cmdBatch := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: nowNano + 3000,
		SubCommands: []*relaypb.RaftCommand{
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
				TimestampNano: nowNano + 3000,
				Job: &relaypb.Job{
					Id:             "job-det-2",
					Queue:          "det-q",
					Payload:        []byte("det-payload-2"),
					Priority:       5,
					MaxAttempts:    2,
					RunAt:          nowNano + 3000,
					CreatedAt:      nowNano + 3000,
					IdempotencyKey: "idemp-det-2",
				},
			},
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
				TimestampNano: nowNano + 5000000000,
			},
		},
	}
	dataBatch, _ := proto.Marshal(cmdBatch)
	log4 := &raft.Log{Index: 4, Term: 1, Data: dataBatch}

	logs := []*raft.Log{log1, log2, log3, log4}

	// Apply to FSM A
	for _, l := range logs {
		_ = fsmA.Apply(l)
	}

	// Apply to FSM B
	for _, l := range logs {
		_ = fsmB.Apply(l)
	}

	// Compare full database states
	stateA, err := fsmA.DumpState()
	require.NoError(t, err)
	stateB, err := fsmB.DumpState()
	require.NoError(t, err)

	require.Equal(t, stateA, stateB, "FSM state must be 100% byte-for-byte deterministic across independent replicas")
}

func TestMixedBatch_PartialFailuresDoNotAbort(t *testing.T) {
	dir := t.TempDir()
	fsm, err := cluster.NewFSM(filepath.Join(dir, "fsm.db"))
	require.NoError(t, err)
	defer fsm.Close()

	nowNano := time.Now().UnixNano()

	// Initial job
	initCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: nowNano,
		Job: &relaypb.Job{
			Id:          "leased-job",
			Queue:       "mixed-q",
			Payload:     []byte("leased-data"),
			MaxAttempts: 3,
			RunAt:       nowNano,
			CreatedAt:   nowNano,
		},
	}
	d0, _ := proto.Marshal(initCmd)
	_ = fsm.Apply(&raft.Log{Index: 1, Data: d0})

	// Reserve it to establish lease_token "2-0"
	resCmd := &relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
		TimestampNano:   nowNano + 10,
		Queue:           "mixed-q",
		WorkerId:        "worker-1",
		LeaseDurationMs: 10000,
	}
	dRes, _ := proto.Marshal(resCmd)
	_ = fsm.Apply(&raft.Log{Index: 2, Data: dRes})

	// Construct a mixed batch:
	// Sub-0: Valid Enqueue (job-valid-1)
	// Sub-1: Ack with stale/mismatched lease token -> MUST fail with ErrStaleLease
	// Sub-2: Valid Enqueue (job-valid-2)
	// Sub-3: Ack with correct lease token ("2-0") -> MUST succeed
	// Sub-4: Nack on non-existent job -> MUST fail with ErrNotFound
	// Sub-5: Valid Enqueue (job-valid-3)
	batchCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: nowNano + 100,
		SubCommands: []*relaypb.RaftCommand{
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
				TimestampNano: nowNano + 100,
				Job: &relaypb.Job{
					Id:          "job-valid-1",
					Queue:       "mixed-q",
					MaxAttempts: 3,
					RunAt:       nowNano + 100,
					CreatedAt:   nowNano + 100,
				},
			},
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
				TimestampNano: nowNano + 100,
				Job:           &relaypb.Job{Id: "leased-job"},
				LeaseToken:    "wrong-stale-token", // Stale!
			},
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
				TimestampNano: nowNano + 100,
				Job: &relaypb.Job{
					Id:          "job-valid-2",
					Queue:       "mixed-q",
					MaxAttempts: 3,
					RunAt:       nowNano + 100,
					CreatedAt:   nowNano + 100,
				},
			},
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
				TimestampNano: nowNano + 100,
				Job:           &relaypb.Job{Id: "leased-job"},
				LeaseToken:    "2-0", // Correct active token
			},
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_NACK,
				TimestampNano: nowNano + 100,
				Job:           &relaypb.Job{Id: "non-existent-job"},
				LeaseToken:    "any-token",
			},
			{
				Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
				TimestampNano: nowNano + 100,
				Job: &relaypb.Job{
					Id:          "job-valid-3",
					Queue:       "mixed-q",
					MaxAttempts: 3,
					RunAt:       nowNano + 100,
					CreatedAt:   nowNano + 100,
				},
			},
		},
	}
	dBatch, _ := proto.Marshal(batchCmd)
	res := fsm.Apply(&raft.Log{Index: 3, Data: dBatch})
	batchRes, ok := res.(*cluster.BatchApplyResult)
	require.True(t, ok)
	require.Len(t, batchRes.Results, 6)

	// Assert individual outcomes:
	// Sub-0: Valid Enqueue
	require.NoError(t, batchRes.Results[0].Err)
	require.Equal(t, "job-valid-1", batchRes.Results[0].Job.Id)

	// Sub-1: Stale lease token
	require.ErrorIs(t, batchRes.Results[1].Err, domain.ErrStaleLease)

	// Sub-2: Valid Enqueue
	require.NoError(t, batchRes.Results[2].Err)
	require.Equal(t, "job-valid-2", batchRes.Results[2].Job.Id)

	// Sub-3: Valid Ack
	require.NoError(t, batchRes.Results[3].Err)

	// Sub-4: Not found
	require.ErrorIs(t, batchRes.Results[4].Err, domain.ErrNotFound)

	// Sub-5: Valid Enqueue
	require.NoError(t, batchRes.Results[5].Err)
	require.Equal(t, "job-valid-3", batchRes.Results[5].Job.Id)

	// Verify the database state: all 3 valid enqueues MUST exist and be committed!
	state, err := fsm.DumpState()
	require.NoError(t, err)

	jobsBucket := state["jobs"]
	foundValid1, foundValid2, foundValid3 := false, false, false
	for _, kv := range jobsBucket {
		if kv.Key == "job-valid-1" {
			foundValid1 = true
		}
		if kv.Key == "job-valid-2" {
			foundValid2 = true
		}
		if kv.Key == "job-valid-3" {
			foundValid3 = true
		}
	}
	require.True(t, foundValid1, "job-valid-1 must be committed despite errors in adjacent sub-commands")
	require.True(t, foundValid2, "job-valid-2 must be committed despite errors in adjacent sub-commands")
	require.True(t, foundValid3, "job-valid-3 must be committed despite errors in adjacent sub-commands")
}

func TestStaleLeaseRejectionAndEmptyQueuePeek(t *testing.T) {
	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)
	defer tc.Shutdown()

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Reserve on an empty queue: Leader peeks local FSM state and immediately returns ErrQueueEmpty
	initialAppliedIndex := leader.Raft.AppliedIndex()
	_, _, err = leader.Batcher.Reserve(ctx, "completely-empty-queue", "worker-1", time.Minute)
	require.ErrorIs(t, err, domain.ErrQueueEmpty)

	// Verify no empty proposal was submitted to Raft (applied index unchanged)
	require.Equal(t, initialAppliedIndex, leader.Raft.AppliedIndex(), "peeking on empty queue must not propose to Raft")

	// 2. Enqueue and Reserve
	j, _, err := leader.Batcher.Enqueue(ctx, "peek-q", []byte("payload"), 0, 0, "", 3)
	require.NoError(t, err)

	res, validToken, err := leader.Batcher.Reserve(ctx, "peek-q", "worker-1", time.Minute)
	require.NoError(t, err)
	require.Equal(t, j.Id, res.Id)

	// 3. Stale lease token rejection on Ack and Nack
	errStaleAck := leader.Batcher.Ack(ctx, j.Id, "stale-token-xyz")
	require.ErrorIs(t, errStaleAck, domain.ErrStaleLease)

	_, errStaleNack := leader.Batcher.Nack(ctx, j.Id, "stale-token-xyz", "some error", 0)
	require.ErrorIs(t, errStaleNack, domain.ErrStaleLease)

	// Valid Ack succeeds
	require.NoError(t, leader.Batcher.Ack(ctx, j.Id, validToken))
}

func TestTimeNeverMovesBackwards(t *testing.T) {
	dir := t.TempDir()
	fsm, err := cluster.NewFSM(filepath.Join(dir, "fsm.db"))
	require.NoError(t, err)
	defer fsm.Close()

	// 1. Apply command with timestamp T = 5000000
	cmd1 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: 5000000,
		Job: &relaypb.Job{
			Id:          uuid.NewString(),
			Queue:       "time-q",
			MaxAttempts: 3,
			RunAt:       5000000,
			CreatedAt:   5000000,
		},
	}
	d1, _ := proto.Marshal(cmd1)
	_ = fsm.Apply(&raft.Log{Index: 1, Data: d1})

	require.Equal(t, int64(5000000), fsm.LastAppliedNow())

	// 2. Apply a command carrying an older timestamp T = 3000000 (clock skew from another node)
	cmd2 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: 3000000,
		Job: &relaypb.Job{
			Id:          uuid.NewString(),
			Queue:       "time-q",
			MaxAttempts: 3,
			RunAt:       3000000,
			CreatedAt:   3000000,
		},
	}
	d2, _ := proto.Marshal(cmd2)
	_ = fsm.Apply(&raft.Log{Index: 2, Data: d2})

	// Time in FSM must NEVER move backwards: max(lastAppliedNow, cmd.TimestampNano)
	require.Equal(t, int64(5000000), fsm.LastAppliedNow(), "FSM lastAppliedNow must never regress backwards")
}

// TestFullThreeNodeClusterRestart tests full 3-node restart:
// Enqueue and ack a mix of jobs, shut down ALL three nodes, restart them
// (FSM dbs wiped, Raft dirs kept), wait for a leader, and assert every
// acknowledged enqueue is present and StateDump is equal on all nodes.
func TestFullThreeNodeClusterRestart(t *testing.T) {
	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)
	defer tc.Shutdown()

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue and ack a mix of jobs across queues
	type recordedJob struct {
		id       string
		queue    string
		payload  []byte
		priority uint32
		acked    bool
	}
	jobs := make([]*recordedJob, 0, 12)

	for i := 0; i < 12; i++ {
		queue := "q-mix-1"
		if i >= 6 {
			queue = "q-mix-2"
		}
		payload := []byte(fmt.Sprintf("mix-job-%d", i))
		priority := uint32(i % 3)
		j, _, err := leader.Batcher.Enqueue(ctx, queue, payload, priority, 0, fmt.Sprintf("idem-key-mix-%d", i), 3)
		require.NoError(t, err)

		rec := &recordedJob{
			id:       j.Id,
			queue:    queue,
			payload:  payload,
			priority: priority,
			acked:    false,
		}

		// Reserve and Ack every other job
		if i%2 == 0 {
			res, token, err := leader.Batcher.Reserve(ctx, queue, "worker-mix", time.Minute)
			require.NoError(t, err)
			err = leader.Batcher.Ack(ctx, res.Id, token)
			require.NoError(t, err)
			rec.acked = true
		}
		jobs = append(jobs, rec)
	}

	waitForReplication(t, tc, 5*time.Second)

	// Capture state dump before shutdown
	dumpBefore, err := tc.StateDump(leader.ID)
	require.NoError(t, err)

	// Shut down ALL three nodes completely
	require.NoError(t, tc.KillNode("node-1"))
	require.NoError(t, tc.KillNode("node-2"))
	require.NoError(t, tc.KillNode("node-3"))

	// Restart all three nodes (FSM dbs are wiped, Raft log/stable/snapshot dirs kept)
	require.NoError(t, tc.RestartNode("node-1"))
	require.NoError(t, tc.RestartNode("node-2"))
	require.NoError(t, tc.RestartNode("node-3"))

	// Wait for a leader to be re-elected among restarted nodes
	newLeader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	// Wait until all nodes have replicated and caught up
	waitForReplication(t, tc, 10*time.Second)

	// Assert every node matches dumpBefore and all nodes have identical StateDump
	require.Eventually(t, func() bool {
		d1, err1 := tc.StateDump("node-1")
		d2, err2 := tc.StateDump("node-2")
		d3, err3 := tc.StateDump("node-3")
		return err1 == nil && err2 == nil && err3 == nil &&
			reflect.DeepEqual(d1, d2) && reflect.DeepEqual(d2, d3) && reflect.DeepEqual(d1, dumpBefore)
	}, 10*time.Second, 100*time.Millisecond, "all nodes must converge to identical state matching pre-shutdown dump")

	// Verify new writes succeed on new leader and replicate
	_, _, err = newLeader.Batcher.Enqueue(ctx, "q-mix-1", []byte("after-restart"), 0, 0, "idem-post-restart", 3)
	require.NoError(t, err)
	waitForReplication(t, tc, 5*time.Second)
}

// TestBatcherLeadershipLoss_MidFlightProposalsDoNotHang verifies that when
// many concurrent proposals are in flight and the leader is killed mid-flight,
// every pending call returns (result or a retryable error) within a timeout.
// No call may hang.
func TestBatcherLeadershipLoss_MidFlightProposalsDoNotHang(t *testing.T) {
	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)
	defer tc.Shutdown()

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	const numProposals = 50
	errCh := make(chan error, numProposals)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < numProposals; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _, err := leader.Batcher.Enqueue(
				ctx,
				"loss-queue",
				[]byte(fmt.Sprintf("loss-payload-%d", idx)),
				0,
				0,
				fmt.Sprintf("loss-key-%d", idx),
				3,
			)
			errCh <- err
		}(i)
	}

	// Mid-flight: kill the leader abruptly!
	time.Sleep(1 * time.Millisecond)
	_ = tc.KillNode(leader.ID)

	// Assert every pending call returns within a timeout (no call may hang)
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Succeeded: all in-flight proposals returned!
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for in-flight proposals to complete after leader kill - proposals hung!")
	}

	close(errCh)
	for err := range errCh {
		if err != nil {
			require.True(t,
				errors.Is(err, cluster.ErrBatcherClosed) ||
					errors.Is(err, cluster.ErrNotLeader) ||
					errors.Is(err, raft.ErrNotLeader) ||
					errors.Is(err, raft.ErrLeadershipLost) ||
					errors.Is(err, context.Canceled),
				"unexpected non-retryable error: %v", err,
			)
		}
	}
}

// TestClusterAndBatcherGoroutineLeak verifies that after the batcher and
// the cluster shut down, no goroutines are leaked using go.uber.org/goleak.
func TestClusterAndBatcherGoroutineLeak(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("testing.(*M).Run"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)

	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, _, err := leader.Batcher.Enqueue(ctx, "leak-q", []byte("leak-payload"), 0, 0, fmt.Sprintf("leak-k-%d", i), 3)
		require.NoError(t, err)
	}

	waitForReplication(t, tc, 5*time.Second)

	// Clean full cluster shutdown
	require.NoError(t, tc.Shutdown())
}

// TestFSMApply_NilJobAndUnknownTypeDoNotCrashOrCorruptBatch verifies that nil Job
// and unknown command types return errors in their respective SubCommandResult,
// never panic, and do not abort or corrupt valid sub-commands in the same batch.
func TestFSMApply_NilJobAndUnknownTypeDoNotCrashOrCorruptBatch(t *testing.T) {
	dir := t.TempDir()
	fsm, err := cluster.NewFSM(filepath.Join(dir, "fsm.db"))
	require.NoError(t, err)
	defer fsm.Close()

	// SubCommand 0: Valid Enqueue
	cmdValidPre := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: 10000000,
		Job: &relaypb.Job{
			Id:          "job-valid-pre",
			Queue:       "validate-q",
			Payload:     []byte("pre-data"),
			Priority:    1,
			MaxAttempts: 3,
			RunAt:       10000000,
			CreatedAt:   10000000,
		},
	}

	// SubCommand 1: Enqueue with nil Job
	cmdNilEnqueue := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: 10000000,
		Job:           nil,
	}

	// SubCommand 2: Ack with nil Job
	cmdNilAck := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
		TimestampNano: 10000000,
		Job:           nil,
		LeaseToken:    "token-xyz",
	}

	// SubCommand 3: Unknown command type
	cmdUnknown := &relaypb.RaftCommand{
		Type:          relaypb.CommandType(999),
		TimestampNano: 10000000,
	}

	// SubCommand 4: Valid Enqueue
	cmdValidPost := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: 10000000,
		Job: &relaypb.Job{
			Id:          "job-valid-post",
			Queue:       "validate-q",
			Payload:     []byte("post-data"),
			Priority:    2,
			MaxAttempts: 3,
			RunAt:       10000000,
			CreatedAt:   10000000,
		},
	}

	batchCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: 10000000,
		CommandId:     uuid.NewString(),
		SubCommands:   []*relaypb.RaftCommand{cmdValidPre, cmdNilEnqueue, cmdNilAck, cmdUnknown, cmdValidPost},
	}

	data, err := proto.Marshal(batchCmd)
	require.NoError(t, err)

	applyRes := fsm.Apply(&raft.Log{Index: 1, Data: data})
	batchRes, ok := applyRes.(*cluster.BatchApplyResult)
	require.True(t, ok)
	require.Len(t, batchRes.Results, 5)

	// Valid sub-commands succeed
	require.NoError(t, batchRes.Results[0].Err, "sub-command 0 (valid enqueue) must succeed")
	require.Equal(t, "job-valid-pre", batchRes.Results[0].Job.Id)

	// Invalid sub-commands return errors cleanly without panicking
	require.Error(t, batchRes.Results[1].Err, "sub-command 1 (nil enqueue) must return error")
	require.Contains(t, batchRes.Results[1].Err.Error(), "non-nil Job")

	require.Error(t, batchRes.Results[2].Err, "sub-command 2 (nil ack) must return error")
	require.Contains(t, batchRes.Results[2].Err.Error(), "job ID")

	require.Error(t, batchRes.Results[3].Err, "sub-command 3 (unknown type) must return error")
	require.Contains(t, batchRes.Results[3].Err.Error(), "unknown command type")

	require.NoError(t, batchRes.Results[4].Err, "sub-command 4 (valid enqueue) must succeed")
	require.Equal(t, "job-valid-post", batchRes.Results[4].Job.Id)

	// Verify both valid jobs are present in FSM state
	dump, err := fsm.DumpState()
	require.NoError(t, err)

	foundPre := false
	foundPost := false
	for _, kv := range dump["jobs"] {
		if kv.Key == "job-valid-pre" {
			foundPre = true
		}
		if kv.Key == "job-valid-post" {
			foundPost = true
		}
	}
	require.True(t, foundPre, "job-valid-pre must exist in FSM state")
	require.True(t, foundPost, "job-valid-post must exist in FSM state")
}

// TestStoreTx_FailurePathsLeaveZeroPartialWrites audits every store.Tx* operation
// invoked by FSM.Apply: all validation and reads must happen before any write,
// so a failed sub-command leaves zero partial writes in the transaction.
func TestStoreTx_FailurePathsLeaveZeroPartialWrites(t *testing.T) {
	dir := t.TempDir()
	fsm, err := cluster.NewFSM(filepath.Join(dir, "fsm.db"))
	require.NoError(t, err)
	defer fsm.Close()

	applyCmd := func(sub *relaypb.RaftCommand, logIdx uint64) cluster.SubCommandResult {
		batchCmd := &relaypb.RaftCommand{
			Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
			TimestampNano: 10000000,
			CommandId:     uuid.NewString(),
			SubCommands:   []*relaypb.RaftCommand{sub},
		}
		data, _ := proto.Marshal(batchCmd)
		res := fsm.Apply(&raft.Log{Index: logIdx, Data: data}).(*cluster.BatchApplyResult)
		return res.Results[0]
	}

	// 1. Initial State: Enqueue 1 valid job to have non-empty baseline buckets
	j1Cmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: 10000000,
		Job: &relaypb.Job{
			Id:          "job-base-1",
			Queue:       "q-fail-test",
			Payload:     []byte("base-payload"),
			Priority:    1,
			MaxAttempts: 3,
			RunAt:       10000000,
			CreatedAt:   10000000,
		},
	}
	res0 := applyCmd(j1Cmd, 1)
	require.NoError(t, res0.Err)

	// Dump baseline state
	baseDump, err := fsm.DumpState()
	require.NoError(t, err)

	// Failure Path 1: Enqueue with empty queue
	res := applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: 10000000,
		Job: &relaypb.Job{
			Id:    "job-bad",
			Queue: "",
		},
	}, 2)
	require.Error(t, res.Err)
	dump, _ := fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed Enqueue must leave zero partial writes")

	// Failure Path 2: Reserve on empty queue
	res = applyCmd(&relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
		TimestampNano:   10000000,
		Queue:           "completely-empty-queue",
		WorkerId:        "worker-1",
		LeaseDurationMs: 60000,
	}, 3)
	require.ErrorIs(t, res.Err, domain.ErrQueueEmpty)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed Reserve on empty queue must leave zero partial writes")

	// Failure Path 3: Ack non-existent job
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "non-existent-id"},
		LeaseToken:    "token-123",
	}, 4)
	require.ErrorIs(t, res.Err, domain.ErrNotFound)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed Ack (not found) must leave zero partial writes")

	// Failure Path 4: Ack with stale lease token on a job that is PENDING (not leased)
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "job-base-1"},
		LeaseToken:    "stale-token",
	}, 5)
	require.Error(t, res.Err)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed Ack (invalid state/token) must leave zero partial writes")

	// Failure Path 5: Nack non-existent job
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_NACK,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "non-existent-id"},
		LeaseToken:    "token-123",
	}, 6)
	require.ErrorIs(t, res.Err, domain.ErrNotFound)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed Nack (not found) must leave zero partial writes")

	// Failure Path 6: Nack with stale lease token on PENDING job
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_NACK,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "job-base-1"},
		LeaseToken:    "stale-token",
	}, 7)
	require.Error(t, res.Err)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed Nack (invalid state/token) must leave zero partial writes")

	// Failure Path 7: ExtendLease non-existent job
	res = applyCmd(&relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_EXTEND_LEASE,
		TimestampNano:   10000000,
		Job:             &relaypb.Job{Id: "non-existent-id"},
		LeaseToken:      "token-123",
		ExtraDurationMs: 60000,
	}, 8)
	require.ErrorIs(t, res.Err, domain.ErrNotFound)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed ExtendLease (not found) must leave zero partial writes")

	// Failure Path 8: ExtendLease on PENDING job
	res = applyCmd(&relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_EXTEND_LEASE,
		TimestampNano:   10000000,
		Job:             &relaypb.Job{Id: "job-base-1"},
		LeaseToken:      "token-123",
		ExtraDurationMs: 60000,
	}, 9)
	require.Error(t, res.Err)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed ExtendLease (invalid state) must leave zero partial writes")

	// Failure Path 9: RequeueDead on non-existent job
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_REQUEUE_DEAD,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "non-existent-id"},
	}, 10)
	require.ErrorIs(t, res.Err, domain.ErrNotFound)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed RequeueDead (not found) must leave zero partial writes")

	// Failure Path 10: RequeueDead on a PENDING job (must be in DEAD state to requeue)
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_REQUEUE_DEAD,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "job-base-1"},
	}, 11)
	require.ErrorIs(t, res.Err, domain.ErrInvalidState)
	dump, _ = fsm.DumpState()
	require.Equal(t, baseDump, dump, "failed RequeueDead (not dead) must leave zero partial writes")

	// Now Reserve the job legally so it is in LEASED state
	resReserve := applyCmd(&relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
		TimestampNano:   10000000,
		Queue:           "q-fail-test",
		WorkerId:        "worker-1",
		LeaseDurationMs: 60000,
	}, 12)
	require.NoError(t, resReserve.Err)

	// Capture new baseline state with 1 LEASED job
	leasedDump, err := fsm.DumpState()
	require.NoError(t, err)

	// Failure Path 11: Ack with wrong lease token on a LEASED job
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "job-base-1"},
		LeaseToken:    "wrong-token-abc",
	}, 13)
	require.ErrorIs(t, res.Err, domain.ErrStaleLease)
	dump, _ = fsm.DumpState()
	require.Equal(t, leasedDump, dump, "failed Ack (stale lease on leased job) must leave zero partial writes")

	// Failure Path 12: Nack with wrong lease token on a LEASED job
	res = applyCmd(&relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_NACK,
		TimestampNano: 10000000,
		Job:           &relaypb.Job{Id: "job-base-1"},
		LeaseToken:    "wrong-token-abc",
	}, 14)
	require.ErrorIs(t, res.Err, domain.ErrStaleLease)
	dump, _ = fsm.DumpState()
	require.Equal(t, leasedDump, dump, "failed Nack (stale lease on leased job) must leave zero partial writes")

	// Failure Path 13: ExtendLease with wrong lease token on a LEASED job
	res = applyCmd(&relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_EXTEND_LEASE,
		TimestampNano:   10000000,
		Job:             &relaypb.Job{Id: "job-base-1"},
		LeaseToken:      "wrong-token-abc",
		ExtraDurationMs: 60000,
	}, 15)
	require.ErrorIs(t, res.Err, domain.ErrStaleLease)
	dump, _ = fsm.DumpState()
	require.Equal(t, leasedDump, dump, "failed ExtendLease (stale lease on leased job) must leave zero partial writes")
}

// mockSnapshotSink implements raft.SnapshotSink for in-memory property testing.
type mockSnapshotSink struct {
	buf    *bytes.Buffer
	closed bool
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	m.closed = true
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "test-snap-sink"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

// TestFSM_PropertyBasedSnapshotDeterminism generates random seeded command sequences,
// applies them to FSM A without snapshots and to FSM B with a Snapshot+Restore mid-flight,
// asserting 100% byte-for-byte identical StateDump across multiple seeds.
func TestFSM_PropertyBasedSnapshotDeterminism(t *testing.T) {
	seeds := []int64{42, 1337, 2026, 99999, 777}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("Seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))

			dirA := t.TempDir()
			dirB := t.TempDir()

			fsmA, err := cluster.NewFSM(filepath.Join(dirA, "fsm.db"))
			require.NoError(t, err)
			defer fsmA.Close()

			fsmB, err := cluster.NewFSM(filepath.Join(dirB, "fsm.db"))
			require.NoError(t, err)
			defer fsmB.Close()

			const totalCommands = 80
			cutPoint := rng.Intn(40) + 20 // between 20 and 60

			var commands []*relaypb.RaftCommand
			var currentNow int64 = 1000000000

			type activeJob struct {
				id    string
				queue string
			}
			var knownJobs []*activeJob

			for i := 0; i < totalCommands; i++ {
				currentNow += int64(rng.Intn(50000) + 1000)

				opType := rng.Intn(5)
				switch opType {
				case 0, 1: // Enqueue
					jobID := fmt.Sprintf("prop-job-%d-%d", seed, i)
					queue := fmt.Sprintf("q-%d", rng.Intn(2))
					delay := int64(0)
					if rng.Intn(3) == 0 {
						delay = int64(rng.Intn(100000) + 10000)
					}
					cmd := &relaypb.RaftCommand{
						Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
						TimestampNano: currentNow,
						Job: &relaypb.Job{
							Id:          jobID,
							Queue:       queue,
							Payload:     []byte(fmt.Sprintf("payload-%d", i)),
							Priority:    uint32(rng.Intn(3)),
							MaxAttempts: 3,
							RunAt:       currentNow + delay,
							CreatedAt:   currentNow,
						},
					}
					commands = append(commands, cmd)
					knownJobs = append(knownJobs, &activeJob{id: jobID, queue: queue})

				case 2: // Reserve
					queue := fmt.Sprintf("q-%d", rng.Intn(2))
					cmd := &relaypb.RaftCommand{
						Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
						TimestampNano:   currentNow,
						Queue:           queue,
						WorkerId:        fmt.Sprintf("worker-%d", rng.Intn(3)),
						LeaseDurationMs: int64(rng.Intn(60000) + 10000),
					}
					commands = append(commands, cmd)

				case 3: // Promote and expire
					cmd := &relaypb.RaftCommand{
						Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
						TimestampNano: currentNow,
					}
					commands = append(commands, cmd)

				case 4: // Ack or Nack on a known job (with valid or random token)
					if len(knownJobs) > 0 {
						target := knownJobs[rng.Intn(len(knownJobs))]
						token := "some-token"
						if rng.Intn(2) == 0 {
							commands = append(commands, &relaypb.RaftCommand{
								Type:          relaypb.CommandType_COMMAND_TYPE_ACK,
								TimestampNano: currentNow,
								Job:           &relaypb.Job{Id: target.id},
								LeaseToken:    token,
							})
						} else {
							commands = append(commands, &relaypb.RaftCommand{
								Type:           relaypb.CommandType_COMMAND_TYPE_NACK,
								TimestampNano:  currentNow,
								Job:            &relaypb.Job{Id: target.id},
								LeaseToken:     token,
								RetryAfterNano: int64(rng.Intn(100000)),
							})
						}
					} else {
						commands = append(commands, &relaypb.RaftCommand{
							Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
							TimestampNano: currentNow,
						})
					}
				}
			}

			// Apply all commands to FSM A sequentially
			for i, cmd := range commands {
				data, _ := proto.Marshal(cmd)
				_ = fsmA.Apply(&raft.Log{Index: uint64(i + 1), Data: data})
			}

			// Apply commands up to cutPoint on FSM B
			for i := 0; i <= cutPoint; i++ {
				data, _ := proto.Marshal(commands[i])
				_ = fsmB.Apply(&raft.Log{Index: uint64(i + 1), Data: data})
			}

			// Mid-flight Snapshot on FSM B
			snap, err := fsmB.Snapshot()
			require.NoError(t, err)

			var snapBuf bytes.Buffer
			sink := &mockSnapshotSink{buf: &snapBuf}
			err = snap.Persist(sink)
			require.NoError(t, err)

			// Restore snapshot on FSM B
			err = fsmB.Restore(io.NopCloser(&snapBuf))
			require.NoError(t, err)

			// Apply remaining commands cutPoint+1..end on FSM B
			for i := cutPoint + 1; i < len(commands); i++ {
				data, _ := proto.Marshal(commands[i])
				_ = fsmB.Apply(&raft.Log{Index: uint64(i + 1), Data: data})
			}

			// Verify byte-for-byte identical state dump
			dumpA, errA := fsmA.DumpState()
			require.NoError(t, errA)

			dumpB, errB := fsmB.DumpState()
			require.NoError(t, errB)

			require.Equal(t, dumpA, dumpB, "FSM A and FSM B with mid-flight snapshot must converge to identical state for seed %d", seed)
			require.Equal(t, fsmA.LastAppliedNow(), fsmB.LastAppliedNow(), "LastAppliedNow must match identically")
		})
	}
}

// TestFSMApply_BatchReserveUsesNewBatchTimeForLeaseExpiresAt proves that a Reserve
// in a batch uses the NEW batch leader time for calculating lease_expires_at,
// and assigns f.lastAppliedNow = now strictly after db.Update succeeds.
func TestFSMApply_BatchReserveUsesNewBatchTimeForLeaseExpiresAt(t *testing.T) {
	dir := t.TempDir()
	fsm, err := cluster.NewFSM(filepath.Join(dir, "fsm.db"))
	require.NoError(t, err)
	defer fsm.Close()

	// Initial Enqueue at old time
	const initialTime = int64(1_000_000)
	enqueueCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
		TimestampNano: initialTime,
		CommandId:     uuid.NewString(),
		Job: &relaypb.Job{
			Id:          "job-time-test",
			Queue:       "time-q",
			Payload:     []byte("time-payload"),
			Priority:    1,
			MaxAttempts: 3,
			RunAt:       initialTime,
			CreatedAt:   initialTime,
		},
	}
	data, err := proto.Marshal(enqueueCmd)
	require.NoError(t, err)
	res := fsm.Apply(&raft.Log{Index: 1, Data: data}).(*cluster.BatchApplyResult)
	require.NoError(t, res.Results[0].Err)
	require.Equal(t, initialTime, fsm.LastAppliedNow())

	// Batch Reserve with NEW future batch leader time
	const newBatchLeaderTime = int64(500_000_000_000) // 500s
	const leaseDurationMs = int64(30_000)             // 30s
	reserveSubCmd := &relaypb.RaftCommand{
		Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
		TimestampNano:   initialTime, // even if sub-cmd has old or 0 time
		CommandId:       uuid.NewString(),
		Queue:           "time-q",
		WorkerId:        "worker-time",
		LeaseDurationMs: leaseDurationMs,
	}

	batchCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: newBatchLeaderTime,
		CommandId:     uuid.NewString(),
		SubCommands:   []*relaypb.RaftCommand{reserveSubCmd},
	}

	dataBatch, err := proto.Marshal(batchCmd)
	require.NoError(t, err)

	applyRes := fsm.Apply(&raft.Log{Index: 2, Data: dataBatch}).(*cluster.BatchApplyResult)
	require.Len(t, applyRes.Results, 1)
	require.NoError(t, applyRes.Results[0].Err)

	expectedExpiresAt := newBatchLeaderTime + (leaseDurationMs * 1_000_000)
	require.Equal(t, expectedExpiresAt, applyRes.Results[0].Job.LeaseExpiresAt, "Reserve in batch MUST use NEW batch leader time for lease_expires_at")
	require.Equal(t, newBatchLeaderTime, fsm.LastAppliedNow(), "f.lastAppliedNow must be updated to new batch leader time")
}

// TestTicker_UnconsumableWorkDoesNotSpin tests that if unconsumable work exists
// in the index (producing 0 promoted/reaped), the ticker re-propose loop stops looping
// rather than spinning infinitely.
func TestTicker_UnconsumableWorkDoesNotSpin(t *testing.T) {
	baseDir := t.TempDir()
	tc, err := cluster.NewTestCluster(baseDir, 1000, 1000)
	require.NoError(t, err)
	defer tc.Shutdown()

	leader, err := tc.WaitForLeader(10 * time.Second)
	require.NoError(t, err)
	require.NotNil(t, leader)

	// Inject unconsumable work directly into BucketIdxSched (key exists, but job does not exist in BucketJobs)
	phantomID := "phantom-unconsumable-job"
	err = leader.FSM.DB().Update(func(tx *bbolt.Tx) error {
		bSched := tx.Bucket(store.BucketIdxSched)
		k := store.ScheduledKey(time.Now().UnixNano()-10_000_000_000, phantomID)
		return bSched.Put(k, []byte("phantom-queue"))
	})
	require.NoError(t, err)

	// Verify PeekRipeWork is true
	require.True(t, leader.FSM.PeekRipeWork(time.Now().UnixNano()))

	startApplied := leader.AppliedIndex()

	// Wait 250ms (at 100ms ticker interval, produces ~2-3 proposals)
	time.Sleep(250 * time.Millisecond)

	currentApplied := leader.AppliedIndex()
	// If the loop spun infinitely, currentApplied would have increased by thousands
	require.LessOrEqual(t, currentApplied-startApplied, uint64(10), "ticker must stop looping on 0 items processed and not spin")
}

// TestFSM_BoundedSweeps_1000PerCommand_Drain2500 asserts that 2500 scheduled jobs
// and 2500 expired leases drain over multiple bounded commands (1000 per command)
// and reach the correct final state.
func TestFSM_BoundedSweeps_1000PerCommand_Drain2500(t *testing.T) {
	dir := t.TempDir()
	fsm, err := cluster.NewFSM(filepath.Join(dir, "fsm.db"))
	require.NoError(t, err)
	defer fsm.Close()

	const t0 = int64(1_000_000_000) // 1s
	const totalScheduled = 2500
	const totalLeased = 2500

	// 1. Enqueue 2500 scheduled jobs with runAt = t0 + 10s (future at t0)
	subCmdsSched := make([]*relaypb.RaftCommand, totalScheduled)
	for i := 0; i < totalScheduled; i++ {
		subCmdsSched[i] = &relaypb.RaftCommand{
			Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
			TimestampNano: t0,
			CommandId:     uuid.NewString(),
			Job: &relaypb.Job{
				Id:          fmt.Sprintf("sched-job-%04d", i),
				Queue:       "bounded-sched-q",
				Payload:     []byte("payload"),
				Priority:    1,
				MaxAttempts: 3,
				RunAt:       t0 + 10_000_000_000,
				CreatedAt:   t0,
			},
		}
	}
	batchSchedCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: t0,
		CommandId:     uuid.NewString(),
		SubCommands:   subCmdsSched,
	}
	dataSched, err := proto.Marshal(batchSchedCmd)
	require.NoError(t, err)
	resSched := fsm.Apply(&raft.Log{Index: 1, Data: dataSched}).(*cluster.BatchApplyResult)
	require.Len(t, resSched.Results, totalScheduled)
	for _, r := range resSched.Results {
		require.NoError(t, r.Err)
	}

	// 2. Enqueue 2500 immediate jobs with runAt = t0
	subCmdsLeased := make([]*relaypb.RaftCommand, totalLeased)
	for i := 0; i < totalLeased; i++ {
		subCmdsLeased[i] = &relaypb.RaftCommand{
			Type:          relaypb.CommandType_COMMAND_TYPE_ENQUEUE,
			TimestampNano: t0,
			CommandId:     uuid.NewString(),
			Job: &relaypb.Job{
				Id:          fmt.Sprintf("lease-job-%04d", i),
				Queue:       "bounded-lease-q",
				Payload:     []byte("payload"),
				Priority:    1,
				MaxAttempts: 1,
				RunAt:       t0,
				CreatedAt:   t0,
			},
		}
	}
	batchLeaseCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: t0,
		CommandId:     uuid.NewString(),
		SubCommands:   subCmdsLeased,
	}
	dataLeased, err := proto.Marshal(batchLeaseCmd)
	require.NoError(t, err)
	resLeased := fsm.Apply(&raft.Log{Index: 2, Data: dataLeased}).(*cluster.BatchApplyResult)
	require.Len(t, resLeased.Results, totalLeased)
	for _, r := range resLeased.Results {
		require.NoError(t, r.Err)
	}

	// 3. Reserve all 2500 jobs with lease duration 5s (expires at t0 + 5s)
	subCmdsReserve := make([]*relaypb.RaftCommand, totalLeased)
	for i := 0; i < totalLeased; i++ {
		subCmdsReserve[i] = &relaypb.RaftCommand{
			Type:            relaypb.CommandType_COMMAND_TYPE_RESERVE,
			TimestampNano:   t0,
			CommandId:       uuid.NewString(),
			Queue:           "bounded-lease-q",
			WorkerId:        "worker-1",
			LeaseDurationMs: 5000,
		}
	}
	batchReserveCmd := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_BATCH,
		TimestampNano: t0,
		CommandId:     uuid.NewString(),
		SubCommands:   subCmdsReserve,
	}
	dataReserve, err := proto.Marshal(batchReserveCmd)
	require.NoError(t, err)
	resReserve := fsm.Apply(&raft.Log{Index: 3, Data: dataReserve}).(*cluster.BatchApplyResult)
	require.Len(t, resReserve.Results, totalLeased)
	for _, r := range resReserve.Results {
		require.NoError(t, r.Err)
	}

	// Advance time to tSweep = t0 + 20s (both scheduled and leased are now ripe)
	const tSweep = t0 + 20_000_000_000
	require.True(t, fsm.PeekRipeWork(tSweep), "PeekRipeWork must be true before draining")

	// Command 1: Must promote exactly 1000 scheduled, reap exactly 1000 leases
	cmdSweep1 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
		TimestampNano: tSweep,
		CommandId:     uuid.NewString(),
	}
	data1, _ := proto.Marshal(cmdSweep1)
	res1 := fsm.Apply(&raft.Log{Index: 4, Data: data1}).(*cluster.BatchApplyResult)
	require.NoError(t, res1.Results[0].Err)
	require.Equal(t, 1000, res1.Results[0].Promoted, "Command 1 must promote exactly 1000 scheduled jobs")
	require.Equal(t, 1000, res1.Results[0].Reaped, "Command 1 must reap exactly 1000 expired leases")
	require.True(t, fsm.PeekRipeWork(tSweep), "Work must still remain after command 1")

	// Command 2: Must promote exactly 1000 scheduled, reap exactly 1000 leases
	cmdSweep2 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
		TimestampNano: tSweep,
		CommandId:     uuid.NewString(),
	}
	data2, _ := proto.Marshal(cmdSweep2)
	res2 := fsm.Apply(&raft.Log{Index: 5, Data: data2}).(*cluster.BatchApplyResult)
	require.NoError(t, res2.Results[0].Err)
	require.Equal(t, 1000, res2.Results[0].Promoted, "Command 2 must promote exactly 1000 scheduled jobs")
	require.Equal(t, 1000, res2.Results[0].Reaped, "Command 2 must reap exactly 1000 expired leases")
	require.True(t, fsm.PeekRipeWork(tSweep), "Work must still remain after command 2")

	// Command 3: Must drain the remaining 500 scheduled and 500 leases
	cmdSweep3 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
		TimestampNano: tSweep,
		CommandId:     uuid.NewString(),
	}
	data3, _ := proto.Marshal(cmdSweep3)
	res3 := fsm.Apply(&raft.Log{Index: 6, Data: data3}).(*cluster.BatchApplyResult)
	require.NoError(t, res3.Results[0].Err)
	require.Equal(t, 500, res3.Results[0].Promoted, "Command 3 must promote remaining 500 scheduled jobs")
	require.Equal(t, 500, res3.Results[0].Reaped, "Command 3 must reap remaining 500 expired leases")

	// Command 4: Zero work remaining
	cmdSweep4 := &relaypb.RaftCommand{
		Type:          relaypb.CommandType_COMMAND_TYPE_PROMOTE_AND_EXPIRE,
		TimestampNano: tSweep,
		CommandId:     uuid.NewString(),
	}
	data4, _ := proto.Marshal(cmdSweep4)
	res4 := fsm.Apply(&raft.Log{Index: 7, Data: data4}).(*cluster.BatchApplyResult)
	require.NoError(t, res4.Results[0].Err)
	require.Equal(t, 0, res4.Results[0].Promoted, "Command 4 must promote 0")
	require.Equal(t, 0, res4.Results[0].Reaped, "Command 4 must reap 0")

	// Final State Assertions
	require.False(t, fsm.PeekRipeWork(tSweep), "PeekRipeWork must be false when all work is drained")

	dump, err := fsm.DumpState()
	require.NoError(t, err)

	require.Empty(t, dump[string(store.BucketIdxSched)], "Scheduled index must be completely drained")
	require.Empty(t, dump[string(store.BucketIdxLeased)], "Leased index must be completely drained")

	readySchedCount := 0
	for _, kv := range dump[string(store.BucketIdxReady)] {
		if strings.HasPrefix(kv.Key, "bounded-sched-q") {
			readySchedCount++
		}
	}
	require.Equal(t, 2500, readySchedCount, "All 2500 scheduled jobs must be promoted to ready index")
	require.Len(t, dump[string(store.BucketIdxDead)], 2500, "All 2500 reaped jobs must be in dead index")
	require.Len(t, dump[string(store.BucketJobs)], 5000, "All 5000 jobs must exist in jobs bucket")
}
