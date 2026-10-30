package chaos

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

// TestChaos_Durability_SIGKILLAllNodes repeats 20 times:
// 1. Boots a 3-node real relayd subprocess cluster with disk BoltStore (fsync ON).
// 2. Acknowledges N writes.
// 3. Immediately terminates ALL 3 nodes simultaneously using SIGKILL (Process.Kill).
// 4. Restarts all 3 nodes on the same data directories.
// 5. Asserts that 100% of the acknowledged jobs are present and intact.
func TestChaos_Durability_SIGKILLAllNodes(t *testing.T) {
	// Build relayd binary once for the test
	baseTempDir := t.TempDir()
	binPath := filepath.Join(baseTempDir, "relayd-durability.exe")
	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/relayd")
	out, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to compile relayd: %s", string(out))

	repetitions := 20
	if val := os.Getenv("CHAOS_DURABILITY_RUNS"); val != "" {
		var n int
		if _, err := fmt.Sscanf(val, "%d", &n); err == nil && n > 0 {
			repetitions = n
		}
	}

	writesPerRep := 25

	for rep := 1; rep <= repetitions; rep++ {
		t.Run(fmt.Sprintf("Round_%d", rep), func(t *testing.T) {
			repDir := t.TempDir()

			type NodeCfg struct {
				id       string
				dataDir  string
				raftPort int
				grpcPort int
				httpPort int
			}

			cfgs := make([]NodeCfg, 3)
			for i := 0; i < 3; i++ {
				id := fmt.Sprintf("node-%d", i+1)
				nodeDir := filepath.Join(repDir, id)
				require.NoError(t, os.MkdirAll(nodeDir, 0755))

				rp, err := getFreePort()
				require.NoError(t, err)
				gp, err := getFreePort()
				require.NoError(t, err)
				hp, err := getFreePort()
				require.NoError(t, err)

				cfgs[i] = NodeCfg{
					id:       id,
					dataDir:  nodeDir,
					raftPort: rp,
					grpcPort: gp,
					httpPort: hp,
				}
			}

			peerList := fmt.Sprintf("%s=127.0.0.1:%d,%s=127.0.0.1:%d,%s=127.0.0.1:%d",
				cfgs[0].id, cfgs[0].grpcPort,
				cfgs[1].id, cfgs[1].grpcPort,
				cfgs[2].id, cfgs[2].grpcPort,
			)

			procs := make([]*exec.Cmd, 3)

			startNode := func(cfg NodeCfg, bootstrap bool, joinAddr string) (*exec.Cmd, error) {
				args := []string{
					fmt.Sprintf("-node-id=%s", cfg.id),
					fmt.Sprintf("-raft-addr=127.0.0.1:%d", cfg.raftPort),
					fmt.Sprintf("-grpc-addr=127.0.0.1:%d", cfg.grpcPort),
					fmt.Sprintf("-http-addr=127.0.0.1:%d", cfg.httpPort),
					fmt.Sprintf("-data-dir=%s", cfg.dataDir),
					fmt.Sprintf("-peers=%s", peerList),
					"-heartbeat-timeout=150ms",
					"-election-timeout=300ms",
					"-leader-lease-timeout=150ms",
				}
				if bootstrap {
					args = append(args, "-bootstrap=true")
				}
				if joinAddr != "" {
					args = append(args, fmt.Sprintf("-join-addr=%s", joinAddr))
				}

				cmd := exec.Command(binPath, args...)
				if err := cmd.Start(); err != nil {
					return nil, err
				}
				return cmd, nil
			}

			// Start node 1 (bootstrap)
			var startErr error
			procs[0], startErr = startNode(cfgs[0], true, "")
			require.NoError(t, startErr)

			// Wait for node 1 to become ready
			require.Eventually(t, func() bool {
				resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", cfgs[0].httpPort))
				if err == nil && resp.StatusCode == http.StatusOK {
					_ = resp.Body.Close()
					return true
				}
				if resp != nil {
					_ = resp.Body.Close()
				}
				return false
			}, 10*time.Second, 100*time.Millisecond)

			// Start node 2 and node 3
			procs[1], startErr = startNode(cfgs[1], false, fmt.Sprintf("127.0.0.1:%d", cfgs[0].grpcPort))
			require.NoError(t, startErr)
			procs[2], startErr = startNode(cfgs[2], false, fmt.Sprintf("127.0.0.1:%d", cfgs[0].grpcPort))
			require.NoError(t, startErr)

			// Ensure cleanup on test exit
			defer func() {
				for _, p := range procs {
					if p != nil && p.Process != nil {
						_ = p.Process.Kill()
						_ = p.Wait()
					}
				}
			}()

			// Wait for cluster quorum and stable leader
			var leaderGRPCPort int
			require.Eventually(t, func() bool {
				for _, cfg := range cfgs {
					resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", cfg.httpPort))
					if err == nil && resp.StatusCode == http.StatusOK {
						_ = resp.Body.Close()
						leaderGRPCPort = cfg.grpcPort
						return true
					}
					if resp != nil {
						_ = resp.Body.Close()
					}
				}
				return false
			}, 15*time.Second, 200*time.Millisecond, "Cluster must elect a leader")

			// Acknowledge N writes
			conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", leaderGRPCPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			client := relaypb.NewRelayServiceClient(conn)

			q := fmt.Sprintf("durability-q-%d", rep)
			type AckedJob struct {
				id      string
				payload []byte
			}
			ackedJobs := make([]AckedJob, writesPerRep)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			for w := 0; w < writesPerRep; w++ {
				payload := []byte(fmt.Sprintf("payload-rep-%d-write-%d-%s", rep, w, uuid.NewString()))
				res, err := client.Enqueue(ctx, &relaypb.EnqueueRequest{
					Queue:          q,
					Payload:        payload,
					IdempotencyKey: fmt.Sprintf("idemp-%d-%d", rep, w),
					MaxAttempts:    3,
				})
				require.NoError(t, err, "Write %d must be acknowledged", w)
				require.NotEmpty(t, res.JobId)
				ackedJobs[w] = AckedJob{
					id:      res.JobId,
					payload: payload,
				}
			}
			_ = conn.Close()

			// --- SIGKILL ALL NODES AT ONCE ---
			var killWg sync.WaitGroup
			for i := 0; i < 3; i++ {
				killWg.Add(1)
				go func(idx int) {
					defer killWg.Done()
					if procs[idx] != nil && procs[idx].Process != nil {
						_ = procs[idx].Process.Kill()
						_ = procs[idx].Wait()
					}
				}(i)
			}
			killWg.Wait()

			// Verify all processes are dead (ports closed)
			for _, cfg := range cfgs {
				require.Eventually(t, func() bool {
					c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.grpcPort), 50*time.Millisecond)
					if err != nil {
						return true
					}
					_ = c.Close()
					return false
				}, 5*time.Second, 50*time.Millisecond)
			}

			// --- RESTART ALL NODES ON SAME DATA DIRECTORIES ---
			for i := 0; i < 3; i++ {
				// No bootstrap flag: nodes recover existing state from disk
				procs[i], startErr = startNode(cfgs[i], false, "")
				require.NoError(t, startErr, "Node %d must restart successfully", i+1)
			}

			// Wait for restarted cluster to elect a leader
			var recoveredLeaderPort int
			require.Eventually(t, func() bool {
				for _, cfg := range cfgs {
					resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", cfg.httpPort))
					if err == nil && resp.StatusCode == http.StatusOK {
						_ = resp.Body.Close()
						recoveredLeaderPort = cfg.grpcPort
						return true
					}
					if resp != nil {
						_ = resp.Body.Close()
					}
				}
				return false
			}, 20*time.Second, 200*time.Millisecond, "Restarted cluster must elect a leader")

			// Connect to recovered cluster
			recConn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", recoveredLeaderPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer recConn.Close()
			recClient := relaypb.NewRelayServiceClient(recConn)

			// --- ASSERT EVERY ACKNOWLEDGED JOB IS PRESENT ---
			verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer verifyCancel()

			for w, acked := range ackedJobs {
				jobRes, err := recClient.GetJob(verifyCtx, &relaypb.GetJobRequest{JobId: acked.id})
				require.NoError(t, err, "Job %s (write %d) must exist after full SIGKILL and restart", acked.id, w)
				require.NotNil(t, jobRes.Job)
				require.Equal(t, acked.id, jobRes.Job.Id)
				require.Equal(t, acked.payload, jobRes.Job.Payload)
				require.Equal(t, relaypb.JobState_JOB_STATE_PENDING, jobRes.Job.State)
			}

			t.Logf("Round %d: PASS (all %d acknowledged writes recovered intact after simultaneous SIGKILL)", rep, writesPerRep)
		})
	}
}

