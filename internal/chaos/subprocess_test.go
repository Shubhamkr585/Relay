package chaos

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// TestChaos_ScenarioA_Subprocess_LeaderAssassination executes Scenario A with real relayd child processes,
// killing the leader process via Process.Kill and restarting with the same data directory.
// Gated by CHAOS_SUBPROCESS=1.
func TestChaos_ScenarioA_Subprocess_LeaderAssassination(t *testing.T) {
	if os.Getenv("CHAOS_SUBPROCESS") != "1" {
		t.Skip("Skipping subprocess scenario; run with CHAOS_SUBPROCESS=1 or make chaos-long")
	}

	baseTempDir := t.TempDir()
	binPath := filepath.Join(baseTempDir, "relayd.exe")
	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/relayd")
	out, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build relayd: %s", string(out))

	runs := getRunsCount()
	for run := 1; run <= runs; run++ {
		t.Run(fmt.Sprintf("Run_%d", run), func(t *testing.T) {
			tempDir := t.TempDir()

			// Ports for 3 nodes
			type NodeConfig struct {
				id       string
				dataDir  string
				raftPort int
				grpcPort int
				httpPort int
			}

			configs := make([]NodeConfig, 3)
			for i := 0; i < 3; i++ {
				id := fmt.Sprintf("node-%d", i+1)
				dir := filepath.Join(tempDir, id)
				require.NoError(t, os.MkdirAll(dir, 0755))

				rp, err := getFreePort()
				require.NoError(t, err)
				gp, err := getFreePort()
				require.NoError(t, err)
				hp, err := getFreePort()
				require.NoError(t, err)

				configs[i] = NodeConfig{
					id:       id,
					dataDir:  dir,
					raftPort: rp,
					grpcPort: gp,
					httpPort: hp,
				}
			}

			peerList := fmt.Sprintf("%s=127.0.0.1:%d,%s=127.0.0.1:%d,%s=127.0.0.1:%d",
				configs[0].id, configs[0].grpcPort,
				configs[1].id, configs[1].grpcPort,
				configs[2].id, configs[2].grpcPort,
			)

			procs := make(map[string]*exec.Cmd)
			var procMu sync.Mutex

			startNode := func(cfg NodeConfig, bootstrap bool, joinAddr string) (*exec.Cmd, error) {
				args := []string{
					fmt.Sprintf("-node-id=%s", cfg.id),
					fmt.Sprintf("-raft-addr=127.0.0.1:%d", cfg.raftPort),
					fmt.Sprintf("-grpc-addr=127.0.0.1:%d", cfg.grpcPort),
					fmt.Sprintf("-http-addr=127.0.0.1:%d", cfg.httpPort),
					fmt.Sprintf("-data-dir=%s", cfg.dataDir),
					fmt.Sprintf("-peers=%s", peerList),
					"-heartbeat-timeout=400ms",
					"-election-timeout=1000ms",
					"-leader-lease-timeout=200ms",
				}
				if bootstrap {
					args = append(args, "-bootstrap=true")
				}
				if joinAddr != "" {
					args = append(args, fmt.Sprintf("-join-addr=%s", joinAddr))
				}

				cmd := exec.Command(binPath, args...)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr

				if err := cmd.Start(); err != nil {
					return nil, err
				}

				procMu.Lock()
				procs[cfg.id] = cmd
				procMu.Unlock()
				return cmd, nil
			}

			defer func() {
				procMu.Lock()
				defer procMu.Unlock()
				for _, cmd := range procs {
					if cmd != nil && cmd.Process != nil {
						_ = cmd.Process.Kill()
					}
				}
			}()

			// Start Node 1 as bootstrap leader
			_, err = startNode(configs[0], true, "")
			require.NoError(t, err)

			// Wait for Node 1 HTTP to be ready
			require.Eventually(t, func() bool {
				resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", configs[0].httpPort))
				if err == nil {
					defer resp.Body.Close()
					return resp.StatusCode == http.StatusOK
				}
				return false
			}, 10*time.Second, 200*time.Millisecond)

			// Start Node 2 and Node 3 joining Node 1
			node1Grpc := fmt.Sprintf("127.0.0.1:%d", configs[0].grpcPort)
			_, err = startNode(configs[1], false, node1Grpc)
			require.NoError(t, err)

			_, err = startNode(configs[2], false, node1Grpc)
			require.NoError(t, err)

			// Wait for 3-node cluster leader
			findLeader := func() (int, string) {
				for i, cfg := range configs {
					resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", cfg.httpPort))
					if err == nil {
						b, _ := io.ReadAll(resp.Body)
						resp.Body.Close()
						s := string(b)
						if strings.Contains(s, "\nrelay_raft_is_leader 1") || strings.Contains(s, "\r\nrelay_raft_is_leader 1") {
							return i, cfg.id
						}
					}
				}
				return -1, ""
			}

			require.Eventually(t, func() bool {
				idx, _ := findLeader()
				return idx >= 0
			}, 15*time.Second, 500*time.Millisecond, "Leader must be elected in subprocess cluster")

			// Clients
			clients := make(map[string]relaypb.RelayServiceClient)
			for _, cfg := range configs {
				conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", cfg.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
				require.NoError(t, err)
				defer conn.Close()
				clients[cfg.id] = relaypb.NewRelayServiceClient(conn)
			}

			queue := "subprocess-chaos-q"
			seed := time.Now().UnixNano()
			tracker := NewInvariantTracker("Scenario A: Subprocess Leader Assassination", seed)
			tracker.SetTransport("real TCP (raft.NewTCPTransport) across independent child processes with real TCP loopback gRPC", "N/A")
			totalJobs := 100
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
			defer cancel()

			// Producers
			var prodWg sync.WaitGroup
			prodWg.Add(1)
			go func() {
				defer prodWg.Done()
				for i := 0; i < totalJobs; i++ {
					idemp := fmt.Sprintf("subproc-job-%d", i)
					isPoison := (i%20 == 0) // 5 poison jobs
					payload := []byte("subproc-data")
					if isPoison {
						payload = []byte("poison-subproc-data")
					}

					tracker.RecordAttemptedEnqueue()
					for {
						leaderIdx, leaderID := findLeader()
						if leaderIdx < 0 {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						client := clients[leaderID]
						reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
						resp, err := client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
							Queue:          queue,
							Payload:        payload,
							IdempotencyKey: idemp,
							MaxAttempts:    3,
						})
						reqCancel()

						if err == nil && resp != nil {
							tracker.RecordEnqueue(resp.JobId, idemp)
							break
						}
						if ctx.Err() != nil {
							tracker.RecordUnacknowledgedEnqueue()
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
				}
			}()

			// Workers
			var workWg sync.WaitGroup
			stopWorkers := make(chan struct{})
			for w := 0; w < 3; w++ {
				workWg.Add(1)
				go func(wID int) {
					defer workWg.Done()
					workerName := fmt.Sprintf("subproc-worker-%d", wID)
					for {
						select {
						case <-stopWorkers:
							return
						case <-ctx.Done():
							return
						default:
						}

						leaderIdx, leaderID := findLeader()
						if leaderIdx < 0 {
							time.Sleep(50 * time.Millisecond)
							continue
						}
						client := clients[leaderID]
						resCtx, resCancel := context.WithTimeout(ctx, 2*time.Second)
						res, err := client.Reserve(resCtx, &relaypb.ReserveRequest{
							Queue:           queue,
							WorkerId:        workerName,
							LeaseDurationMs: 2000,
						})
						resCancel()

						if err == nil && res != nil && res.Job != nil {
							tracker.RecordReservation(res.Job.Id, workerName, res.LeaseToken)

							if strings.HasPrefix(string(res.Job.Payload), "poison-") {
								tracker.RecordFault("POISON_NACK", workerName, fmt.Sprintf("nacking poison job %s", res.Job.Id))
								for {
									nackCtx, nackCancel := context.WithTimeout(ctx, 2*time.Second)
									_, nErr := client.Nack(nackCtx, &relaypb.NackRequest{
										JobId:        res.Job.Id,
										LeaseToken:   res.LeaseToken,
										ErrorMsg:     "subprocess poison failure",
										RetryAfterMs: 0,
									})
									nackCancel()
									if nErr == nil {
										break
									}
									st, _ := status.FromError(nErr)
									if st.Code() == codes.FailedPrecondition {
										break
									}
									if ctx.Err() != nil {
										break
									}
									time.Sleep(100 * time.Millisecond)
									_, newLeaderID := findLeader()
									if newLeaderID != "" {
										client = clients[newLeaderID]
									}
								}
								continue
							}

							for {
								ackCtx, ackCancel := context.WithTimeout(ctx, 2*time.Second)
								_, aErr := client.Ack(ackCtx, &relaypb.AckRequest{
									JobId:      res.Job.Id,
									LeaseToken: res.LeaseToken,
								})
								ackCancel()
								if aErr == nil {
									tracker.RecordComplete(res.Job.Id)
									break
								}
								st, _ := status.FromError(aErr)
								if st.Code() == codes.FailedPrecondition {
									if strings.Contains(aErr.Error(), "JOB_STATE_DONE") {
										tracker.RecordComplete(res.Job.Id)
										break
									}
									tracker.RecordStaleAck(true)
									break
								}
								if ctx.Err() != nil {
									break
								}
								time.Sleep(100 * time.Millisecond)
								_, newLeaderID := findLeader()
								if newLeaderID != "" {
									client = clients[newLeaderID]
								}
							}
						} else {
							time.Sleep(50 * time.Millisecond)
						}
					}
				}(w)
			}

			// Assassinate leader once during load
			time.Sleep(2 * time.Second)
			lIdx, lID := findLeader()
			if lIdx >= 0 {
				tracker.RecordLeaderKill()
				tracker.RecordFault("KILL_LEADER", lID, "killed relayd child process via Process.Kill")
				procMu.Lock()
				cmd := procs[lID]
				if cmd != nil && cmd.Process != nil {
					_ = cmd.Process.Kill()
					_, _ = cmd.Process.Wait()
				}
				procMu.Unlock()

				// Wait while leader is down to observe new leader elected
				var newLeaderID string
				for attempt := 0; attempt < 30; attempt++ {
					time.Sleep(200 * time.Millisecond)
					nIdx, nID := findLeader()
					if nIdx >= 0 && nID != lID {
						newLeaderID = nID
						break
					}
				}
				if newLeaderID != "" {
					tracker.RecordFailover()
					tracker.RecordFault("FAILOVER", newLeaderID, fmt.Sprintf("subprocess failover from %s to %s", lID, newLeaderID))
				}

				// Wait and restart the killed process with the same data directory
				time.Sleep(1 * time.Second)
				tracker.RecordFault("RESTART_NODE", lID, "restarted relayd child process with same data dir")
				_, _ = startNode(configs[lIdx], false, "")

				// If failover wasn't detected while down, check again after restart
				if newLeaderID == "" {
					time.Sleep(2 * time.Second)
					_, finalLeaderID := findLeader()
					if finalLeaderID != "" && finalLeaderID != lID {
						tracker.RecordFailover()
						tracker.RecordFault("FAILOVER", finalLeaderID, fmt.Sprintf("subprocess failover from %s to %s", lID, finalLeaderID))
					}
				}
			}

			prodWg.Wait()
			close(stopWorkers)
			workWg.Wait()

			// Drain remaining
			require.Eventually(t, func() bool {
				lIdx, lID := findLeader()
				if lIdx < 0 {
					return false
				}
				resp, err := clients[lID].Stats(ctx, &relaypb.StatsRequest{Queue: queue})
				if err == nil && resp.PendingCount == 0 && resp.LeasedCount == 0 {
					// Query dead jobs into tracker
					deadResp, dErr := clients[lID].ListDead(ctx, &relaypb.ListDeadRequest{
						Queue: queue,
						Limit: 1000,
					})
					if dErr == nil && deadResp != nil {
						for _, dj := range deadResp.Jobs {
							tracker.RecordDead(dj.Id)
						}
					}
					return true
				}
				// Reserve and ack/nack if any
				res, err := clients[lID].Reserve(ctx, &relaypb.ReserveRequest{
					Queue:           queue,
					WorkerId:        "drainer",
					LeaseDurationMs: 2000,
				})
				if err == nil && res != nil && res.Job != nil {
					tracker.RecordReservation(res.Job.Id, "drainer", res.LeaseToken)
					if strings.HasPrefix(string(res.Job.Payload), "poison-") {
						tracker.RecordFault("POISON_NACK", "drainer", fmt.Sprintf("draining poison job %s", res.Job.Id))
						for {
							nackCtx, nackCancel := context.WithTimeout(ctx, 2*time.Second)
							_, nErr := clients[lID].Nack(nackCtx, &relaypb.NackRequest{
								JobId:        res.Job.Id,
								LeaseToken:   res.LeaseToken,
								ErrorMsg:     "poison failure during drain",
								RetryAfterMs: 0,
							})
							nackCancel()
							if nErr == nil {
								break
							}
							st, _ := status.FromError(nErr)
							if st.Code() == codes.FailedPrecondition {
								break
							}
							if ctx.Err() != nil {
								break
							}
							time.Sleep(100 * time.Millisecond)
							_, lID = findLeader()
							if lID == "" {
								break
							}
						}
					} else {
						for {
							ackCtx, ackCancel := context.WithTimeout(ctx, 2*time.Second)
							_, aErr := clients[lID].Ack(ackCtx, &relaypb.AckRequest{
								JobId:      res.Job.Id,
								LeaseToken: res.LeaseToken,
							})
							ackCancel()
							if aErr == nil {
								tracker.RecordComplete(res.Job.Id)
								break
							}
							st, _ := status.FromError(aErr)
							if st.Code() == codes.FailedPrecondition {
								if strings.Contains(aErr.Error(), "JOB_STATE_DONE") {
									tracker.RecordComplete(res.Job.Id)
								}
								break
							}
							if ctx.Err() != nil {
								break
							}
							time.Sleep(100 * time.Millisecond)
							_, lID = findLeader()
							if lID == "" {
								break
							}
						}
					}
				}
				return false
			}, 25*time.Second, 400*time.Millisecond)

			// Ensure all dead jobs are queried into tracker
			if _, lID := findLeader(); lID != "" {
				deadResp, dErr := clients[lID].ListDead(ctx, &relaypb.ListDeadRequest{
					Queue: queue,
					Limit: 1000,
				})
				if dErr == nil && deadResp != nil {
					for _, dj := range deadResp.Jobs {
						tracker.RecordDead(dj.Id)
					}
				}
			}

			report := tracker.EvaluateInvariants(0)
			tracker.PrintReport(os.Stdout, "Scenario A: Subprocess Leader Assassination", report)
			require.True(t, report.Inv1_NoLostJobsPassed, "Inv 1 (Zero Lost Jobs) failed")
			require.True(t, report.Inv2_NoStuckJobsPassed, "Inv 2 (Zero Stuck Jobs) failed")
			require.Greater(t, report.DeadLetterJobs, 0, "Subprocess scenario must have Dead-Lettered jobs")
			require.Greater(t, report.FailoversObserved, int64(0), "Subprocess scenario must observe leader failover")
			require.True(t, report.AllPassed, "All invariants must pass")
		})
	}
}
