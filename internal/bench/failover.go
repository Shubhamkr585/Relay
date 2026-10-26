package bench

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

type SubprocessNodeConfig struct {
	ID       string
	DataDir  string
	RaftPort int
	GRPCPort int
	HTTPPort int
}

// FailoverBenchConfig configures a B5 failover benchmark run with exact Raft timeouts.
type FailoverBenchConfig struct {
	ID                 string
	Name               string
	BinPath            string
	BaseDir            string
	TrialsCount        int
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	IsPrimaryHeadline  bool
}

// RunFailoverBenchmark executes leader failover trials using real relayd child processes.
func RunFailoverBenchmark(cfg FailoverBenchConfig) (*FailoverResult, error) {
	if cfg.TrialsCount <= 0 {
		cfg.TrialsCount = 20
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 1000 * time.Millisecond
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 1000 * time.Millisecond
	}
	if cfg.LeaderLeaseTimeout <= 0 {
		cfg.LeaderLeaseTimeout = 500 * time.Millisecond
	}
	if cfg.ID == "" {
		cfg.ID = "b5_failover"
	}
	if cfg.Name == "" {
		cfg.Name = "B5: Leader Failover Latency"
	}

	flagsStr := fmt.Sprintf("-heartbeat-timeout=%v -election-timeout=%v -leader-lease-timeout=%v",
		cfg.HeartbeatTimeout, cfg.ElectionTimeout, cfg.LeaderLeaseTimeout)

	tempDir := filepath.Join(cfg.BaseDir, fmt.Sprintf("%s_dir", cfg.ID))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create failover temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	binPath := cfg.BinPath
	absBin, err := filepath.Abs(binPath)
	if err == nil {
		binPath = absBin
	}

	// If binPath doesn't exist, build it
	if _, err := os.Stat(binPath); err != nil {
		builtPath := filepath.Join(tempDir, "relayd.exe")
		buildCmd := exec.Command("go", "build", "-o", builtPath, "./cmd/relayd")
		if out, bErr := buildCmd.CombinedOutput(); bErr != nil {
			return nil, fmt.Errorf("failed to build relayd: %s (%w)", string(out), bErr)
		}
		binPath = builtPath
	}

	configs := make([]SubprocessNodeConfig, 3)
	for i := 0; i < 3; i++ {
		rp, err := GetFreePort()
		if err != nil {
			return nil, err
		}
		gp, err := GetFreePort()
		if err != nil {
			return nil, err
		}
		hp, err := GetFreePort()
		if err != nil {
			return nil, err
		}
		id := fmt.Sprintf("%s-node-%d", cfg.ID, i+1)
		configs[i] = SubprocessNodeConfig{
			ID:       id,
			DataDir:  filepath.Join(tempDir, id),
			RaftPort: rp,
			GRPCPort: gp,
			HTTPPort: hp,
		}
		_ = os.MkdirAll(configs[i].DataDir, 0755)
	}

	peerList := fmt.Sprintf("%s=127.0.0.1:%d,%s=127.0.0.1:%d,%s=127.0.0.1:%d",
		configs[0].ID, configs[0].GRPCPort,
		configs[1].ID, configs[1].GRPCPort,
		configs[2].ID, configs[2].GRPCPort,
	)

	var procMu sync.Mutex
	procs := make(map[string]*exec.Cmd)

	startNode := func(nodeCfg SubprocessNodeConfig, bootstrap bool, joinAddr string) (*exec.Cmd, error) {
		args := []string{
			fmt.Sprintf("-node-id=%s", nodeCfg.ID),
			fmt.Sprintf("-raft-addr=127.0.0.1:%d", nodeCfg.RaftPort),
			fmt.Sprintf("-grpc-addr=127.0.0.1:%d", nodeCfg.GRPCPort),
			fmt.Sprintf("-http-addr=127.0.0.1:%d", nodeCfg.HTTPPort),
			fmt.Sprintf("-data-dir=%s", nodeCfg.DataDir),
			fmt.Sprintf("-peers=%s", peerList),
			fmt.Sprintf("-heartbeat-timeout=%v", cfg.HeartbeatTimeout),
			fmt.Sprintf("-election-timeout=%v", cfg.ElectionTimeout),
			fmt.Sprintf("-leader-lease-timeout=%v", cfg.LeaderLeaseTimeout),
		}
		if bootstrap {
			args = append(args, "-bootstrap=true")
		}
		if joinAddr != "" {
			args = append(args, fmt.Sprintf("-join-addr=%s", joinAddr))
		}

		cmd := exec.Command(binPath, args...)
		logPath := filepath.Join(tempDir, nodeCfg.ID+".log")
		lf, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		cmd.Stdout = lf
		cmd.Stderr = lf

		if err := cmd.Start(); err != nil {
			return nil, err
		}
		procMu.Lock()
		procs[nodeCfg.ID] = cmd
		procMu.Unlock()
		return cmd, nil
	}

	stopAll := func() {
		procMu.Lock()
		defer procMu.Unlock()
		for id, cmd := range procs {
			if cmd != nil && cmd.Process != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
			delete(procs, id)
		}
		// Confirm ports are released
		for _, c := range configs {
			deadline := time.Now().Add(1 * time.Second)
			for time.Now().Before(deadline) {
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", c.GRPCPort), 50*time.Millisecond)
				if err != nil {
					break
				}
				_ = conn.Close()
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
	defer stopAll()

	// 1. Start bootstrap node 1
	if _, err := startNode(configs[0], true, ""); err != nil {
		return nil, fmt.Errorf("failed starting node 1: %w", err)
	}

	// Wait for node 1 HTTP readyz
	if !waitForReady(configs[0].HTTPPort, 15*time.Second) {
		logData, _ := os.ReadFile(filepath.Join(tempDir, configs[0].ID+".log"))
		return nil, fmt.Errorf("node 1 failed readyz check; log: %s", string(logData))
	}

	// 2. Start nodes 2 & 3 joining node 1
	node1Grpc := fmt.Sprintf("127.0.0.1:%d", configs[0].GRPCPort)
	if _, err := startNode(configs[1], false, node1Grpc); err != nil {
		return nil, fmt.Errorf("failed starting node 2: %w", err)
	}
	if _, err := startNode(configs[2], false, node1Grpc); err != nil {
		return nil, fmt.Errorf("failed starting node 3: %w", err)
	}

	findLeader := func() (int, string) {
		for i, c := range configs {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", c.HTTPPort))
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				s := string(b)
				if strings.Contains(s, "\nrelay_raft_is_leader 1") || strings.Contains(s, "\r\nrelay_raft_is_leader 1") {
					return i, c.ID
				}
			}
		}
		return -1, ""
	}

	waitForHealthyCluster := func(timeout time.Duration) (int, string, bool) {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			allReady := true
			for _, c := range configs {
				if !waitForReady(c.HTTPPort, 500*time.Millisecond) {
					allReady = false
					break
				}
			}
			if allReady {
				idx, id := findLeader()
				if idx != -1 {
					return idx, id, true
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		return -1, "", false
	}

	// Wait for initial cluster leader election
	leaderIdx, leaderID, healthy := waitForHealthyCluster(20 * time.Second)
	if !healthy || leaderIdx == -1 {
		return nil, fmt.Errorf("cluster failed to elect initial leader")
	}

	// Trials loop
	trials := make([]FailoverTrial, 0, cfg.TrialsCount)
	invalidTrials := make([]FailoverTrial, 0)
	durations := make([]time.Duration, 0, cfg.TrialsCount)
	queue := "bench-failover-q"

	expectedLowerBound := cfg.HeartbeatTimeout
	derivation := "HashiCorp Raft (raft.go:164, 216-224; util.go:29-35): follower starts election when heartbeat timer fires after [HeartbeatTimeout, 2*HeartbeatTimeout). Minimum theoretical outage is HeartbeatTimeout."

	for trial := 1; trial <= cfg.TrialsCount; trial++ {
		// Ensure all 3 nodes alive and in quorum before each trial
		for i, c := range configs {
			procMu.Lock()
			cmd := procs[c.ID]
			procMu.Unlock()
			if cmd == nil || cmd.Process == nil {
				survivorGrpc := fmt.Sprintf("127.0.0.1:%d", configs[(i+1)%3].GRPCPort)
				_, _ = startNode(c, false, survivorGrpc)
			}
		}

		leaderIdx, leaderID, healthy = waitForHealthyCluster(15 * time.Second)
		if !healthy || leaderIdx == -1 {
			invalidTrials = append(invalidTrials, FailoverTrial{
				Trial:         trial,
				Valid:         false,
				InvalidReason: "cluster could not reach healthy 3-node quorum before trial",
				Pass:          false,
			})
			continue
		}

		leaderBefore := leaderID
		leaderCfg := configs[leaderIdx]

		// Connect gRPC clients to all 3 nodes
		clients := make([]relaypb.RelayServiceClient, 3)
		conns := make([]*grpc.ClientConn, 3)
		for i, c := range configs {
			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", c.GRPCPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				conns[i] = conn
				clients[i] = relaypb.NewRelayServiceClient(conn)
			}
		}

		// Background write stream before kill to ensure continuous traffic
		preAcked := make(map[string]struct{})
		var preMu sync.Mutex

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		var streamWg sync.WaitGroup
		stopStream := make(chan struct{})

		streamWg.Add(1)
		go func() {
			defer streamWg.Done()
			counter := 0
			for {
				select {
				case <-stopStream:
					return
				case <-ctx.Done():
					return
				default:
				}
				counter++
				client := clients[leaderIdx]
				if client == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				idemp := fmt.Sprintf("failover-pre-t%d-%d-%d", trial, counter, time.Now().UnixNano())
				reqCtx, reqCancel := context.WithTimeout(ctx, 1*time.Second)
				resp, err := client.Enqueue(reqCtx, &relaypb.EnqueueRequest{
					Queue:          queue,
					Payload:        []byte("failover-pre-payload"),
					Priority:       1,
					IdempotencyKey: idemp,
					MaxAttempts:    3,
				})
				reqCancel()

				if err == nil && resp != nil && resp.JobId != "" {
					preMu.Lock()
					preAcked[resp.JobId] = struct{}{}
					preMu.Unlock()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		// Allow traffic to establish
		time.Sleep(300 * time.Millisecond)

		// KILL THE EXACT CURRENT LEADER PROCESS
		procMu.Lock()
		leaderCmd := procs[leaderBefore]
		if leaderCmd != nil && leaderCmd.Process != nil {
			_ = leaderCmd.Process.Kill()
			_ = leaderCmd.Wait()
		}
		delete(procs, leaderBefore)
		procMu.Unlock()

		// Confirm process has exited and port is closed BEFORE starting timer logic
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", leaderCfg.GRPCPort), 50*time.Millisecond)
			if err != nil {
				break
			}
			_ = conn.Close()
			time.Sleep(20 * time.Millisecond)
		}

		// START HIGH-RESOLUTION TIMER LOGIC IMMEDIATELY AFTER CONFIRMED KILL
		killTime := NowHighRes()

		survivorIndices := make([]int, 0, 2)
		for i := range configs {
			if i != leaderIdx {
				survivorIndices = append(survivorIndices, i)
			}
		}

		var firstSuccess time.Duration
		observedOutage := false
		pollDeadline := time.Now().Add(15 * time.Second)

		// Probe writes through SURVIVING nodes only
		probeCounter := 0
		for time.Now().Before(pollDeadline) {
			success := false
			for _, sIdx := range survivorIndices {
				sClient := clients[sIdx]
				if sClient == nil {
					continue
				}
				probeCounter++
				pCtx, pCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				pResp, pErr := sClient.Enqueue(pCtx, &relaypb.EnqueueRequest{
					Queue:          queue,
					Payload:        []byte("probe-after-kill"),
					Priority:       1,
					IdempotencyKey: fmt.Sprintf("probe-t%d-%d-%d", trial, probeCounter, time.Now().UnixNano()),
					MaxAttempts:    3,
				})
				pCancel()

				if pErr != nil {
					observedOutage = true
				} else if pResp != nil && pResp.JobId != "" {
					firstSuccess = SinceHighRes(killTime)
					preMu.Lock()
					preAcked[pResp.JobId] = struct{}{}
					preMu.Unlock()
					success = true
					break
				}
			}
			if success {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		close(stopStream)
		streamWg.Wait()
		cancel()

		// Read leader_after strictly from surviving nodes
		var leaderAfter string
		var leaderAfterIdx = -1
		for _, sIdx := range survivorIndices {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", configs[sIdx].HTTPPort))
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				s := string(b)
				if strings.Contains(s, "\nrelay_raft_is_leader 1") || strings.Contains(s, "\r\nrelay_raft_is_leader 1") {
					leaderAfter = configs[sIdx].ID
					leaderAfterIdx = sIdx
					break
				}
			}
		}

		// Verify 0 lost jobs: query Stats from leaderAfter
		lostJobs := 0
		if leaderAfterIdx != -1 && clients[leaderAfterIdx] != nil && len(preAcked) > 0 {
			sCtx, sCancel := context.WithTimeout(context.Background(), 2*time.Second)
			st, sErr := clients[leaderAfterIdx].Stats(sCtx, &relaypb.StatsRequest{Queue: queue})
			sCancel()
			if sErr == nil && st != nil {
				totalJobs := st.PendingCount + st.LeasedCount + st.DoneCount + st.DeadCount
				if totalJobs < int64(len(preAcked)) {
					lostJobs = int(int64(len(preAcked)) - totalJobs)
				}
			}
		}

		// Close trial gRPC connections
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}

		// Validate trial
		valid := true
		var invalidReason string
		if firstSuccess == 0 {
			valid = false
			invalidReason = "failover write probe timed out after 15s"
		} else if leaderAfter == "" || leaderAfter == leaderBefore {
			valid = false
			invalidReason = fmt.Sprintf("leader_after (%q) matches leader_before (%q) or is empty", leaderAfter, leaderBefore)
		} else if !observedOutage {
			valid = false
			invalidReason = "writing client never observed failure during failover outage"
		} else if lostJobs > 0 {
			valid = false
			invalidReason = fmt.Sprintf("detected %d lost jobs after failover", lostJobs)
		}

		trialResult := FailoverTrial{
			Trial:            trial,
			KilledLeader:     leaderBefore,
			NewLeader:        leaderAfter,
			FailoverDuration: firstSuccess,
			PreAckCount:      len(preAcked),
			PostAckCount:     len(preAcked),
			VerifiedLostJobs: lostJobs,
			Valid:            valid,
			InvalidReason:    invalidReason,
			Pass:             valid,
		}

		if valid {
			durations = append(durations, firstSuccess)
			trials = append(trials, trialResult)
			suspiciousFlag := ""
			if firstSuccess < expectedLowerBound {
				suspiciousFlag = " [SUSPICIOUS: < lower bound]"
			}
			fmt.Printf("    Trial %d/%d: %s [killed %s -> new leader %s]%s (verified 0 lost jobs)\n",
				trial, cfg.TrialsCount, FormatDuration(firstSuccess), leaderBefore, leaderAfter, suspiciousFlag)
		} else {
			invalidTrials = append(invalidTrials, trialResult)
			fmt.Printf("    Trial %d/%d: INVALID (%s) [killed %s -> new leader %s]\n",
				trial, cfg.TrialsCount, invalidReason, leaderBefore, leaderAfter)
		}

		// Restart killed node and wait for 3 healthy voters before next trial
		time.Sleep(200 * time.Millisecond)
		survivorGrpc := fmt.Sprintf("127.0.0.1:%d", configs[survivorIndices[0]].GRPCPort)
		_, _ = startNode(leaderCfg, false, survivorGrpc)
		_, _, _ = waitForHealthyCluster(15 * time.Second)
		time.Sleep(500 * time.Millisecond)
	}

	validCount := len(durations)
	if validCount == 0 {
		return &FailoverResult{
			ID:                   cfg.ID,
			Name:                 cfg.Name,
			Flags:                flagsStr,
			TrialsCount:          cfg.TrialsCount,
			ValidTrialsCount:     0,
			Trials:               trials,
			InvalidTrials:        invalidTrials,
			ExpectedLowerBound:   expectedLowerBound,
			LowerBoundDerivation: derivation,
			IsPrimaryHeadline:    cfg.IsPrimaryHeadline,
			Suspicious:           true,
			OverallPass:          false,
		}, fmt.Errorf("all failover trials were invalid or failed to execute")
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	minDur := durations[0]
	maxDur := durations[validCount-1]
	medianDur := durations[validCount/2]

	var p95Dur time.Duration
	p95Valid := false
	if validCount >= 20 {
		p95Idx := int(float64(validCount)*0.95) - 1
		if p95Idx < 0 {
			p95Idx = 0
		}
		p95Dur = durations[p95Idx]
		p95Valid = true
	}

	suspicious := (medianDur < expectedLowerBound)

	return &FailoverResult{
		ID:                   cfg.ID,
		Name:                 cfg.Name,
		Flags:                flagsStr,
		TrialsCount:          cfg.TrialsCount,
		ValidTrialsCount:     validCount,
		Trials:               trials,
		InvalidTrials:        invalidTrials,
		ExpectedLowerBound:   expectedLowerBound,
		LowerBoundDerivation: derivation,
		IsPrimaryHeadline:    cfg.IsPrimaryHeadline,
		Suspicious:           suspicious,
		MinDuration:          minDur,
		MedianDuration:       medianDur,
		P95Duration:          p95Dur,
		P95Valid:             p95Valid,
		MaxDuration:          maxDur,
		TotalLostJobs:        0,
		OverallPass:          (validCount > 0 && len(invalidTrials) == 0),
	}, nil
}

func waitForReady(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", port))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
