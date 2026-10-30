package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
	"github.com/shubhamkr585/relay/internal/cluster"
	"github.com/shubhamkr585/relay/internal/server"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	var (
		nodeID             = flag.String("node-id", getEnv("RELAY_NODE_ID", "node-1"), "Unique node identifier")
		raftAddr           = flag.String("raft-addr", getEnv("RELAY_RAFT_ADDR", "127.0.0.1:7000"), "Raft internal communication address")
		grpcAddr           = flag.String("grpc-addr", getEnv("RELAY_GRPC_ADDR", "127.0.0.1:50051"), "gRPC service bind address")
		httpAddr           = flag.String("http-addr", getEnv("RELAY_HTTP_ADDR", "127.0.0.1:8080"), "HTTP /metrics and health endpoints bind address")
		dataDir            = flag.String("data-dir", getEnv("RELAY_DATA_DIR", "data"), "Base data directory")
		bootstrap          = flag.Bool("bootstrap", getEnv("RELAY_BOOTSTRAP", "false") == "true", "Bootstrap Raft cluster as initial single node")
		joinAddr           = flag.String("join-addr", getEnv("RELAY_JOIN_ADDR", ""), "gRPC address of existing cluster leader to join")
		peerMapStr         = flag.String("peers", getEnv("RELAY_PEERS", ""), "Comma-separated list of peer mappings, e.g. node-1=127.0.0.1:50051,node-2=127.0.0.1:50052")
		heartbeatTimeout   = flag.Duration("heartbeat-timeout", 1000*time.Millisecond, "Raft heartbeat timeout")
		electionTimeout    = flag.Duration("election-timeout", 1000*time.Millisecond, "Raft election timeout")
		leaderLeaseTimeout = flag.Duration("leader-lease-timeout", 500*time.Millisecond, "Raft leader lease timeout")
		staleReads         = flag.Bool("stale-reads", getEnv("RELAY_STALE_READS", "false") == "true", "Serve reads locally without Raft leader verification (may return stale data)")
	)
	flag.Parse()

	// Structured JSON logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With(slog.String("node_id", *nodeID))

	logger.Info("Starting Relay daemon",
		slog.String("raft_addr", *raftAddr),
		slog.String("grpc_addr", *grpcAddr),
		slog.String("http_addr", *httpAddr),
		slog.String("data_dir", *dataDir),
		slog.Bool("bootstrap", *bootstrap),
	)

	nodeDir := filepath.Join(*dataDir, *nodeID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		logger.Error("Failed to create node data directory", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Phase 2 Invariant: Wipe ephemeral FSM db on startup.
	// State is reconstructed deterministically from Raft log and snapshots.
	fsmPath := filepath.Join(nodeDir, "fsm.db")
	_ = os.Remove(fsmPath)

	fsm, err := cluster.NewFSM(fsmPath)
	if err != nil {
		logger.Error("Failed to initialize FSM", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Initialize BoltStore for Raft log and stable storage (fsync ON)
	raftDBPath := filepath.Join(nodeDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(raftDBPath)
	if err != nil {
		logger.Error("Failed to initialize Raft BoltStore", slog.String("error", err.Error()))
		_ = fsm.Close()
		os.Exit(1)
	}

	// File snapshot store
	snapDir := filepath.Join(nodeDir, "snapshots")
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 3, os.Stderr)
	if err != nil {
		logger.Error("Failed to initialize Raft SnapshotStore", slog.String("error", err.Error()))
		_ = boltStore.Close()
		_ = fsm.Close()
		os.Exit(1)
	}

	// Real TCP Transport
	tcpAddr, err := net.ResolveTCPAddr("tcp", *raftAddr)
	if err != nil {
		logger.Error("Failed to resolve Raft TCP address", slog.String("error", err.Error()))
		_ = boltStore.Close()
		_ = fsm.Close()
		os.Exit(1)
	}
	transport, err := raft.NewTCPTransport(*raftAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		logger.Error("Failed to create Raft TCP transport", slog.String("error", err.Error()))
		_ = boltStore.Close()
		_ = fsm.Close()
		os.Exit(1)
	}

	// Raft configuration
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(*nodeID)
	raftCfg.HeartbeatTimeout = *heartbeatTimeout
	raftCfg.ElectionTimeout = *electionTimeout
	raftCfg.LeaderLeaseTimeout = *leaderLeaseTimeout

	hasState, err := raft.HasExistingState(boltStore, boltStore, snapStore)
	if err != nil {
		logger.Error("Failed to check existing Raft state", slog.String("error", err.Error()))
		_ = transport.Close()
		_ = boltStore.Close()
		_ = fsm.Close()
		os.Exit(1)
	}

	if *bootstrap && !hasState {
		logger.Info("Bootstrapping initial single-node Raft cluster")
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftCfg.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := raft.BootstrapCluster(raftCfg, boltStore, boltStore, snapStore, transport, cfg); err != nil {
			logger.Error("Failed to bootstrap Raft cluster", slog.String("error", err.Error()))
			_ = transport.Close()
			_ = boltStore.Close()
			_ = fsm.Close()
			os.Exit(1)
		}
	}

	r, err := raft.NewRaft(raftCfg, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		logger.Error("Failed to initialize Raft node", slog.String("error", err.Error()))
		_ = transport.Close()
		_ = boltStore.Close()
		_ = fsm.Close()
		os.Exit(1)
	}

	// Start Leader Batcher
	batcherCfg := cluster.DefaultLeaderBatcherConfig()
	batcher := cluster.NewLeaderBatcher(r, fsm, batcherCfg)

	// Build peer mapping for gRPC forwarding
	peerMap := make(map[string]string)
	if *peerMapStr != "" {
		pairs := strings.Split(*peerMapStr, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) == 2 {
				peerMap[parts[0]] = parts[1]
			}
		}
	}

	peerResolver := func(leaderID raft.ServerID, leaderRaftAddr raft.ServerAddress) (string, error) {
		// 1. Check peer map by node ID
		if addr, ok := peerMap[string(leaderID)]; ok {
			return addr, nil
		}
		// 2. Check peer map by Raft address
		if addr, ok := peerMap[string(leaderRaftAddr)]; ok {
			return addr, nil
		}
		// 3. Fallback to joinAddr if set
		if *joinAddr != "" {
			return *joinAddr, nil
		}
		return "", fmt.Errorf("unable to resolve gRPC address for leader %s (%s)", leaderID, leaderRaftAddr)
	}

	metrics := server.NewMetrics()
	srvCfg := server.ServerConfig{
		NodeID:       *nodeID,
		GRPCAddr:     *grpcAddr,
		HTTPAddr:     *httpAddr,
		PeerResolver: peerResolver,
		StaleReads:   *staleReads,
	}
	srv := server.NewServer(srvCfg, r, fsm, batcher, metrics, logger)

	if err := srv.Start(*grpcAddr, *httpAddr); err != nil {
		logger.Error("Failed to start server", slog.String("error", err.Error()))
		batcher.Stop()
		_ = r.Shutdown().Error()
		_ = transport.Close()
		_ = boltStore.Close()
		_ = fsm.Close()
		os.Exit(1)
	}

	// Auto-join existing cluster if joinAddr is configured and not bootstrapped
	if *joinAddr != "" && !*bootstrap {
		go func() {
			time.Sleep(500 * time.Millisecond)
			logger.Info("Attempting to join cluster via leader", slog.String("join_addr", *joinAddr))
			for i := 0; i < 30; i++ {
				conn, err := grpc.NewClient(*joinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err == nil {
					client := relaypb.NewRelayServiceClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, joinErr := client.Join(ctx, &relaypb.JoinRequest{
						NodeId:      *nodeID,
						RaftAddress: string(transport.LocalAddr()),
					})
					cancel()
					_ = conn.Close()

					if joinErr == nil {
						logger.Info("Successfully joined Raft cluster!")
						return
					}
					logger.Warn("Join attempt returned error, retrying...", slog.String("error", joinErr.Error()))
				}
				time.Sleep(1 * time.Second)
			}
			logger.Error("Failed to join Raft cluster after 30 attempts")
		}()
	}

	// Graceful shutdown handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("Received shutdown signal, terminating node gracefully...")

	srv.Stop()
	batcher.Stop()

	shutdownFuture := r.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		logger.Warn("Raft shutdown returned error", slog.String("error", err.Error()))
	}

	_ = transport.Close()
	_ = boltStore.Close()
	_ = fsm.Close()

	logger.Info("Relayd shutdown complete")
}
