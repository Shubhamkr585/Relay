#!/usr/bin/env bash
# scripts/demo-failover.sh
# Non-interactive / interactive demonstration of 3-node leader failover & convergence

set -euo pipefail

echo "================================================================================"
echo "  RELAY DEMO: 3-Node Raft Cluster Leader Failover & Transparent Forwarding"
echo "================================================================================"

echo -e "\n[1/6] Starting 3-node Docker cluster..."
docker compose down -v 2>/dev/null || true
docker compose up -d

echo -e "\n[2/6] Waiting for Raft cluster quorum and leader election..."
LEADER_READY=false
for i in $(seq 1 30); do
  sleep 1
  if curl -sf http://localhost:8080/healthz >/dev/null 2>&1; then
    LEADER_READY=true
    break
  fi
done

if [ "$LEADER_READY" = false ]; then
  echo "Error: Timed out waiting for node-1 healthz" >&2
  exit 1
fi
echo "-> Cluster formed. Node 1 is active leader."

echo -e "\n[3/6] Enqueuing 3 jobs through Follower Node-2 (127.0.0.1:50052)..."
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-payload-1" --idempotency-key "demo-1"
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-payload-2" --idempotency-key "demo-2"
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-payload-3" --idempotency-key "demo-3"
echo "-> Successfully enqueued 3 jobs via follower (forwarded to leader)."

echo -e "\n[4/6] Killing Leader Container (relay-node-1) with SIGKILL..."
docker stop -t 1 relay-node-1
echo "-> Leader relay-node-1 killed. Raft election underway across surviving nodes..."
sleep 3

echo -e "\n[5/6] Enqueuing 2 jobs through surviving node (127.0.0.1:50052)..."
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-failover-payload-4" --idempotency-key "demo-4"
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-failover-payload-5" --idempotency-key "demo-5"
echo "-> Successfully enqueued 2 jobs during failover through surviving node."

echo -e "\n[6/6] Bringing back old leader (relay-node-1) and checking convergence..."
docker start relay-node-1
sleep 4

echo -e "\n--- FINAL QUEUE STATS ACROSS ALL 3 NODES ---"
echo "Node 1 (127.0.0.1:50051):"
STATS1=$(go run ./cmd/relay-cli stats --server localhost:50051 --queue demo)
echo "$STATS1"

echo "Node 2 (127.0.0.1:50052):"
STATS2=$(go run ./cmd/relay-cli stats --server localhost:50052 --queue demo)
echo "$STATS2"

echo "Node 3 (127.0.0.1:50053):"
STATS3=$(go run ./cmd/relay-cli stats --server localhost:50053 --queue demo)
echo "$STATS3"

for s in "$STATS1" "$STATS2" "$STATS3"; do
  if ! echo "$s" | grep -q '"pending_count": 5'; then
    echo "Error: Mismatch in stats! Expected pending_count: 5" >&2
    exit 1
  fi
done

echo "================================================================================"
echo "  DEMO SUCCESSFUL: All 3 nodes converged to 5 pending jobs with zero data loss!"
echo "================================================================================"

docker compose down -v
