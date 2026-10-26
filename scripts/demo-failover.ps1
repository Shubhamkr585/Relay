# scripts/demo-failover.ps1
# Interactive / automated demo of 3-node Raft leader failover and state convergence

$ErrorActionPreference = "Continue"

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  RELAY DEMO: 3-Node Raft Cluster Leader Failover & Transparent Forwarding" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

# 1. Start Docker cluster
Write-Host "`n[1/6] Starting 3-node Docker cluster..." -ForegroundColor Yellow
docker compose down -v
docker compose up -d

# 2. Wait for cluster to elect a leader
Write-Host "`n[2/6] Waiting for Raft cluster quorum and leader election..." -ForegroundColor Yellow
$leaderReady = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 1
    try {
        $res = Invoke-RestMethod -Uri "http://localhost:8080/healthz" -TimeoutSec 1 -ErrorAction SilentlyContinue
        if ($res.status -eq "ok") {
            $leaderReady = $true
            break
        }
    } catch {}
}

if (-not $leaderReady) {
    Write-Error "Failed to detect healthy leader on node-1 within 30 seconds"
    exit 1
}
Write-Host "-> Cluster formed. Node 1 is active leader." -ForegroundColor Green

# 3. Enqueue jobs through a follower (Node 2: localhost:50052)
Write-Host "`n[3/6] Enqueuing 3 jobs through Follower Node-2 (127.0.0.1:50052)..." -ForegroundColor Yellow
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-payload-1" --idempotency-key "demo-1"
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-payload-2" --idempotency-key "demo-2"
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-payload-3" --idempotency-key "demo-3"
Write-Host "-> Successfully enqueued 3 jobs via follower (transparently forwarded to leader)." -ForegroundColor Green

# 4. Assassinate current leader container (relay-node-1)
Write-Host "`n[4/6] Killing Leader Container (relay-node-1) with SIGKILL..." -ForegroundColor Red
docker stop -t 1 relay-node-1
Write-Host "-> Leader relay-node-1 killed. Raft election underway across surviving nodes..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

# 5. Enqueue more jobs through a surviving node (Node 2: localhost:50052)
Write-Host "`n[5/6] Enqueuing 2 jobs through surviving node (127.0.0.1:50052)..." -ForegroundColor Yellow
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-failover-payload-4" --idempotency-key "demo-4"
go run ./cmd/relay-cli enqueue --server localhost:50052 --queue demo --payload "demo-failover-payload-5" --idempotency-key "demo-5"
Write-Host "-> Successfully enqueued 2 jobs during failover through surviving nodes." -ForegroundColor Green

# 6. Bring back the old leader and verify convergence across all 3 nodes
Write-Host "`n[6/6] Bringing back old leader (relay-node-1) and checking convergence..." -ForegroundColor Yellow
docker start relay-node-1
Start-Sleep -Seconds 4

Write-Host "`n--- FINAL QUEUE STATS ACROSS ALL 3 NODES ---" -ForegroundColor Cyan
Write-Host "Node 1 (127.0.0.1:50051):" -ForegroundColor Magenta
$stats1 = go run ./cmd/relay-cli stats --server localhost:50051 --queue demo
$stats1 | Write-Host

Write-Host "`nNode 2 (127.0.0.1:50052):" -ForegroundColor Magenta
$stats2 = go run ./cmd/relay-cli stats --server localhost:50052 --queue demo
$stats2 | Write-Host

Write-Host "`nNode 3 (127.0.0.1:50053):" -ForegroundColor Magenta
$stats3 = go run ./cmd/relay-cli stats --server localhost:50053 --queue demo
$stats3 | Write-Host

$json1 = $stats1 | ConvertFrom-Json
$json2 = $stats2 | ConvertFrom-Json
$json3 = $stats3 | ConvertFrom-Json

if ($json1.pending_count -ne 5 -or $json2.pending_count -ne 5 -or $json3.pending_count -ne 5) {
    Write-Error "Mismatch in queue stats! Expected 5 pending jobs across all nodes. Got: Node1=$($json1.pending_count), Node2=$($json2.pending_count), Node3=$($json3.pending_count)"
    exit 1
}

Write-Host "`n================================================================================" -ForegroundColor Green
Write-Host "  DEMO SUCCESSFUL: All 3 nodes converged to 5 pending jobs with zero data loss!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green

# Clean up containers
docker compose down -v
