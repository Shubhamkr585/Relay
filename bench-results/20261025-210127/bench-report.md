# Relay Benchmark Verification Report (Phase 5)

**Execution Date:** 2026-10-25T21:01:27+05:30  
**Execution Mode:** `quick`  
**Git Commit:** `643f20ebad7975dcec5dfe6de01569deb6f8a965` (Dirty: `true`)  
**Configuration:** Raft WAL fsync ON, FSM bbolt NoSync; B1 engine fsync mode stated separately  
**Environment:** AMD Ryzen 5 7530U with Radeon Graphics, 12 Logical Cores, 7.31 GB RAM, Microsoft Windows 11 Home Single Language (amd64), Go go1.27.1  
**Storage Media:** SSD | **Power Plan:** HP Optimized (Modern Standby) | **Power Source:** AC Power (Plugged in)  
**Timer Implementation:** Windows QueryPerformanceCounter (QPC freq: 10.00 MHz)  
**Timer Clock Resolution:** 0.10µs (100 ns) | **GOMAXPROCS:** 12  

> [!WARNING]
> **DIRTY WORKING TREE WARNING:** The repository contains uncommitted working tree changes (`git_dirty=true`). Results must not be used for official releases until committed!

> [!WARNING]
> **SUB-OPTIMAL POWER PLAN WARNING:** The host system is running under power scheme `HP Optimized (Modern Standby)`. For reproducible full benchmark runs, set the OS power plan to High/Best Performance to avoid CPU throttling.

> [!NOTE]
> **Hardware & Setup Disclaimer:** All cluster nodes and benchmarks were executed on a single physical host sharing one physical SSD. Absolute throughput and latency are hardware-dependent. Durability OFF (`NoSync`) numbers are provided strictly for isolating fsync cost and are labeled **NOT DURABLE, comparison only**.

## B0b. Raw Disk fsync Baseline (Hardware Ceiling)

Direct micro-benchmark issuing small 256-byte sequential appends with immediate `fsync()` per operation.

| Metric | Measured Value (Median [Min - Max]) |
| :--- | :--- |
| **Throughput (fsyncs/sec)** | 1693 |
| **p50 Latency** | 529.10µs |
| **p95 Latency** | 1.06ms |
| **p99 Latency** | 1.22ms |
| **Max Latency** | 48.41ms |

## B1. Single-Node Storage Engine (No Raft)

Direct `bbolt` storage engine throughput and latency across concurrency levels, testing unbatched vs. group-commit micro-batcher, and engine fsync ON vs. engine fsync OFF.

| Configuration | Concurrency | Engine Durability | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Little's Law | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Unbatched (0ms/1-op) | 1 | fsync ON (Durable) | 562 | 1.68ms | 2.45ms | 3.20ms | 8.45ms | PASS | PASS |
| Unbatched (0ms/1-op) | 8 | fsync ON (Durable) | 490 | 14.13ms | 29.93ms | 52.76ms | 69.45ms | PASS | PASS |
| Unbatched (0ms/1-op) | 32 | fsync ON (Durable) | 530 | 57.02ms | 78.25ms | 101.01ms | 113.63ms | PASS | PASS |
| Unbatched (0ms/1-op) | 64 | fsync ON (Durable) | 501 | 110.39ms | 233.37ms | 365.28ms | 419.32ms | PASS | PASS |
| Group Commit (2ms/64-op) | 1 | fsync ON (Durable) | 214 | 4.50ms | 5.85ms | 7.14ms | 11.85ms | PASS | PASS |
| Group Commit (2ms/64-op) | 8 | fsync ON (Durable) | 1405 | 5.67ms | 6.48ms | 6.95ms | 8.45ms | PASS | PASS |
| Group Commit (2ms/64-op) | 32 | fsync ON (Durable) | 2988 | 10.12ms | 13.46ms | 25.72ms | 54.61ms | PASS | PASS |
| Group Commit (2ms/64-op) | 64 | fsync ON (Durable) | 4907 | 13.04ms | 15.94ms | 23.59ms | 47.90ms | PASS | PASS |
| Unbatched (0ms/1-op) | 1 | NoSync *(NOT DURABLE, comparison only)* | 5669 | 161.60µs | 312.60µs | 541.60µs | 6.28ms | PASS | PASS |
| Unbatched (0ms/1-op) | 8 | NoSync *(NOT DURABLE, comparison only)* | 5543 | 1.39ms | 2.10ms | 2.88ms | 12.11ms | PASS | PASS |
| Unbatched (0ms/1-op) | 32 | NoSync *(NOT DURABLE, comparison only)* | 5630 | 5.67ms | 6.69ms | 7.39ms | 9.81ms | PASS | PASS |
| Unbatched (0ms/1-op) | 64 | NoSync *(NOT DURABLE, comparison only)* | 5629 | 11.30ms | 14.15ms | 15.97ms | 29.77ms | PASS | PASS |
| Group Commit (2ms/64-op) | 1 | NoSync *(NOT DURABLE, comparison only)* | 102 | 4.41ms | 24.78ms | 68.13ms | 470.58ms | PASS | PASS |
| Group Commit (2ms/64-op) | 8 | NoSync *(NOT DURABLE, comparison only)* | 1861 | 3.62ms | 7.42ms | 16.39ms | 41.39ms | PASS | PASS |
| Group Commit (2ms/64-op) | 32 | NoSync *(NOT DURABLE, comparison only)* | 6599 | 4.24ms | 6.48ms | 19.80ms | 90.52ms | PASS | PASS |
| Group Commit (2ms/64-op) | 64 | NoSync *(NOT DURABLE, comparison only)* | 8684 | 6.32ms | 11.89ms | 21.10ms | 71.56ms | PASS | PASS |

## B2. 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)

End-to-end gRPC clients submitting `Enqueue` operations across 3 Raft replicas with fsync ON. Compares leader micro-batcher (2ms/64-op) against unbatched (0ms/1-op).

| Mode | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Errors / Timeouts | Little's Law | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Leader Micro-Batcher (2ms/64-op) | 1 | 121 | 7.42ms | 9.68ms | 11.77ms | 152.49ms | 0 / 0 | PASS | PASS |
| Leader Micro-Batcher (2ms/64-op) | 8 | 530 | 11.36ms | 25.28ms | 99.82ms | 180.94ms | 0 / 0 | PASS | PASS |
| Leader Micro-Batcher (2ms/64-op) | 32 | 1679 | 16.93ms | 33.50ms | 74.81ms | 108.74ms | 0 / 0 | PASS | PASS |
| Leader Micro-Batcher (2ms/64-op) | 64 | 1284 | 37.96ms | 105.07ms | 355.89ms | 368.96ms | 0 / 0 | PASS | PASS |
| Leader Micro-Batcher (2ms/64-op) | 128 | 93 | 68.21ms | 93.93ms | 101.57ms | 103.70ms | 286 / 37 | INCONSISTENT (measured mean 68.33ms vs expected 1.38s, ratio 0.05x) | FAIL (286 err, 37 timeout) |
| Unbatched (0ms/1-op) | 1 | 10 | 29.90ms | 433.36ms | 857.25ms | 857.25ms | 0 / 0 | PASS | PASS |
| Unbatched (0ms/1-op) | 8 | 9 | 660.67ms | 1.41s | 1.45s | 1.45s | 0 / 0 | PASS | PASS |
| Unbatched (0ms/1-op) | 32 | 5 | 1.92s | 2.75s | 2.75s | 2.75s | 0 / 3 | INCONSISTENT (measured mean 1.83s vs expected 6.40s, ratio 0.29x) | PASS |
| Unbatched (0ms/1-op) | 64 | 12 | 2.34s | 2.62s | 2.66s | 2.66s | 1 / 29 | INCONSISTENT (measured mean 2.27s vs expected 5.34s, ratio 0.43x) | FAIL (1 err, 29 timeout) |
| Unbatched (0ms/1-op) | 128 | 9 | 642.16ms | 1.94s | 2.13s | 2.13s | 791 / 0 | INCONSISTENT (measured mean 915.06ms vs expected 14.78s, ratio 0.06x) | FAIL (791 err, 0 timeout) |

> **Unbatched Scaling Observation:** Under unbatched operations (0ms/1-op), concurrency scaling from c=1 to c=128 shifted throughput from 10 to 9 ops/sec (p50: 29.90ms to 642.16ms) with 791 errors and 0 timeouts, reflecting Raft log lock serialization and synchronous disk sync per transaction.

## B3. Open-Loop Latency (Eliminating Coordinated Omission)

Requests dispatched on a strict fixed timeline at 25%, 50%, and 80% of max measured B2 throughput. Latency is measured from scheduled send time to eliminate coordinated omission.

| Offered Target Rate | Achieved Rate (ops/sec) | p50 Latency | p95 Latency | p99 Latency | p99.9 Latency | Max Latency | Saturation Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 420 req/s (25%) | 418 | 15.04ms | 35.30ms | 68.30ms | 87.69ms | 90.59ms | Nominal |
| 839 req/s (50%) | 827 | 20.62ms | 80.45ms | 126.65ms | 135.35ms | 142.32ms | Nominal |
| 1343 req/s (80%) | 851 | 412.61ms | 1.07s | 1.10s | 1.11s | 1.11s | **SATURATED** (achieved 851 of offered 1343) |

> **Measured Open-Loop Tail Metrics:** p95 latencies scaled monotonically across offered rates (35.30ms -> 80.45ms -> 1.07s).

## B4. End-to-End Pipeline Flow (Enqueue -> Reserve -> Ack)

Concurrent producers enqueuing jobs paired with concurrent workers reserving and acknowledging leases across worker pool sizes and producer pacing modes.

| Workload Mode | Producers | Workers | Completed Jobs/sec | p50 Latency | p95 Latency | p99 Latency | Max Latency | End Backlog | Sanity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Unthrottled Producers | 8 | 8 | 244 | 1.55s | 2.05s | 2.08s | 2.10s | 0 | PASS |
| Unthrottled Producers | 8 | 32 | 12 | 97.28ms | 129.63ms | 161.25ms | 161.25ms | 11 | FAIL |
| Unthrottled Producers | 8 | 64 | 0 | 0.0µs | 0.0µs | 0.0µs | 0.0µs | 0 | FAIL |
| Rate-Limited (50% Capacity) | 8 | 8 | 64 | 75.85ms | 403.40ms | 468.33ms | 468.33ms | 0 | FAIL |
| Rate-Limited (50% Capacity) | 8 | 32 | 4 | 94.53ms | 122.61ms | 122.61ms | 122.61ms | 0 | PASS |
| Rate-Limited (50% Capacity) | 8 | 64 | 16 | 531.35ms | 912.34ms | 955.10ms | 955.10ms | 0 | PASS |

> **Rate-Limited Pipeline Check (Workers=8):** Offered target 122 jobs/sec, achieved completion of 64 jobs/sec and accumulated 0 backlog jobs. **[FLAGGED: achieved 64 jobs/sec far below offered 122 jobs/sec]**

> **Rate-Limited Pipeline Check (Workers=32):** Offered target 6 jobs/sec, achieved completion of 4 jobs/sec and accumulated 0 backlog jobs. **[FLAGGED: achieved 4 jobs/sec far below offered 6 jobs/sec]**

> **Rate-Limited Pipeline Check (Workers=64):** Offered target 50 jobs/sec, achieved completion of 16 jobs/sec and accumulated 0 backlog jobs. **[FLAGGED: achieved 16 jobs/sec far below offered 50 jobs/sec]**

## B5. Leader Failover Latency (Real relayd Subprocesses)

3 independent `relayd` child processes under steady write traffic. Old leader is killed via `Process.Kill`; failover duration is measured from kill time until the first successful write through a surviving node.

### B5: Leader Failover Latency (Production Defaults)

**Raft Flags:** `-heartbeat-timeout=1s -election-timeout=1s -leader-lease-timeout=500ms` | **Total Trials:** 3 | **Valid Trials:** 3 | **Verified Data Loss:** 0 jobs

| Metric | Measured Duration | Expected Lower Bound | Lower Bound Status |
| :--- | :--- | :--- | :--- |
| **Min Failover Time** | 2.36s | 1.00s | PASS (>= lower bound) |
| **Median Failover Time** | 2.50s | 1.00s | PASS (>= lower bound) |
| **p95 Failover Time** | n too small | 1.00s | PASS (>= lower bound) |
| **Max Failover Time** | 2.52s | 1.00s | PASS (>= lower bound) |

#### Valid Trials (Included in Percentiles)

| Trial | Killed Leader | New Leader | Failover Time | Pre-Ack Jobs | Lost Jobs | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | b5_failover_default-node-1 | b5_failover_default-node-3 | 2.36s | 12 | 0 | PASS |
| 2 | b5_failover_default-node-3 | b5_failover_default-node-1 | 2.50s | 11 | 0 | PASS |
| 3 | b5_failover_default-node-1 | b5_failover_default-node-2 | 2.52s | 12 | 0 | PASS |

### B5: Leader Failover Latency (Aggressive Timeouts)

**Raft Flags:** `-heartbeat-timeout=100ms -election-timeout=300ms -leader-lease-timeout=100ms` | **Total Trials:** 3 | **Valid Trials:** 3 | **Verified Data Loss:** 0 jobs

| Metric | Measured Duration | Expected Lower Bound | Lower Bound Status |
| :--- | :--- | :--- | :--- |
| **Min Failover Time** | 178.34ms | 300.00ms | **SUSPICIOUS** (< election timeout) |
| **Median Failover Time** | 229.76ms | 300.00ms | **SUSPICIOUS** (< election timeout) |
| **p95 Failover Time** | n too small | 300.00ms | **SUSPICIOUS** (< election timeout) |
| **Max Failover Time** | 311.51ms | 300.00ms | **SUSPICIOUS** (< election timeout) |

#### Valid Trials (Included in Percentiles)

| Trial | Killed Leader | New Leader | Failover Time | Pre-Ack Jobs | Lost Jobs | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | b5_failover_aggressive-node-1 | b5_failover_aggressive-node-3 | 229.76ms | 18 | 0 | PASS |
| 2 | b5_failover_aggressive-node-3 | b5_failover_aggressive-node-2 | 178.34ms | 15 | 0 | PASS |
| 3 | b5_failover_aggressive-node-2 | b5_failover_aggressive-node-3 | 311.51ms | 18 | 0 | PASS |

