# Relay Benchmark Verification Report (Phase 5)

**Execution Date:** 2026-10-25T13:25:06+05:30  
**Git Commit:** `cce684709f81ab4afc65d6e804ec2c030d50ac30`  
**Environment:** AMD Ryzen 5 7530U with Radeon Graphics, 12 Logical Cores, 7.31 GB RAM, Microsoft Windows 11 Home Single Language (amd64), Go go1.27.1  
**Storage Media:** SSD | **Power Plan:** HP Optimized (Modern Standby | **GOMAXPROCS:** 12  
**Relay Configuration:** Batch Window: `2ms`, Max Batch: `64`, Heartbeat: `100ms`, Election: `300ms`, Fsync: `Strict (fsync ON for Raft WAL + bbolt FSM)`  

> [!NOTE]
> **Hardware & Setup Disclaimer:** All cluster nodes and benchmarks were executed on a single physical host sharing one physical SSD. Absolute throughput and latency are hardware-dependent. Durability OFF (`NoSync`) numbers are provided strictly for isolating fsync cost and are labeled **NOT DURABLE, comparison only**.

## B0b. Raw Disk fsync Baseline (Hardware Ceiling)

Direct micro-benchmark issuing small 256-byte sequential appends with immediate `fsync()` per operation.

| Metric | Measured Value (Median [Min - Max]) |
| :--- | :--- |
| **Throughput (fsyncs/sec)** | 1675 |
| **p50 Latency** | 530.0µs |
| **p95 Latency** | 1.09ms |
| **p99 Latency** | 1.50ms |
| **Max Latency** | 42.27ms |

## B1. Single-Node Storage Engine (No Raft)

Direct `bbolt` storage engine throughput and latency across concurrency levels, testing unbatched vs. group-commit micro-batcher, and fsync ON vs. fsync OFF.

| Configuration | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Unbatched (0ms/1-op) | 1 | 557 | 1.62ms | 2.75ms | 3.33ms | 20.89ms | PASS |
| Unbatched (0ms/1-op) | 32 | 490 | 57.94ms | 102.65ms | 120.03ms | 127.02ms | PASS |
| Group Commit (2ms/64-op) | 1 | 240 | 4.15ms | 4.89ms | 5.73ms | 28.53ms | PASS |
| Group Commit (2ms/64-op) | 32 | 3007 | 10.56ms | 12.94ms | 14.83ms | 30.93ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 1 | 383 | 2.59ms | 3.13ms | 3.62ms | 4.81ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 32 | 7825 | 4.12ms | 5.36ms | 6.01ms | 7.70ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 1 | 5404 | 0.0µs | 1.07ms | 2.29ms | 9.25ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 32 | 5460 | 5.33ms | 10.56ms | 15.30ms | 27.47ms | PASS |

## B2. 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)

End-to-end gRPC clients submitting `Enqueue` operations across 3 Raft replicas with fsync ON. Compares leader micro-batcher (2ms/64-op) against unbatched (0ms/1-op).

| Mode | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Group Commit (2ms/64-op) | 1 | 120 | 7.04ms | 13.24ms | 20.91ms | 105.61ms | PASS |
| Group Commit (2ms/64-op) | 32 | 1773 | 15.28ms | 31.99ms | 79.72ms | 129.51ms | PASS |

## B3. Open-Loop Latency (Eliminating Coordinated Omission)

Requests dispatched on a strict fixed timeline at 25%, 50%, and 80% of max measured B2 throughput. Latency is measured from scheduled send time.

| Offered Target Rate | Achieved Rate (ops/sec) | p50 Latency | p95 Latency | p99 Latency | p99.9 Latency | Max Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 443 req/s (25%) | 441 | 10.24ms | 18.26ms | 25.69ms | 34.98ms | 35.27ms |
| 887 req/s (50%) | 876 | 18.54ms | 69.21ms | 90.82ms | 105.50ms | 108.35ms |
| 1419 req/s (80%) | 1413 | 12.78ms | 21.12ms | 27.79ms | 33.58ms | 39.51ms |

## B4. End-to-End Pipeline Flow (Enqueue -> Reserve -> Ack)

Concurrent producers enqueuing jobs paired with concurrent workers reserving and acknowledging leases.

| Metric | Value (Median [Min - Max]) |
| :--- | :--- |
| **Completed Jobs Throughput** | 162 jobs/sec |
| **Enqueue-to-Ack p50 Latency** | 622.34ms |
| **Enqueue-to-Ack p95 Latency** | 2.38s |
| **Enqueue-to-Ack p99 Latency** | 3.82s |
| **Enqueue-to-Ack Max Latency** | 4.06s |

## B5. Leader Failover Latency (Real relayd Subprocesses)

3 independent `relayd` child processes under steady write traffic. Old leader is killed via `SIGKILL`; failover duration is measured from kill time until the first successful write through a surviving node. Total Trials: 3.

| Metric | Measured Duration |
| :--- | :--- |
| **Min Failover Time** | 533.24ms |
| **Median Failover Time** | 806.30ms |
| **p95 Failover Time** | 806.30ms |
| **Max Failover Time** | 838.13ms |
| **Total Lost Jobs Across Trials** | 0 (100% Intact) |

