# Relay Benchmark Verification Report (Phase 5)

**Execution Date:** 2026-10-25T13:21:00+05:30  
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
| **Throughput (fsyncs/sec)** | 1606 |
| **p50 Latency** | 534.0µs |
| **p95 Latency** | 1.07ms |
| **p99 Latency** | 1.20ms |
| **Max Latency** | 59.17ms |

## B1. Single-Node Storage Engine (No Raft)

Direct `bbolt` storage engine throughput and latency across concurrency levels, testing unbatched vs. group-commit micro-batcher, and fsync ON vs. fsync OFF.

| Configuration | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Unbatched (0ms/1-op) | 1 | 390 | 2.18ms | 4.37ms | 7.62ms | 32.49ms | PASS |
| Unbatched (0ms/1-op) | 32 | 370 | 67.07ms | 171.85ms | 204.22ms | 238.49ms | PASS |
| Group Commit (2ms/64-op) | 1 | 215 | 4.33ms | 5.98ms | 7.43ms | 24.10ms | PASS |
| Group Commit (2ms/64-op) | 32 | 2410 | 12.37ms | 19.69ms | 26.51ms | 84.09ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 1 | 365 | 2.64ms | 3.53ms | 5.04ms | 9.47ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 32 | 4861 | 6.04ms | 9.83ms | 15.46ms | 23.26ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 1 | 4823 | 0.0µs | 1.04ms | 1.52ms | 9.00ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 32 | 4261 | 6.97ms | 11.79ms | 14.79ms | 25.05ms | PASS |

## B2. 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)

End-to-end gRPC clients submitting `Enqueue` operations across 3 Raft replicas with fsync ON. Compares leader micro-batcher (2ms/64-op) against unbatched (0ms/1-op).

| Mode | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Group Commit (2ms/64-op) | 1 | 110 | 7.73ms | 14.16ms | 45.54ms | 85.72ms | PASS |
| Group Commit (2ms/64-op) | 32 | 1415 | 20.38ms | 41.15ms | 63.36ms | 104.65ms | PASS |

## B3. Open-Loop Latency (Eliminating Coordinated Omission)

Requests dispatched on a strict fixed timeline at 25%, 50%, and 80% of max measured B2 throughput. Latency is measured from scheduled send time.

| Offered Target Rate | Achieved Rate (ops/sec) | p50 Latency | p95 Latency | p99 Latency | p99.9 Latency | Max Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 354 req/s (25%) | 0 | 0.0µs | 0.0µs | 0.0µs | 0.0µs | 0.0µs |
| 707 req/s (50%) | 532 | 434.46ms | 757.03ms | 799.48ms | 819.46ms | 820.35ms |
| 1132 req/s (80%) | 728 | 569.71ms | 987.79ms | 1.08s | 1.10s | 1.11s |

## B4. End-to-End Pipeline Flow (Enqueue -> Reserve -> Ack)

Concurrent producers enqueuing jobs paired with concurrent workers reserving and acknowledging leases.

| Metric | Value (Median [Min - Max]) |
| :--- | :--- |
| **Completed Jobs Throughput** | 8 jobs/sec |
| **Enqueue-to-Ack p50 Latency** | 1.03s |
| **Enqueue-to-Ack p95 Latency** | 1.28s |
| **Enqueue-to-Ack p99 Latency** | 1.33s |
| **Enqueue-to-Ack Max Latency** | 1.33s |

## B5. Leader Failover Latency (Real relayd Subprocesses)

3 independent `relayd` child processes under steady write traffic. Old leader is killed via `SIGKILL`; failover duration is measured from kill time until the first successful write through a surviving node. Total Trials: 3.

| Metric | Measured Duration |
| :--- | :--- |
| **Min Failover Time** | 625.57ms |
| **Median Failover Time** | 944.88ms |
| **p95 Failover Time** | 944.88ms |
| **Max Failover Time** | 5.66s |
| **Total Lost Jobs Across Trials** | 0 (100% Intact) |

