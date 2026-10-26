# Relay Benchmark Verification Report (Phase 5)

**Execution Date:** 2026-10-25T13:09:25+05:30  
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
| **Throughput (fsyncs/sec)** | 1287 |
| **p50 Latency** | 546.0µs |
| **p95 Latency** | 1.31ms |
| **p99 Latency** | 2.36ms |
| **Max Latency** | 19.03ms |

## B1. Single-Node Storage Engine (No Raft)

Direct `bbolt` storage engine throughput and latency across concurrency levels, testing unbatched vs. group-commit micro-batcher, and fsync ON vs. fsync OFF.

| Configuration | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Unbatched (0ms/1-op) | 1 | 411 | 2.17ms | 3.68ms | 5.03ms | 72.46ms | PASS |
| Unbatched (0ms/1-op) | 32 | 216 | 113.87ms | 268.96ms | 318.19ms | 334.18ms | PASS |
| Group Commit (2ms/64-op) | 1 | 153 | 5.50ms | 11.83ms | 28.01ms | 37.54ms | PASS |
| Group Commit (2ms/64-op) | 32 | 2271 | 12.78ms | 23.17ms | 30.32ms | 42.46ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 1 | 379 | 2.60ms | 3.15ms | 3.62ms | 4.53ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 32 | 6826 | 4.68ms | 6.09ms | 7.09ms | 7.93ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 1 | 2990 | 0.0µs | 1.51ms | 2.52ms | 24.02ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 32 | 4064 | 7.42ms | 12.50ms | 14.75ms | 17.05ms | PASS |

## B2. 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)

End-to-end gRPC clients submitting `Enqueue` operations across 3 Raft replicas with fsync ON. Compares leader micro-batcher (2ms/64-op) against unbatched (0ms/1-op).

| Mode | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Group Commit (2ms/64-op) | 1 | 98 | 8.87ms | 16.02ms | 43.04ms | 59.78ms | PASS |
| Group Commit (2ms/64-op) | 32 | 1059 | 25.98ms | 62.47ms | 123.81ms | 185.23ms | FAIL |

## B3. Open-Loop Latency (Eliminating Coordinated Omission)

Requests dispatched on a strict fixed timeline at 25%, 50%, and 80% of max measured B2 throughput. Latency is measured from scheduled send time.

| Offered Target Rate | Achieved Rate (ops/sec) | p50 Latency | p95 Latency | p99 Latency | p99.9 Latency | Max Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 265 req/s (25%) | 261 | 13.90ms | 28.64ms | 46.27ms | 54.41ms | 54.41ms |
| 530 req/s (50%) | 450 | 68.64ms | 285.46ms | 510.36ms | 533.81ms | 534.90ms |
| 847 req/s (80%) | 366 | 331.48ms | 1.21s | 1.27s | 1.29s | 1.29s |

## B4. End-to-End Pipeline Flow (Enqueue -> Reserve -> Ack)

Concurrent producers enqueuing jobs paired with concurrent workers reserving and acknowledging leases.

| Metric | Value (Median [Min - Max]) |
| :--- | :--- |
| **Completed Jobs Throughput** | 0 jobs/sec |
| **Enqueue-to-Ack p50 Latency** | 0.0µs |
| **Enqueue-to-Ack p95 Latency** | 0.0µs |
| **Enqueue-to-Ack p99 Latency** | 0.0µs |
| **Enqueue-to-Ack Max Latency** | 0.0µs |

