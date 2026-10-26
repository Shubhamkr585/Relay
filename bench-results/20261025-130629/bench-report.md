# Relay Benchmark Verification Report (Phase 5)

**Execution Date:** 2026-10-25T13:06:29+05:30  
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
| **Throughput (fsyncs/sec)** | 806 |
| **p50 Latency** | 888.0µs |
| **p95 Latency** | 2.60ms |
| **p99 Latency** | 8.00ms |
| **Max Latency** | 95.20ms |

## B1. Single-Node Storage Engine (No Raft)

Direct `bbolt` storage engine throughput and latency across concurrency levels, testing unbatched vs. group-commit micro-batcher, and fsync ON vs. fsync OFF.

| Configuration | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Unbatched (0ms/1-op) | 1 | 372 | 2.19ms | 5.00ms | 11.36ms | 86.79ms | PASS |
| Unbatched (0ms/1-op) | 32 | 321 | 91.09ms | 165.26ms | 218.28ms | 237.44ms | PASS |
| Group Commit (2ms/64-op) | 1 | 199 | 4.80ms | 6.99ms | 9.18ms | 13.10ms | PASS |
| Group Commit (2ms/64-op) | 32 | 2752 | 11.21ms | 15.81ms | 26.46ms | 48.01ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 1 | 383 | 2.58ms | 3.02ms | 3.36ms | 30.68ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 32 | 6293 | 4.80ms | 7.24ms | 10.51ms | 15.45ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 1 | 2204 | 0.0µs | 2.01ms | 3.60ms | 22.52ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 32 | 2838 | 10.39ms | 18.01ms | 24.68ms | 33.74ms | PASS |

## B2. 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)

End-to-end gRPC clients submitting `Enqueue` operations across 3 Raft replicas with fsync ON. Compares leader micro-batcher (2ms/64-op) against unbatched (0ms/1-op).

| Mode | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Group Commit (2ms/64-op) | 1 | 94 | 9.33ms | 15.37ms | 36.09ms | 91.24ms | PASS |
| Group Commit (2ms/64-op) | 32 | 927 | 23.57ms | 135.51ms | 219.22ms | 258.32ms | FAIL |

## B3. Open-Loop Latency (Eliminating Coordinated Omission)

Requests dispatched on a strict fixed timeline at 25%, 50%, and 80% of max measured B2 throughput. Latency is measured from scheduled send time.

| Offered Target Rate | Achieved Rate (ops/sec) | p50 Latency | p95 Latency | p99 Latency | p99.9 Latency | Max Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 232 req/s (25%) | 223 | 34.42ms | 325.93ms | 430.96ms | 456.87ms | 456.87ms |
| 463 req/s (50%) | 452 | 69.72ms | 174.76ms | 192.38ms | 204.55ms | 204.98ms |
| 741 req/s (80%) | 511 | 395.38ms | 888.57ms | 931.45ms | 953.11ms | 954.46ms |

## B4. End-to-End Pipeline Flow (Enqueue -> Reserve -> Ack)

Concurrent producers enqueuing jobs paired with concurrent workers reserving and acknowledging leases.

| Metric | Value (Median [Min - Max]) |
| :--- | :--- |
| **Completed Jobs Throughput** | 59 jobs/sec |
| **Enqueue-to-Ack p50 Latency** | 2.44s |
| **Enqueue-to-Ack p95 Latency** | 4.20s |
| **Enqueue-to-Ack p99 Latency** | 4.53s |
| **Enqueue-to-Ack Max Latency** | 4.59s |

