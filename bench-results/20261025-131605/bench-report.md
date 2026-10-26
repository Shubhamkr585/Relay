# Relay Benchmark Verification Report (Phase 5)

**Execution Date:** 2026-10-25T13:16:05+05:30  
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
| **Throughput (fsyncs/sec)** | 1761 |
| **p50 Latency** | 528.0µs |
| **p95 Latency** | 1.06ms |
| **p99 Latency** | 1.29ms |
| **Max Latency** | 53.29ms |

## B1. Single-Node Storage Engine (No Raft)

Direct `bbolt` storage engine throughput and latency across concurrency levels, testing unbatched vs. group-commit micro-batcher, and fsync ON vs. fsync OFF.

| Configuration | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Unbatched (0ms/1-op) | 1 | 579 | 1.59ms | 3.14ms | 5.02ms | 10.28ms | PASS |
| Unbatched (0ms/1-op) | 32 | 512 | 54.57ms | 120.00ms | 203.17ms | 245.02ms | PASS |
| Group Commit (2ms/64-op) | 1 | 243 | 4.12ms | 4.77ms | 5.39ms | 47.07ms | PASS |
| Group Commit (2ms/64-op) | 32 | 2848 | 10.35ms | 15.93ms | 20.73ms | 97.48ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 1 | 373 | 2.63ms | 3.28ms | 4.42ms | 9.89ms | PASS |
| Group Commit (2ms/64-op) *(NOT DURABLE, comparison only)* | 32 | 6454 | 4.68ms | 6.93ms | 12.64ms | 39.34ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 1 | 5128 | 0.0µs | 1.25ms | 1.89ms | 5.89ms | PASS |
| Unbatched (0ms/1-op) *(NOT DURABLE, comparison only)* | 32 | 4237 | 7.48ms | 10.54ms | 12.77ms | 22.63ms | PASS |

## B2. 3-Node Raft Cluster Closed-Loop Enqueue (Strict Durability ON)

End-to-end gRPC clients submitting `Enqueue` operations across 3 Raft replicas with fsync ON. Compares leader micro-batcher (2ms/64-op) against unbatched (0ms/1-op).

| Mode | Concurrency | Throughput (ops/sec) | p50 Latency | p95 Latency | p99 Latency | Max Latency | Sanity Check |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Group Commit (2ms/64-op) | 1 | 128 | 7.33ms | 10.30ms | 17.59ms | 52.15ms | FAIL |
| Group Commit (2ms/64-op) | 32 | 1243 | 21.08ms | 53.54ms | 133.62ms | 143.87ms | PASS |

## B3. Open-Loop Latency (Eliminating Coordinated Omission)

Requests dispatched on a strict fixed timeline at 25%, 50%, and 80% of max measured B2 throughput. Latency is measured from scheduled send time.

| Offered Target Rate | Achieved Rate (ops/sec) | p50 Latency | p95 Latency | p99 Latency | p99.9 Latency | Max Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 311 req/s (25%) | 308 | 12.98ms | 46.80ms | 122.42ms | 148.17ms | 148.17ms |
| 621 req/s (50%) | 614 | 47.97ms | 131.30ms | 166.90ms | 193.75ms | 195.03ms |
| 994 req/s (80%) | 494 | 155.49ms | 1.27s | 1.33s | 1.35s | 1.35s |

## B4. End-to-End Pipeline Flow (Enqueue -> Reserve -> Ack)

Concurrent producers enqueuing jobs paired with concurrent workers reserving and acknowledging leases.

| Metric | Value (Median [Min - Max]) |
| :--- | :--- |
| **Completed Jobs Throughput** | 30 jobs/sec |
| **Enqueue-to-Ack p50 Latency** | 563.95ms |
| **Enqueue-to-Ack p95 Latency** | 1.17s |
| **Enqueue-to-Ack p99 Latency** | 1.63s |
| **Enqueue-to-Ack Max Latency** | 1.63s |

## B5. Leader Failover Latency (Real relayd Subprocesses)

3 independent `relayd` child processes under steady write traffic. Old leader is killed via `SIGKILL`; failover duration is measured from kill time until the first successful write through a surviving node. Total Trials: 3.

| Metric | Measured Duration |
| :--- | :--- |
| **Min Failover Time** | 528.23ms |
| **Median Failover Time** | 914.03ms |
| **p95 Failover Time** | 914.03ms |
| **Max Failover Time** | 922.04ms |
| **Total Lost Jobs Across Trials** | 0 (100% Intact) |

