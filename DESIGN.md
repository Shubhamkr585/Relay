# Relay Architecture & Systems Design

This document details the internal architecture, determinism rules, storage engine layout, consensus replication, and design trade-offs in **Relay** (`github.com/shubhamkr585/relay`).

---

## 1. At-Least-Once Delivery & Idempotency

In distributed systems with arbitrary network delays, worker crashes, and partitions, true "exactly-once" message processing cannot be achieved without external two-phase commit (2PC) between the queue and worker-side side-effects. Relay provides **at-least-once delivery** paired with two foundational primitives that allow applications to achieve safe once-only execution:

### 1.1 Producer Idempotency Keys
- **File / Implementation:** [`internal/store/bbolt_tx.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/store/bbolt_tx.go) (`PutIdempotencyKey`, `GetIdempotencyKey`)
- Clients submit an `idempotency_key` on `Enqueue`.
- Relay maintains an atomic secondary index in the `idx_idempotency` bucket mapping:
  `queue_name + \x00 + idempotency_key -> job_id`
- Re-transmitting an enqueue with the same key returns the existing `job_id` with `deduplicated = true` without creating redundant queue entries.

### 1.2 Monotonic Fencing Lease Tokens
- **File / Implementation:** [`internal/cluster/fsm.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/cluster/fsm.go) (`FSM.applyBatch`, `FSM.Apply`)
- Every job reservation transitions state from `PENDING` to `LEASED`, increments `attempts`, and issues an opaque `lease_token`.
- To prevent a slow, zombie worker (e.g. paused due to garbage collection or network delay) from completing work after its lease has expired, lease tokens are derived inside `FSM.Apply` as:
  ```text
  <raft_log_index>-<batch_position>
  ```
- Because Raft log indices are strictly monotonically increasing, any subsequent reservation of the same job generates a strictly higher token.
- When an expired worker attempts to call `Ack` or `Nack`, [`internal/cluster/fsm.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/cluster/fsm.go) detects the token mismatch against the active state in bbolt and rejects the mutation with `ErrStaleLease` (`codes.FailedPrecondition`).

---

## 2. Deterministic Raft Finite State Machine (FSM)

A core requirement of replicated state machines via Raft is **strict determinism**: given an identical sequence of Raft log entries, every replica (and any node replaying logs during crash recovery) must reach a byte-for-byte identical state.

### 2.1 Elimination of Clock and Random Sources
- **File / Implementation:** [`internal/cluster/fsm.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/cluster/fsm.go) (`FSM.Apply`)
- `FSM.Apply` is a pure function of `(CurrentState, Command) -> NextState`. It never invokes `time.Now()` or random number generators.
- Mutating commands contain a leader-stamped `timestamp_nano`.
- The FSM tracks a strictly monotonic logical clock:
  ```go
  if cmd.TimestampNano > f.lastAppliedNow {
      f.lastAppliedNow = cmd.TimestampNano
  }
  ```
- All lease expiration checks, delayed run schedules, and attempt timeouts evaluate against `f.lastAppliedNow`. This monotonic timestamp is persisted in the `meta` bucket and restored across snapshots.

### 2.2 Deterministic Exponential Backoff Jitter
- **File / Implementation:** [`internal/domain/backoff.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/domain/backoff.go) (`DeterministicJitter`)
- Standard random jitter causes replica divergence when replayed on different machines.
- Relay computes backoff jitter deterministically using an FNV-1a 32-bit hash of `(job_id, attempt)`:
  ```go
  h := fnv.New32a()
  _, _ = h.Write([]byte(jobID))
  _ = binary.Write(h, binary.BigEndian, attempt)
  ratio := float64(h.Sum32()%10000) / 10000.0
  ```
  Every replica computes the exact same nanosecond delay without random drift.

### 2.3 Bounded Promotion and Expiration
- **File / Implementation:** [`internal/cluster/fsm.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/cluster/fsm.go) (`FSM.applyPromoteAndExpire`)
- Background maintenance commands (`COMMAND_TYPE_PROMOTE_AND_EXPIRE`) promote ripe scheduled jobs and expire timed-out leases.
- To prevent unbounded transaction sizes that could pause Raft heartbeats, every promotion and expiration sweep is bounded to a maximum of 1,000 items per command.

---

## 3. Storage Engine & Index Layout (`bbolt`)

Relay uses `bbolt` (an embedded copy-on-write B+ Tree database) for local state storage. All bucket keys use Big-Endian byte encoding so that byte-slice lexicographical ordering matches chronological and numerical ordering.

### 3.1 Bucket Layout

| Bucket Name | Key Format | Value Format | Purpose |
| :--- | :--- | :--- | :--- |
| `jobs` | `job_id` (string UUID) | Protobuf `Job` bytes | Primary record storage and point lookups |
| `idx_ready` | `queue + \x00 + priority(1B) + created_at(8B BE) + job_id` | Empty | Priority seek: fetches lowest priority byte, oldest job first |
| `idx_scheduled` | `run_at(8B BE) + job_id` | `queue` | Chronological range scan for delayed & backoff jobs |
| `idx_leased` | `lease_expires_at(8B BE) + job_id` | `queue` | Chronological range scan to reap expired leases |
| `idx_dead` | `queue + \x00 + created_at(8B BE) + job_id` | Empty | Chronological inspection of quarantined DLQ jobs |
| `idx_idempotency` | `queue + \x00 + idempotency_key` | `job_id` | O(log N) enqueue deduplication check |
| `meta` | `key` (e.g. `last_applied_now`) | Int64 Big-Endian bytes | Persisted monotonic clock state |

---

## 4. Durability & Startup Wipe-and-Rebuild Model

### 4.1 Wipe-and-Rebuild Durability
- **File / Implementation:** [`internal/cluster/node.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/cluster/node.go) (`openFSM`)
- The FSM database is opened with `bbolt.Options{NoSync: true}`.
- Durability is guaranteed by the Raft write-ahead log (`raft-boltdb`), which issues a synchronous `fsync` for every quorum entry commit.
- On startup, the local FSM bbolt file is wiped and reconstructed cleanly by restoring the latest Raft snapshot followed by replaying all committed WAL entries up to the current applied index.
- This design guarantees that uncommitted FSM writes or partial disk states after a process crash can never corrupt cluster state.

### 4.2 Snapshot and Restore
- **File / Implementation:** [`internal/cluster/fsm.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/cluster/fsm.go) (`FSM.Snapshot`, `FSM.Restore`)
- `Snapshot()` acquires a read transaction on bbolt and streams the consistent database file directly to the Raft snapshot sink (`sink.Write`), including the persisted monotonic time.
- `Restore()` closes the current bbolt database, atomically replaces the file on disk with the snapshot stream, and re-opens the engine.

---

## 5. Group-Commit Micro-Batching

- **File / Implementation:** [`internal/cluster/batcher.go`](file:///c:/Users/BIT/OneDrive/Desktop/projects/Relay/internal/cluster/batcher.go) (`LeaderBatcher.Run`, `LeaderBatcher.flushBatch`)
- Naive synchronous Raft proposals commit one log entry per client call, causing throughput to be bounded by sequential disk `fsync` latency.
- The `LeaderBatcher` sits in front of `raft.Apply`:
  1. Incoming mutations (`Enqueue`, `Reserve`, `Ack`, `Nack`) push proposals into a bounded channel.
  2. The batcher collects proposals until either **64 operations** accumulate or **2 milliseconds** elapse.
  3. All operations in the batch are serialized into a single `CommandBatch` message, replicated in **one Raft consensus round-trip**, and applied in **one atomic bbolt transaction**.
  4. Response channels are unblocked concurrently upon batch application.

---

## 6. Failure Modes & Operational Behavior

### 6.1 Follower Crash
- If a follower crashes, quorum (2 of 3 nodes) remains intact. Writes continue without interruption.
- When the follower restarts, Raft catches up by replaying missed log entries from the leader, or streaming a snapshot if logs were compacted.

### 6.2 Leader Crash & Failover
- If the leader crashes, heartbeat timeouts trigger an election among the surviving nodes.
- In-flight requests on the failed leader return connection errors to the client.

### 6.3 Ambiguous Retries
- If the leader crashes after replicating a command but before returning a response to the client, the client receives an error.
- Clients retry using the identical `idempotency_key`. The new leader detects the existing key in `idx_idempotency` and returns the previously generated `job_id`, preserving exact deduplication.

### 6.4 Worker Crash Mid-Execution
- If a worker crashes while holding a leased job, the lease expires when `lease_expires_at` is reached.
- The leader's periodic maintenance command reclaims the job, increments its attempt counter, and re-queues it for delivery (or moves it to DLQ if max attempts are exceeded).

---

## 7. Qualitative Architectural Trade-Offs

| System Dimension | Relay (Go + Raft + bbolt) | Redis + BullMQ | Amazon SQS |
| :--- | :--- | :--- | :--- |
| **Durability Model** | Quorum write-ahead log with strict `fsync` on every commit. | In-memory execution; async replication can lose acknowledged writes on master crash. | Multi-datacenter managed replication across cloud availability zones. |
| **Consistency** | Strongly consistent state machine via Raft leader leases and quorum consensus. | Eventual consistency; partition failover can result in split-brain execution. | Best-effort ordering on standard queues; FIFO queues require group IDs. |
| **Operational Simplicity** | Single static binary; zero external dependencies or database daemons. | Requires external Redis cluster, Sentinel, and Lua scripting maintenance. | Fully managed cloud service; requires cloud credentials and network egress. |
| **Failure Recovery** | Automatic deterministic log replay from snapshots on startup. | Manual cluster promotion or Sentinel failover tooling. | Managed failover transparent to consumers. |

---

## 8. Known Bottlenecks & Future Work

1. **Multi-Raft Partitioning:** Relay currently runs a single consensus group. Partitioning queues across multiple Raft consensus groups will enable horizontal write scaling across large node clusters.
2. **Transport Security & Auth:** `Join` RPCs and client gRPC connections operate over plaintext TCP. Adding mutual TLS (mTLS) and token authentication is planned for production deployments.
3. **Read Leases for Queries:** Follower reads (`Stats`, `ListDead`) serve state directly from local bbolt instances, which can reflect minor replication lag. Implementing Raft leader read-leases will enable linearizable reads across all nodes.
