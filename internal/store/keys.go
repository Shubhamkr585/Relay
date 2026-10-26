package store

import (
	"encoding/binary"
)

// Bucket names inside BBolt database
var (
	BucketJobs      = []byte("jobs")
	BucketIdxReady  = []byte("idx_ready")
	BucketIdxSched  = []byte("idx_scheduled")
	BucketIdxLeased = []byte("idx_leased")
	BucketIdxDead   = []byte("idx_dead")
	BucketIdxIdemp  = []byte("idx_idempotency")
	BucketMeta      = []byte("meta")
)

var (
	KeyLastAppliedNow = []byte("last_applied_now")
)

var AllBuckets = [][]byte{
	BucketJobs,
	BucketIdxReady,
	BucketIdxSched,
	BucketIdxLeased,
	BucketIdxDead,
	BucketIdxIdemp,
	BucketMeta,
}

// readyKey constructs: queue + \x00 + priority(1B) + created_at_be(8B) + job_id
// This guarantees that a cursor seek to (queue + \x00) finds the highest priority (0 <= 255)
// and earliest created job first, with zero table scans.
func readyKey(queue string, priority uint8, createdAt int64, jobID string) []byte {
	qBytes := []byte(queue)
	jBytes := []byte(jobID)
	key := make([]byte, len(qBytes)+1+1+8+len(jBytes))

	copy(key[0:], qBytes)
	key[len(qBytes)] = 0x00
	key[len(qBytes)+1] = priority
	binary.BigEndian.PutUint64(key[len(qBytes)+2:], uint64(createdAt))
	copy(key[len(qBytes)+10:], jBytes)
	return key
}

// readyPrefix returns the prefix: queue + \x00
func readyPrefix(queue string) []byte {
	prefix := make([]byte, len(queue)+1)
	copy(prefix, []byte(queue))
	prefix[len(queue)] = 0x00
	return prefix
}

// extractJobIDFromReadyKey extracts job_id from readyKey
func extractJobIDFromReadyKey(queueLen int, key []byte) string {
	return string(key[queueLen+10:])
}

// ScheduledKey constructs: run_at_be(8B) + \x00 + job_id (exported for testing)
func ScheduledKey(runAt int64, jobID string) []byte {
	return scheduledKey(runAt, jobID)
}

// scheduledKey constructs: run_at_be(8B) + \x00 + job_id
func scheduledKey(runAt int64, jobID string) []byte {
	jBytes := []byte(jobID)
	key := make([]byte, 8+1+len(jBytes))
	binary.BigEndian.PutUint64(key[0:], uint64(runAt))
	key[8] = 0x00
	copy(key[9:], jBytes)
	return key
}

// ExtractRunAtAndJobIDFromScheduledKey extracts run_at and job_id from scheduledKey
func ExtractRunAtAndJobIDFromScheduledKey(key []byte) (int64, string) {
	runAt := int64(binary.BigEndian.Uint64(key[0:8]))
	jobID := string(key[9:])
	return runAt, jobID
}

// leasedKey constructs: lease_expires_at_be(8B) + \x00 + job_id
func leasedKey(leaseExpiresAt int64, jobID string) []byte {
	jBytes := []byte(jobID)
	key := make([]byte, 8+1+len(jBytes))
	binary.BigEndian.PutUint64(key[0:], uint64(leaseExpiresAt))
	key[8] = 0x00
	copy(key[9:], jBytes)
	return key
}

// ExtractLeaseExpiresAtAndJobID extracts lease_expires_at and job_id from leasedKey
func ExtractLeaseExpiresAtAndJobID(key []byte) (int64, string) {
	expiresAt := int64(binary.BigEndian.Uint64(key[0:8]))
	jobID := string(key[9:])
	return expiresAt, jobID
}

func extractRunAtAndJobIDFromScheduledKey(key []byte) (int64, string) {
	return ExtractRunAtAndJobIDFromScheduledKey(key)
}

func extractLeaseExpiresAtAndJobID(key []byte) (int64, string) {
	return ExtractLeaseExpiresAtAndJobID(key)
}

// deadKey constructs: queue + \x00 + created_at_be(8B) + job_id
func deadKey(queue string, createdAt int64, jobID string) []byte {
	qBytes := []byte(queue)
	jBytes := []byte(jobID)
	key := make([]byte, len(qBytes)+1+8+len(jBytes))

	copy(key[0:], qBytes)
	key[len(qBytes)] = 0x00
	binary.BigEndian.PutUint64(key[len(qBytes)+1:], uint64(createdAt))
	copy(key[len(qBytes)+9:], jBytes)
	return key
}

// deadPrefix returns prefix: queue + \x00
func deadPrefix(queue string) []byte {
	prefix := make([]byte, len(queue)+1)
	copy(prefix, []byte(queue))
	prefix[len(queue)] = 0x00
	return prefix
}

// extractJobIDFromDeadKey extracts job_id from deadKey
func extractJobIDFromDeadKey(queueLen int, key []byte) string {
	return string(key[queueLen+9:])
}

// idempotencyKey constructs: queue + \x00 + idempotency_key
func idempotencyKey(queue, idempKey string) []byte {
	qBytes := []byte(queue)
	iBytes := []byte(idempKey)
	key := make([]byte, len(qBytes)+1+len(iBytes))
	copy(key[0:], qBytes)
	key[len(qBytes)] = 0x00
	copy(key[len(qBytes)+1:], iBytes)
	return key
}
