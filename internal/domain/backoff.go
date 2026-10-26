package domain

import (
	"crypto/rand"
	"encoding/binary"
	"hash/fnv"
	"math/big"
	"time"
)

const (
	DefaultBaseDelay = 100 * time.Millisecond
	DefaultMaxDelay  = 10 * time.Minute
)

// DeterministicJitter computes pseudo-random jitter derived strictly from FNV-1a hash of (jobID, attempt).
// Jitter is bounded in [0, base) to guarantee 100% determinism across all Raft nodes without any PRNG.
func DeterministicJitter(jobID string, attempt uint32, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(jobID))
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], attempt)
	_, _ = h.Write(buf[:])
	sum := h.Sum64()
	return time.Duration(sum % uint64(base))
}

// CalculateBackoff computes exponential backoff with jitter:
// base * 2^attempt + jitter.
func CalculateBackoff(attempt uint32, base time.Duration, maxDelay time.Duration, jitter time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultBaseDelay
	}
	if maxDelay <= 0 {
		maxDelay = DefaultMaxDelay
	}

	// Guard against shift overflow when attempts is large
	shift := attempt
	if shift > 30 {
		shift = 30
	}

	multiplier := int64(1) << shift
	exponential := base * time.Duration(multiplier)

	// Clamp to maxDelay
	if exponential > maxDelay || exponential < 0 {
		exponential = maxDelay
	}

	delay := exponential + jitter
	if delay > maxDelay || delay < 0 {
		delay = maxDelay
	}
	return delay
}

// CalculateDeterministicBackoff calculates backoff with deterministic jitter derived from jobID and attempt.
func CalculateDeterministicBackoff(jobID string, attempt uint32, base time.Duration, maxDelay time.Duration) time.Duration {
	jitter := DeterministicJitter(jobID, attempt, base)
	return CalculateBackoff(attempt, base, maxDelay, jitter)
}

// CalculateBackoffWithRandom uses cryptographic random jitter in the range [0, base].
// Maintained for backward compatibility with single-node tests.
func CalculateBackoffWithRandom(attempt uint32, base time.Duration, maxDelay time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultBaseDelay
	}

	var jitter time.Duration
	if base > 0 {
		nBig, err := rand.Int(rand.Reader, big.NewInt(int64(base)))
		if err == nil {
			jitter = time.Duration(nBig.Int64())
		}
	}

	return CalculateBackoff(attempt, base, maxDelay, jitter)
}
