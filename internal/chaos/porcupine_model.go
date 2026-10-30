package chaos

import (
	"fmt"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// QueueOpType defines the queue operation type for linearizability checking.
type QueueOpType int

const (
	OpEnqueue QueueOpType = iota
	OpReserve
	OpAck
)

func (t QueueOpType) String() string {
	switch t {
	case OpEnqueue:
		return "Enqueue"
	case OpReserve:
		return "Reserve"
	case OpAck:
		return "Ack"
	default:
		return "Unknown"
	}
}

// QueueJobState tracks a job's sequential lifecycle state.
type QueueJobState int

const (
	JobStatePending QueueJobState = iota + 1
	JobStateLeased
	JobStateDone
)

// QueueInput represents operation inputs to the queue.
type QueueInput struct {
	Op         QueueOpType
	Queue      string
	Key        string // Unique job key / idempotency key
	JobID      string
	WorkerID   string
	LeaseToken string
}

// QueueOutput represents operation outputs returned from the queue.
type QueueOutput struct {
	Ok         bool
	Key        string // Unique job key / idempotency key
	JobID      string
	LeaseToken string
	TimedOut   bool
}

// SequentialQueueState is the state for a single queue partition in Porcupine.
type SequentialQueueState struct {
	Jobs map[string]QueueJobState
}

// clone creates a deep copy of the sequential state to maintain functional purity.
func (s SequentialQueueState) clone() SequentialQueueState {
	cloned := SequentialQueueState{
		Jobs: make(map[string]QueueJobState, len(s.Jobs)),
	}
	for k, v := range s.Jobs {
		cloned.Jobs[k] = v
	}
	return cloned
}

// QueueModel provides the Porcupine sequential model for Relay queue operations
// assuming long leases where lease expiration does not occur.
var QueueModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, op := range history {
			input := op.Input.(QueueInput)
			m[input.Queue] = append(m[input.Queue], op)
		}
		var partitions [][]porcupine.Operation
		for _, ops := range m {
			partitions = append(partitions, ops)
		}
		return partitions
	},

	Init: func() interface{} {
		return SequentialQueueState{
			Jobs: make(map[string]QueueJobState),
		}
	},

	Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
		st := state.(SequentialQueueState)
		in := input.(QueueInput)
		out := output.(QueueOutput)

		switch in.Op {
		case OpEnqueue:
			if out.TimedOut {
				// Indeterminate: either succeeded or failed.
				// If not enqueued yet, it could have taken effect.
				if _, exists := st.Jobs[in.Key]; !exists {
					next := st.clone()
					next.Jobs[in.Key] = JobStatePending
					return true, next
				}
				return true, st
			}
			if out.Ok {
				// Must not duplicate an active job
				if _, exists := st.Jobs[in.Key]; exists {
					return false, st
				}
				next := st.clone()
				next.Jobs[in.Key] = JobStatePending
				return true, next
			}
			// Failed enqueue without timeout
			return true, st

		case OpReserve:
			if out.TimedOut {
				return true, st
			}
			if out.Ok && out.Key != "" {
				// The reserved job must be currently in JobStatePending
				currentState, exists := st.Jobs[out.Key]
				if !exists || currentState != JobStatePending {
					return false, st
				}
				next := st.clone()
				next.Jobs[out.Key] = JobStateLeased
				return true, next
			}
			if !out.Ok {
				// Empty queue return is valid if no jobs are pending
				hasPending := false
				for _, state := range st.Jobs {
					if state == JobStatePending {
						hasPending = true
						break
					}
				}
				if !hasPending {
					return true, st
				}
				// If there were pending jobs, returning empty is invalid in sequential queue
				return false, st
			}
			return true, st

		case OpAck:
			if out.TimedOut {
				if currentState, exists := st.Jobs[in.Key]; exists && currentState == JobStateLeased {
					next := st.clone()
					next.Jobs[in.Key] = JobStateDone
					return true, next
				}
				return true, st
			}
			if out.Ok {
				// The job must currently be leased
				currentState, exists := st.Jobs[in.Key]
				if !exists || currentState != JobStateLeased {
					return false, st
				}
				next := st.clone()
				next.Jobs[in.Key] = JobStateDone
				return true, next
			}
			return true, st

		default:
			return false, st
		}
	},

	Equal: func(s1, s2 interface{}) bool {
		st1 := s1.(SequentialQueueState)
		st2 := s2.(SequentialQueueState)
		if len(st1.Jobs) != len(st2.Jobs) {
			return false
		}
		for k, v1 := range st1.Jobs {
			if v2, ok := st2.Jobs[k]; !ok || v1 != v2 {
				return false
			}
		}
		return true
	},

	DescribeOperation: func(input interface{}, output interface{}) string {
		in := input.(QueueInput)
		out := output.(QueueOutput)
		return fmt.Sprintf("%s(key=%s, job=%s, token=%s) -> (ok=%v, key=%s, job=%s, timedOut=%v)",
			in.Op, in.Key, in.JobID, in.LeaseToken, out.Ok, out.Key, out.JobID, out.TimedOut)
	},
}

type inFlightOp struct {
	clientID int
	input    QueueInput
	call     int64
}

// LinearizabilityRecorder records concurrent client invocations for Porcupine verification.
type LinearizabilityRecorder struct {
	mu         sync.Mutex
	nextID     int64
	inFlight   map[int64]inFlightOp
	operations []porcupine.Operation
}

// NewLinearizabilityRecorder creates a thread-safe operation history recorder.
func NewLinearizabilityRecorder() *LinearizabilityRecorder {
	return &LinearizabilityRecorder{
		inFlight:   make(map[int64]inFlightOp),
		operations: make([]porcupine.Operation, 0, 1000),
	}
}

// RecordInvocation marks the start time of a client request.
func (r *LinearizabilityRecorder) RecordInvocation(clientID int, input QueueInput) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	opID := r.nextID
	r.nextID++
	r.inFlight[opID] = inFlightOp{
		clientID: clientID,
		input:    input,
		call:     time.Now().UnixNano(),
	}
	return opID
}

// RecordReturn marks the completion time of a client request and appends to the history.
func (r *LinearizabilityRecorder) RecordReturn(opID int64, output QueueOutput) {
	r.mu.Lock()
	defer r.mu.Unlock()

	inFlight, ok := r.inFlight[opID]
	if !ok {
		return
	}
	delete(r.inFlight, opID)

	r.operations = append(r.operations, porcupine.Operation{
		ClientId: inFlight.clientID,
		Input:    inFlight.input,
		Call:     inFlight.call,
		Output:   output,
		Return:   time.Now().UnixNano(),
	})
}

// CheckLinearizability checks if the recorded history is linearizable with Porcupine.
func (r *LinearizabilityRecorder) CheckLinearizability() bool {
	r.mu.Lock()
	history := make([]porcupine.Operation, len(r.operations))
	copy(history, r.operations)
	r.mu.Unlock()

	return porcupine.CheckOperations(QueueModel, history)
}

// OperationsCount returns the number of completed operations in history.
func (r *LinearizabilityRecorder) OperationsCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.operations)
}

