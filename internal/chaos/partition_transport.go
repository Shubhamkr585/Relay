package chaos

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

var ErrPartitioned = errors.New("network partition: packet dropped by chaos proxy")

// PartitionableTransport wraps a raft.Transport (such as a real TCP transport or loopback transport)
// to provide programmatic network partition simulation by dropping all incoming and outgoing RPCs.
type PartitionableTransport struct {
	underlying raft.Transport
	nodeID     raft.ServerID
	localAddr  raft.ServerAddress

	mu        sync.RWMutex
	isolated  bool
	dropPeers map[raft.ServerID]bool

	inCh   chan raft.RPC
	stopCh chan struct{}
}

// NewPartitionableTransport wraps an existing raft.Transport with partition fault injection.
func NewPartitionableTransport(nodeID raft.ServerID, underlying raft.Transport) *PartitionableTransport {
	pt := &PartitionableTransport{
		underlying: underlying,
		nodeID:     nodeID,
		localAddr:  underlying.LocalAddr(),
		dropPeers:  make(map[raft.ServerID]bool),
		inCh:       make(chan raft.RPC, 1024),
		stopCh:     make(chan struct{}),
	}

	// Intercept incoming RPCs from underlying consumer
	go pt.forwardConsumer()

	return pt
}

// forwardConsumer reads from the underlying Consumer channel and filters out dropped traffic.
func (pt *PartitionableTransport) forwardConsumer() {
	consumer := pt.underlying.Consumer()
	for {
		select {
		case <-pt.stopCh:
			return
		case rpc, ok := <-consumer:
			if !ok {
				return
			}

			pt.mu.RLock()
			isolated := pt.isolated
			pt.mu.RUnlock()

			if isolated {
				// Silently drop or reject incoming RPC
				rpc.Respond(nil, ErrPartitioned)
				continue
			}

			select {
			case pt.inCh <- rpc:
			case <-pt.stopCh:
				return
			}
		}
	}
}

// SetIsolated drops all incoming and outgoing RPCs for this transport.
func (pt *PartitionableTransport) SetIsolated(isolated bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.isolated = isolated
}

// IsIsolated returns whether this transport is currently dropping all traffic.
func (pt *PartitionableTransport) IsIsolated() bool {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.isolated
}

// Consumer returns the intercepted incoming RPC channel.
func (pt *PartitionableTransport) Consumer() <-chan raft.RPC {
	return pt.inCh
}

// LocalAddr returns the local address.
func (pt *PartitionableTransport) LocalAddr() raft.ServerAddress {
	return pt.localAddr
}

// AppendEntries sends an AppendEntries RPC unless partitioned.
func (pt *PartitionableTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	pt.mu.RLock()
	isolated := pt.isolated || pt.dropPeers[id]
	pt.mu.RUnlock()

	if isolated {
		return ErrPartitioned
	}
	return pt.underlying.AppendEntries(id, target, args, resp)
}

// AppendEntriesPipeline creates a pipeline unless partitioned.
func (pt *PartitionableTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	pt.mu.RLock()
	isolated := pt.isolated || pt.dropPeers[id]
	pt.mu.RUnlock()

	if isolated {
		return nil, ErrPartitioned
	}
	return pt.underlying.AppendEntriesPipeline(id, target)
}

// RequestVote sends a RequestVote RPC unless partitioned.
func (pt *PartitionableTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	pt.mu.RLock()
	isolated := pt.isolated || pt.dropPeers[id]
	pt.mu.RUnlock()

	if isolated {
		return ErrPartitioned
	}
	return pt.underlying.RequestVote(id, target, args, resp)
}

// InstallSnapshot sends a snapshot unless partitioned.
func (pt *PartitionableTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	pt.mu.RLock()
	isolated := pt.isolated || pt.dropPeers[id]
	pt.mu.RUnlock()

	if isolated {
		return ErrPartitioned
	}
	return pt.underlying.InstallSnapshot(id, target, args, resp, data)
}

// TimeoutNow sends TimeoutNow unless partitioned.
func (pt *PartitionableTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	pt.mu.RLock()
	isolated := pt.isolated || pt.dropPeers[id]
	pt.mu.RUnlock()

	if isolated {
		return ErrPartitioned
	}
	return pt.underlying.TimeoutNow(id, target, args, resp)
}

// EncodePeer delegates to the underlying transport.
func (pt *PartitionableTransport) EncodePeer(id raft.ServerID, p raft.ServerAddress) []byte {
	return pt.underlying.EncodePeer(id, p)
}

// DecodePeer delegates to the underlying transport.
func (pt *PartitionableTransport) DecodePeer(p []byte) raft.ServerAddress {
	return pt.underlying.DecodePeer(p)
}

// SetHeartbeatHandler delegates to the underlying transport.
func (pt *PartitionableTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	pt.underlying.SetHeartbeatHandler(func(rpc raft.RPC) {
		pt.mu.RLock()
		isolated := pt.isolated
		pt.mu.RUnlock()

		if isolated {
			rpc.Respond(nil, ErrPartitioned)
			return
		}
		if cb != nil {
			cb(rpc)
		}
	})
}

// Close stops the transport forwarder and closes the underlying transport.
func (pt *PartitionableTransport) Close() error {
	pt.mu.Lock()
	select {
	case <-pt.stopCh:
	default:
		close(pt.stopCh)
	}
	pt.mu.Unlock()

	if closer, ok := pt.underlying.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// NewRealTCPTransport creates a real TCP Raft transport wrapped in PartitionableTransport.
func NewRealTCPTransport(nodeID raft.ServerID, maxPool int, timeout time.Duration) (*PartitionableTransport, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("failed to bind ephemeral port: %w", err)
	}
	addr := l.Addr().(*net.TCPAddr)
	_ = l.Close()

	tcpTrans, err := raft.NewTCPTransport(
		addr.String(),
		addr,
		maxPool,
		timeout,
		io.Discard,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft TCP transport for %s: %w", nodeID, err)
	}

	return NewPartitionableTransport(nodeID, tcpTrans), nil
}
