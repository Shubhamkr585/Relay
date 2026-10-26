package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	relaypb "github.com/shubhamkr585/relay/api/proto/v1"
)

const HeaderForwardedBy = "x-forwarded-by"

// PeerResolver maps a Raft leader ID and Raft address to its corresponding gRPC address.
type PeerResolver func(leaderID raft.ServerID, leaderRaftAddr raft.ServerAddress) (string, error)

// Forwarder manages transparent forwarding of mutating RPCs to the Raft leader.
type Forwarder struct {
	nodeID   string
	raft     *raft.Raft
	resolver PeerResolver

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewForwarder creates a new Forwarder instance.
func NewForwarder(nodeID string, r *raft.Raft, resolver PeerResolver) *Forwarder {
	return &Forwarder{
		nodeID:   nodeID,
		raft:     r,
		resolver: resolver,
		conns:    make(map[string]*grpc.ClientConn),
	}
}

// CheckLoopGuard checks if the request has already been forwarded once.
func (f *Forwarder) CheckLoopGuard(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if vals := md.Get(HeaderForwardedBy); len(vals) > 0 {
			return status.Errorf(codes.Unavailable, "forwarding loop detected: already forwarded by %s", vals[0])
		}
	}
	return nil
}

// getLeaderClient resolves the current leader's gRPC address and returns a gRPC client, leader address, and context.
func (f *Forwarder) getLeaderClient(ctx context.Context) (relaypb.RelayServiceClient, string, context.Context, error) {
	if err := f.CheckLoopGuard(ctx); err != nil {
		return nil, "", nil, err
	}

	leaderAddr, leaderID := f.raft.LeaderWithID()
	if leaderAddr == "" {
		return nil, "", nil, status.Error(codes.Unavailable, "no Raft leader currently elected")
	}

	var grpcAddr string
	if f.resolver != nil {
		var err error
		grpcAddr, err = f.resolver(leaderID, leaderAddr)
		if err != nil {
			return nil, "", nil, status.Errorf(codes.Unavailable, "failed to resolve leader gRPC address: %v", err)
		}
	} else {
		grpcAddr = string(leaderAddr)
	}

	conn, err := f.getOrCreateConn(grpcAddr)
	if err != nil {
		return nil, "", nil, status.Errorf(codes.Unavailable, "failed to connect to leader: %v", err)
	}

	// Propagate loop guard metadata
	outMD := metadata.Pairs(HeaderForwardedBy, f.nodeID)
	forwardCtx := metadata.NewOutgoingContext(ctx, outMD)

	return relaypb.NewRelayServiceClient(conn), grpcAddr, forwardCtx, nil
}

func (f *Forwarder) removeConn(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if conn, ok := f.conns[addr]; ok {
		_ = conn.Close()
		delete(f.conns, addr)
	}
}

func (f *Forwarder) getOrCreateConn(addr string) (*grpc.ClientConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if conn, ok := f.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	f.conns[addr] = conn
	return conn, nil
}

// ForwardEnqueue forwards an Enqueue request to the leader.
func (f *Forwarder) ForwardEnqueue(ctx context.Context, req *relaypb.EnqueueRequest) (*relaypb.EnqueueResponse, error) {
	client, addr, fCtx, err := f.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	res, err := client.Enqueue(fCtx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		f.removeConn(addr)
	}
	return res, err
}

// ForwardReserve forwards a Reserve request to the leader.
func (f *Forwarder) ForwardReserve(ctx context.Context, req *relaypb.ReserveRequest) (*relaypb.ReserveResponse, error) {
	client, addr, fCtx, err := f.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	res, err := client.Reserve(fCtx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		f.removeConn(addr)
	}
	return res, err
}

// ForwardAck forwards an Ack request to the leader.
func (f *Forwarder) ForwardAck(ctx context.Context, req *relaypb.AckRequest) (*relaypb.AckResponse, error) {
	client, addr, fCtx, err := f.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	res, err := client.Ack(fCtx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		f.removeConn(addr)
	}
	return res, err
}

// ForwardNack forwards a Nack request to the leader.
func (f *Forwarder) ForwardNack(ctx context.Context, req *relaypb.NackRequest) (*relaypb.NackResponse, error) {
	client, addr, fCtx, err := f.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	res, err := client.Nack(fCtx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		f.removeConn(addr)
	}
	return res, err
}

// ForwardExtendLease forwards an ExtendLease request to the leader.
func (f *Forwarder) ForwardExtendLease(ctx context.Context, req *relaypb.ExtendLeaseRequest) (*relaypb.ExtendLeaseResponse, error) {
	client, addr, fCtx, err := f.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	res, err := client.ExtendLease(fCtx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		f.removeConn(addr)
	}
	return res, err
}

// ForwardRequeueDead forwards a RequeueDead request to the leader.
func (f *Forwarder) ForwardRequeueDead(ctx context.Context, req *relaypb.RequeueDeadRequest) (*relaypb.RequeueDeadResponse, error) {
	client, addr, fCtx, err := f.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	res, err := client.RequeueDead(fCtx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		f.removeConn(addr)
	}
	return res, err
}

// ForwardJoin forwards a Join request to the leader.
func (f *Forwarder) ForwardJoin(ctx context.Context, req *relaypb.JoinRequest) (*relaypb.JoinResponse, error) {
	client, addr, fCtx, err := f.getLeaderClient(ctx)
	if err != nil {
		return nil, err
	}
	res, err := client.Join(fCtx, req)
	if err != nil && status.Code(err) == codes.Unavailable {
		f.removeConn(addr)
	}
	return res, err
}

// Close closes all cached client connections.
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for addr, conn := range f.conns {
		_ = conn.Close()
		delete(f.conns, addr)
	}
	return nil
}
