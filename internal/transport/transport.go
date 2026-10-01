// Package transport carries Raft messages between nodes over gRPC.
//
// Each peer gets one long-lived client connection and one sender goroutine
// fed by a bounded queue. The node's event loop only ever does a non-blocking
// send into that queue, so a slow or unreachable peer costs the node nothing
// but dropped messages, which Raft is built to tolerate.
package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	raftv1 "github.com/bhattt1/RaftBasedDistributedKey/internal/gen/raft/v1"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
)

// Handler is the node as the transport sees it. *node.Node implements it.
type Handler interface {
	// Call delivers a request and returns the node's response.
	Call(ctx context.Context, m raft.Message) (raft.Message, error)
	// Step delivers the response to a request this node sent.
	Step(ctx context.Context, m raft.Message) error
	ReportSnapshot(peer string, ok bool)
	OpenSnapshotFile() (storage.SnapshotHeader, int64, io.ReadCloser, error)
	NewIncomingSnapshot() (*storage.IncomingSnapshot, error)
	InstallSnapshot(ctx context.Context, m raft.Message, in *storage.IncomingSnapshot) (raft.Message, error)
}

// Observer receives transport events for metrics.
type Observer interface {
	RPCFailed(rpc string)
	MessageDropped(reason string)
	SnapshotSent(bytes int64, d time.Duration, err error)
}

type nopObserver struct{}

func (nopObserver) RPCFailed(string)                         {}
func (nopObserver) MessageDropped(string)                    {}
func (nopObserver) SnapshotSent(int64, time.Duration, error) {}

// Config configures the transport.
type Config struct {
	ClusterID string
	ID        string
	// Peers maps every other node's ID to its Raft address.
	Peers map[string]string

	// ServerTLS and ClientTLS enable mutual TLS between peers when both are
	// set. ClientTLS is called with the ID of the peer being dialled so the
	// configuration can pin that peer's identity. Both nil means plaintext,
	// which is for local development only.
	ServerTLS *tls.Config
	ClientTLS func(peerID string) *tls.Config

	// RPCTimeout bounds one vote or append RPC.
	RPCTimeout time.Duration
	// SnapshotTimeout bounds one whole snapshot transfer.
	SnapshotTimeout time.Duration
	// SnapshotChunkBytes is the size of each streamed snapshot chunk.
	SnapshotChunkBytes int
	// MaxMessageBytes bounds a single gRPC message in either direction.
	MaxMessageBytes int
	// QueueSize bounds the messages waiting for each peer.
	QueueSize int

	Logger   *slog.Logger
	Observer Observer
}

func (c *Config) setDefaults() {
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = time.Second
	}
	if c.SnapshotTimeout <= 0 {
		c.SnapshotTimeout = 2 * time.Minute
	}
	if c.SnapshotChunkBytes <= 0 {
		c.SnapshotChunkBytes = 256 << 10
	}
	if c.MaxMessageBytes <= 0 {
		c.MaxMessageBytes = 8 << 20
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 64
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if c.Observer == nil {
		c.Observer = nopObserver{}
	}
}

type peerConn struct {
	id     string
	conn   *grpc.ClientConn
	client raftv1.RaftClient
	queue  chan raft.Message
	// sendingSnapshot allows one snapshot transfer to a peer at a time.
	sendingSnapshot atomic.Bool
}

// Transport implements node.Transport and serves the peer RPCs.
type Transport struct {
	raftv1.UnimplementedRaftServer

	cfg     Config
	log     *slog.Logger
	obs     Observer
	peers   map[string]*peerConn
	handler atomic.Pointer[Handler]
	server  *grpc.Server

	// receiving allows one incoming snapshot at a time.
	receiving atomic.Bool

	ctx       context.Context // cancelled by Close; parent of every RPC
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New creates the transport and its client connections. Connections are
// established lazily, in the background, with backoff; New does not block on
// any peer being reachable.
func New(cfg Config) (*Transport, error) {
	cfg.setDefaults()
	if (cfg.ServerTLS == nil) != (cfg.ClientTLS == nil) {
		return nil, errors.New("transport: ServerTLS and ClientTLS must be set together")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{cfg: cfg, log: cfg.Logger, obs: cfg.Observer, peers: map[string]*peerConn{}, ctx: ctx, cancel: cancel}

	for id, addr := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		creds := insecure.NewCredentials()
		if cfg.ClientTLS != nil {
			creds = credentials.NewTLS(cfg.ClientTLS(id))
		}
		// "passthrough" hands the address to the dialer untouched, so the
		// host name is looked up again on every connection attempt. gRPC's
		// default DNS resolver caches the answer and re-resolves at most
		// once every 30 seconds; a peer that restarts with a new IP address
		// (routine for containers) would be unreachable for that long, and
		// with it possibly the quorum. The five-node demo found exactly
		// that: three of five nodes up, and no leader for 26 seconds.
		conn, err := grpc.NewClient("passthrough:///"+addr,
			grpc.WithTransportCredentials(creds),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(cfg.MaxMessageBytes),
				grpc.MaxCallSendMsgSize(cfg.MaxMessageBytes),
			),
			// Reconnect attempts to a dead peer back off up to three
			// seconds. RPCs issued meanwhile fail immediately instead of
			// queueing, so a recovering peer is not met by a burst.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 3 * time.Second},
				MinConnectTimeout: 2 * time.Second,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
		)
		if err != nil {
			t.closeConns()
			cancel()
			return nil, fmt.Errorf("transport: creating client for %s (%s): %w", id, addr, err)
		}
		p := &peerConn{id: id, conn: conn, client: raftv1.NewRaftClient(conn), queue: make(chan raft.Message, cfg.QueueSize)}
		t.peers[id] = p
		t.wg.Add(1)
		go t.sendLoop(p)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxMessageBytes),
		grpc.MaxSendMsgSize(cfg.MaxMessageBytes),
		grpc.MaxConcurrentStreams(64),
		grpc.ConnectionTimeout(5 * time.Second),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	}
	if cfg.ServerTLS != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(cfg.ServerTLS)))
	}
	t.server = grpc.NewServer(opts...)
	raftv1.RegisterRaftServer(t.server, t)
	return t, nil
}

// Serve accepts peer connections on lis and hands requests to h. It returns
// when the transport is closed.
func (t *Transport) Serve(lis net.Listener, h Handler) error {
	t.handler.Store(&h)
	err := t.server.Serve(lis)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Close stops serving, stops the sender goroutines and closes connections.
func (t *Transport) Close() {
	t.closeOnce.Do(func() {
		t.cancel()
		// Give in-flight RPCs a moment to finish, then cut them off.
		stopped := make(chan struct{})
		go func() {
			t.server.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.server.Stop()
			<-stopped
		}
		t.wg.Wait()
		t.closeConns()
	})
}

func (t *Transport) closeConns() {
	for _, p := range t.peers {
		p.conn.Close()
	}
}

// ---- sending ---------------------------------------------------------------

// Send queues a message for its recipient. It never blocks.
func (t *Transport) Send(m raft.Message) {
	p, ok := t.peers[m.To]
	if !ok {
		return
	}
	if m.Type == raft.MsgSnap {
		t.startSnapshot(p, m)
		return
	}
	select {
	case p.queue <- m:
	default:
		// The peer is not keeping up. Dropping is safe: the leader resends
		// on the next heartbeat, and candidates retry on the next timeout.
		t.obs.MessageDropped("peer_queue_full")
	}
}

func (t *Transport) sendLoop(p *peerConn) {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case m := <-p.queue:
			t.sendOne(p, m)
		}
	}
}

func (t *Transport) sendOne(p *peerConn, m raft.Message) {
	h := t.handler.Load()
	if h == nil {
		return
	}
	ctx, cancel := context.WithTimeout(t.ctx, t.cfg.RPCTimeout)
	defer cancel()

	var resp raft.Message
	var err error
	var rpc string
	switch m.Type {
	case raft.MsgPreVote:
		rpc = "PreVote"
		var r *raftv1.VoteResponse
		if r, err = p.client.PreVote(ctx, voteRequest(t, m)); err == nil {
			resp = voteResponseMessage(m, r)
		}
	case raft.MsgVote:
		rpc = "RequestVote"
		var r *raftv1.VoteResponse
		if r, err = p.client.RequestVote(ctx, voteRequest(t, m)); err == nil {
			resp = voteResponseMessage(m, r)
		}
	case raft.MsgApp:
		rpc = "AppendEntries"
		var r *raftv1.AppendEntriesResponse
		if r, err = p.client.AppendEntries(ctx, appendRequest(t, m)); err == nil {
			resp = appendResponseMessage(m, r)
		}
	default:
		// Responses travel as RPC replies, never through Send.
		return
	}
	if err != nil {
		t.obs.RPCFailed(rpc)
		return
	}
	// A late response is still delivered; the core checks its term and
	// position and ignores it if it no longer applies.
	(*h).Step(ctx, resp)
}

// startSnapshot streams the newest snapshot to a peer in its own goroutine,
// so that a transfer lasting seconds does not hold up heartbeats to the same
// peer.
func (t *Transport) startSnapshot(p *peerConn, m raft.Message) {
	h := t.handler.Load()
	if h == nil {
		return
	}
	if !p.sendingSnapshot.CompareAndSwap(false, true) {
		return // one is already on its way and will report its own outcome
	}
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		defer p.sendingSnapshot.Store(false)
		start := time.Now()
		sent, err := t.sendSnapshot(p, m, *h)
		t.obs.SnapshotSent(sent, time.Since(start), err)
		if err != nil {
			t.obs.RPCFailed("InstallSnapshot")
			t.log.Warn("sending snapshot failed", "peer", p.id, "error", err)
			(*h).ReportSnapshot(p.id, false)
			return
		}
		t.log.Info("snapshot sent", "peer", p.id, "bytes", sent, "took", time.Since(start))
	}()
}

func (t *Transport) sendSnapshot(p *peerConn, m raft.Message, h Handler) (int64, error) {
	_, size, rc, err := h.OpenSnapshotFile()
	if err != nil {
		return 0, fmt.Errorf("opening snapshot: %w", err)
	}
	defer rc.Close()

	ctx, cancel := context.WithTimeout(t.ctx, t.cfg.SnapshotTimeout)
	defer cancel()
	stream, err := p.client.InstallSnapshot(ctx)
	if err != nil {
		return 0, err
	}
	// The file is read and sent one chunk at a time; it is never held in
	// memory whole.
	buf := make([]byte, t.cfg.SnapshotChunkBytes)
	var sent int64
	first := true
	for {
		n, rerr := io.ReadFull(rc, buf)
		if n > 0 || first {
			chunk := &raftv1.SnapshotChunk{Data: buf[:n]}
			if first {
				chunk.Header, chunk.TotalSize = t.header(m), uint64(size)
				first = false
			}
			if err := stream.Send(chunk); err != nil {
				// The real reason is on the receive side of the stream.
				if _, rerr := stream.CloseAndRecv(); rerr != nil {
					return sent, rerr
				}
				return sent, err
			}
			sent += int64(n)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return sent, fmt.Errorf("reading snapshot: %w", rerr)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return sent, err
	}
	msg := snapshotResponseMessage(m, resp)
	if err := h.Step(ctx, msg); err != nil {
		return sent, err
	}
	if msg.Reject {
		return sent, errors.New("follower rejected the snapshot")
	}
	return sent, nil
}

// ---- receiving -------------------------------------------------------------

// check validates the header of an incoming request. Requests from another
// cluster, addressed to another node, or from a node outside the configured
// voter set are refused before they reach Raft. With mutual TLS the sender's
// claimed ID must also match the identity in its certificate.
func (t *Transport) check(ctx context.Context, h *raftv1.Header) (Handler, error) {
	hp := t.handler.Load()
	if hp == nil {
		return nil, status.Error(codes.Unavailable, "node is starting")
	}
	switch {
	case h == nil:
		return nil, status.Error(codes.InvalidArgument, "missing header")
	case h.GetClusterId() != t.cfg.ClusterID:
		return nil, status.Errorf(codes.FailedPrecondition, "request is for cluster %q, this node belongs to %q", h.GetClusterId(), t.cfg.ClusterID)
	case h.GetTo() != t.cfg.ID:
		return nil, status.Errorf(codes.FailedPrecondition, "request is addressed to %q, this node is %q", h.GetTo(), t.cfg.ID)
	}
	if _, ok := t.peers[h.GetFrom()]; !ok {
		return nil, status.Errorf(codes.PermissionDenied, "sender %q is not a member of this cluster", h.GetFrom())
	}
	if t.cfg.ServerTLS != nil {
		id, err := certificateIdentity(ctx)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		if id != h.GetFrom() {
			return nil, status.Errorf(codes.PermissionDenied, "certificate identifies %q but the request claims to be from %q", id, h.GetFrom())
		}
	}
	return *hp, nil
}

// certificateIdentity returns the node ID in the verified client certificate.
func certificateIdentity(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("no peer information")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return "", errors.New("no verified client certificate")
	}
	return info.State.VerifiedChains[0][0].Subject.CommonName, nil
}

func nodeError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

func (t *Transport) vote(ctx context.Context, typ raft.MessageType, req *raftv1.VoteRequest) (*raftv1.VoteResponse, error) {
	h, err := t.check(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	resp, err := h.Call(ctx, voteMessage(typ, req))
	if err != nil {
		return nil, nodeError(err)
	}
	return &raftv1.VoteResponse{Header: t.respHeader(resp), Granted: !resp.Reject}, nil
}

func (t *Transport) PreVote(ctx context.Context, req *raftv1.VoteRequest) (*raftv1.VoteResponse, error) {
	return t.vote(ctx, raft.MsgPreVote, req)
}

func (t *Transport) RequestVote(ctx context.Context, req *raftv1.VoteRequest) (*raftv1.VoteResponse, error) {
	return t.vote(ctx, raft.MsgVote, req)
}

func (t *Transport) AppendEntries(ctx context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	h, err := t.check(ctx, req.GetHeader())
	if err != nil {
		return nil, err
	}
	m, err := appendMessage(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	resp, err := h.Call(ctx, m)
	if err != nil {
		return nil, nodeError(err)
	}
	return &raftv1.AppendEntriesResponse{
		Header:        t.respHeader(resp),
		Success:       !resp.Reject,
		MatchIndex:    resp.MatchIndex,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

func (t *Transport) InstallSnapshot(stream grpc.ClientStreamingServer[raftv1.SnapshotChunk, raftv1.InstallSnapshotResponse]) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h, err := t.check(ctx, first.GetHeader())
	if err != nil {
		return err
	}
	if !t.receiving.CompareAndSwap(false, true) {
		return status.Error(codes.ResourceExhausted, "another snapshot is being received")
	}
	defer t.receiving.Store(false)

	in, err := h.NewIncomingSnapshot()
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	// Chunks go straight to a temporary file. The store enforces the size
	// limit as bytes arrive and verifies checksums and cluster identity at
	// the end; nothing is trusted or installed before that.
	for chunk := first; ; {
		if _, err := in.Write(chunk.GetData()); err != nil {
			in.Discard()
			return status.Error(codes.InvalidArgument, err.Error())
		}
		chunk, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			in.Discard() // interrupted transfer: remove the partial file
			return err
		}
	}
	meta, err := in.Finish()
	if err != nil {
		in.Discard()
		return status.Error(codes.InvalidArgument, err.Error())
	}
	hdr := first.GetHeader()
	m := raft.Message{Type: raft.MsgSnap, From: hdr.GetFrom(), To: hdr.GetTo(), Term: hdr.GetTerm(), SnapIndex: meta.Index, SnapTerm: meta.Term}
	resp, err := h.InstallSnapshot(ctx, m, in)
	if err != nil {
		return nodeError(err)
	}
	return stream.SendAndClose(&raftv1.InstallSnapshotResponse{Header: t.respHeader(resp), Success: !resp.Reject, MatchIndex: resp.MatchIndex})
}
