// Package memnet is an in-process network for running several nodes in one
// test binary. It delivers Raft messages between nodes over channels and can
// partition, block individual directions, and lose messages on demand.
//
// It carries requests and responses as independent one-way messages, which
// is a harsher model than the real gRPC transport: a response can be lost or
// delayed independently of its request.
package memnet

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
)

// Endpoint is the part of a node the network needs. *node.Node implements it.
type Endpoint interface {
	Step(ctx context.Context, m raft.Message) error
	ReportSnapshot(peer string, ok bool)
	OpenSnapshotFile() (storage.SnapshotHeader, int64, io.ReadCloser, error)
	NewIncomingSnapshot() (*storage.IncomingSnapshot, error)
	InstallSnapshot(ctx context.Context, m raft.Message, in *storage.IncomingSnapshot) (raft.Message, error)
}

type link struct{ from, to string }

// Network connects endpoints by ID.
type Network struct {
	mu       sync.Mutex
	rng      *rand.Rand
	nodes    map[string]Endpoint
	blocked  map[link]bool
	queues   map[link]chan raft.Message
	dropRate float64
	maxDelay time.Duration
	closed   bool
	wg       sync.WaitGroup
	stop     chan struct{}
}

// New returns an empty network.
func New(seed int64) *Network {
	return &Network{
		rng:     rand.New(rand.NewSource(seed)),
		nodes:   map[string]Endpoint{},
		blocked: map[link]bool{},
		queues:  map[link]chan raft.Message{},
		stop:    make(chan struct{}),
	}
}

// Attach makes an endpoint reachable under id.
func (nw *Network) Attach(id string, ep Endpoint) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[id] = ep
}

// Detach makes id unreachable, as if its process had exited.
func (nw *Network) Detach(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.nodes, id)
}

// Partition splits the network into groups. A node can reach only nodes in
// its own group; a node in no group is isolated.
func (nw *Network) Partition(all []string, groups ...[]string) {
	group := map[string]int{}
	for i, g := range groups {
		for _, id := range g {
			group[id] = i + 1
		}
	}
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for _, a := range all {
		for _, b := range all {
			nw.blocked[link{a, b}] = a != b && (group[a] == 0 || group[a] != group[b])
		}
	}
}

// BlockLink drops messages travelling from one node to another, in that
// direction only.
func (nw *Network) BlockLink(from, to string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.blocked[link{from, to}] = true
}

// Heal removes every partition and blocked link.
func (nw *Network) Heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.blocked = map[link]bool{}
}

// SetFaults makes the network lose a fraction of messages and delay each one
// by up to maxDelay.
func (nw *Network) SetFaults(dropRate float64, maxDelay time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.dropRate, nw.maxDelay = dropRate, maxDelay
}

// Close stops all delivery goroutines and waits for them.
func (nw *Network) Close() {
	nw.mu.Lock()
	if !nw.closed {
		nw.closed = true
		close(nw.stop)
	}
	nw.mu.Unlock()
	nw.wg.Wait()
}

// Port is one node's view of the network. It implements node.Transport.
type Port struct {
	nw *Network
	id string
}

// Port returns the transport for the node with the given id.
func (nw *Network) Port(id string) *Port { return &Port{nw: nw, id: id} }

// Send queues a message. It never blocks: when the queue to a peer is full
// the message is dropped, as a real transport under back-pressure would.
func (p *Port) Send(m raft.Message) {
	nw := p.nw
	l := link{p.id, m.To}
	nw.mu.Lock()
	if nw.closed {
		nw.mu.Unlock()
		return
	}
	q, ok := nw.queues[l]
	if !ok {
		q = make(chan raft.Message, 256)
		nw.queues[l] = q
		nw.wg.Add(1)
		go nw.deliverLoop(l, q)
	}
	nw.mu.Unlock()
	select {
	case q <- m:
	default:
		nw.failed(m)
	}
}

func (nw *Network) deliverLoop(l link, q chan raft.Message) {
	defer nw.wg.Done()
	for {
		select {
		case <-nw.stop:
			return
		case m := <-q:
			nw.deliver(l, m)
		}
	}
}

// reachable reports the endpoint for a link if a message may pass right now,
// and applies random loss and delay.
func (nw *Network) reachable(l link) (Endpoint, time.Duration, bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	ep, ok := nw.nodes[l.to]
	if !ok || nw.blocked[l] {
		return nil, 0, false
	}
	if nw.dropRate > 0 && nw.rng.Float64() < nw.dropRate {
		return nil, 0, false
	}
	var delay time.Duration
	if nw.maxDelay > 0 {
		delay = time.Duration(nw.rng.Int63n(int64(nw.maxDelay)))
	}
	return ep, delay, true
}

func (nw *Network) deliver(l link, m raft.Message) {
	dst, delay, ok := nw.reachable(l)
	if !ok {
		nw.failed(m)
		return
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-nw.stop:
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if m.Type != raft.MsgSnap {
		dst.Step(ctx, m)
		return
	}
	nw.sendSnapshot(ctx, l, m, dst)
}

// sendSnapshot copies the sender's newest snapshot file to the receiver in
// chunks, the way the gRPC transport streams it.
func (nw *Network) sendSnapshot(ctx context.Context, l link, m raft.Message, dst Endpoint) {
	nw.mu.Lock()
	src := nw.nodes[l.from]
	nw.mu.Unlock()
	if src == nil {
		return
	}
	resp, err := func() (raft.Message, error) {
		h, _, rc, err := src.OpenSnapshotFile()
		if err != nil {
			return raft.Message{}, err
		}
		defer rc.Close()
		in, err := dst.NewIncomingSnapshot()
		if err != nil {
			return raft.Message{}, err
		}
		if _, err := io.CopyBuffer(in, rc, make([]byte, 4096)); err != nil {
			in.Discard()
			return raft.Message{}, err
		}
		meta, err := in.Finish()
		if err != nil {
			in.Discard()
			return raft.Message{}, err
		}
		if meta != h.Meta {
			in.Discard()
			return raft.Message{}, io.ErrUnexpectedEOF
		}
		m.SnapIndex, m.SnapTerm = meta.Index, meta.Term
		return dst.InstallSnapshot(ctx, m, in)
	}()
	if err != nil {
		src.ReportSnapshot(l.to, false)
		return
	}
	// The response travels back over the reverse link and can be lost too.
	if _, _, ok := nw.reachable(link{l.to, l.from}); ok {
		src.Step(ctx, resp)
	} else {
		src.ReportSnapshot(l.to, false)
	}
}

// failed tells the sender about a snapshot that could not be delivered, as a
// failed RPC would.
func (nw *Network) failed(m raft.Message) {
	if m.Type != raft.MsgSnap {
		return
	}
	nw.mu.Lock()
	src := nw.nodes[m.From]
	nw.mu.Unlock()
	if src != nil {
		src.ReportSnapshot(m.To, false)
	}
}
