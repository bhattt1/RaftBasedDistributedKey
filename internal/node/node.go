// Package node runs one server: it owns the Raft state machine, the
// write-ahead log and the key-value store, and connects them to the network
// and to clients.
//
// Ownership is the central design decision. One goroutine, the event loop in
// run, is the only code that touches the Raft core, the WAL writer, the
// key-value store and the table of waiting clients. Everything else talks to
// it through bounded channels. There are no locks around consensus state
// because nothing shares it.
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
)

// Transport sends Raft messages to peers.
type Transport interface {
	// Send queues a message for delivery. It must not block: the event loop
	// calls it, and a slow or dead peer must not stall the node. Dropping
	// the message is always acceptable; Raft retries.
	Send(m raft.Message)
}

// Observer receives events for metrics. All methods must be cheap and safe
// to call from any goroutine.
type Observer interface {
	ProposalDone(outcome string, d time.Duration)
	Applied(entries int, d time.Duration)
	LeaderChanged()
	SnapshotCreated(bytes int, d time.Duration, err error)
	SnapshotInstalled()
	Rejected(reason string)
}

type nopObserver struct{}

func (nopObserver) ProposalDone(string, time.Duration)        {}
func (nopObserver) Applied(int, time.Duration)                {}
func (nopObserver) LeaderChanged()                            {}
func (nopObserver) SnapshotCreated(int, time.Duration, error) {}
func (nopObserver) SnapshotInstalled()                        {}
func (nopObserver) Rejected(string)                           {}

// Config configures a node.
type Config struct {
	ID    string
	Peers []string // every voter, including ID

	// TickInterval is the length of one Raft tick.
	TickInterval time.Duration
	// ElectionTicks and HeartbeatTicks are in ticks. A follower starts an
	// election after between ElectionTicks and 2*ElectionTicks ticks without
	// hearing from a leader.
	ElectionTicks  int
	HeartbeatTicks int
	// DisablePreVote and DisableCheckQuorum exist for tests that need to
	// show what those mechanisms are for.
	DisablePreVote     bool
	DisableCheckQuorum bool

	// MaxPendingProposals bounds both the queue of proposals waiting for
	// the event loop and the number of proposals waiting to commit.
	MaxPendingProposals int
	// MaxInbox bounds queued peer messages.
	MaxInbox int
	// MaxBatch bounds how many queued inputs are handled before the loop
	// persists and sends. Everything handled in one round shares one fsync.
	MaxBatch int

	// SnapshotThreshold is how many entries are applied between snapshots.
	SnapshotThreshold uint64
	// SnapshotBytes also triggers a snapshot once the entries applied since
	// the last one add up to this many bytes. Entries stay in memory until a
	// snapshot lets them go, and an entry can be up to 64 KiB, so a count
	// alone would not bound the memory the log uses.
	SnapshotBytes int64
	// SnapshotTrailing entries stay in the in-memory log behind a snapshot
	// so that a slightly lagging follower can catch up without one.
	SnapshotTrailing uint64

	// Limits for the key-value store. Zero means kv.DefaultLimits. Tests
	// set smaller limits; servers must all use the same ones.
	Limits kv.Limits

	Logger   *slog.Logger
	Observer Observer
	// Seed for election timeout randomisation. Zero picks one from the
	// clock.
	Seed int64
}

func (c *Config) setDefaults() {
	if c.TickInterval <= 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.ElectionTicks <= 0 {
		c.ElectionTicks = 10
	}
	if c.HeartbeatTicks <= 0 {
		c.HeartbeatTicks = 2
	}
	if c.MaxPendingProposals <= 0 {
		c.MaxPendingProposals = 1024
	}
	if c.MaxInbox <= 0 {
		c.MaxInbox = 1024
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 256
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 10000
	}
	if c.SnapshotBytes <= 0 {
		c.SnapshotBytes = 64 << 20
	}
	if c.Limits == (kv.Limits{}) {
		c.Limits = kv.DefaultLimits
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if c.Observer == nil {
		c.Observer = nopObserver{}
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
}

// Errors returned by Propose. The comments say what each one tells the
// client about whether its command took effect; that is the part a client
// must get right.
var (
	// ErrOverloaded: the node is not accepting more work right now. The
	// command was not submitted and did not take effect.
	ErrOverloaded = errors.New("node: overloaded")
	// ErrProposalDropped: the command was placed in this node's log but a
	// different entry was committed at that position after a leader change.
	// The command did not take effect through this proposal.
	ErrProposalDropped = errors.New("node: leadership lost before the command committed")
	// ErrOutcomeUnknown: the caller stopped waiting (deadline, cancellation
	// or shutdown) after the command had been handed to the node. The
	// command may still commit later.
	ErrOutcomeUnknown = errors.New("node: outcome unknown")
	// ErrStopped: the node is shut down and takes no new commands.
	ErrStopped = errors.New("node: stopped")
)

// NotLeaderError means this node is not the leader. The command was not
// submitted. Leader is this node's best guess at who is, and may be empty
// or out of date.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "node: not the leader, and no leader is known"
	}
	return "node: not the leader; try " + e.Leader
}

// Status is a snapshot of a node's state for diagnostics and health checks.
type Status struct {
	raft.Status
	// SnapshotIndex is the log index covered by the newest snapshot.
	SnapshotIndex uint64
	Keys          int
	StateBytes    int64
	Sessions      int
	Pending       int
	// Failed is set after a storage failure; the node has stopped.
	Failed error
}

type proposal struct {
	data []byte
	done chan proposalResult // buffered, so the loop never blocks on it
}

type proposalResult struct {
	res kv.Result
	err error
}

// waiter is a client waiting for the entry at some log index. It is completed
// only by the entry with the same term: the pair (index, term) identifies one
// specific entry, whereas the index alone may later hold another command.
type waiter struct {
	term uint64
	done chan proposalResult
}

type inbound struct {
	msg   raft.Message
	reply chan raft.Message         // nil for one-way delivery
	snap  *storage.IncomingSnapshot // set for MsgSnap
}

type snapshotReport struct {
	peer string
	ok   bool
}

type snapshotDone struct {
	meta  raft.SnapshotMeta
	bytes int
	took  time.Duration
	err   error
}

// Node is a running server.
type Node struct {
	cfg       Config
	log       *slog.Logger
	obs       Observer
	store     *storage.Store
	transport Transport
	peers     map[string]bool

	// Channels into the event loop.
	proposeC  chan *proposal
	inboxC    chan inbound
	reportC   chan snapshotReport
	snapDoneC chan snapshotDone
	stopC     chan struct{}
	done      chan struct{} // closed when the loop has exited
	stopOnce  sync.Once
	bg        sync.WaitGroup // background snapshot writers

	statusMu sync.RWMutex
	status   Status

	// Everything below is owned by the event loop goroutine.
	core         *raft.Raft
	sm           *kv.Store
	waiters      map[uint64]waiter
	calls        map[uint64]chan raft.Message
	callSeq      uint64
	incoming     *storage.IncomingSnapshot
	appliedTerm  uint64
	snapIndex    uint64
	bytesSince   int64 // bytes of entries applied since the last snapshot
	snapshotting bool
	lastLeader   string
	lastRole     raft.Role
}

// Start recovers state from the store and starts the event loop.
func Start(cfg Config, store *storage.Store, transport Transport) (*Node, error) {
	cfg.setDefaults()
	initial := store.InitialState()

	sm := kv.New(cfg.Limits)
	if initial.Snapshot.Index > 0 {
		_, rc, err := store.OpenSnapshotPayload()
		if err != nil {
			return nil, fmt.Errorf("opening snapshot: %w", err)
		}
		err = sm.Restore(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("restoring state from snapshot: %w", err)
		}
		if sm.AppliedIndex() != initial.Snapshot.Index {
			return nil, fmt.Errorf("snapshot file covers index %d but its contents were taken at index %d", initial.Snapshot.Index, sm.AppliedIndex())
		}
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	core, err := raft.New(raft.Config{
		ID:                    cfg.ID,
		Peers:                 cfg.Peers,
		ElectionTicks:         cfg.ElectionTicks,
		HeartbeatTicks:        cfg.HeartbeatTicks,
		PreVote:               !cfg.DisablePreVote,
		CheckQuorum:           !cfg.DisableCheckQuorum,
		MaxUncommittedEntries: cfg.MaxPendingProposals,
		Rand:                  rng.Intn,
	}, initial)
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:         cfg,
		log:         cfg.Logger,
		obs:         cfg.Observer,
		store:       store,
		transport:   transport,
		peers:       make(map[string]bool, len(cfg.Peers)),
		proposeC:    make(chan *proposal, cfg.MaxPendingProposals),
		inboxC:      make(chan inbound, cfg.MaxInbox),
		reportC:     make(chan snapshotReport, len(cfg.Peers)),
		snapDoneC:   make(chan snapshotDone, 1),
		stopC:       make(chan struct{}),
		done:        make(chan struct{}),
		core:        core,
		sm:          sm,
		waiters:     make(map[uint64]waiter),
		calls:       make(map[uint64]chan raft.Message),
		appliedTerm: initial.Snapshot.Term,
		snapIndex:   initial.Snapshot.Index,
	}
	for _, p := range cfg.Peers {
		n.peers[p] = true
	}
	n.publishStatus(nil)
	n.log.Info("node starting",
		"term", initial.HardState.Term, "voted_for", initial.HardState.Vote,
		"snapshot_index", initial.Snapshot.Index, "log_entries", len(initial.Entries), "commit", initial.Commit)
	go n.run()
	return n, nil
}

// ---- API for clients -------------------------------------------------------

// Propose replicates a command and returns the result of applying it. It
// returns once the command is committed by a quorum and applied on this
// node, or with an error. See the error variables for what each error says
// about whether the command took effect.
func (n *Node) Propose(ctx context.Context, cmd []byte) (kv.Result, error) {
	start := time.Now()
	res, err := n.propose(ctx, cmd)
	n.obs.ProposalDone(outcome(err), time.Since(start))
	return res, err
}

func (n *Node) propose(ctx context.Context, cmd []byte) (kv.Result, error) {
	p := &proposal{data: cmd, done: make(chan proposalResult, 1)}
	select {
	case <-n.done:
		return kv.Result{}, ErrStopped
	default:
	}
	// Never block here. A full queue means the loop is behind; telling the
	// client to back off is better than letting requests pile up.
	select {
	case n.proposeC <- p:
	default:
		n.obs.Rejected("proposal_queue_full")
		return kv.Result{}, ErrOverloaded
	}
	select {
	case r := <-p.done:
		return r.res, r.err
	case <-ctx.Done():
		// The loop may already have appended the command, and it may yet
		// commit. Report exactly that; do not claim it failed.
		return kv.Result{}, fmt.Errorf("%w: %v", ErrOutcomeUnknown, ctx.Err())
	case <-n.done:
		// The loop answers every proposal it has seen before closing done,
		// so prefer that answer if it is there.
		select {
		case r := <-p.done:
			return r.res, r.err
		default:
			return kv.Result{}, fmt.Errorf("%w: node stopped", ErrOutcomeUnknown)
		}
	}
}

func outcome(err error) string {
	var nl *NotLeaderError
	switch {
	case err == nil:
		return "ok"
	case errors.As(err, &nl):
		return "not_leader"
	case errors.Is(err, ErrOverloaded):
		return "overloaded"
	case errors.Is(err, ErrProposalDropped):
		return "dropped"
	case errors.Is(err, ErrOutcomeUnknown):
		return "unknown"
	case errors.Is(err, ErrStopped):
		return "stopped"
	}
	return "error"
}

// Status returns the most recently published state.
func (n *Node) Status() Status {
	n.statusMu.RLock()
	defer n.statusMu.RUnlock()
	return n.status
}

// Done is closed when the event loop has exited, whether through Stop or
// because storage failed.
func (n *Node) Done() <-chan struct{} { return n.done }

// Stop shuts the node down and waits for the event loop and any background
// snapshot to finish, or for ctx to end. Commands that were accepted but not
// yet answered are answered with ErrOutcomeUnknown: they may still commit on
// other nodes.
func (n *Node) Stop(ctx context.Context) error {
	n.stopOnce.Do(func() { close(n.stopC) })
	finished := make(chan struct{})
	go func() {
		<-n.done
		n.bg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("node: shutdown did not finish: %w", ctx.Err())
	}
}

// ---- API for the transport -------------------------------------------------

// Step delivers a message that needs no reply through the transport, such as
// the response to a request this node sent.
func (n *Node) Step(ctx context.Context, m raft.Message) error {
	return n.enqueue(ctx, inbound{msg: m})
}

// Call delivers a request from a peer and returns this node's response. The
// response is produced only after anything it depends on is durable.
func (n *Node) Call(ctx context.Context, m raft.Message) (raft.Message, error) {
	return n.call(ctx, inbound{msg: m, reply: make(chan raft.Message, 1)})
}

// InstallSnapshot delivers a fully received and verified snapshot. On
// success or failure the node takes ownership of in.
func (n *Node) InstallSnapshot(ctx context.Context, m raft.Message, in *storage.IncomingSnapshot) (raft.Message, error) {
	resp, err := n.call(ctx, inbound{msg: m, reply: make(chan raft.Message, 1), snap: in})
	if err != nil && errors.Is(err, errNotQueued) {
		in.Discard()
	}
	return resp, err
}

var errNotQueued = errors.New("node: message was not queued")

func (n *Node) enqueue(ctx context.Context, in inbound) error {
	if !n.peers[in.msg.From] || in.msg.From == n.cfg.ID {
		return fmt.Errorf("%w: unknown peer %q", errNotQueued, in.msg.From)
	}
	select {
	case n.inboxC <- in:
		return nil
	case <-ctx.Done():
		n.obs.Rejected("inbox_full")
		return fmt.Errorf("%w: %v", errNotQueued, ctx.Err())
	case <-n.done:
		return fmt.Errorf("%w: %v", errNotQueued, ErrStopped)
	}
}

func (n *Node) call(ctx context.Context, in inbound) (raft.Message, error) {
	if err := n.enqueue(ctx, in); err != nil {
		return raft.Message{}, err
	}
	select {
	case resp := <-in.reply:
		return resp, nil
	case <-ctx.Done():
		return raft.Message{}, ctx.Err()
	case <-n.done:
		return raft.Message{}, ErrStopped
	}
}

// ReportSnapshot tells the node how sending a snapshot to peer ended.
func (n *Node) ReportSnapshot(peer string, ok bool) {
	select {
	case n.reportC <- snapshotReport{peer: peer, ok: ok}:
	case <-n.done:
	default:
		// The loop is busy and the small buffer is full. Raft has its own
		// retry timer for snapshots whose outcome it never hears about.
	}
}

// OpenSnapshotFile opens the newest snapshot for sending to a peer.
func (n *Node) OpenSnapshotFile() (storage.SnapshotHeader, int64, io.ReadCloser, error) {
	return n.store.OpenSnapshotFile()
}

// NewIncomingSnapshot starts receiving a snapshot from a peer.
func (n *Node) NewIncomingSnapshot() (*storage.IncomingSnapshot, error) {
	return n.store.NewIncomingSnapshot()
}

// ---- the event loop --------------------------------------------------------

func (n *Node) run() {
	defer close(n.done)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		var err error
		select {
		case <-n.stopC:
			n.shutdown(nil)
			return
		case <-ticker.C:
			n.core.Tick()
		case in := <-n.inboxC:
			err = n.handleInbound(in)
		case p := <-n.proposeC:
			n.handleProposal(p)
		case r := <-n.reportC:
			n.core.ReportSnapshot(r.peer, r.ok)
		case d := <-n.snapDoneC:
			err = n.handleSnapshotDone(d)
		}

		// Take whatever else is already waiting, up to a bound, so that a
		// burst of proposals and messages is persisted with one fsync and
		// replicated in one batch.
	batch:
		for i := 0; i < n.cfg.MaxBatch && err == nil; i++ {
			select {
			case in := <-n.inboxC:
				err = n.handleInbound(in)
			case p := <-n.proposeC:
				n.handleProposal(p)
			default:
				break batch
			}
		}

		if err == nil {
			err = n.processReady()
		}
		if err != nil {
			// A node that cannot persist cannot keep its promises. Stop
			// participating entirely rather than limp along.
			n.log.Error("storage failure, node is stopping", "error", err)
			n.shutdown(err)
			return
		}
		n.publishStatus(nil)
	}
}

func (n *Node) handleProposal(p *proposal) {
	if len(n.waiters) >= n.cfg.MaxPendingProposals {
		n.obs.Rejected("too_many_pending")
		p.done <- proposalResult{err: ErrOverloaded}
		return
	}
	index, term, err := n.core.Propose(p.data)
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		p.done <- proposalResult{err: &NotLeaderError{Leader: n.core.Leader()}}
	case errors.Is(err, raft.ErrOverloaded):
		n.obs.Rejected("too_many_uncommitted")
		p.done <- proposalResult{err: ErrOverloaded}
	case err != nil:
		p.done <- proposalResult{err: err}
	default:
		n.waiters[index] = waiter{term: term, done: p.done}
	}
}

func (n *Node) handleInbound(in inbound) error {
	if in.reply != nil {
		n.callSeq++
		in.msg.Seq = n.callSeq
		n.calls[in.msg.Seq] = in.reply
	}
	if in.snap == nil {
		n.core.Step(in.msg)
		return nil
	}
	// A snapshot is handled on its own: flush what came before so the Ready
	// that follows belongs to this snapshot alone.
	if err := n.processReady(); err != nil {
		in.snap.Discard()
		return err
	}
	n.incoming = in.snap
	n.core.Step(in.msg)
	err := n.processReady()
	if n.incoming != nil {
		// Raft did not want it (stale, or from a deposed leader).
		n.incoming.Discard()
		n.incoming = nil
	}
	return err
}

// processReady carries out everything Raft asks for, in the order that keeps
// its promises: persist, then send, then apply.
func (n *Node) processReady() error {
	for n.core.HasReady() {
		rd := n.core.Ready()

		if rd.Snapshot != nil {
			if err := n.installSnapshot(rd); err != nil {
				return err
			}
		}

		var hs *raft.HardState
		if rd.HardStateChanged {
			hs = &rd.HardState
		}
		// One write and one fsync for the whole batch. Nothing below this
		// line happens unless it succeeded.
		if err := n.store.Save(hs, rd.Entries, rd.Commit, hs != nil || len(rd.Entries) > 0); err != nil {
			return err
		}

		// Votes, append acknowledgements and the leader's own replication
		// requests leave only now, after the state they vouch for is on disk.
		//
		// The leader's requests could safely leave before its own write (it
		// counts its own copy only in Advance, after the write). That was
		// tried and measured; on the test machine it made no difference
		// outside run-to-run noise, so the simpler order stayed. See
		// benchmarks/experiments/overlapped-replication.md.
		for _, m := range rd.Messages {
			n.dispatch(m)
		}

		n.applyBatch(rd.CommittedEntries)
		n.core.Advance()

		if len(rd.Entries) > 0 && n.core.Role() != raft.Leader {
			n.dropOverwrittenWaiters()
		}
	}
	n.maybeSnapshot()
	return nil
}

func (n *Node) dispatch(m raft.Message) {
	if m.Type.IsResponse() && m.Seq != 0 {
		if reply, ok := n.calls[m.Seq]; ok {
			delete(n.calls, m.Seq)
			reply <- m
			return
		}
	}
	n.transport.Send(m)
}

func (n *Node) applyBatch(entries []raft.Entry) {
	if len(entries) == 0 {
		return
	}
	start := time.Now()
	for _, e := range entries {
		n.apply(e)
	}
	n.obs.Applied(len(entries), time.Since(start))
}

// apply runs one committed entry through the state machine and completes the
// client waiting for it, if there is one on this node.
func (n *Node) apply(e raft.Entry) {
	var res kv.Result
	if e.Type == raft.EntryCommand {
		res = n.sm.Apply(e.Index, e.Data)
	} else {
		n.sm.Skip(e.Index)
	}
	n.appliedTerm = e.Term
	n.bytesSince += int64(len(e.Data))
	w, ok := n.waiters[e.Index]
	if !ok {
		return
	}
	delete(n.waiters, e.Index)
	if w.term == e.Term {
		w.done <- proposalResult{res: res}
	} else {
		w.done <- proposalResult{err: ErrProposalDropped}
	}
}

// dropOverwrittenWaiters fails clients whose proposals were replaced in the
// log by another leader's entries. They learn promptly, and definitely, that
// their command did not commit through this proposal.
func (n *Node) dropOverwrittenWaiters() {
	last := n.core.LastIndex()
	for index, w := range n.waiters {
		term, ok := n.core.Term(index)
		// Either the log was cut back to before this index, or the index
		// now holds an entry from another term.
		if index > last || (ok && term != w.term) {
			delete(n.waiters, index)
			w.done <- proposalResult{err: ErrProposalDropped}
		}
	}
}

func (n *Node) installSnapshot(rd raft.Ready) error {
	in := n.incoming
	n.incoming = nil
	if in == nil {
		return errors.New("node: Raft asked to install a snapshot that was not received")
	}
	if err := n.store.InstallSnapshot(in, rd.SnapshotKeepsLog); err != nil {
		in.Discard()
		return fmt.Errorf("installing snapshot: %w", err)
	}
	_, rc, err := n.store.OpenSnapshotPayload()
	if err != nil {
		return fmt.Errorf("opening installed snapshot: %w", err)
	}
	err = n.sm.Restore(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("restoring state from installed snapshot: %w", err)
	}
	n.appliedTerm, n.snapIndex, n.bytesSince = rd.Snapshot.Term, rd.Snapshot.Index, 0
	n.obs.SnapshotInstalled()
	n.log.Info("installed snapshot from leader", "index", rd.Snapshot.Index, "term", rd.Snapshot.Term, "kept_log_suffix", rd.SnapshotKeepsLog)

	// Proposals made here while this node was a leader are no longer
	// traceable entry by entry. At or below the snapshot point the entry
	// may or may not have been ours; say so honestly.
	for index, w := range n.waiters {
		if index <= rd.Snapshot.Index {
			delete(n.waiters, index)
			w.done <- proposalResult{err: fmt.Errorf("%w: superseded by a snapshot", ErrOutcomeUnknown)}
		}
	}
	n.dropOverwrittenWaiters()
	return nil
}

// maybeSnapshot starts a snapshot when enough entries have been applied since
// the last one.
//
// The state is serialised here, on the loop, between two applied commands, so
// it is exactly the state at one log index. Writing and syncing the file,
// which is the slow part, runs in the background; the loop keeps serving.
func (n *Node) maybeSnapshot() {
	applied := n.sm.AppliedIndex()
	if n.snapshotting || applied == n.snapIndex {
		return
	}
	if applied < n.snapIndex+n.cfg.SnapshotThreshold && n.bytesSince < n.cfg.SnapshotBytes {
		return
	}
	n.bytesSince = 0
	meta := raft.SnapshotMeta{Index: applied, Term: n.appliedTerm}
	start := time.Now()
	data := n.sm.Snapshot()
	n.snapshotting = true
	n.bg.Add(1)
	go func() {
		defer n.bg.Done()
		err := n.store.CreateSnapshot(meta, data)
		n.snapDoneC <- snapshotDone{meta: meta, bytes: len(data), took: time.Since(start), err: err}
	}()
}

func (n *Node) handleSnapshotDone(d snapshotDone) error {
	n.snapshotting = false
	n.obs.SnapshotCreated(d.bytes, d.took, d.err)
	if d.err != nil {
		// Not fatal: the log is intact and simply stays longer. The next
		// threshold crossing tries again.
		n.log.Error("snapshot failed", "index", d.meta.Index, "error", d.err)
		return nil
	}
	if d.meta.Index > n.snapIndex {
		n.snapIndex = d.meta.Index
	}
	// Only now, with the snapshot durable and discoverable by recovery, is
	// it safe to drop the log entries it covers.
	if d.meta.Index > n.cfg.SnapshotTrailing {
		n.core.CompactTo(d.meta.Index - n.cfg.SnapshotTrailing)
	}
	if err := n.store.Compact(d.meta.Index); err != nil {
		return fmt.Errorf("compacting log after snapshot: %w", err)
	}
	n.log.Info("snapshot created", "index", d.meta.Index, "term", d.meta.Term, "bytes", d.bytes, "took", d.took)
	return nil
}

// shutdown answers everyone still waiting and records why the loop ended.
func (n *Node) shutdown(failure error) {
	reason := fmt.Errorf("%w: node stopped", ErrOutcomeUnknown)
	for index, w := range n.waiters {
		delete(n.waiters, index)
		w.done <- proposalResult{err: reason}
	}
	// Proposals still in the queue were never given to Raft.
	for {
		select {
		case p := <-n.proposeC:
			p.done <- proposalResult{err: ErrStopped}
			continue
		default:
		}
		break
	}
	if n.incoming != nil {
		n.incoming.Discard()
		n.incoming = nil
	}
	n.publishStatus(failure)
	if failure == nil {
		n.log.Info("node stopped")
	}
}

func (n *Node) publishStatus(failure error) {
	st := Status{
		Status:        n.core.Status(),
		SnapshotIndex: n.snapIndex,
		Keys:          n.sm.Len(),
		StateBytes:    n.sm.Size(),
		Sessions:      n.sm.Sessions(),
		Pending:       len(n.waiters),
		Failed:        failure,
	}
	if failure != nil {
		// A failed node must not look like a leader to health checks.
		st.Role, st.Leader = raft.Follower, ""
	}
	if st.Leader != n.lastLeader || st.Role != n.lastRole {
		if st.Leader != n.lastLeader {
			n.obs.LeaderChanged()
		}
		n.log.Info("raft state changed", "role", st.Role.String(), "term", st.Term, "leader", st.Leader)
		n.lastLeader, n.lastRole = st.Leader, st.Role
	}
	n.statusMu.Lock()
	n.status = st
	n.statusMu.Unlock()
}
