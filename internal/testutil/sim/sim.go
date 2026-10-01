// Package sim runs a whole Raft cluster inside one goroutine with a simulated
// clock, a simulated network and simulated durable storage.
//
// Everything is driven by one seeded random number generator, so a run is
// fully determined by its Config. A failing seed can be replayed exactly,
// which is what makes rare interleavings debuggable.
//
// The simulator checks the Raft safety properties after every step, so tests
// only have to create interesting schedules.
package sim

import (
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
	"sort"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// StateMachine is the replicated application the simulator applies committed
// entries to.
type StateMachine interface {
	// Apply executes a command entry and returns its result.
	Apply(e raft.Entry) any
	Snapshot() []byte
	Restore(data []byte) error
}

// Config describes a simulated cluster.
type Config struct {
	Nodes int
	Seed  int64

	ElectionTicks  int
	HeartbeatTicks int
	PreVote        bool
	CheckQuorum    bool

	// Messages are delivered after a random delay in [MinDelay, MaxDelay]
	// ticks. A range wider than one tick reorders messages.
	MinDelay, MaxDelay int
	// DropRate and DupRate are probabilities in [0, 1).
	DropRate, DupRate float64

	// SnapshotEvery takes a snapshot whenever this many entries have been
	// applied since the last one. Zero disables snapshots.
	SnapshotEvery uint64
	// SnapshotTrailing entries are kept in the log behind a snapshot.
	SnapshotTrailing uint64

	MaxEntriesPerAppend   int
	MaxUncommittedEntries int

	// ForgetCommit restarts nodes with a commit index of zero instead of the
	// persisted one, to exercise relearning the commit index from the leader.
	ForgetCommit bool

	// NewStateMachine builds the application for one node. The default is a
	// state machine that hashes everything it applies.
	NewStateMachine func() StateMachine
}

type envelope struct {
	at       int
	seq      uint64
	msg      raft.Message
	snapData []byte
}

type queue []*envelope

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(*envelope)) }
func (q *queue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

// node is one simulated server: the volatile Raft instance plus the state
// that survives a crash.
type node struct {
	id   string
	raft *raft.Raft // nil while crashed
	sm   StateMachine

	// Durable state.
	hs       raft.HardState
	commit   uint64
	snap     raft.SnapshotMeta
	snapData []byte
	walBase  uint64 // index of the entry before wal[0]
	wal      []raft.Entry

	applied      uint64
	seenCommit   uint64
	incomingSnap []byte
	incarnation  int
}

type committedEntry struct {
	term uint64
	data []byte
	typ  raft.EntryType
	full bool // data and typ are known (set when some node applied it)
}

// Cluster is a simulated Raft cluster.
type Cluster struct {
	cfg   Config
	rng   *rand.Rand
	now   int
	seq   uint64
	ids   []string
	nodes map[string]*node
	net   queue
	// blocked[from][to] drops messages on that directed link.
	blocked map[string]map[string]bool

	// Global bookkeeping for invariant checks.
	leaders   map[uint64]string         // term -> the only node allowed to lead it
	committed map[uint64]committedEntry // index -> the entry committed there
	maxCommit uint64

	trace []string

	// OnApply, if set, is called whenever a node applies an entry.
	OnApply func(nodeID string, e raft.Entry, result any)
	// Delivered and Dropped count messages, for tests that want to confirm a
	// fault schedule actually interfered with traffic.
	Delivered, Dropped int
}

// New builds a cluster. All nodes start up, connected, with empty state.
func New(cfg Config) *Cluster {
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 2
	}
	if cfg.MaxDelay < cfg.MinDelay {
		cfg.MaxDelay = cfg.MinDelay
	}
	if cfg.NewStateMachine == nil {
		cfg.NewStateMachine = func() StateMachine { return &HashMachine{} }
	}
	c := &Cluster{
		cfg:       cfg,
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		nodes:     make(map[string]*node),
		blocked:   make(map[string]map[string]bool),
		leaders:   make(map[uint64]string),
		committed: make(map[uint64]committedEntry),
	}
	for i := 1; i <= cfg.Nodes; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.nodes[id] = &node{id: id}
		c.blocked[id] = make(map[string]bool)
		c.start(id)
	}
	return c
}

// IDs returns the node IDs in order.
func (c *Cluster) IDs() []string { return append([]string(nil), c.ids...) }

// Now returns the current simulated tick.
func (c *Cluster) Now() int { return c.now }

// Rand exposes the simulation's random source so tests can draw their
// schedule from the same seed.
func (c *Cluster) Rand() *rand.Rand { return c.rng }

func (c *Cluster) start(id string) {
	n := c.nodes[id]
	n.incarnation++
	n.sm = c.cfg.NewStateMachine()
	if n.snapData != nil {
		if err := n.sm.Restore(n.snapData); err != nil {
			panic(fmt.Sprintf("sim: %s cannot restore its snapshot: %v", id, err))
		}
	}
	n.applied = n.snap.Index
	n.seenCommit = n.snap.Index

	// Mirror what real storage does on recovery: entries covered by the
	// snapshot are not handed back.
	var entries []raft.Entry
	for _, e := range n.wal {
		if e.Index > n.snap.Index {
			entries = append(entries, e)
		}
	}
	commit := n.commit
	if c.cfg.ForgetCommit {
		commit = 0
	}
	rcfg := raft.Config{
		ID:                    id,
		Peers:                 c.ids,
		ElectionTicks:         c.cfg.ElectionTicks,
		HeartbeatTicks:        c.cfg.HeartbeatTicks,
		PreVote:               c.cfg.PreVote,
		CheckQuorum:           c.cfg.CheckQuorum,
		MaxEntriesPerAppend:   c.cfg.MaxEntriesPerAppend,
		MaxUncommittedEntries: c.cfg.MaxUncommittedEntries,
		SnapshotRetryTicks:    5 * c.cfg.ElectionTicks,
		Rand:                  c.rng.Intn,
		Trace: func(format string, args ...any) {
			c.tracef(format, args...)
		},
	}
	r, err := raft.New(rcfg, raft.InitialState{HardState: n.hs, Commit: commit, Snapshot: n.snap, Entries: entries})
	if err != nil {
		panic(fmt.Sprintf("sim: restarting %s: %v", id, err))
	}
	n.raft = r
}

// ---- fault injection -------------------------------------------------------

// Crash stops a node. Volatile state is lost; durable state is kept.
func (c *Cluster) Crash(id string) {
	n := c.nodes[id]
	if n.raft == nil {
		return
	}
	c.tracef("%s: CRASH", id)
	n.raft = nil
	n.sm = nil
	n.incomingSnap = nil
}

// Restart starts a crashed node from its durable state.
func (c *Cluster) Restart(id string) {
	if c.nodes[id].raft != nil {
		return
	}
	c.tracef("%s: RESTART", id)
	c.start(id)
}

// Up reports whether a node is running.
func (c *Cluster) Up(id string) bool { return c.nodes[id].raft != nil }

// Partition splits the cluster into groups. Nodes can talk only within their
// own group; a node listed in no group is isolated.
func (c *Cluster) Partition(groups ...[]string) {
	group := make(map[string]int)
	for i, g := range groups {
		for _, id := range g {
			group[id] = i + 1
		}
	}
	for _, a := range c.ids {
		for _, b := range c.ids {
			c.blocked[a][b] = a != b && (group[a] == 0 || group[a] != group[b])
		}
	}
	c.tracef("PARTITION %v", groups)
}

// BlockLink drops messages travelling from one node to another, in that
// direction only.
func (c *Cluster) BlockLink(from, to string) {
	c.blocked[from][to] = true
	c.tracef("BLOCK %s -> %s", from, to)
}

// Heal removes all partitions and blocked links.
func (c *Cluster) Heal() {
	for _, a := range c.ids {
		for _, b := range c.ids {
			c.blocked[a][b] = false
		}
	}
	c.tracef("HEAL")
}

// SetNetwork changes delay and loss settings mid-run.
func (c *Cluster) SetNetwork(minDelay, maxDelay int, drop, dup float64) {
	c.cfg.MinDelay, c.cfg.MaxDelay, c.cfg.DropRate, c.cfg.DupRate = minDelay, maxDelay, drop, dup
}

// ---- driving the cluster ---------------------------------------------------

// Propose submits a command at one node.
func (c *Cluster) Propose(id string, data []byte) (index, term uint64, err error) {
	n := c.nodes[id]
	if n.raft == nil {
		return 0, 0, raft.ErrNotLeader
	}
	return n.raft.Propose(data)
}

// Step advances the simulation by one tick and checks every invariant.
func (c *Cluster) Step() error {
	c.now++
	for _, id := range c.ids {
		if n := c.nodes[id]; n.raft != nil {
			n.raft.Tick()
		}
	}
	for c.net.Len() > 0 && c.net[0].at <= c.now {
		env := heap.Pop(&c.net).(*envelope)
		c.deliver(env)
	}
	for _, id := range c.ids {
		if err := c.drain(c.nodes[id]); err != nil {
			return err
		}
	}
	return c.check()
}

// Run steps n times.
func (c *Cluster) Run(n int) error {
	for i := 0; i < n; i++ {
		if err := c.Step(); err != nil {
			return err
		}
	}
	return nil
}

// RunUntil steps until cond holds or max ticks pass. It reports whether cond
// held.
func (c *Cluster) RunUntil(max int, cond func() bool) (bool, error) {
	for i := 0; i < max; i++ {
		if cond() {
			return true, nil
		}
		if err := c.Step(); err != nil {
			return false, err
		}
	}
	return cond(), nil
}

func (c *Cluster) deliver(env *envelope) {
	m := env.msg
	n := c.nodes[m.To]
	if n.raft == nil || c.blocked[m.From][m.To] {
		c.Dropped++
		c.snapshotLost(m)
		return
	}
	c.Delivered++
	if m.Type == raft.MsgSnap {
		n.incomingSnap = env.snapData
	}
	n.raft.Step(m)
}

// drain processes Ready for one node until it has nothing left to do, in the
// order the Ready contract requires: persist, send, apply, advance.
func (c *Cluster) drain(n *node) error {
	for n.raft != nil && n.raft.HasReady() {
		rd := n.raft.Ready()

		if rd.Snapshot != nil {
			if n.incomingSnap == nil {
				return fmt.Errorf("%s: Ready carries a snapshot but none was received", n.id)
			}
			keep := n.wal[:0:0]
			// The core's decision must agree with what is really stored.
			t, ok := walTerm(n, rd.Snapshot.Index)
			if stored := ok && t == rd.Snapshot.Term; stored != rd.SnapshotKeepsLog {
				return fmt.Errorf("%s: Ready says SnapshotKeepsLog=%v for snapshot %d, but the stored log says %v", n.id, rd.SnapshotKeepsLog, rd.Snapshot.Index, stored)
			}
			if rd.SnapshotKeepsLog {
				for _, e := range n.wal {
					if e.Index > rd.Snapshot.Index {
						keep = append(keep, e)
					}
				}
			}
			n.wal, n.walBase = keep, rd.Snapshot.Index
			n.snap, n.snapData = *rd.Snapshot, n.incomingSnap
			n.incomingSnap = nil
			if err := n.sm.Restore(n.snapData); err != nil {
				return fmt.Errorf("%s: restoring snapshot: %w", n.id, err)
			}
			n.applied = rd.Snapshot.Index
			if n.commit < rd.Snapshot.Index {
				n.commit = rd.Snapshot.Index
			}
		}
		if rd.HardStateChanged {
			if rd.HardState.Term < n.hs.Term {
				return fmt.Errorf("%s: term went backwards from %d to %d", n.id, n.hs.Term, rd.HardState.Term)
			}
			if rd.HardState.Term == n.hs.Term && n.hs.Vote != "" && rd.HardState.Vote != n.hs.Vote {
				return fmt.Errorf("%s: vote in term %d changed from %q to %q", n.id, n.hs.Term, n.hs.Vote, rd.HardState.Vote)
			}
			n.hs = rd.HardState
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			if first <= n.commit {
				return fmt.Errorf("%s: storing entry %d would overwrite committed index %d", n.id, first, n.commit)
			}
			if first > n.walBase+uint64(len(n.wal))+1 {
				return fmt.Errorf("%s: storing entry %d leaves a gap after %d", n.id, first, n.walBase+uint64(len(n.wal)))
			}
			n.wal = append(n.wal[:first-n.walBase-1:first-n.walBase-1], rd.Entries...)
		}
		n.commit = rd.Commit

		// Everything above is "on disk". Only now may messages leave.
		for _, m := range rd.Messages {
			c.send(n, m)
		}

		for _, e := range rd.CommittedEntries {
			if e.Index != n.applied+1 {
				return fmt.Errorf("%s: applying index %d after %d", n.id, e.Index, n.applied)
			}
			if err := c.recordApply(n, e); err != nil {
				return err
			}
			var res any
			if e.Type == raft.EntryCommand {
				res = n.sm.Apply(e)
			}
			n.applied = e.Index
			if c.OnApply != nil {
				c.OnApply(n.id, e, res)
			}
		}
		n.raft.Advance()

		if c.cfg.SnapshotEvery > 0 && n.applied-n.snap.Index >= c.cfg.SnapshotEvery {
			c.takeSnapshot(n)
		}
	}
	return nil
}

func walTerm(n *node, index uint64) (uint64, bool) {
	if index <= n.walBase || index > n.walBase+uint64(len(n.wal)) {
		return 0, false
	}
	return n.wal[index-n.walBase-1].Term, true
}

func (c *Cluster) takeSnapshot(n *node) {
	term, ok := n.raft.Term(n.applied)
	if !ok {
		return
	}
	n.snap = raft.SnapshotMeta{Index: n.applied, Term: term}
	n.snapData = n.sm.Snapshot()
	compactTo := uint64(0)
	if n.applied > c.cfg.SnapshotTrailing {
		compactTo = n.applied - c.cfg.SnapshotTrailing
	}
	n.raft.CompactTo(compactTo)
	if compactTo > n.walBase {
		n.wal = append([]raft.Entry(nil), n.wal[compactTo-n.walBase:]...)
		n.walBase = compactTo
	}
	c.tracef("%s: snapshot at %d, log compacted to %d", n.id, n.applied, compactTo)
}

func (c *Cluster) send(from *node, m raft.Message) {
	var snapData []byte
	if m.Type == raft.MsgSnap {
		// The real transport reads the position from the snapshot file.
		m.SnapIndex, m.SnapTerm = from.snap.Index, from.snap.Term
		snapData = from.snapData
	}
	if c.cfg.DropRate > 0 && c.rng.Float64() < c.cfg.DropRate {
		c.Dropped++
		c.snapshotLost(m)
		return
	}
	copies := 1
	if c.cfg.DupRate > 0 && c.rng.Float64() < c.cfg.DupRate {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		delay := c.cfg.MinDelay
		if c.cfg.MaxDelay > c.cfg.MinDelay {
			delay += c.rng.Intn(c.cfg.MaxDelay - c.cfg.MinDelay + 1)
		}
		c.seq++
		heap.Push(&c.net, &envelope{at: c.now + 1 + delay, seq: c.seq, msg: m, snapData: snapData})
	}
}

// snapshotLost mirrors the real transport: a snapshot travels over an RPC
// with a deadline, so the sender usually learns when the transfer failed and
// can try again. A lost response is deliberately not reported here, which
// leaves the leader's own retry timer to recover from it.
func (c *Cluster) snapshotLost(m raft.Message) {
	if m.Type != raft.MsgSnap {
		return
	}
	if sender := c.nodes[m.From]; sender.raft != nil {
		sender.raft.ReportSnapshot(m.To, false)
	}
}

// ---- invariants ------------------------------------------------------------

// recordApply enforces state machine safety: once any node applies an entry
// at an index, every node applies that same entry there.
func (c *Cluster) recordApply(n *node, e raft.Entry) error {
	if e.Index > n.raft.Status().Commit {
		return fmt.Errorf("%s: applied index %d beyond commit index %d", n.id, e.Index, n.raft.Status().Commit)
	}
	prev, ok := c.committed[e.Index]
	if ok && prev.term != e.Term {
		return fmt.Errorf("STATE MACHINE SAFETY: %s applied term %d at index %d, but term %d was committed there", n.id, e.Term, e.Index, prev.term)
	}
	if ok && prev.full && (prev.typ != e.Type || !bytes.Equal(prev.data, e.Data)) {
		return fmt.Errorf("STATE MACHINE SAFETY: %s applied different data at index %d", n.id, e.Index)
	}
	c.committed[e.Index] = committedEntry{term: e.Term, data: e.Data, typ: e.Type, full: true}
	return nil
}

func (c *Cluster) check() error {
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.raft == nil {
			continue
		}
		st := n.raft.Status()

		// Election safety: at most one leader per term, ever.
		if st.Role == raft.Leader {
			if prev, ok := c.leaders[st.Term]; ok && prev != id {
				return fmt.Errorf("ELECTION SAFETY: %s and %s both led term %d", prev, id, st.Term)
			}
			if _, ok := c.leaders[st.Term]; !ok {
				c.leaders[st.Term] = id
				// Leader completeness: a new leader holds every entry that
				// was committed before it was elected.
				for i := st.FirstIndex; i <= c.maxCommit; i++ {
					t, ok := n.raft.Term(i)
					if !ok || t != c.committed[i].term {
						return fmt.Errorf("LEADER COMPLETENESS: %s became leader of term %d without committed entry %d (term %d)", id, st.Term, i, c.committed[i].term)
					}
				}
			}
		}

		if st.Commit < n.seenCommit {
			return fmt.Errorf("%s: commit index went backwards from %d to %d", id, n.seenCommit, st.Commit)
		}
		if st.Applied > st.Commit || st.Commit > st.LastIndex {
			return fmt.Errorf("%s: applied %d, commit %d, last %d are out of order", id, st.Applied, st.Commit, st.LastIndex)
		}
		// Log matching, restricted to what matters: every node agrees on the
		// term of every index it considers committed.
		for i := max(n.seenCommit+1, st.FirstIndex); i <= st.Commit; i++ {
			t, _ := n.raft.Term(i)
			if prev, ok := c.committed[i]; ok {
				if prev.term != t {
					return fmt.Errorf("COMMIT SAFETY: %s committed term %d at index %d, but term %d was committed there", id, t, i, prev.term)
				}
			} else {
				c.committed[i] = committedEntry{term: t}
			}
		}
		n.seenCommit = st.Commit
		if st.Commit > c.maxCommit {
			c.maxCommit = st.Commit
		}
	}
	return nil
}

// ---- observation -----------------------------------------------------------

// Leader returns the leader with the highest term among running nodes, or ""
// if no running node believes it is leader.
func (c *Cluster) Leader() string {
	best, bestTerm := "", uint64(0)
	for _, id := range c.ids {
		if n := c.nodes[id]; n.raft != nil && n.raft.Role() == raft.Leader && n.raft.CurrentTerm() >= bestTerm {
			best, bestTerm = id, n.raft.CurrentTerm()
		}
	}
	return best
}

// Leaders returns every running node that currently believes it is leader.
// During a partition there can be more than one, in different terms.
func (c *Cluster) Leaders() []string {
	var out []string
	for _, id := range c.ids {
		if n := c.nodes[id]; n.raft != nil && n.raft.Role() == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// Status returns one node's Raft status. The node must be up.
func (c *Cluster) Status(id string) raft.Status { return c.nodes[id].raft.Status() }

// Applied returns the index of the last entry a node applied.
func (c *Cluster) Applied(id string) uint64 { return c.nodes[id].applied }

// Machine returns a node's state machine (nil while crashed).
func (c *Cluster) Machine(id string) StateMachine { return c.nodes[id].sm }

// MaxCommit returns the highest index any node has ever reported committed.
func (c *Cluster) MaxCommit() uint64 { return c.maxCommit }

// CommittedData returns the command committed at index, if a node applied it.
func (c *Cluster) CommittedData(index uint64) ([]byte, bool) {
	e, ok := c.committed[index]
	return e.data, ok && e.full
}

// CommittedTerm returns the term of the entry committed at index, or 0 if
// nothing is known to be committed there.
func (c *Cluster) CommittedTerm(index uint64) uint64 { return c.committed[index].term }

// SnapshotIndex returns the index of a node's latest durable snapshot.
func (c *Cluster) SnapshotIndex(id string) uint64 { return c.nodes[id].snap.Index }

// Converged reports whether every running node has applied everything that
// was ever committed.
func (c *Cluster) Converged() bool {
	for _, id := range c.ids {
		if n := c.nodes[id]; n.raft != nil && n.applied < c.maxCommit {
			return false
		}
	}
	return true
}

func (c *Cluster) tracef(format string, args ...any) {
	const keep = 400
	c.trace = append(c.trace, fmt.Sprintf("[t=%d] ", c.now)+fmt.Sprintf(format, args...))
	if len(c.trace) > keep {
		c.trace = c.trace[len(c.trace)-keep:]
	}
}

// Trace returns the most recent role changes and injected faults, oldest
// first. Print it when a run fails.
func (c *Cluster) Trace() []string { return append([]string(nil), c.trace...) }

// Describe returns a one-line summary of every node, for failure messages.
func (c *Cluster) Describe() string {
	var b bytes.Buffer
	ids := append([]string(nil), c.ids...)
	sort.Strings(ids)
	for _, id := range ids {
		n := c.nodes[id]
		if n.raft == nil {
			fmt.Fprintf(&b, "%s: down (term %d, wal %d..%d)\n", id, n.hs.Term, n.walBase+1, n.walBase+uint64(len(n.wal)))
			continue
		}
		st := n.raft.Status()
		fmt.Fprintf(&b, "%s: %s term=%d leader=%q commit=%d applied=%d log=%d..%d\n", id, st.Role, st.Term, st.Leader, st.Commit, st.Applied, st.FirstIndex, st.LastIndex)
	}
	return b.String()
}
