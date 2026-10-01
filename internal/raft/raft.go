package raft

import (
	"fmt"
	"math"
	"sort"
)

// progress is what a leader tracks about one follower.
type progress struct {
	// match is the highest index known to be replicated on the follower.
	match uint64
	// next is the index of the next entry to send.
	next uint64
	// inflight is set after sending a non-empty append. It is cleared when
	// the follower reports progress or rejects the request, and the batch is
	// sent again on the next heartbeat if neither happens. It limits each
	// follower to one outstanding batch between heartbeats.
	inflight bool
	// snapshotting is set while a snapshot transfer to the follower is
	// outstanding. No appends are sent meanwhile.
	snapshotting bool
	// snapshotAge counts ticks since the transfer started.
	snapshotAge int
	// recentActive records whether the follower answered anything since the
	// last check-quorum round.
	recentActive bool
}

// Raft is one node's consensus state. It is not safe for concurrent use: a
// single goroutine must own it.
type Raft struct {
	cfg    Config
	id     string
	peers  []string // sorted, includes id
	quorum int

	term uint64
	vote string
	role Role
	lead string

	log     *raftLog
	commit  uint64
	applied uint64

	// Leader state.
	prs       map[string]*progress
	needBcast bool
	// Candidate and pre-candidate state: who answered, and how.
	votes map[string]bool

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int // randomised, in ticks

	// Outputs accumulated until the next Ready.
	msgs            []Message
	hsDirty         bool
	pendingSnap     *SnapshotMeta
	pendingSnapKeep bool

	// Remembered between Ready and Advance.
	readyLast    uint64
	readyApplied uint64
}

// New creates a node from recovered state. A node with empty state starts as
// a follower in term 0.
func New(cfg Config, st InitialState) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	log, err := newLog(st.Snapshot, st.Entries)
	if err != nil {
		return nil, err
	}
	commit := st.Commit
	if commit < st.Snapshot.Index {
		commit = st.Snapshot.Index
	}
	if commit > log.lastIndex() {
		return nil, fmt.Errorf("raft: recovered commit index %d is beyond the last log index %d", commit, log.lastIndex())
	}
	if log.lastTerm() > st.HardState.Term {
		return nil, fmt.Errorf("raft: recovered log has term %d but the stored current term is %d", log.lastTerm(), st.HardState.Term)
	}
	peers := append([]string(nil), cfg.Peers...)
	sort.Strings(peers)
	r := &Raft{
		cfg:     cfg,
		id:      cfg.ID,
		peers:   peers,
		quorum:  len(peers)/2 + 1,
		term:    st.HardState.Term,
		vote:    st.HardState.Vote,
		role:    Follower,
		log:     log,
		commit:  commit,
		applied: st.Snapshot.Index,
	}
	r.resetElectionTimeout()
	return r, nil
}

// ---- inputs ----------------------------------------------------------------

// Tick advances logical time by one unit.
func (r *Raft) Tick() {
	if r.role == Leader {
		r.tickLeader()
		return
	}
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.campaign()
	}
}

func (r *Raft) tickLeader() {
	r.electionElapsed++
	r.heartbeatElapsed++
	if r.electionElapsed >= r.cfg.ElectionTicks {
		r.electionElapsed = 0
		if r.cfg.CheckQuorum && !r.quorumActive() {
			r.tracef("leader lost contact with a quorum in term %d, stepping down", r.term)
			r.becomeFollower(r.term, "")
			return
		}
		for _, pr := range r.prs {
			pr.recentActive = false
		}
	}
	for _, pr := range r.prs {
		if !pr.snapshotting {
			continue
		}
		// Backstop for a transfer whose outcome was never reported, for
		// example because the follower's answer was lost. Giving up on it
		// lets the next heartbeat start a fresh one.
		if pr.snapshotAge++; pr.snapshotAge >= r.cfg.SnapshotRetryTicks {
			pr.snapshotting = false
		}
	}
	if r.heartbeatElapsed >= r.cfg.HeartbeatTicks {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

// Propose appends a command to the leader's log and returns the position it
// was given. The entry is committed only if that exact (index, term) pair is
// later delivered through Ready.CommittedEntries; the same index with a
// different term means the proposal was overwritten by another leader.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if r.log.lastIndex()-r.commit >= uint64(r.cfg.MaxUncommittedEntries) {
		return 0, 0, ErrOverloaded
	}
	e := Entry{Index: r.log.lastIndex() + 1, Term: r.term, Type: EntryCommand, Data: data}
	r.log.append(e)
	// Replication is deferred to the next Ready so that proposals arriving
	// together leave in one batch.
	r.needBcast = true
	return e.Index, e.Term, nil
}

// Step feeds one received message into the node.
func (r *Raft) Step(m Message) {
	if !r.isPeer(m.From) || m.From == r.id {
		return
	}
	// PreVote messages never change anyone's term, so they are handled before
	// the term comparison below. A PreVote request carries a term the sender
	// has not actually entered; treating it as a real higher term would let a
	// partitioned node disrupt a healthy leader, which is exactly what PreVote
	// exists to prevent.
	switch m.Type {
	case MsgPreVote:
		r.handlePreVote(m)
		return
	case MsgPreVoteResp:
		r.handlePreVoteResp(m)
		return
	}

	switch {
	case m.Term > r.term:
		// A real higher term always wins, whatever this node believes about
		// its current leader.
		lead := ""
		if m.Type == MsgApp || m.Type == MsgSnap {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.term:
		// Answer stale requests so the sender learns the current term.
		// Stale responses carry no usable information and are dropped.
		switch m.Type {
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From, Term: r.term, Seq: m.Seq, Reject: true})
		case MsgApp:
			r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Seq: m.Seq, Reject: true, LogIndex: m.LogIndex})
		case MsgSnap:
			r.send(Message{Type: MsgSnapResp, To: m.From, Term: r.term, Seq: m.Seq, Reject: true})
		}
		return
	}

	// From here on m.Term == r.term.
	switch m.Type {
	case MsgVote:
		r.handleVote(m)
	case MsgVoteResp:
		r.handleVoteResp(m)
	case MsgApp:
		r.handleAppend(m)
	case MsgAppResp:
		r.handleAppendResp(m)
	case MsgSnap:
		r.handleSnapshot(m)
	case MsgSnapResp:
		r.handleSnapshotResp(m)
	}
}

// ---- outputs ---------------------------------------------------------------

// HasReady reports whether Ready would return any work.
func (r *Raft) HasReady() bool {
	return r.hsDirty || len(r.msgs) > 0 || r.pendingSnap != nil ||
		r.log.stable < r.log.lastIndex() || r.applied < r.commit ||
		(r.role == Leader && r.needBcast)
}

// Ready returns the pending work. The caller must finish it and call Advance
// before calling Ready again.
func (r *Raft) Ready() Ready {
	if r.role == Leader && r.needBcast {
		r.bcastAppend()
	}
	r.needBcast = false

	rd := Ready{
		HardState:        HardState{Term: r.term, Vote: r.vote},
		HardStateChanged: r.hsDirty,
		Snapshot:         r.pendingSnap,
		SnapshotKeepsLog: r.pendingSnapKeep,
		Commit:           r.commit,
		Messages:         r.msgs,
	}
	if r.log.stable < r.log.lastIndex() {
		rd.Entries = r.log.slice(r.log.stable+1, r.log.lastIndex(), math.MaxInt, math.MaxInt)
	}
	if r.applied < r.commit {
		rd.CommittedEntries = r.log.slice(r.applied+1, r.commit, r.cfg.MaxApplyBatch, math.MaxInt)
	}
	r.msgs = nil
	r.hsDirty = false
	r.pendingSnap = nil
	r.readyLast = r.log.lastIndex()
	r.readyApplied = r.applied + uint64(len(rd.CommittedEntries))
	return rd
}

// Advance tells the node that the last Ready has been fully processed: its
// entries are durable and its committed entries are applied.
func (r *Raft) Advance() {
	if r.readyLast > r.log.stable {
		r.log.stable = r.readyLast
	}
	r.applied = r.readyApplied
	// The leader's own copy counts toward the quorum only now that it is
	// durable.
	if r.role == Leader && r.maybeCommit() {
		r.needBcast = true
	}
}

// CompactTo discards in-memory log entries up to and including index. The
// caller must hold a durable snapshot covering at least index.
func (r *Raft) CompactTo(index uint64) {
	if index > r.applied {
		index = r.applied
	}
	r.log.compactTo(index)
}

// ReportSnapshot tells the leader how a snapshot transfer ended. A successful
// transfer is also reported by the follower itself through MsgSnapResp; this
// call exists so a failed transfer is retried instead of waiting forever.
func (r *Raft) ReportSnapshot(peer string, ok bool) {
	if r.role != Leader {
		return
	}
	if pr := r.prs[peer]; pr != nil && pr.snapshotting && !ok {
		// Fall back to probing. The next heartbeat finds the follower still
		// behind the compaction point and starts another transfer.
		pr.snapshotting = false
	}
}

// Status returns a copy of the node's state.
func (r *Raft) Status() Status {
	s := Status{
		ID:         r.id,
		Role:       r.role,
		Term:       r.term,
		Vote:       r.vote,
		Leader:     r.lead,
		Commit:     r.commit,
		Applied:    r.applied,
		LastIndex:  r.log.lastIndex(),
		FirstIndex: r.log.firstIndex(),
	}
	if r.role == Leader {
		for _, p := range r.peers {
			if p == r.id {
				continue
			}
			pr := r.prs[p]
			s.Peers = append(s.Peers, PeerStatus{ID: p, Match: pr.match, Next: pr.next, Snapshotting: pr.snapshotting, RecentActive: pr.recentActive})
		}
	}
	return s
}

// Term returns the term of the log entry at index, if it is still in memory.
func (r *Raft) Term(index uint64) (uint64, bool) { return r.log.term(index) }

// Role returns the current role.
func (r *Raft) Role() Role { return r.role }

// Leader returns the node this node believes is leader, or "".
func (r *Raft) Leader() string { return r.lead }

// CurrentTerm returns the node's current term.
func (r *Raft) CurrentTerm() uint64 { return r.term }

// ---- role transitions ------------------------------------------------------

// becomeFollower moves the node to the follower role. When term is higher
// than the current term the vote is cleared; otherwise the vote for the
// current term is kept, because a node may vote only once per term.
//
// It does not reset the election timer. The timer is reset only by the events
// that justify it: granting a vote, or hearing from the current leader.
func (r *Raft) becomeFollower(term uint64, lead string) {
	if term > r.term {
		r.term = term
		r.vote = ""
		r.hsDirty = true
	}
	if r.role != Follower {
		r.resetElectionTimeout()
		r.tracef("%s -> follower in term %d", r.role, r.term)
	}
	r.role = Follower
	r.lead = lead
	r.prs = nil
	r.votes = nil
	r.needBcast = false
}

func (r *Raft) campaign() {
	r.resetElectionTimeout()
	if r.cfg.PreVote {
		r.becomePreCandidate()
		return
	}
	r.becomeCandidate()
}

// becomePreCandidate asks peers whether they would vote for this node in the
// next term. Nothing durable changes: the term is not incremented and no vote
// is cast.
func (r *Raft) becomePreCandidate() {
	r.role = PreCandidate
	r.lead = ""
	r.votes = map[string]bool{r.id: true}
	r.tracef("pre-candidate for term %d", r.term+1)
	if r.wonVotes() {
		r.becomeCandidate()
		return
	}
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{Type: MsgPreVote, To: p, Term: r.term + 1, LogIndex: r.log.lastIndex(), LogTerm: r.log.lastTerm()})
		}
	}
}

func (r *Raft) becomeCandidate() {
	r.role = Candidate
	r.lead = ""
	r.term++
	r.vote = r.id
	r.hsDirty = true
	r.votes = map[string]bool{r.id: true}
	r.electionElapsed = 0
	r.tracef("candidate in term %d", r.term)
	if r.wonVotes() {
		r.becomeLeader()
		return
	}
	// These requests leave only after the new term and the self-vote are
	// durable, because they are part of the same Ready.
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{Type: MsgVote, To: p, Term: r.term, LogIndex: r.log.lastIndex(), LogTerm: r.log.lastTerm()})
		}
	}
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.lead = r.id
	r.votes = nil
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.prs = make(map[string]*progress, len(r.peers))
	for _, p := range r.peers {
		if p != r.id {
			r.prs[p] = &progress{next: r.log.lastIndex() + 1}
		}
	}
	// A leader may not count replicas to commit entries from earlier terms
	// (Raft paper, section 5.4.2). Appending an entry of its own term gives it
	// something it is allowed to commit, and committing that entry commits
	// everything before it. Until then the leader does not know its own
	// commit index.
	r.log.append(Entry{Index: r.log.lastIndex() + 1, Term: r.term, Type: EntryNoop})
	r.needBcast = true
	r.tracef("leader in term %d, no-op at index %d", r.term, r.log.lastIndex())
}

// ---- elections -------------------------------------------------------------

func (r *Raft) handlePreVote(m Message) {
	// Grant only if the sender would be campaigning in a term newer than
	// ours, its log is at least as up to date, and we have no reason to
	// believe a leader is alive. None of this changes our term, vote or timer.
	noLeader := r.lead == "" || r.electionElapsed >= r.cfg.ElectionTicks
	if r.role == Leader {
		noLeader = false
	}
	grant := m.Term > r.term && noLeader && r.log.isUpToDate(m.LogIndex, m.LogTerm)
	resp := Message{Type: MsgPreVoteResp, To: m.From, Seq: m.Seq}
	if grant {
		resp.Term = m.Term
	} else {
		resp.Term = r.term
		resp.Reject = true
	}
	r.send(resp)
}

func (r *Raft) handlePreVoteResp(m Message) {
	if m.Reject && m.Term > r.term {
		// The responder is in a genuinely higher term. Catch up to it.
		r.becomeFollower(m.Term, "")
		return
	}
	if r.role != PreCandidate {
		return
	}
	if !m.Reject && m.Term != r.term+1 {
		return // a grant for an earlier pre-vote round
	}
	r.recordVote(m.From, !m.Reject)
	switch {
	case r.wonVotes():
		r.becomeCandidate()
	case r.lostVotes():
		r.becomeFollower(r.term, "")
	}
}

func (r *Raft) handleVote(m Message) {
	canVote := r.vote == m.From || (r.vote == "" && r.lead == "")
	if canVote && r.log.isUpToDate(m.LogIndex, m.LogTerm) {
		r.vote = m.From
		r.hsDirty = true
		r.electionElapsed = 0
		// The response is released by Ready, after the vote is durable.
		r.send(Message{Type: MsgVoteResp, To: m.From, Term: r.term, Seq: m.Seq})
		return
	}
	r.send(Message{Type: MsgVoteResp, To: m.From, Term: r.term, Seq: m.Seq, Reject: true})
}

func (r *Raft) handleVoteResp(m Message) {
	if r.role != Candidate {
		return
	}
	r.recordVote(m.From, !m.Reject)
	switch {
	case r.wonVotes():
		r.becomeLeader()
	case r.lostVotes():
		r.becomeFollower(r.term, "")
	}
}

// recordVote keeps the first answer from each peer. A duplicated or retried
// response cannot be counted twice.
func (r *Raft) recordVote(from string, granted bool) {
	if _, ok := r.votes[from]; !ok {
		r.votes[from] = granted
	}
}

func (r *Raft) wonVotes() bool {
	granted := 0
	for _, g := range r.votes {
		if g {
			granted++
		}
	}
	return granted >= r.quorum
}

func (r *Raft) lostVotes() bool {
	rejected := 0
	for _, g := range r.votes {
		if !g {
			rejected++
		}
	}
	return rejected > len(r.peers)-r.quorum
}

// ---- replication: follower side --------------------------------------------

func (r *Raft) handleAppend(m Message) {
	resp := Message{Type: MsgAppResp, To: m.From, Term: r.term, Seq: m.Seq, LogIndex: m.LogIndex}
	if r.role == Leader {
		// Two leaders in one term would violate election safety. Refuse
		// rather than corrupt state.
		r.tracef("leader received AppendEntries from %s in its own term %d", m.From, r.term)
		resp.Reject = true
		r.send(resp)
		return
	}
	if r.role != Follower {
		r.becomeFollower(r.term, m.From)
	}
	r.lead = m.From
	r.electionElapsed = 0

	for i, e := range m.Entries {
		if e.Index != m.LogIndex+1+uint64(i) {
			resp.Reject = true
			resp.ConflictIndex = r.commit + 1
			r.send(resp)
			return
		}
	}

	if m.LogIndex < r.commit {
		// Everything up to our commit index is already known to match any
		// legitimate leader, so there is nothing to check or change there.
		// Tell the leader to continue from the commit index.
		resp.MatchIndex = r.commit
		r.send(resp)
		return
	}

	if !r.log.matches(m.LogIndex, m.LogTerm) {
		resp.Reject = true
		resp.ConflictIndex, resp.ConflictTerm = r.conflictHint(m.LogIndex)
		r.send(resp)
		return
	}

	// Skip entries we already hold. Truncate only at the first real
	// conflict: a short or delayed request must not cut off a longer suffix
	// that a newer request already delivered.
	for i, e := range m.Entries {
		if r.log.matches(e.Index, e.Term) {
			continue
		}
		if e.Index <= r.commit {
			panic(fmt.Sprintf("raft: %s asked to overwrite committed entry %d (commit %d)", r.id, e.Index, r.commit))
		}
		r.log.truncateFrom(e.Index)
		r.log.append(m.Entries[i:]...)
		break
	}

	// Only the prefix this request proved to match may be committed. Entries
	// beyond it could still be a stale suffix from an older leader.
	lastNew := m.LogIndex + uint64(len(m.Entries))
	if c := min(m.Commit, lastNew); c > r.commit {
		r.commit = c
	}
	resp.MatchIndex = lastNew
	// If our last entry is from the leader's own term, our whole log matches
	// the leader's: only that leader creates entries in its term, we can
	// only have received them from it, and it never removes its own
	// entries. Saying so lets the leader learn about entries we already
	// hold when the acknowledgement for them was lost, without resending.
	if r.log.lastTerm() == m.Term {
		resp.MatchIndex = r.log.lastIndex()
	}
	r.send(resp)
}

// conflictHint describes why prevIndex did not match, so the leader can skip
// back a whole term at a time instead of one index per round trip.
func (r *Raft) conflictHint(prevIndex uint64) (index, term uint64) {
	if prevIndex > r.log.lastIndex() {
		return r.log.lastIndex() + 1, 0
	}
	term, _ = r.log.term(prevIndex)
	index = prevIndex
	for index > r.log.firstIndex() {
		if t, ok := r.log.term(index - 1); !ok || t != term {
			break
		}
		index--
	}
	return index, term
}

func (r *Raft) handleSnapshot(m Message) {
	resp := Message{Type: MsgSnapResp, To: m.From, Term: r.term, Seq: m.Seq}
	if r.role == Leader {
		resp.Reject = true
		r.send(resp)
		return
	}
	if r.role != Follower {
		r.becomeFollower(r.term, m.From)
	}
	r.lead = m.From
	r.electionElapsed = 0

	if m.SnapIndex <= r.commit {
		// A stale snapshot. Installing it would roll applied state back, so
		// ignore it and report where we actually are.
		resp.MatchIndex = r.commit
		r.send(resp)
		return
	}
	snap := SnapshotMeta{Index: m.SnapIndex, Term: m.SnapTerm}
	if r.log.matches(snap.Index, snap.Term) {
		// Our log already contains the snapshot's last entry, so whatever
		// follows it is consistent with the leader and can be kept.
		//
		// The stored log is told to keep its suffix only if that entry has
		// actually reached storage. If it is still waiting in this Ready,
		// storage does not have it yet: the stored log is discarded and the
		// entries after the snapshot are written fresh, from this Ready.
		r.pendingSnapKeep = snap.Index <= r.log.stable
		r.log.compactTo(snap.Index)
	} else {
		r.pendingSnapKeep = false
		r.log.restore(snap)
	}
	r.commit = snap.Index
	r.applied = snap.Index
	r.pendingSnap = &snap
	resp.MatchIndex = snap.Index
	r.send(resp)
}

// ---- replication: leader side ----------------------------------------------

func (r *Raft) bcastAppend() {
	for _, p := range r.peers {
		if p != r.id && !r.prs[p].inflight {
			r.sendAppend(p)
		}
	}
}

// bcastHeartbeat runs every heartbeat interval. A heartbeat is an
// AppendEntries request carrying whatever the follower still lacks, which is
// nothing when it is caught up. It doubles as the retransmission timer: a
// batch whose acknowledgement never arrived is simply sent again here.
func (r *Raft) bcastHeartbeat() {
	for _, p := range r.peers {
		if p == r.id {
			continue
		}
		if r.prs[p].snapshotting {
			// Position (0, 0) matches every log. The follower answers with
			// its commit index, which also tells us if the snapshot turned
			// out to be unnecessary.
			r.send(Message{Type: MsgApp, To: p, Term: r.term, Commit: r.commit})
			continue
		}
		r.sendAppend(p)
	}
}

func (r *Raft) sendAppend(to string) {
	pr := r.prs[to]
	if pr.snapshotting {
		return
	}
	prev := pr.next - 1
	prevTerm, ok := r.log.term(prev)
	if !ok {
		// The entries this follower needs were compacted away. The transport
		// streams the current snapshot file and fills in its position.
		pr.snapshotting = true
		pr.snapshotAge = 0
		pr.inflight = false
		r.send(Message{Type: MsgSnap, To: to, Term: r.term})
		return
	}
	ents := r.log.slice(pr.next, r.log.lastIndex(), r.cfg.MaxEntriesPerAppend, r.cfg.MaxBytesPerAppend)
	r.send(Message{Type: MsgApp, To: to, Term: r.term, LogIndex: prev, LogTerm: prevTerm, Entries: ents, Commit: r.commit})
	if len(ents) > 0 {
		pr.inflight = true
	}
}

func (r *Raft) handleAppendResp(m Message) {
	if r.role != Leader {
		return
	}
	pr := r.prs[m.From]
	pr.recentActive = true

	if m.Reject {
		// Act on a rejection only if it refers to the position we would send
		// next. Otherwise it answers an older request and following it could
		// move next backwards past entries the follower has since accepted.
		if pr.snapshotting || m.LogIndex != pr.next-1 {
			return
		}
		next := m.ConflictIndex
		if m.ConflictTerm != 0 {
			if idx := r.lastIndexOfTerm(m.ConflictTerm); idx != 0 {
				next = idx + 1
			}
		}
		next = min(next, m.LogIndex)
		next = max(next, pr.match+1, 1)
		pr.next = next
		pr.inflight = false
		r.sendAppend(m.From)
		return
	}

	// A success is safe to apply in any order: match only moves forward.
	if m.MatchIndex <= pr.match {
		// Nothing new: a duplicate, a response that arrived out of order,
		// or the answer to an empty heartbeat. It must not trigger another
		// send. If every response did, each duplicated or delayed response
		// would start one more request-response chain alongside the
		// existing ones, and under steady load the traffic would grow
		// without bound.
		return
	}
	pr.match = m.MatchIndex
	if pr.next < pr.match+1 {
		pr.next = pr.match + 1
	}
	pr.inflight = false
	if pr.snapshotting {
		if _, ok := r.log.term(pr.match); !ok {
			return // still behind the compaction point; the snapshot is needed
		}
		pr.snapshotting = false
	}
	if r.maybeCommit() {
		r.needBcast = true
	}
	if pr.next <= r.log.lastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleSnapshotResp(m Message) {
	if r.role != Leader {
		return
	}
	pr := r.prs[m.From]
	pr.recentActive = true
	if m.Reject {
		return
	}
	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
	}
	pr.next = pr.match + 1
	pr.snapshotting = false
	pr.inflight = false
	if r.maybeCommit() {
		r.needBcast = true
	}
	r.sendAppend(m.From)
}

// maybeCommit advances the commit index to the highest index stored on a
// quorum, provided that entry is from the current term. Entries from earlier
// terms become committed only as part of that prefix: counting replicas for
// an old-term entry directly is unsafe (Raft paper, figure 8).
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		if p == r.id {
			matches = append(matches, r.log.stable)
		} else {
			matches = append(matches, r.prs[p].match)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	n := matches[r.quorum-1]
	if n <= r.commit {
		return false
	}
	if t, ok := r.log.term(n); !ok || t != r.term {
		return false
	}
	r.commit = n
	return true
}

func (r *Raft) lastIndexOfTerm(term uint64) uint64 {
	for i := r.log.lastIndex(); i >= r.log.firstIndex(); i-- {
		t, _ := r.log.term(i)
		if t == term {
			return i
		}
		if t < term {
			break
		}
	}
	return 0
}

// quorumActive reports whether a quorum, counting this node, has responded
// since the last check.
func (r *Raft) quorumActive() bool {
	active := 1
	for _, pr := range r.prs {
		if pr.recentActive {
			active++
		}
	}
	return active >= r.quorum
}

// ---- helpers ---------------------------------------------------------------

func (r *Raft) send(m Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func (r *Raft) isPeer(id string) bool {
	i := sort.SearchStrings(r.peers, id)
	return i < len(r.peers) && r.peers[i] == id
}

func (r *Raft) resetElectionTimeout() {
	r.electionTimeout = r.cfg.ElectionTicks + r.cfg.Rand(r.cfg.ElectionTicks)
}

func (r *Raft) tracef(format string, args ...any) {
	if r.cfg.Trace != nil {
		r.cfg.Trace("%s: "+format, append([]any{r.id}, args...)...)
	}
}

// LastIndex returns the index of the last entry in the log.
func (r *Raft) LastIndex() uint64 { return r.log.lastIndex() }
