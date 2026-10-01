package raft

import (
	"fmt"
	"testing"
)

// ---- helpers ---------------------------------------------------------------

func peerIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	return ids
}

// newNode builds node n1 of an n-node cluster. Rand always returns 0, so the
// election timeout is exactly ElectionTicks (10).
func newNode(t *testing.T, n int, st InitialState, mod func(*Config)) *Raft {
	t.Helper()
	cfg := Config{
		ID:             "n1",
		Peers:          peerIDs(n),
		ElectionTicks:  10,
		HeartbeatTicks: 2,
		PreVote:        true,
		CheckQuorum:    true,
		Rand:           func(int) int { return 0 },
	}
	if mod != nil {
		mod(&cfg)
	}
	r, err := New(cfg, st)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

// ents builds a log starting at index 1 with the given terms.
func ents(terms ...uint64) []Entry {
	out := make([]Entry, len(terms))
	for i, term := range terms {
		out[i] = Entry{Index: uint64(i + 1), Term: term, Type: EntryCommand, Data: []byte{byte(i + 1)}}
	}
	return out
}

// withLog is the recovered state of a node holding the given log.
func withLog(term uint64, terms ...uint64) InitialState {
	return InitialState{HardState: HardState{Term: term}, Entries: ents(terms...)}
}

// drain processes one Ready the way a driver would and returns it.
func drain(r *Raft) Ready {
	rd := r.Ready()
	r.Advance()
	return rd
}

// drainAll processes Ready until the node is idle and returns every message.
func drainAll(r *Raft) []Message {
	var msgs []Message
	for r.HasReady() {
		msgs = append(msgs, drain(r).Messages...)
	}
	return msgs
}

// makeLeader runs n1 through a full election, with every peer saying yes.
func makeLeader(t *testing.T, r *Raft) {
	t.Helper()
	for i := 0; r.role == Follower && i < 100; i++ {
		r.Tick()
	}
	for _, p := range r.peers {
		if p != r.id && r.role == PreCandidate {
			r.Step(Message{Type: MsgPreVoteResp, From: p, Term: r.term + 1})
		}
	}
	for _, p := range r.peers {
		if p != r.id && r.role == Candidate {
			r.Step(Message{Type: MsgVoteResp, From: p, Term: r.term})
		}
	}
	if r.role != Leader {
		t.Fatalf("node did not become leader, role is %s", r.role)
	}
	drainAll(r)
}

func only(t *testing.T, msgs []Message, typ MessageType) Message {
	t.Helper()
	var found []Message
	for _, m := range msgs {
		if m.Type == typ {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one %s message, got %d in %+v", typ, len(found), msgs)
	}
	return found[0]
}

func logTerms(r *Raft) []uint64 {
	var out []uint64
	for i := r.log.firstIndex(); i <= r.log.lastIndex(); i++ {
		term, _ := r.log.term(i)
		out = append(out, term)
	}
	return out
}

func equalTerms(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---- elections -------------------------------------------------------------

func TestVoteComparesLastTermBeforeLength(t *testing.T) {
	tests := []struct {
		name               string
		voterLog           []uint64
		candIndex, candLog uint64
		wantGrant          bool
	}{
		{"candidate has higher last term but shorter log", []uint64{1, 1, 1}, 1, 2, true},
		{"candidate has lower last term but longer log", []uint64{1, 2}, 5, 1, false},
		{"same last term, candidate longer", []uint64{1, 2}, 3, 2, true},
		{"same last term, same length", []uint64{1, 2}, 2, 2, true},
		{"same last term, candidate shorter", []uint64{1, 2, 2}, 2, 2, false},
		{"empty voter log accepts anything", nil, 0, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newNode(t, 3, withLog(2, tc.voterLog...), nil)
			r.Step(Message{Type: MsgVote, From: "n2", Term: 3, LogIndex: tc.candIndex, LogTerm: tc.candLog})
			resp := only(t, drain(r).Messages, MsgVoteResp)
			if got := !resp.Reject; got != tc.wantGrant {
				t.Fatalf("grant = %v, want %v", got, tc.wantGrant)
			}
		})
	}
}

func TestVotesOnlyOncePerTerm(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)

	r.Step(Message{Type: MsgVote, From: "n2", Term: 5})
	if resp := only(t, drain(r).Messages, MsgVoteResp); resp.Reject {
		t.Fatal("first request in the term should be granted")
	}
	r.Step(Message{Type: MsgVote, From: "n3", Term: 5})
	if resp := only(t, drain(r).Messages, MsgVoteResp); !resp.Reject {
		t.Fatal("a second candidate in the same term must be refused")
	}
	// A retry from the candidate we already voted for gets the same answer.
	r.Step(Message{Type: MsgVote, From: "n2", Term: 5})
	if resp := only(t, drain(r).Messages, MsgVoteResp); resp.Reject {
		t.Fatal("a repeated request from the same candidate should be granted again")
	}
	if r.vote != "n2" {
		t.Fatalf("vote = %q, want n2", r.vote)
	}
}

func TestVoteSurvivesRestartAndBlocksSecondVote(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)
	r.Step(Message{Type: MsgVote, From: "n2", Term: 5})
	rd := drain(r)

	// "Crash" and recover from exactly what the Ready said to persist.
	r2 := newNode(t, 3, InitialState{HardState: rd.HardState}, nil)
	r2.Step(Message{Type: MsgVote, From: "n3", Term: 5})
	if resp := only(t, drain(r2).Messages, MsgVoteResp); !resp.Reject {
		t.Fatal("after a restart the node must remember it already voted in term 5")
	}
}

func TestVoteIsInSameReadyAsItsHardState(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)
	r.Step(Message{Type: MsgVote, From: "n2", Term: 5})
	rd := r.Ready()
	if !rd.HardStateChanged || rd.HardState != (HardState{Term: 5, Vote: "n2"}) {
		t.Fatalf("Ready must carry the new hard state with the vote response, got %+v changed=%v", rd.HardState, rd.HardStateChanged)
	}
	if !rd.MustSync() {
		t.Fatal("a granted vote must be synced before the response is sent")
	}
	only(t, rd.Messages, MsgVoteResp)
}

func TestCandidatePersistsTermAndSelfVoteWithItsRequests(t *testing.T) {
	r := newNode(t, 3, InitialState{HardState: HardState{Term: 4}}, func(c *Config) { c.PreVote = false })
	for i := 0; i < 10; i++ {
		if r.HasReady() {
			t.Fatalf("unexpected Ready before the election timeout (tick %d)", i)
		}
		r.Tick()
	}
	rd := r.Ready()
	if r.role != Candidate {
		t.Fatalf("role = %s, want candidate", r.role)
	}
	if !rd.HardStateChanged || rd.HardState != (HardState{Term: 5, Vote: "n1"}) {
		t.Fatalf("hard state = %+v changed=%v, want term 5 voted for n1", rd.HardState, rd.HardStateChanged)
	}
	votes := 0
	for _, m := range rd.Messages {
		if m.Type == MsgVote && m.Term == 5 {
			votes++
		}
	}
	if votes != 2 {
		t.Fatalf("want 2 vote requests in the same Ready, got %d", votes)
	}
}

func TestDuplicateVoteResponsesCountOnce(t *testing.T) {
	r := newNode(t, 5, InitialState{}, func(c *Config) { c.PreVote = false })
	for r.role == Follower {
		r.Tick()
	}
	drain(r)
	// Quorum is 3. Self plus n2 is 2, no matter how often n2 answers.
	for i := 0; i < 5; i++ {
		r.Step(Message{Type: MsgVoteResp, From: "n2", Term: r.term})
	}
	if r.role != Candidate {
		t.Fatalf("role = %s after duplicated votes from one peer, want candidate", r.role)
	}
	r.Step(Message{Type: MsgVoteResp, From: "n3", Term: r.term})
	if r.role != Leader {
		t.Fatalf("role = %s after a real third vote, want leader", r.role)
	}
}

func TestFirstVoteAnswerFromAPeerWins(t *testing.T) {
	r := newNode(t, 3, InitialState{}, func(c *Config) { c.PreVote = false })
	for r.role == Follower {
		r.Tick()
	}
	r.Step(Message{Type: MsgVoteResp, From: "n2", Term: r.term, Reject: true})
	r.Step(Message{Type: MsgVoteResp, From: "n2", Term: r.term})
	if r.role == Leader {
		t.Fatal("a peer that rejected cannot later be counted as a grant in the same election")
	}
}

func TestCandidateSteppingDownOnMajorityRejection(t *testing.T) {
	r := newNode(t, 3, InitialState{}, func(c *Config) { c.PreVote = false })
	for r.role == Follower {
		r.Tick()
	}
	term := r.term
	r.Step(Message{Type: MsgVoteResp, From: "n2", Term: term, Reject: true})
	r.Step(Message{Type: MsgVoteResp, From: "n3", Term: term, Reject: true})
	if r.role != Follower || r.term != term || r.vote != "n1" {
		t.Fatalf("role=%s term=%d vote=%q, want follower in the same term still holding its self-vote", r.role, r.term, r.vote)
	}
}

func TestHigherTermMessageMakesLeaderFollow(t *testing.T) {
	for _, typ := range []MessageType{MsgVote, MsgVoteResp, MsgApp, MsgAppResp, MsgSnapResp} {
		t.Run(typ.String(), func(t *testing.T) {
			r := newNode(t, 3, InitialState{}, nil)
			makeLeader(t, r)
			term := r.term
			r.Step(Message{Type: typ, From: "n2", Term: term + 3, Reject: typ.IsResponse()})
			if r.role != Follower || r.term != term+3 {
				t.Fatalf("role=%s term=%d, want follower in term %d", r.role, r.term, term+3)
			}
			wantLead := ""
			if typ == MsgApp {
				wantLead = "n2"
			}
			if r.lead != wantLead {
				t.Fatalf("leader = %q, want %q", r.lead, wantLead)
			}
			if rd := r.Ready(); !rd.HardStateChanged || rd.HardState.Term != term+3 {
				t.Fatalf("the new term must be persisted, got %+v changed=%v", rd.HardState, rd.HardStateChanged)
			}
		})
	}
}

func TestStaleTermRequestsGetCurrentTermBack(t *testing.T) {
	tests := []struct {
		req  MessageType
		resp MessageType
	}{{MsgVote, MsgVoteResp}, {MsgApp, MsgAppResp}, {MsgSnap, MsgSnapResp}}
	for _, tc := range tests {
		t.Run(tc.req.String(), func(t *testing.T) {
			r := newNode(t, 3, withLog(7, 1, 1), nil)
			r.electionElapsed = 4
			r.Step(Message{Type: tc.req, From: "n2", Term: 6, Seq: 42, SnapIndex: 9, SnapTerm: 6,
				LogIndex: 2, LogTerm: 1, Entries: []Entry{{Index: 3, Term: 6}}})
			rd := drain(r)
			resp := only(t, rd.Messages, tc.resp)
			if !resp.Reject || resp.Term != 7 || resp.Seq != 42 {
				t.Fatalf("response = %+v, want a rejection carrying term 7 and seq 42", resp)
			}
			if r.log.lastIndex() != 2 || rd.Snapshot != nil || r.lead != "" {
				t.Fatal("a stale request must not change the log, install a snapshot or set a leader")
			}
			if r.electionElapsed != 4 {
				t.Fatal("a stale request must not reset the election timer")
			}
		})
	}
}

func TestElectionTimerResetOnlyByLegitimateEvents(t *testing.T) {
	setup := func(t *testing.T) *Raft {
		r := newNode(t, 3, withLog(2, 1, 2, 2), nil)
		for i := 0; i < 6; i++ {
			r.Tick()
		}
		return r
	}

	t.Run("rejected vote request does not reset", func(t *testing.T) {
		r := setup(t)
		// Higher term, but the candidate's log is behind ours.
		r.Step(Message{Type: MsgVote, From: "n2", Term: 3, LogIndex: 1, LogTerm: 1})
		if resp := only(t, drain(r).Messages, MsgVoteResp); !resp.Reject {
			t.Fatal("expected the vote to be rejected")
		}
		if r.term != 3 {
			t.Fatalf("term = %d, the higher term must still be adopted", r.term)
		}
		if r.electionElapsed != 6 {
			t.Fatalf("electionElapsed = %d, want 6: a refused candidate must not postpone our own election", r.electionElapsed)
		}
	})
	t.Run("pre-vote request does not reset", func(t *testing.T) {
		r := setup(t)
		r.Step(Message{Type: MsgPreVote, From: "n2", Term: 3, LogIndex: 3, LogTerm: 2})
		if r.electionElapsed != 6 {
			t.Fatalf("electionElapsed = %d, want 6", r.electionElapsed)
		}
	})
	t.Run("granted vote resets", func(t *testing.T) {
		r := setup(t)
		r.Step(Message{Type: MsgVote, From: "n2", Term: 3, LogIndex: 3, LogTerm: 2})
		if r.electionElapsed != 0 {
			t.Fatalf("electionElapsed = %d, want 0", r.electionElapsed)
		}
	})
	t.Run("append from the current leader resets even when it is rejected", func(t *testing.T) {
		r := setup(t)
		r.Step(Message{Type: MsgApp, From: "n2", Term: 2, LogIndex: 9, LogTerm: 2})
		if resp := only(t, drain(r).Messages, MsgAppResp); !resp.Reject {
			t.Fatal("expected the append to be rejected for a missing previous entry")
		}
		if r.electionElapsed != 0 || r.lead != "n2" {
			t.Fatalf("electionElapsed=%d lead=%q, want 0 and n2: the leader is alive even if our log is behind", r.electionElapsed, r.lead)
		}
	})
}

// ---- PreVote and check-quorum ----------------------------------------------

func TestPreCandidateDoesNotTouchDurableState(t *testing.T) {
	r := newNode(t, 3, InitialState{HardState: HardState{Term: 4}}, nil)
	for i := 0; i < 10; i++ {
		r.Tick()
	}
	if r.role != PreCandidate {
		t.Fatalf("role = %s, want pre-candidate", r.role)
	}
	rd := drain(r)
	if rd.HardStateChanged || r.term != 4 || r.vote != "" {
		t.Fatalf("a pre-candidate changed durable state: term=%d vote=%q changed=%v", r.term, r.vote, rd.HardStateChanged)
	}
	for _, m := range rd.Messages {
		if m.Type != MsgPreVote || m.Term != 5 {
			t.Fatalf("unexpected message %+v, want PreVote for prospective term 5", m)
		}
	}
	if len(rd.Messages) != 2 {
		t.Fatalf("want 2 pre-vote requests, got %d", len(rd.Messages))
	}
}

func TestPreVoteRequestIsNotARealHigherTerm(t *testing.T) {
	r := newNode(t, 3, InitialState{HardState: HardState{Term: 4, Vote: "n3"}}, nil)
	r.Step(Message{Type: MsgPreVote, From: "n2", Term: 9})
	rd := drain(r)
	if r.term != 4 || r.vote != "n3" || rd.HardStateChanged {
		t.Fatalf("PreVote changed term or vote: term=%d vote=%q", r.term, r.vote)
	}
	resp := only(t, rd.Messages, MsgPreVoteResp)
	if resp.Reject || resp.Term != 9 {
		t.Fatalf("response = %+v, want a grant echoing prospective term 9", resp)
	}
}

func TestPreVoteRejectedWhileLeaderIsHealthy(t *testing.T) {
	// Rand returns 9, so this node's own election timeout is 19 ticks.
	r := newNode(t, 3, InitialState{HardState: HardState{Term: 4}}, func(c *Config) {
		c.Rand = func(int) int { return 9 }
	})
	r.Step(Message{Type: MsgApp, From: "n3", Term: 4}) // establishes n3 as leader
	drain(r)

	r.Step(Message{Type: MsgPreVote, From: "n2", Term: 5})
	resp := only(t, drain(r).Messages, MsgPreVoteResp)
	if !resp.Reject || resp.Term != 4 {
		t.Fatalf("response = %+v, want a rejection with our real term 4", resp)
	}

	// After a full base election timeout without hearing from the leader,
	// the node is willing to let someone else try.
	for i := 0; i < 10; i++ {
		r.Tick()
	}
	if r.role != Follower {
		t.Fatalf("role = %s, the node should still be a follower", r.role)
	}
	r.Step(Message{Type: MsgPreVote, From: "n2", Term: 5})
	if resp := only(t, drain(r).Messages, MsgPreVoteResp); resp.Reject {
		t.Fatal("pre-vote should be granted once the leader has been silent for an election timeout")
	}
}

func TestLeaderRejectsPreVote(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)
	makeLeader(t, r)
	r.Step(Message{Type: MsgPreVote, From: "n2", Term: r.term + 1, LogIndex: 99, LogTerm: 99})
	if resp := only(t, drain(r).Messages, MsgPreVoteResp); !resp.Reject {
		t.Fatal("an active leader must not grant pre-votes")
	}
	if r.role != Leader {
		t.Fatal("a pre-vote request must not depose the leader")
	}
}

func TestRealHigherTermVoteIsHonouredDespiteHealthyLeader(t *testing.T) {
	// There is deliberately no rule that ignores real higher terms while a
	// leader looks healthy. PreVote keeps disruptive candidates from getting
	// this far; if one does, the term still wins.
	r := newNode(t, 3, InitialState{HardState: HardState{Term: 4}}, nil)
	r.Step(Message{Type: MsgApp, From: "n3", Term: 4})
	drain(r)
	r.Step(Message{Type: MsgVote, From: "n2", Term: 5})
	resp := only(t, drain(r).Messages, MsgVoteResp)
	if resp.Reject || r.term != 5 || r.vote != "n2" || r.lead != "" {
		t.Fatalf("term=%d vote=%q lead=%q reject=%v, want term 5 voted for n2", r.term, r.vote, r.lead, resp.Reject)
	}
}

func TestPreVoteRejectionWithHigherTermUpdatesTerm(t *testing.T) {
	r := newNode(t, 3, InitialState{HardState: HardState{Term: 4}}, nil)
	for r.role == Follower {
		r.Tick()
	}
	r.Step(Message{Type: MsgPreVoteResp, From: "n2", Term: 8, Reject: true})
	if r.role != Follower || r.term != 8 {
		t.Fatalf("role=%s term=%d, want follower in term 8", r.role, r.term)
	}
}

func TestStalePreVoteGrantIsIgnored(t *testing.T) {
	r := newNode(t, 5, InitialState{HardState: HardState{Term: 6}}, nil)
	for r.role == Follower {
		r.Tick()
	}
	// Grants for an older pre-vote round (prospective term 5) must not count
	// toward this round (prospective term 7).
	r.Step(Message{Type: MsgPreVoteResp, From: "n2", Term: 5})
	r.Step(Message{Type: MsgPreVoteResp, From: "n3", Term: 5})
	if r.role != PreCandidate {
		t.Fatalf("role = %s, stale grants must not complete the pre-vote", r.role)
	}
}

func TestCheckQuorum(t *testing.T) {
	t.Run("steps down without a quorum", func(t *testing.T) {
		r := newNode(t, 3, InitialState{}, nil)
		makeLeader(t, r)
		term := r.term
		for i := 0; i < 10; i++ {
			r.Tick()
			drainAll(r)
		}
		if r.role != Follower || r.lead != "" || r.term != term {
			t.Fatalf("role=%s lead=%q term=%d, want a leaderless follower in the same term", r.role, r.lead, r.term)
		}
		if _, _, err := r.Propose([]byte("x")); err != ErrNotLeader {
			t.Fatalf("Propose error = %v, want ErrNotLeader", err)
		}
	})
	t.Run("stays leader while a quorum answers", func(t *testing.T) {
		r := newNode(t, 3, InitialState{}, nil)
		makeLeader(t, r)
		for i := 0; i < 50; i++ {
			r.Tick()
			for _, m := range drainAll(r) {
				if m.Type == MsgApp && m.To == "n2" {
					r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, LogIndex: m.LogIndex, MatchIndex: m.LogIndex + uint64(len(m.Entries))})
				}
			}
		}
		if r.role != Leader {
			t.Fatalf("role = %s, want leader: one of two followers is a quorum with the leader", r.role)
		}
	})
	t.Run("disabled", func(t *testing.T) {
		r := newNode(t, 3, InitialState{}, func(c *Config) { c.CheckQuorum = false })
		makeLeader(t, r)
		for i := 0; i < 50; i++ {
			r.Tick()
			drainAll(r)
		}
		if r.role != Leader {
			t.Fatalf("role = %s, want leader", r.role)
		}
	})
}

// ---- leader replication state ----------------------------------------------

func TestNewLeaderInitialisesProgressAndAppendsNoop(t *testing.T) {
	r := newNode(t, 3, withLog(2, 1, 2), nil)
	makeLeader(t, r)
	if r.log.lastIndex() != 3 {
		t.Fatalf("last index = %d, want 3 (two old entries plus the no-op)", r.log.lastIndex())
	}
	noop := r.log.entries[len(r.log.entries)-1]
	if noop.Type != EntryNoop || noop.Term != r.term || len(noop.Data) != 0 {
		t.Fatalf("last entry = %+v, want an empty no-op in term %d", noop, r.term)
	}
	for _, p := range []string{"n2", "n3"} {
		pr := r.prs[p]
		if pr.match != 0 {
			t.Fatalf("%s match = %d, want 0: nothing is known about the follower yet", p, pr.match)
		}
		if pr.next != 3 {
			t.Fatalf("%s next = %d, want 3 (leader's last index + 1 at election time)", p, pr.next)
		}
	}
	if r.commit != 0 {
		t.Fatalf("commit = %d, want 0 until a quorum stores the no-op", r.commit)
	}
}

func TestLeaderCommitsOldTermEntriesOnlyThroughCurrentTerm(t *testing.T) {
	// Figure 8 of the Raft paper. n1 leads term 3 with two entries from
	// earlier terms that are already on a quorum.
	r := newNode(t, 3, withLog(2, 1, 2), nil)
	makeLeader(t, r)
	if r.term != 3 {
		t.Fatalf("term = %d, want 3", r.term)
	}

	r.Step(Message{Type: MsgAppResp, From: "n2", Term: 3, MatchIndex: 2})
	if r.commit != 0 {
		t.Fatalf("commit = %d: an entry from term 2 was committed by counting replicas in term 3", r.commit)
	}

	// Once the term-3 no-op is on a quorum, everything before it commits too.
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: 3, MatchIndex: 3})
	if r.commit != 3 {
		t.Fatalf("commit = %d, want 3", r.commit)
	}
	rd := drain(r)
	if len(rd.CommittedEntries) != 3 || rd.CommittedEntries[0].Index != 1 || rd.CommittedEntries[2].Type != EntryNoop {
		t.Fatalf("committed entries = %+v, want indexes 1..3 in order ending in the no-op", rd.CommittedEntries)
	}
}

func TestEntriesHeldByAQuorumOfFollowersCommitBeforeTheLeadersWriteFinishes(t *testing.T) {
	// The commit rule counts durable copies, whoever holds them. If both
	// followers of a three-node cluster hold an entry before the leader's own
	// write has finished, the entry is on a quorum of disks and is committed
	// without the leader's copy. The node's event loop does not create this
	// situation today (it writes before it sends), but the rule must not
	// depend on that.
	r := newNode(t, 3, InitialState{}, nil)
	makeLeader(t, r)
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: 1})
	r.Step(Message{Type: MsgAppResp, From: "n3", Term: r.term, MatchIndex: 1})
	drainAll(r)

	index, _, _ := r.Propose([]byte("a"))
	rd := r.Ready() // the leader's write is in progress: no Advance yet
	if len(rd.Entries) != 1 {
		t.Fatalf("Ready.Entries = %+v", rd.Entries)
	}
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: index})
	if r.commit >= index {
		t.Fatalf("commit = %d with one follower and an unfinished leader write: no quorum of durable copies", r.commit)
	}
	r.Step(Message{Type: MsgAppResp, From: "n3", Term: r.term, MatchIndex: index})
	if r.commit != index {
		t.Fatalf("commit = %d, want %d: two of three nodes hold the entry durably", r.commit, index)
	}
	r.Advance()
}

func TestLeaderCountsItsOwnCopyOnlyAfterPersisting(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)
	makeLeader(t, r)
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: 1})
	if r.commit != 1 {
		t.Fatalf("commit = %d, want 1 after the no-op reaches a quorum", r.commit)
	}
	drainAll(r)

	index, _, err := r.Propose([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	// n3 somehow acknowledges before the leader's own write has finished.
	// The leader's copy is not durable yet, so with n2 silent there is no
	// quorum of durable copies.
	r.Step(Message{Type: MsgAppResp, From: "n3", Term: r.term, MatchIndex: index})
	if r.commit >= index {
		t.Fatalf("commit = %d: the leader counted its own unpersisted entry", r.commit)
	}
	rd := r.Ready()
	if len(rd.Entries) != 1 || rd.Entries[0].Index != index {
		t.Fatalf("Ready.Entries = %+v, want the proposed entry", rd.Entries)
	}
	if len(rd.CommittedEntries) != 0 {
		t.Fatal("nothing may be applied before the entry is durable on the leader")
	}
	r.Advance() // the driver has synced the entry
	if r.commit != index {
		t.Fatalf("commit = %d, want %d after Advance", r.commit, index)
	}
}

func TestStaleTermResponsesAreIgnored(t *testing.T) {
	r := newNode(t, 3, withLog(2, 1, 2), nil)
	makeLeader(t, r)
	before := *r.prs["n2"]
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term - 1, MatchIndex: 99})
	r.Step(Message{Type: MsgSnapResp, From: "n2", Term: r.term - 1, MatchIndex: 99})
	r.Step(Message{Type: MsgVoteResp, From: "n2", Term: r.term - 1})
	if *r.prs["n2"] != before || r.commit != 0 || r.role != Leader {
		t.Fatalf("a response from an old term changed leader state: %+v", *r.prs["n2"])
	}
}

func TestStaleRejectionDoesNotMoveNextBackwards(t *testing.T) {
	r := newNode(t, 3, withLog(1, 1, 1, 1, 1, 1), nil)
	makeLeader(t, r) // no-op at 6, next = 6
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: 6})
	drainAll(r)
	if r.prs["n2"].next != 7 {
		t.Fatalf("next = %d, want 7", r.prs["n2"].next)
	}
	// A rejection for a much older probe arrives late.
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, Reject: true, LogIndex: 2, ConflictIndex: 1})
	if pr := r.prs["n2"]; pr.next != 7 || pr.match != 6 {
		t.Fatalf("stale rejection changed progress to next=%d match=%d", pr.next, pr.match)
	}
}

func TestSuccessResponsesApplyInAnyOrder(t *testing.T) {
	r := newNode(t, 3, withLog(1, 1, 1, 1), nil)
	makeLeader(t, r)
	for _, match := range []uint64{4, 2, 4, 1, 3} {
		r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: match})
	}
	if pr := r.prs["n2"]; pr.match != 4 || pr.next != 5 {
		t.Fatalf("match=%d next=%d, want 4 and 5", pr.match, pr.next)
	}
}

func TestRejectionUsesConflictHint(t *testing.T) {
	tests := []struct {
		name          string
		conflictIndex uint64
		conflictTerm  uint64
		wantNext      uint64
	}{
		{"follower log is short", 3, 0, 3},
		{"leader has the conflicting term: retry after its last entry of that term", 2, 2, 5},
		{"leader lacks the conflicting term: skip that whole term", 4, 3, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Leader log terms: 1 1 2 2 4 4 4, then the no-op at 8 in term 5.
			r := newNode(t, 3, withLog(4, 1, 1, 2, 2, 4, 4, 4), nil)
			makeLeader(t, r)
			if r.prs["n2"].next != 8 {
				t.Fatalf("next = %d, want 8", r.prs["n2"].next)
			}
			r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, Reject: true, LogIndex: 7,
				ConflictIndex: tc.conflictIndex, ConflictTerm: tc.conflictTerm})
			if got := r.prs["n2"].next; got != tc.wantNext {
				t.Fatalf("next = %d, want %d", got, tc.wantNext)
			}
			app := only(t, drainAll(r), MsgApp)
			if app.LogIndex != tc.wantNext-1 {
				t.Fatalf("retry uses previous index %d, want %d", app.LogIndex, tc.wantNext-1)
			}
		})
	}
}

func TestProposalsLeaveInOneBatchWithOneOutstandingPerFollower(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)
	makeLeader(t, r)
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: 1})
	r.Step(Message{Type: MsgAppResp, From: "n3", Term: r.term, MatchIndex: 1})
	drainAll(r)

	for i := 0; i < 3; i++ {
		if _, _, err := r.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	msgs := drain(r).Messages
	if len(msgs) != 2 {
		t.Fatalf("want one append per follower, got %d messages", len(msgs))
	}
	for _, m := range msgs {
		if m.Type != MsgApp || len(m.Entries) != 3 || m.LogIndex != 1 {
			t.Fatalf("message = %+v, want one append carrying all three proposals after index 1", m)
		}
	}
	// A further proposal waits: each follower already has a batch in flight.
	if _, _, err := r.Propose([]byte("later")); err != nil {
		t.Fatal(err)
	}
	if msgs := drain(r).Messages; len(msgs) != 0 {
		t.Fatalf("sent %d messages while a batch was outstanding, want 0", len(msgs))
	}
	// The acknowledgement releases the next batch to that follower only.
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: 4})
	app := only(t, drainAll(r), MsgApp)
	if app.To != "n2" || len(app.Entries) != 1 || app.LogIndex != 4 {
		t.Fatalf("message = %+v, want the remaining entry sent to n2", app)
	}
}

func TestProposeErrors(t *testing.T) {
	r := newNode(t, 3, InitialState{}, func(c *Config) { c.MaxUncommittedEntries = 3 })
	if _, _, err := r.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("follower Propose error = %v, want ErrNotLeader", err)
	}
	makeLeader(t, r) // the no-op is uncommitted entry number one
	for i := 0; i < 2; i++ {
		if _, _, err := r.Propose([]byte("x")); err != nil {
			t.Fatalf("proposal %d: %v", i, err)
		}
	}
	last := r.log.lastIndex()
	if _, _, err := r.Propose([]byte("x")); err != ErrOverloaded {
		t.Fatalf("Propose error = %v, want ErrOverloaded with 3 uncommitted entries", err)
	}
	if r.log.lastIndex() != last {
		t.Fatal("a rejected proposal must not be appended")
	}
}

func TestSingleNodeClusterElectsAndCommits(t *testing.T) {
	r := newNode(t, 1, InitialState{}, nil)
	for i := 0; i < 10; i++ {
		r.Tick()
	}
	if r.role != Leader {
		t.Fatalf("role = %s, want leader", r.role)
	}
	index, term, err := r.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	var applied []Entry
	for r.HasReady() {
		applied = append(applied, drain(r).CommittedEntries...)
	}
	if len(applied) != 2 || applied[1].Index != index || applied[1].Term != term {
		t.Fatalf("applied = %+v, want the no-op then the proposal", applied)
	}
}

// ---- follower log handling -------------------------------------------------

func TestFollowerChecksPreviousEntry(t *testing.T) {
	tests := []struct {
		name              string
		prevIndex         uint64
		prevTerm          uint64
		wantReject        bool
		wantConflictIndex uint64
		wantConflictTerm  uint64
	}{
		{"matching previous entry", 3, 2, false, 0, 0},
		{"empty prefix always matches", 0, 0, false, 0, 0},
		{"term mismatch reports first index of the conflicting term", 3, 3, true, 3, 2},
		{"term mismatch inside a longer run", 2, 9, true, 1, 1},
		{"previous index beyond the log", 7, 2, true, 4, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newNode(t, 3, withLog(3, 1, 1, 2), nil)
			r.Step(Message{Type: MsgApp, From: "n2", Term: 3, LogIndex: tc.prevIndex, LogTerm: tc.prevTerm,
				Entries: []Entry{{Index: tc.prevIndex + 1, Term: 3}}})
			resp := only(t, drain(r).Messages, MsgAppResp)
			if resp.Reject != tc.wantReject {
				t.Fatalf("reject = %v, want %v", resp.Reject, tc.wantReject)
			}
			if resp.LogIndex != tc.prevIndex {
				t.Fatalf("response echoes index %d, want %d", resp.LogIndex, tc.prevIndex)
			}
			if tc.wantReject {
				if resp.ConflictIndex != tc.wantConflictIndex || resp.ConflictTerm != tc.wantConflictTerm {
					t.Fatalf("hint = (index %d, term %d), want (%d, %d)", resp.ConflictIndex, resp.ConflictTerm, tc.wantConflictIndex, tc.wantConflictTerm)
				}
				if !equalTerms(logTerms(r), []uint64{1, 1, 2}) {
					t.Fatalf("a rejected append changed the log to %v", logTerms(r))
				}
			} else if resp.MatchIndex != tc.prevIndex+1 {
				t.Fatalf("match = %d, want %d", resp.MatchIndex, tc.prevIndex+1)
			}
		})
	}
}

func TestFollowerReplacesConflictingUncommittedSuffix(t *testing.T) {
	st := withLog(2, 1, 1, 2, 2)
	st.Commit = 2
	r := newNode(t, 3, st, nil)
	r.Step(Message{Type: MsgApp, From: "n2", Term: 3, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 3, Data: []byte("x")}, {Index: 4, Term: 3}, {Index: 5, Term: 3}}})
	rd := r.Ready()
	if !equalTerms(logTerms(r), []uint64{1, 1, 3, 3, 3}) {
		t.Fatalf("log terms = %v, want [1 1 3 3 3]", logTerms(r))
	}
	// The driver learns about the replacement from the first index.
	if len(rd.Entries) != 3 || rd.Entries[0].Index != 3 {
		t.Fatalf("Ready.Entries = %+v, want the three replacement entries starting at index 3", rd.Entries)
	}
	if resp := only(t, rd.Messages, MsgAppResp); resp.Reject || resp.MatchIndex != 5 {
		t.Fatalf("response = %+v, want success with match 5", resp)
	}
}

func TestFollowerNeverRewritesCommittedPrefix(t *testing.T) {
	st := withLog(2, 1, 1, 2, 2)
	st.Commit = 3
	r := newNode(t, 3, st, nil)
	// A confused or delayed request tries to rewrite index 2 onwards.
	r.Step(Message{Type: MsgApp, From: "n2", Term: 2, LogIndex: 1, LogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 2}, {Index: 3, Term: 2}}})
	rd := r.Ready()
	if !equalTerms(logTerms(r), []uint64{1, 1, 2, 2}) || len(rd.Entries) != 0 {
		t.Fatalf("committed prefix was modified: log terms %v", logTerms(r))
	}
	if resp := only(t, rd.Messages, MsgAppResp); resp.Reject || resp.MatchIndex != 3 {
		t.Fatalf("response = %+v, want success pointing the leader at commit index 3", resp)
	}
}

func TestShortOrDelayedAppendDoesNotTruncate(t *testing.T) {
	r := newNode(t, 3, withLog(1, 1, 1, 1, 1, 1), nil)

	// A request from the leader of term 2 that covers only a prefix of what
	// we hold. Entries 3..5 are from term 1; whether they survive in the new
	// leader's log is not yet known.
	r.Step(Message{Type: MsgApp, From: "n2", Term: 2, LogIndex: 1, LogTerm: 1, Entries: []Entry{{Index: 2, Term: 1}}})
	rd := drain(r)
	if r.log.lastIndex() != 5 || len(rd.Entries) != 0 {
		t.Fatalf("last index = %d, want 5: matching entries must not cut the suffix", r.log.lastIndex())
	}
	if resp := only(t, rd.Messages, MsgAppResp); resp.MatchIndex != 2 {
		t.Fatalf("match = %d, want 2: only the checked prefix is acknowledged", resp.MatchIndex)
	}

	// A heartbeat is an append with no entries.
	r.Step(Message{Type: MsgApp, From: "n2", Term: 2, LogIndex: 3, LogTerm: 1})
	rd = drain(r)
	if r.log.lastIndex() != 5 {
		t.Fatalf("last index = %d, want 5: a heartbeat must not truncate", r.log.lastIndex())
	}
	if resp := only(t, rd.Messages, MsgAppResp); resp.MatchIndex != 3 {
		t.Fatalf("match = %d, want 3", resp.MatchIndex)
	}
}

func TestFollowerReportsWholeLogWhenItEndsInTheLeadersTerm(t *testing.T) {
	// The follower already holds entries 1..5, all from term 1, and the
	// leader of term 1 sends a delayed request covering only up to index 2.
	// Everything the follower holds from term 1 came from this leader, so
	// it can vouch for all five.
	r := newNode(t, 3, withLog(1, 1, 1, 1, 1, 1), nil)
	r.Step(Message{Type: MsgApp, From: "n2", Term: 1, LogIndex: 1, LogTerm: 1, Entries: []Entry{{Index: 2, Term: 1}}})
	rd := drain(r)
	if resp := only(t, rd.Messages, MsgAppResp); resp.Reject || resp.MatchIndex != 5 {
		t.Fatalf("response = %+v, want match 5", resp)
	}
	// The commit index still follows only what this request verified.
	r.Step(Message{Type: MsgApp, From: "n2", Term: 1, LogIndex: 1, LogTerm: 1, Commit: 5})
	if r.commit != 1 {
		t.Fatalf("commit = %d, want 1", r.commit)
	}
}

func TestResponsesWithoutProgressDoNotTriggerSends(t *testing.T) {
	// Regression test for traffic amplification found by the linearizability
	// simulation: with duplicated messages and steady client load, every
	// repeated acknowledgement started another replication round, and the
	// number of messages in flight grew exponentially.
	r := newNode(t, 3, InitialState{}, nil)
	makeLeader(t, r)
	for i := 0; i < 5; i++ {
		r.Propose([]byte{byte(i)})
	}
	drainAll(r)

	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: 3})
	first := drainAll(r)
	if app := only(t, first, MsgApp); app.To != "n2" || app.LogIndex != 3 {
		t.Fatalf("an advancing response should release the next batch, got %+v", first)
	}
	// The same acknowledgement again, an older one, and one for an empty
	// heartbeat: none of them is news.
	for _, match := range []uint64{3, 3, 2, 0, 3} {
		r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: match})
		for _, m := range drainAll(r) {
			if m.To == "n2" {
				t.Fatalf("a response with match %d (already known: 3) triggered %+v", match, m)
			}
		}
	}
}

func TestHeartbeatResendsAnUnacknowledgedBatch(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)
	makeLeader(t, r) // the no-op has been sent to both followers
	r.Propose([]byte("a"))
	// A batch is already outstanding to each follower, so the proposal
	// waits its turn.
	if msgs := drainAll(r); len(msgs) != 0 {
		t.Fatalf("sent %+v while a batch was outstanding", msgs)
	}
	// The outstanding request or its acknowledgement is lost. Nothing
	// happens until the heartbeat timer, which sends everything the follower
	// is not known to have.
	r.Tick()
	if msgs := drainAll(r); len(msgs) != 0 {
		t.Fatalf("resent before the heartbeat interval: %+v", msgs)
	}
	r.Tick()
	var resent *Message
	for _, m := range drainAll(r) {
		if m.To == "n2" && m.Type == MsgApp {
			resent = &m
		}
	}
	if resent == nil || len(resent.Entries) != 2 || resent.LogIndex != 0 {
		t.Fatalf("heartbeat = %+v, want the no-op and the proposal resent from index 1", resent)
	}
}

func TestFollowerCommitIsBoundedByVerifiedPrefix(t *testing.T) {
	// Entries 4 and 5 are leftovers from a deposed leader. The new leader's
	// heartbeat verifies only up to index 3 but reports commit index 5.
	r := newNode(t, 3, withLog(1, 1, 1, 1, 1, 1), nil)
	r.Step(Message{Type: MsgApp, From: "n2", Term: 2, LogIndex: 3, LogTerm: 1, Commit: 5})
	rd := drain(r)
	if r.commit != 3 {
		t.Fatalf("commit = %d, want 3: unverified entries 4 and 5 may not match the leader", r.commit)
	}
	if len(rd.CommittedEntries) != 3 || rd.CommittedEntries[2].Index != 3 {
		t.Fatalf("committed entries = %+v, want indexes 1..3", rd.CommittedEntries)
	}
	// The leader's commit index also caps it from the other side.
	r.Step(Message{Type: MsgApp, From: "n2", Term: 2, LogIndex: 3, LogTerm: 1, Commit: 4,
		Entries: []Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}}})
	if r.commit != 4 {
		t.Fatalf("commit = %d, want 4", r.commit)
	}
}

func TestMalformedAppendIsRejected(t *testing.T) {
	r := newNode(t, 3, withLog(1, 1, 1), nil)
	r.Step(Message{Type: MsgApp, From: "n2", Term: 1, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 1}, {Index: 7, Term: 1}}})
	if resp := only(t, drain(r).Messages, MsgAppResp); !resp.Reject {
		t.Fatal("entries with a gap in their indexes must be rejected")
	}
	if r.log.lastIndex() != 2 {
		t.Fatalf("last index = %d, want 2", r.log.lastIndex())
	}
}

func TestAppliesInOrderInBoundedBatches(t *testing.T) {
	st := withLog(1, 1, 1, 1, 1, 1)
	r := newNode(t, 3, st, func(c *Config) { c.MaxApplyBatch = 2 })
	r.Step(Message{Type: MsgApp, From: "n2", Term: 1, LogIndex: 5, LogTerm: 1, Commit: 5})
	next := uint64(1)
	for r.HasReady() {
		rd := drain(r)
		if len(rd.CommittedEntries) > 2 {
			t.Fatalf("batch of %d exceeds MaxApplyBatch", len(rd.CommittedEntries))
		}
		for _, e := range rd.CommittedEntries {
			if e.Index != next {
				t.Fatalf("applied index %d, want %d", e.Index, next)
			}
			next++
		}
		if r.applied > r.commit {
			t.Fatalf("applied %d exceeds commit %d", r.applied, r.commit)
		}
	}
	if next != 6 {
		t.Fatalf("applied up to %d, want 5", next-1)
	}
}

func TestUnknownPeerIsIgnored(t *testing.T) {
	r := newNode(t, 3, InitialState{}, nil)
	r.Step(Message{Type: MsgVote, From: "intruder", Term: 50})
	r.Step(Message{Type: MsgApp, From: "n1", Term: 50}) // a message claiming to be from ourselves
	if r.term != 0 || r.HasReady() {
		t.Fatalf("term=%d hasReady=%v, a message from outside the voter set must have no effect", r.term, r.HasReady())
	}
}

// ---- snapshots and compaction ----------------------------------------------

func TestStaleSnapshotIsIgnored(t *testing.T) {
	st := withLog(2, 1, 1, 2, 2, 2)
	st.Commit = 4
	r := newNode(t, 3, st, nil)
	drainAll(r)
	r.Step(Message{Type: MsgSnap, From: "n2", Term: 2, SnapIndex: 3, SnapTerm: 2})
	rd := drain(r)
	if rd.Snapshot != nil {
		t.Fatal("a snapshot behind the commit index must not be installed")
	}
	if r.commit != 4 || r.applied != 4 || r.log.lastIndex() != 5 {
		t.Fatalf("state rolled back: commit=%d applied=%d last=%d", r.commit, r.applied, r.log.lastIndex())
	}
	if resp := only(t, rd.Messages, MsgSnapResp); resp.Reject || resp.MatchIndex != 4 {
		t.Fatalf("response = %+v, want the follower's real position 4", resp)
	}
}

func TestSnapshotKeepsMatchingSuffix(t *testing.T) {
	st := withLog(1, 1, 1, 1, 1, 1)
	st.Commit = 1
	r := newNode(t, 3, st, nil)
	drainAll(r)
	r.Step(Message{Type: MsgSnap, From: "n2", Term: 1, SnapIndex: 3, SnapTerm: 1})
	rd := drain(r)
	if rd.Snapshot == nil || *rd.Snapshot != (SnapshotMeta{Index: 3, Term: 1}) {
		t.Fatalf("Ready.Snapshot = %+v, want index 3 term 1", rd.Snapshot)
	}
	if r.log.firstIndex() != 4 || r.log.lastIndex() != 5 {
		t.Fatalf("log = %d..%d, want 4..5: entries after a matching snapshot point are kept", r.log.firstIndex(), r.log.lastIndex())
	}
	if r.commit != 3 || r.applied != 3 {
		t.Fatalf("commit=%d applied=%d, want 3 and 3", r.commit, r.applied)
	}
	if len(rd.CommittedEntries) != 0 {
		t.Fatal("entries covered by the snapshot must not be applied again")
	}
}

func TestSnapshotAmongUnpersistedEntriesKeepsTheRest(t *testing.T) {
	// Regression test for a bug the chaos simulation found. Entries 1..4
	// arrive and, before the driver has persisted them, a snapshot covering
	// index 2 arrives. Entries 3 and 4 must still be handed to the driver.
	r := newNode(t, 3, InitialState{HardState: HardState{Term: 1}}, nil)
	r.Step(Message{Type: MsgApp, From: "n2", Term: 1, Entries: ents(1, 1, 1, 1)})
	r.Step(Message{Type: MsgSnap, From: "n2", Term: 1, SnapIndex: 2, SnapTerm: 1})
	rd := drain(r)
	if rd.Snapshot == nil || rd.Snapshot.Index != 2 {
		t.Fatalf("Ready.Snapshot = %+v, want index 2", rd.Snapshot)
	}
	if len(rd.Entries) != 2 || rd.Entries[0].Index != 3 || rd.Entries[1].Index != 4 {
		t.Fatalf("Ready.Entries = %+v, want entries 3 and 4", rd.Entries)
	}
	if r.log.stable != 4 {
		t.Fatalf("stable = %d, want 4", r.log.stable)
	}
}

func TestSnapshotDiscardsConflictingLog(t *testing.T) {
	r := newNode(t, 3, withLog(1, 1, 1, 1, 1, 1), nil)
	r.Step(Message{Type: MsgSnap, From: "n2", Term: 3, SnapIndex: 3, SnapTerm: 2})
	rd := drain(r)
	if rd.Snapshot == nil {
		t.Fatal("snapshot should be installed")
	}
	if r.log.lastIndex() != 3 || r.log.lastTerm() != 2 || len(r.log.entries) != 0 {
		t.Fatalf("log last = (%d, term %d) with %d entries, want (3, term 2) and none", r.log.lastIndex(), r.log.lastTerm(), len(r.log.entries))
	}
	// The log continues from the snapshot.
	r.Step(Message{Type: MsgApp, From: "n2", Term: 3, LogIndex: 3, LogTerm: 2, Entries: []Entry{{Index: 4, Term: 3}}})
	if resp := only(t, drain(r).Messages, MsgAppResp); resp.Reject || resp.MatchIndex != 4 {
		t.Fatalf("append after snapshot = %+v, want success with match 4", resp)
	}
}

func TestLeaderSendsSnapshotWhenEntriesAreCompacted(t *testing.T) {
	st := withLog(1, 1, 1, 1, 1, 1)
	st.Commit = 5
	r := newNode(t, 3, st, nil)
	drainAll(r)
	makeLeader(t, r) // no-op at 6
	r.CompactTo(4)

	if _, ok := r.Term(3); ok {
		t.Fatal("index 3 should be compacted")
	}
	if term, ok := r.Term(4); !ok || term != 1 {
		t.Fatalf("Term(4) = %d,%v: the compaction boundary keeps its term for consistency checks", term, ok)
	}
	if r.log.firstIndex() != 5 || r.log.lastIndex() != 6 {
		t.Fatalf("log = %d..%d, want 5..6", r.log.firstIndex(), r.log.lastIndex())
	}

	// n2 is far behind: its next entry no longer exists.
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, Reject: true, LogIndex: 5, ConflictIndex: 2})
	snap := only(t, drainAll(r), MsgSnap)
	if snap.To != "n2" || !r.prs["n2"].snapshotting {
		t.Fatalf("want a snapshot to n2, got %+v", snap)
	}

	// n3 is only slightly behind and is served from the log.
	r.Step(Message{Type: MsgAppResp, From: "n3", Term: r.term, Reject: true, LogIndex: 5, ConflictIndex: 5})
	app := only(t, drainAll(r), MsgApp)
	if app.To != "n3" || app.LogIndex != 4 || app.LogTerm != 1 || len(app.Entries) != 2 {
		t.Fatalf("append to n3 = %+v, want entries 5..6 after the boundary (4, term 1)", app)
	}

	// While the snapshot is outstanding n2 gets heartbeats but no entries.
	for i := 0; i < 2; i++ {
		r.Tick()
	}
	for _, m := range drainAll(r) {
		if m.To == "n2" && (m.Type != MsgApp || len(m.Entries) != 0 || m.LogIndex != 0) {
			t.Fatalf("message to snapshotting follower = %+v, want an empty heartbeat at position 0", m)
		}
	}

	// The follower reports the snapshot installed; replication resumes after it.
	r.Step(Message{Type: MsgSnapResp, From: "n2", Term: r.term, MatchIndex: 5})
	if pr := r.prs["n2"]; pr.snapshotting || pr.match != 5 || pr.next != 6 {
		t.Fatalf("progress after snapshot = %+v, want match 5 next 6", *pr)
	}
	app = only(t, drainAll(r), MsgApp)
	if app.To != "n2" || app.LogIndex != 5 || len(app.Entries) != 1 {
		t.Fatalf("append after snapshot = %+v, want entry 6", app)
	}
}

func TestFailedSnapshotIsRetried(t *testing.T) {
	st := withLog(1, 1, 1, 1)
	st.Commit = 3
	r := newNode(t, 3, st, nil)
	drainAll(r)
	makeLeader(t, r)
	r.CompactTo(3)
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, Reject: true, LogIndex: 3, ConflictIndex: 1})
	only(t, drainAll(r), MsgSnap)

	r.ReportSnapshot("n2", false)
	r.Tick()
	r.Tick() // heartbeat interval
	var snaps int
	for _, m := range drainAll(r) {
		if m.Type == MsgSnap && m.To == "n2" {
			snaps++
		}
	}
	if snaps != 1 {
		t.Fatalf("want the snapshot to be sent again after a reported failure, got %d", snaps)
	}
}

func TestSnapshotWithUnknownOutcomeIsRetried(t *testing.T) {
	// Regression test for a stall the chaos simulation found: the follower
	// installed a snapshot but its answer was lost, the leader compacted
	// further, and nothing ever started a second transfer.
	st := withLog(1, 1, 1, 1)
	st.Commit = 3
	r := newNode(t, 3, st, func(c *Config) {
		c.SnapshotRetryTicks = 30
		c.CheckQuorum = false
	})
	drainAll(r)
	makeLeader(t, r)
	r.CompactTo(3)
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, Reject: true, LogIndex: 3, ConflictIndex: 1})
	only(t, drainAll(r), MsgSnap)

	snaps := 0
	for i := 0; i < 29; i++ {
		r.Tick()
		for _, m := range drainAll(r) {
			if m.Type == MsgSnap {
				snaps++
			}
		}
	}
	if snaps != 0 {
		t.Fatalf("snapshot resent %d times before the retry timer expired", snaps)
	}
	for i := 0; i < 3; i++ {
		r.Tick()
		for _, m := range drainAll(r) {
			if m.Type == MsgSnap && m.To == "n2" {
				snaps++
			}
		}
	}
	if snaps != 1 {
		t.Fatalf("want exactly one retry after the timer expired, got %d", snaps)
	}
}

func TestSnapshotUnnecessaryAfterAll(t *testing.T) {
	// The follower turns out to be ahead of the compaction point (the
	// rejection that triggered the snapshot was about an old probe).
	st := withLog(1, 1, 1, 1, 1)
	st.Commit = 4
	r := newNode(t, 3, st, nil)
	drainAll(r)
	makeLeader(t, r)
	r.CompactTo(3)
	r.prs["n2"].next = 2
	r.sendAppend("n2")
	only(t, drainAll(r), MsgSnap)

	// The heartbeat response reveals the follower's commit index is 4.
	r.Step(Message{Type: MsgAppResp, From: "n2", Term: r.term, MatchIndex: 4})
	if pr := r.prs["n2"]; pr.snapshotting || pr.next != 5 {
		t.Fatalf("progress = %+v, want normal replication from 5", *pr)
	}
}

// ---- recovery --------------------------------------------------------------

func TestNewRejectsInconsistentRecoveredState(t *testing.T) {
	base := Config{ID: "n1", Peers: peerIDs(3), ElectionTicks: 10, HeartbeatTicks: 2, Rand: func(int) int { return 0 }}
	tests := []struct {
		name string
		st   InitialState
	}{
		{"gap in the log", InitialState{HardState: HardState{Term: 1}, Entries: []Entry{{Index: 1, Term: 1}, {Index: 3, Term: 1}}}},
		{"log does not continue from the snapshot", InitialState{HardState: HardState{Term: 1}, Snapshot: SnapshotMeta{Index: 5, Term: 1}, Entries: []Entry{{Index: 8, Term: 1}}}},
		{"commit beyond the log", InitialState{HardState: HardState{Term: 1}, Commit: 4, Entries: ents(1, 1)}},
		{"log term newer than the stored term", InitialState{HardState: HardState{Term: 1}, Entries: ents(1, 2)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(base, tc.st); err == nil {
				t.Fatal("New accepted inconsistent state")
			}
		})
	}
}

func TestRecoveredNodeDoesNotApplyBeyondKnownCommit(t *testing.T) {
	// Five entries are on disk but only two are known to be committed. The
	// rest may still be replaced by a new leader, so they must not be applied.
	st := withLog(1, 1, 1, 1, 1, 1)
	st.Commit = 2
	r := newNode(t, 3, st, nil)
	var applied []uint64
	for r.HasReady() {
		for _, e := range drain(r).CommittedEntries {
			applied = append(applied, e.Index)
		}
	}
	if len(applied) != 2 || applied[1] != 2 {
		t.Fatalf("applied %v after recovery, want only indexes 1 and 2", applied)
	}
}

func TestConfigValidation(t *testing.T) {
	ok := Config{ID: "n1", Peers: peerIDs(3), ElectionTicks: 10, HeartbeatTicks: 2, Rand: func(int) int { return 0 }}
	tests := []struct {
		name string
		mod  func(*Config)
	}{
		{"empty id", func(c *Config) { c.ID = "" }},
		{"self not in peers", func(c *Config) { c.ID = "n9" }},
		{"duplicate peer", func(c *Config) { c.Peers = []string{"n1", "n2", "n2"} }},
		{"election not longer than heartbeat", func(c *Config) { c.ElectionTicks = 2 }},
		{"no heartbeat", func(c *Config) { c.HeartbeatTicks = 0 }},
		{"no rand", func(c *Config) { c.Rand = nil }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ok
			tc.mod(&cfg)
			if _, err := New(cfg, InitialState{}); err == nil {
				t.Fatal("New accepted an invalid config")
			}
		})
	}
	if _, err := New(ok, InitialState{}); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
