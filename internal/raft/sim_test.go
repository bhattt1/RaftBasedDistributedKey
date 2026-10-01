package raft_test

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/sim"
)

// These tests run whole clusters in the deterministic simulator. Each test is
// repeated over many seeds. To replay one failing run exactly:
//
//	RAFT_SIM_SEED=1234 go test ./internal/raft -run 'TestSimChaos'
//
// RAFT_SIM_SEEDS=N changes how many seeds each test tries.

func seeds(t *testing.T, defaultCount int) []int64 {
	t.Helper()
	if s := os.Getenv("RAFT_SIM_SEED"); s != "" {
		seed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("bad RAFT_SIM_SEED %q: %v", s, err)
		}
		return []int64{seed}
	}
	count := defaultCount
	if s := os.Getenv("RAFT_SIM_SEEDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			t.Fatalf("bad RAFT_SIM_SEEDS %q", s)
		}
		count = n
	}
	if testing.Short() && count > 5 {
		count = 5
	}
	out := make([]int64, count)
	for i := range out {
		out[i] = int64(i + 1)
	}
	return out
}

// forSeeds runs fn once per seed as a subtest named after the seed.
func forSeeds(t *testing.T, count int, fn func(t *testing.T, seed int64)) {
	t.Helper()
	for _, seed := range seeds(t, count) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { fn(t, seed) })
	}
}

// h wraps a cluster with test helpers that fail with the seed and a trace.
type h struct {
	t    *testing.T
	c    *sim.Cluster
	seed int64
	next int
	// acked maps a log index to the command acknowledged there.
	acked map[uint64][]byte
	// pending maps a proposal position to its command.
	pending map[[2]uint64][]byte
}

func newH(t *testing.T, cfg sim.Config) *h {
	t.Helper()
	x := &h{t: t, c: sim.New(cfg), seed: cfg.Seed, acked: map[uint64][]byte{}, pending: map[[2]uint64][]byte{}}
	// A proposal is acknowledged when any node applies the exact entry
	// (same index and term) that Propose returned.
	x.c.OnApply = func(_ string, e raft.Entry, _ any) {
		key := [2]uint64{e.Index, e.Term}
		if data, ok := x.pending[key]; ok {
			x.acked[e.Index] = data
			delete(x.pending, key)
		}
	}
	return x
}

func (x *h) fail(format string, args ...any) {
	x.t.Helper()
	x.t.Fatalf("seed %d: %s\n--- nodes ---\n%s--- trace (oldest first) ---\n%s",
		x.seed, fmt.Sprintf(format, args...), x.c.Describe(), strings.Join(x.c.Trace(), "\n"))
}

func (x *h) run(n int) {
	x.t.Helper()
	if err := x.c.Run(n); err != nil {
		x.fail("invariant violated: %v", err)
	}
}

func (x *h) until(max int, what string, cond func() bool) {
	x.t.Helper()
	ok, err := x.c.RunUntil(max, cond)
	if err != nil {
		x.fail("invariant violated: %v", err)
	}
	if !ok {
		x.fail("gave up after %d ticks waiting for: %s", max, what)
	}
}

// waitLeader waits until some node leads and has committed an entry of its
// own term, which is when it can serve requests.
func (x *h) waitLeader() string {
	x.t.Helper()
	x.until(3000, "a leader that has committed its no-op", func() bool {
		id := x.c.Leader()
		if id == "" {
			return false
		}
		// Terms never decrease along the log, so the leader has committed an
		// entry of its own term exactly when the entry at its commit index
		// has that term.
		st := x.c.Status(id)
		return st.Commit > 0 && x.c.CommittedTerm(st.Commit) == st.Term
	})
	return x.c.Leader()
}

// propose submits a fresh command at node id and remembers it if accepted.
func (x *h) propose(id string) bool {
	x.next++
	data := []byte(fmt.Sprintf("cmd-%d", x.next))
	index, term, err := x.c.Propose(id, data)
	if err != nil {
		return false
	}
	x.pending[[2]uint64{index, term}] = data
	return true
}

// commit proposes n commands at the current leader and waits for all of them
// to be acknowledged.
func (x *h) commit(n int) {
	x.t.Helper()
	for i := 0; i < n; i++ {
		before := len(x.acked)
		tries := 0
		x.until(5000, "a proposal to be acknowledged", func() bool {
			if len(x.acked) > before {
				return true
			}
			// Like a client with a retry timer: an earlier attempt may have
			// gone to a leader that has since been deposed.
			if tries%25 == 0 {
				if id := x.c.Leader(); id != "" {
					x.propose(id)
				}
			}
			tries++
			return false
		})
	}
}

// settle heals everything, restarts crashed nodes and waits for the cluster
// to agree, then checks that every acknowledged command is still there.
func (x *h) settle() {
	x.t.Helper()
	x.c.Heal()
	x.c.SetNetwork(0, 1, 0, 0)
	for _, id := range x.c.IDs() {
		x.c.Restart(id)
	}
	x.commit(1)
	x.until(5000, "all nodes to apply everything committed", x.c.Converged)
	x.checkAcked()
	x.checkMachinesAgree()
}

func (x *h) checkAcked() {
	x.t.Helper()
	for index, want := range x.acked {
		got, ok := x.c.CommittedData(index)
		if !ok || !bytes.Equal(got, want) {
			x.fail("acknowledged command %q at index %d was lost (found %q)", want, index, got)
		}
	}
}

func (x *h) checkMachinesAgree() {
	x.t.Helper()
	var ref []byte
	for _, id := range x.c.IDs() {
		if !x.c.Up(id) {
			continue
		}
		state := x.c.Machine(id).Snapshot()
		if ref == nil {
			ref = state
		} else if !bytes.Equal(ref, state) {
			x.fail("state machines differ after convergence (%s)", id)
		}
	}
}

func others(c *sim.Cluster, exclude ...string) []string {
	var out []string
	for _, id := range c.IDs() {
		skip := false
		for _, e := range exclude {
			skip = skip || e == id
		}
		if !skip {
			out = append(out, id)
		}
	}
	return out
}

func base(seed int64, nodes int) sim.Config {
	return sim.Config{Nodes: nodes, Seed: seed, PreVote: true, CheckQuorum: true, MinDelay: 0, MaxDelay: 2}
}

// ---- scenarios -------------------------------------------------------------

func TestSimInitialElection(t *testing.T) {
	for _, nodes := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			forSeeds(t, 30, func(t *testing.T, seed int64) {
				x := newH(t, base(seed, nodes))
				leader := x.waitLeader()
				x.run(200)
				if got := x.c.Leaders(); len(got) != 1 || got[0] != leader {
					t.Fatalf("seed %d: leaders after a quiet period = %v, want only %s", seed, got, leader)
				}
			})
		})
	}
}

func TestSimSplitVotesResolve(t *testing.T) {
	// Without PreVote and with slow, lossy links, candidates often collide.
	// Randomised timeouts must still produce a leader.
	forSeeds(t, 40, func(t *testing.T, seed int64) {
		cfg := base(seed, 5)
		cfg.PreVote = false
		cfg.MaxDelay = 6
		cfg.DropRate = 0.2
		x := newH(t, cfg)
		x.waitLeader()
		x.commit(3)
	})
}

func TestSimReplicationConverges(t *testing.T) {
	forSeeds(t, 20, func(t *testing.T, seed int64) {
		x := newH(t, base(seed, 3))
		x.commit(40)
		x.settle()
		for _, id := range x.c.IDs() {
			if x.c.Applied(id) != x.c.MaxCommit() {
				x.fail("%s applied %d, want %d", id, x.c.Applied(id), x.c.MaxCommit())
			}
		}
	})
}

func TestSimDuplicatedReorderedLossyNetwork(t *testing.T) {
	forSeeds(t, 40, func(t *testing.T, seed int64) {
		cfg := base(seed, 3)
		cfg.MaxDelay = 12 // longer than a heartbeat: responses arrive out of order
		cfg.DropRate = 0.15
		cfg.DupRate = 0.3
		x := newH(t, cfg)
		x.commit(30)
		x.settle()
	})
}

func TestSimFollowerUnavailable(t *testing.T) {
	forSeeds(t, 20, func(t *testing.T, seed int64) {
		x := newH(t, base(seed, 3))
		leader := x.waitLeader()
		follower := others(x.c, leader)[0]
		x.c.Crash(follower)
		x.commit(20) // two of three is still a quorum
		if x.c.Leader() != leader {
			x.fail("leader changed from %s although it kept a quorum", leader)
		}
		x.c.Restart(follower)
		x.until(2000, "the restarted follower to catch up", x.c.Converged)
		x.settle()
	})
}

func TestSimLeaderUnavailable(t *testing.T) {
	forSeeds(t, 30, func(t *testing.T, seed int64) {
		x := newH(t, base(seed, 3))
		x.commit(10)
		old := x.c.Leader()
		oldTerm := x.c.Status(old).Term
		x.c.Crash(old)
		x.until(3000, "the remaining majority to elect a new leader", func() bool {
			id := x.c.Leader()
			return id != "" && id != old && x.c.Status(id).Term > oldTerm
		})
		x.commit(10)
		x.c.Restart(old)
		x.settle()
		if st := x.c.Status(old); st.Role == raft.Leader && st.Term == oldTerm {
			x.fail("the restarted node still leads its old term")
		}
	})
}

func TestSimIsolatedLeaderCannotCommit(t *testing.T) {
	forSeeds(t, 30, func(t *testing.T, seed int64) {
		x := newH(t, base(seed, 5))
		x.commit(5)
		old := x.c.Leader()
		buddy := others(x.c, old)[0]
		majority := others(x.c, old, buddy)
		commitBefore := x.c.Status(old).Commit

		// The old leader keeps one follower: two nodes of five, a minority.
		x.c.Partition([]string{old, buddy}, majority)

		// Requests sent to the isolated leader are accepted into its log
		// but can never be acknowledged.
		var stranded [][2]uint64
		for i := 0; i < 5; i++ {
			if index, term, err := x.c.Propose(old, []byte(fmt.Sprintf("stranded-%d", i))); err == nil {
				stranded = append(stranded, [2]uint64{index, term})
			}
		}
		if len(stranded) == 0 {
			x.fail("the old leader refused proposals immediately; expected it to accept them until check-quorum fires")
		}

		// Check-quorum makes it give up leadership within two base election
		// timeouts, without ever advancing its commit index.
		x.until(40, "the isolated leader to step down", func() bool { return x.c.Status(old).Role != raft.Leader })
		if got := x.c.Status(old).Commit; got != commitBefore {
			x.fail("isolated leader advanced its commit index from %d to %d", commitBefore, got)
		}
		if _, _, err := x.c.Propose(old, []byte("late")); err != raft.ErrNotLeader {
			x.fail("isolated ex-leader accepted a proposal: err=%v", err)
		}

		// Meanwhile the majority side makes progress.
		x.until(3000, "the majority to elect a leader", func() bool {
			id := x.c.Leader()
			return id != "" && id != old && id != buddy
		})
		x.commit(10)

		x.settle()
		// The stranded entries were overwritten, never applied.
		for _, s := range stranded {
			if x.c.CommittedTerm(s[0]) == s[1] {
				x.fail("entry (%d, term %d) proposed to the isolated leader was committed", s[0], s[1])
			}
		}
	})
}

func TestSimQuorumLossThenRecovery(t *testing.T) {
	forSeeds(t, 20, func(t *testing.T, seed int64) {
		x := newH(t, base(seed, 3))
		x.commit(10)
		leader := x.c.Leader()
		down := others(x.c, leader)
		for _, id := range down {
			x.c.Crash(id)
		}
		before := x.c.MaxCommit()
		x.c.Propose(leader, []byte("no-quorum"))
		x.run(300)
		if x.c.MaxCommit() != before {
			x.fail("commit index advanced from %d to %d with one node of three alive", before, x.c.MaxCommit())
		}
		if len(x.c.Leaders()) != 0 {
			x.fail("a node still claims leadership without a quorum: %v", x.c.Leaders())
		}
		for _, id := range down {
			x.c.Restart(id)
		}
		x.commit(5)
		x.settle()
	})
}

func TestSimPartitionedFollowerDoesNotDisrupt(t *testing.T) {
	// With PreVote, a node cut off from the cluster cannot raise its term, so
	// it has nothing to depose the leader with when it comes back.
	forSeeds(t, 20, func(t *testing.T, seed int64) {
		x := newH(t, base(seed, 3))
		x.commit(5)
		leader := x.c.Leader()
		term := x.c.Status(leader).Term
		lonely := others(x.c, leader)[0]
		x.c.Partition([]string{lonely}, others(x.c, lonely))
		x.run(500)
		if got := x.c.Status(lonely).Term; got != term {
			x.fail("isolated follower raised its term from %d to %d despite PreVote", term, got)
		}
		x.c.Heal()
		x.commit(5)
		if x.c.Leader() != leader || x.c.Status(leader).Term != term {
			x.fail("leadership changed after the isolated follower rejoined (leader %s, term %d)", x.c.Leader(), x.c.Status(leader).Term)
		}
		x.settle()
	})
}

func TestSimWithoutPreVoteRejoiningNodeForcesElection(t *testing.T) {
	// The contrast case, to show what PreVote buys: without it the isolated
	// node keeps incrementing its term and the cluster must move to a higher
	// term once it is back. Safety still holds.
	forSeeds(t, 10, func(t *testing.T, seed int64) {
		cfg := base(seed, 3)
		cfg.PreVote = false
		x := newH(t, cfg)
		x.commit(5)
		leader := x.c.Leader()
		term := x.c.Status(leader).Term
		lonely := others(x.c, leader)[0]
		x.c.Partition([]string{lonely}, others(x.c, lonely))
		x.run(300)
		if got := x.c.Status(lonely).Term; got <= term {
			x.fail("expected the isolated node to have raised its term above %d, got %d", term, got)
		}
		x.c.Heal()
		x.commit(5)
		if got := x.c.Status(x.c.Leader()).Term; got <= term {
			x.fail("cluster term %d did not move past %d", got, term)
		}
		x.settle()
	})
}

func TestSimAsymmetricLink(t *testing.T) {
	// One follower stops receiving from the leader but everything else
	// works. It starts pre-voting; the other follower still hears the
	// leader and refuses, so the cluster keeps its leader and its term.
	forSeeds(t, 20, func(t *testing.T, seed int64) {
		x := newH(t, base(seed, 3))
		x.commit(5)
		leader := x.c.Leader()
		term := x.c.Status(leader).Term
		deaf := others(x.c, leader)[0]
		x.c.BlockLink(leader, deaf)
		for i := 0; i < 20; i++ {
			x.commit(1)
			x.run(20)
		}
		if x.c.Leader() != leader || x.c.Status(leader).Term != term {
			x.fail("leader or term changed under a one-way link failure (leader %s term %d)", x.c.Leader(), x.c.Status(x.c.Leader()).Term)
		}
		x.settle()
	})
}

func TestSimFollowerCatchesUpFromSnapshot(t *testing.T) {
	forSeeds(t, 20, func(t *testing.T, seed int64) {
		cfg := base(seed, 3)
		cfg.SnapshotEvery = 20
		cfg.SnapshotTrailing = 5
		x := newH(t, cfg)
		leader := x.waitLeader()
		behind := others(x.c, leader)[0]
		x.c.Crash(behind)
		x.commit(100)
		if got := x.c.Status(leader).FirstIndex; got < 50 {
			x.fail("leader log starts at %d, expected compaction well past the crashed follower", got)
		}
		x.c.Restart(behind)
		x.until(3000, "the lagging follower to install a snapshot and catch up", func() bool {
			return x.c.Converged() && x.c.SnapshotIndex(behind) > 0
		})
		if st := x.c.Status(behind); st.FirstIndex <= 1 {
			x.fail("follower log still starts at %d, so it did not catch up through a snapshot", st.FirstIndex)
		}
		x.commit(10)
		x.settle()
	})
}

func TestSimFullClusterRestart(t *testing.T) {
	for _, forget := range []bool{false, true} {
		t.Run(fmt.Sprintf("forgetCommit=%v", forget), func(t *testing.T) {
			forSeeds(t, 20, func(t *testing.T, seed int64) {
				cfg := base(seed, 3)
				cfg.SnapshotEvery = 15
				cfg.SnapshotTrailing = 3
				cfg.ForgetCommit = forget
				x := newH(t, cfg)
				x.commit(40)
				for _, id := range x.c.IDs() {
					x.c.Crash(id)
				}
				for _, id := range x.c.IDs() {
					x.c.Restart(id)
				}
				x.commit(5)
				x.settle()
			})
		})
	}
}

// TestSimChaos throws crashes, partitions, one-way link failures and network
// trouble at a cluster while clients keep proposing, then checks that the
// cluster recovers and nothing acknowledged was lost. The simulator checks
// election safety, leader completeness and state machine safety at every step.
func TestSimChaos(t *testing.T) {
	for _, nodes := range []int{3, 5} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			forSeeds(t, 60, func(t *testing.T, seed int64) {
				cfg := base(seed, nodes)
				cfg.SnapshotEvery = 25
				cfg.SnapshotTrailing = 5
				cfg.PreVote = seed%4 != 0
				cfg.CheckQuorum = seed%5 != 0
				cfg.ForgetCommit = seed%3 == 0
				x := newH(t, cfg)
				rng := x.c.Rand()
				ids := x.c.IDs()

				for step := 0; step < 3000; step++ {
					switch r := rng.Intn(1000); {
					case r < 6:
						x.c.Crash(ids[rng.Intn(len(ids))])
					case r < 16:
						x.c.Restart(ids[rng.Intn(len(ids))])
					case r < 20:
						// Split into two random groups.
						var a, b []string
						for _, id := range ids {
							if rng.Intn(2) == 0 {
								a = append(a, id)
							} else {
								b = append(b, id)
							}
						}
						x.c.Partition(a, b)
					case r < 24:
						x.c.BlockLink(ids[rng.Intn(len(ids))], ids[rng.Intn(len(ids))])
					case r < 32:
						x.c.Heal()
					case r < 36:
						x.c.SetNetwork(0, rng.Intn(15), rng.Float64()*0.3, rng.Float64()*0.3)
					case r < 300:
						// Clients talk to anyone who claims to be leader,
						// including stale leaders on the wrong side of a
						// partition.
						for _, id := range x.c.Leaders() {
							x.propose(id)
						}
					}
					x.run(1)
				}
				x.settle()
				if len(x.acked) == 0 {
					x.fail("no proposal was ever acknowledged; the schedule exercised nothing")
				}
			})
		})
	}
}
