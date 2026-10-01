package linearizability

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/sim"
)

// The tests in this file drive simulated clients against the deterministic
// cluster simulator while it suffers crashes, partitions and message loss,
// record what every client saw, and check the history.
//
// Time is the simulator's event counter, so the recorded order of
// invocations and responses is exact and a failing seed replays exactly:
//
//	LIN_SIM_SEED=42 go test ./tests/linearizability -run TestSimulatedHistories

// kvMachine adapts the real key-value store to the simulator.
type kvMachine struct{ store *kv.Store }

func newKVMachine() sim.StateMachine { return &kvMachine{store: kv.New(kv.DefaultLimits)} }

func (m *kvMachine) Apply(e raft.Entry) any    { return m.store.Apply(e.Index, e.Data) }
func (m *kvMachine) Snapshot() []byte          { return m.store.Snapshot() }
func (m *kvMachine) Restore(data []byte) error { return m.store.Restore(bytes.NewReader(data)) }

// bug selects a deliberately broken client or server behaviour, used to show
// that the checker notices.
type bug int

const (
	noBug bug = iota
	// bugLocalReads answers reads from the memory of any node that believes
	// it is leader, without going through the log.
	bugLocalReads
	// bugFreshIdentityOnRetry retries a mutation under a new request
	// identity, which defeats de-duplication.
	bugFreshIdentityOnRetry
)

// simClient is one closed-loop client: it issues an operation, waits for the
// answer, retries on another node when the attempt is cut off, and gives up
// after too many attempts or too long.
type simClient struct {
	id   int
	name string
	seq  uint64

	active bool
	input  Input
	cmd    kv.Command
	call   int64
	// begun is the tick the operation started; submitted counts attempts a
	// node accepted into its log. Only those can have had any effect.
	begun     int
	submitted int
	// The current attempt: waiting for (index, term) to be applied on node.
	waiting   bool
	node      string
	index     uint64
	term      uint64
	startedAt int
	preferred string
	lastRead  map[string]string // what this client last saw per key
	written   int
}

type run struct {
	t       *testing.T
	c       *sim.Cluster
	rng     *rand.Rand
	bug     bug
	clients []*simClient
	clock   int64
	history []porcupine.Operation
	// Counters, to confirm a run exercised what it claims to.
	completed, unknown, retries, refused int
}

const (
	// attemptTimeoutTicks is how long a client waits for one submitted
	// attempt before trying another node.
	attemptTimeoutTicks = 40
	// maxSubmissions bounds how many times one operation is submitted.
	maxSubmissions = 6
	// opDeadlineTicks bounds one operation as a whole, like a client-side
	// deadline.
	opDeadlineTicks = 400
)

var simKeys = []string{"x", "y", "z"}

func newRun(t *testing.T, seed int64, b bug) *run {
	cfg := sim.Config{
		Nodes: 5, Seed: seed, PreVote: true, CheckQuorum: b != bugLocalReads,
		MinDelay: 0, MaxDelay: 3, SnapshotEvery: 30, SnapshotTrailing: 5,
		NewStateMachine: newKVMachine,
	}
	r := &run{t: t, c: sim.New(cfg), bug: b}
	// Client decisions use their own generator so that changing the client
	// mix does not perturb the cluster's fault schedule for a given seed.
	r.rng = rand.New(rand.NewSource(seed ^ 0x5eed))
	for i := 0; i < 6; i++ {
		r.clients = append(r.clients, &simClient{id: i, name: fmt.Sprintf("client-%d", i), lastRead: map[string]string{}})
	}
	r.c.OnApply = r.onApply
	return r
}

func (r *run) tick() int64 {
	r.clock++
	return r.clock
}

// begin starts a new operation for an idle client.
func (r *run) begin(cl *simClient) {
	key := simKeys[r.rng.Intn(len(simKeys))]
	cl.written++
	value := fmt.Sprintf("%s-%d", cl.name, cl.written) // unique, so reads identify their writer
	var in Input
	switch p := r.rng.Intn(100); {
	case p < 35:
		in = Input{Op: kv.OpRead, Key: key}
	case p < 65:
		in = Input{Op: kv.OpPut, Key: key, Value: value}
	case p < 90:
		// Compare against what this client last read: sometimes right,
		// sometimes stale, which is what makes CAS interesting.
		if last, ok := cl.lastRead[key]; ok {
			in = Input{Op: kv.OpCAS, Key: key, ExpectValue: last, Value: value}
		} else {
			in = Input{Op: kv.OpCAS, Key: key, ExpectAbsent: true, Value: value}
		}
	default:
		in = Input{Op: kv.OpDelete, Key: key}
	}
	cl.active, cl.input, cl.submitted, cl.begun, cl.call = true, in, 0, r.c.Now(), r.tick()
	cl.cmd = kv.Command{Op: in.Op, Key: in.Key}
	switch in.Op {
	case kv.OpPut:
		cl.cmd.Value = []byte(in.Value)
	case kv.OpCAS:
		cl.cmd.Value = []byte(in.Value)
		if in.ExpectAbsent {
			cl.cmd.Expect = kv.ExpectAbsent
		} else {
			cl.cmd.Expect, cl.cmd.ExpectValue = kv.ExpectValue, []byte(in.ExpectValue)
		}
	}
	if in.Op.IsMutation() {
		cl.seq++
		cl.cmd.ClientID, cl.cmd.Seq = cl.name, cl.seq
	}
	r.submit(cl)
}

// submit makes one attempt at the client's current operation.
func (r *run) submit(cl *simClient) {
	if cl.submitted >= maxSubmissions || r.c.Now()-cl.begun > opDeadlineTicks {
		r.giveUp(cl)
		return
	}
	if cl.submitted > 0 && r.bug == bugFreshIdentityOnRetry && cl.cmd.Op.IsMutation() {
		cl.seq++
		cl.cmd.Seq = cl.seq
	}
	ids := r.c.IDs()
	node := cl.preferred
	if node == "" || !r.c.Up(node) {
		node = ids[r.rng.Intn(len(ids))]
	}
	cl.preferred = ""

	if r.bug == bugLocalReads && cl.cmd.Op == kv.OpRead && r.c.Up(node) && r.c.Status(node).Role == raft.Leader {
		// The broken shortcut: trust the role flag and read local memory.
		r.finish(cl, OutputOf(r.localRead(node, cl.cmd)))
		cl.preferred = node
		return
	}
	index, term, err := r.c.Propose(node, cl.cmd.Encode())
	if err != nil {
		// Refused outright (not the leader, or overloaded): nothing was
		// submitted. Try someone else on a later tick.
		cl.waiting = false
		return
	}
	if cl.submitted > 0 {
		r.retries++
	}
	cl.submitted++
	cl.waiting, cl.node, cl.index, cl.term, cl.startedAt = true, node, index, term, r.c.Now()
}

// giveUp ends an operation the client could not complete. If no node ever
// accepted it, it definitely did not happen and leaves no trace in the
// history. Otherwise its outcome is unknown.
func (r *run) giveUp(cl *simClient) {
	if cl.submitted == 0 {
		r.refused++
		cl.active, cl.waiting = false, false
		return
	}
	r.finish(cl, Output{Unknown: true})
}

// localRead evaluates a read against a copy of one node's state.
func (r *run) localRead(node string, cmd kv.Command) kv.Result {
	copyOf := kv.New(kv.DefaultLimits)
	if err := copyOf.Restore(bytes.NewReader(r.c.Machine(node).Snapshot())); err != nil {
		r.t.Fatal(err)
	}
	return copyOf.Apply(copyOf.AppliedIndex()+1, cmd.Encode())
}

func (r *run) finish(cl *simClient, out Output) {
	ret := r.tick()
	if out.Unknown {
		ret = Infinity
		r.unknown++
	} else {
		r.completed++
		if cl.input.Op == kv.OpRead {
			if out.Status == kv.StatusOK {
				cl.lastRead[cl.input.Key] = out.Value
			} else {
				delete(cl.lastRead, cl.input.Key)
			}
		}
	}
	r.history = append(r.history, porcupine.Operation{ClientId: cl.id, Input: cl.input, Call: cl.call, Output: out, Return: ret})
	cl.active, cl.waiting = false, false
}

// onApply completes the client waiting for exactly this entry on this node.
// The same index with a different term means the client's entry was
// overwritten after a leader change; that attempt is over.
func (r *run) onApply(node string, e raft.Entry, result any) {
	for _, cl := range r.clients {
		if !cl.waiting || cl.node != node || cl.index != e.Index {
			continue
		}
		if cl.term != e.Term {
			cl.waiting = false
			continue
		}
		res := result.(kv.Result)
		switch res.Status {
		case kv.StatusOK, kv.StatusNotFound, kv.StatusCASFailed:
			cl.preferred = node
			r.finish(cl, OutputOf(res))
		default:
			r.t.Fatalf("client %s got unexpected status %s for %+v", cl.name, res.Status, cl.cmd)
		}
	}
}

// step advances the cluster one tick and then lets clients act.
func (r *run) step(startNew bool) {
	if err := r.c.Step(); err != nil {
		r.t.Fatalf("raft invariant violated: %v\n%s", err, strings.Join(r.c.Trace(), "\n"))
	}
	for _, cl := range r.clients {
		switch {
		case !cl.active:
			if startNew && r.rng.Intn(4) == 0 {
				r.begin(cl)
			}
		case cl.waiting && (!r.c.Up(cl.node) || r.c.Now()-cl.startedAt > attemptTimeoutTicks):
			// The node died or the attempt timed out. Its outcome is not
			// known; retrying under the same identity is what makes that
			// safe.
			cl.waiting = false
			r.submit(cl)
		case !cl.waiting:
			r.submit(cl)
		}
	}
}

// chaos runs the fault schedule with clients active, then heals the cluster
// and lets every outstanding operation finish or time out.
func (r *run) chaos(ticks int) {
	rng := r.c.Rand()
	ids := r.c.IDs()
	for i := 0; i < ticks; i++ {
		switch p := rng.Intn(1000); {
		case p < 5:
			r.c.Crash(ids[rng.Intn(len(ids))])
		case p < 14:
			r.c.Restart(ids[rng.Intn(len(ids))])
		case p < 19:
			var a, b []string
			for _, id := range ids {
				if rng.Intn(2) == 0 {
					a = append(a, id)
				} else {
					b = append(b, id)
				}
			}
			r.c.Partition(a, b)
		case p < 23:
			// Cut the current leader off on its own: the stale-leader case.
			if l := r.c.Leader(); l != "" {
				var rest []string
				for _, id := range ids {
					if id != l {
						rest = append(rest, id)
					}
				}
				r.c.Partition([]string{l}, rest)
			}
		case p < 27:
			r.c.BlockLink(ids[rng.Intn(len(ids))], ids[rng.Intn(len(ids))])
		case p < 37:
			r.c.Heal()
		case p < 41:
			r.c.SetNetwork(0, rng.Intn(10), rng.Float64()*0.2, rng.Float64()*0.2)
		}
		r.step(true)
	}
	r.c.Heal()
	r.c.SetNetwork(0, 1, 0, 0)
	for _, id := range ids {
		r.c.Restart(id)
	}
	for i := 0; i < 2000; i++ {
		busy := false
		for _, cl := range r.clients {
			busy = busy || cl.active
		}
		if !busy {
			break
		}
		r.step(false)
	}
	for _, cl := range r.clients {
		if cl.active {
			r.giveUp(cl)
		}
	}
}

func simSeeds(t *testing.T, n int) []int64 {
	if s := os.Getenv("LIN_SIM_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("bad LIN_SIM_SEED: %v", err)
		}
		return []int64{v}
	}
	if s := os.Getenv("LIN_SIM_SEEDS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			n = v
		}
	}
	if testing.Short() {
		n = min(n, 5)
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i + 1)
	}
	return out
}

func TestSimulatedHistoriesAreLinearizable(t *testing.T) {
	var ops, completed, unknown, retries int
	seeds := simSeeds(t, 60)
	for _, seed := range seeds {
		r := newRun(t, seed, noBug)
		r.chaos(2500)
		began := time.Now()
		res := Check(r.history, 30*time.Second)
		if took := time.Since(began); took > 5*time.Second {
			t.Logf("seed %d: the check took %v for %d operations (%d unknown)", seed, took, len(r.history), r.unknown)
		}
		switch res.Verdict {
		case porcupine.Ok:
		case porcupine.Unknown:
			// Running out of time is not evidence of anything.
			t.Fatalf("seed %d: the checker timed out on %d operations; the result is inconclusive", seed, len(r.history))
		default:
			t.Fatalf("seed %d: history is NOT linearizable (%d operations)\n%s\n--- cluster trace ---\n%s",
				seed, len(r.history), res.Explanation, strings.Join(r.c.Trace(), "\n"))
		}
		ops, completed, unknown, retries = ops+len(r.history), completed+r.completed, unknown+r.unknown, retries+r.retries
	}
	t.Logf("checked %d operations across %d seeds: %d completed, %d with unknown outcome, %d retried attempts", ops, len(seeds), completed, unknown, retries)
	// Guard against a vacuous pass. Some schedules keep the cluster broken
	// for most of a run, which is fine, but across seeds clients must have
	// got real work done and must have hit the awkward paths.
	if completed/len(seeds) < 200 {
		t.Fatalf("only %d operations completed per seed on average; the schedule starved the clients", completed/len(seeds))
	}
	if len(seeds) >= 5 && (unknown == 0 || retries == 0) {
		t.Fatal("no operation timed out or was retried; the fault schedule did not exercise the interesting paths")
	}
}

// The two tests below break the system on purpose and require the checker to
// notice. They are what justify reading the test above as meaningful.

func TestCheckerCatchesReadsServedFromLocalState(t *testing.T) {
	requireViolation(t, bugLocalReads, "reads answered from a leader's memory without consensus")
}

func TestCheckerCatchesRetriesUnderANewIdentity(t *testing.T) {
	requireViolation(t, bugFreshIdentityOnRetry, "retries that bypass de-duplication")
}

func requireViolation(t *testing.T, b bug, what string) {
	t.Helper()
	const seeds = 40
	illegal := 0
	var example string
	for seed := int64(1); seed <= seeds; seed++ {
		r := newRun(t, seed, b)
		r.chaos(2500)
		res := Check(r.history, 10*time.Second)
		if res.Verdict == porcupine.Illegal {
			illegal++
			if example == "" {
				example = fmt.Sprintf("seed %d:\n%s", seed, res.Explanation)
			}
		}
	}
	if illegal == 0 {
		t.Fatalf("with %s, none of %d seeds produced a history the checker rejected; the harness cannot detect this class of bug", what, seeds)
	}
	t.Logf("with %s, the checker rejected %d of %d histories", what, illegal, seeds)
	if testing.Verbose() {
		// Show one, truncated: the full explanation lists every operation.
		lines := strings.Split(example, "\n")
		t.Logf("example violation (first lines):\n%s", strings.Join(lines[:min(len(lines), 12)], "\n"))
	}
}
