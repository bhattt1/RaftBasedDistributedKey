package node_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/testcluster"
)

// Every test in this package must leave no goroutines behind: a node that is
// stopped has to take its event loop, its snapshot writer and its waiting
// callers with it.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func put(key, value string) kv.Command {
	return kv.Command{Op: kv.OpPut, Key: key, Value: []byte(value)}
}

func get(key string) kv.Command { return kv.Command{Op: kv.OpRead, Key: key} }

func mustDo(t *testing.T, cl *testcluster.Client, cmd kv.Command) kv.Result {
	t.Helper()
	res, err := cl.Do(ctxT(t, 20*time.Second), cmd)
	if err != nil {
		t.Fatalf("%s %q: %v", cmd.Op, cmd.Key, err)
	}
	return res
}

func wantValue(t *testing.T, cl *testcluster.Client, key, value string) {
	t.Helper()
	res := mustDo(t, cl, get(key))
	if res.Status != kv.StatusOK || string(res.Value) != value {
		t.Fatalf("get %q = (%s, %q), want %q", key, res.Status, res.Value, value)
	}
}

// ---- basics ----------------------------------------------------------------

func TestReplicatedOperations(t *testing.T) {
	for _, nodes := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			c := testcluster.New(t, nodes, nil)
			c.WaitLeader()
			cl := c.Client("client")

			if res := mustDo(t, cl, get("a")); res.Status != kv.StatusNotFound {
				t.Fatalf("get of a missing key = %s", res.Status)
			}
			first := mustDo(t, cl, put("a", "1"))
			wantValue(t, cl, "a", "1")

			cas := mustDo(t, cl, kv.Command{Op: kv.OpCAS, Key: "a", Expect: kv.ExpectRevision, ExpectRevision: first.Revision, Value: []byte("2")})
			if cas.Status != kv.StatusOK {
				t.Fatalf("CAS = %s", cas.Status)
			}
			cas = mustDo(t, cl, kv.Command{Op: kv.OpCAS, Key: "a", Expect: kv.ExpectValue, ExpectValue: []byte("1"), Value: []byte("3")})
			if cas.Status != kv.StatusCASFailed {
				t.Fatalf("CAS against an old value = %s, want CAS_FAILED", cas.Status)
			}
			wantValue(t, cl, "a", "2")

			if res := mustDo(t, cl, kv.Command{Op: kv.OpDelete, Key: "a"}); !res.Existed {
				t.Fatal("delete did not report the key existed")
			}
			if res := mustDo(t, cl, get("a")); res.Status != kv.StatusNotFound {
				t.Fatalf("get after delete = %s", res.Status)
			}

			// Every node ends up having applied the same log.
			c.WaitConverged(1)
		})
	}
}

func TestFollowerRefusesWithLeaderHint(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	leader := c.WaitLeader()
	for _, id := range c.Others(leader) {
		// A follower learns who leads from the first heartbeat.
		c.Eventually(5*time.Second, id+" to learn the leader", func() bool { return c.Node(id).Status().Leader == leader })
		for _, cmd := range []kv.Command{get("k"), {Op: kv.OpPut, Key: "k", Value: []byte("v"), ClientID: "c", Seq: 1}} {
			_, err := c.Node(id).Propose(ctxT(t, time.Second), cmd.Encode())
			var nl *node.NotLeaderError
			if !errors.As(err, &nl) {
				t.Fatalf("%s on follower %s: err = %v, want NotLeaderError", cmd.Op, id, err)
			}
			if nl.Leader != leader {
				t.Fatalf("follower %s hinted %q, the leader is %s", id, nl.Leader, leader)
			}
		}
	}
	// The refused write did not happen anywhere.
	if res := mustDo(t, c.Client("reader"), get("k")); res.Status != kv.StatusNotFound {
		t.Fatalf("a write refused by followers is visible: %s", res.Status)
	}
}

// ---- atomic compare-and-swap -----------------------------------------------

// Many clients increment one counter using read-then-CAS. If the comparison
// and the write were not one atomic step in the apply path, two clients could
// both succeed from the same starting value and an increment would be lost.
func TestConcurrentCASLosesNoIncrements(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	c.WaitLeader()
	mustDo(t, c.Client("init"), put("counter", "0"))

	const clients, perClient = 8, 15
	var succeeded, conflicts atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl := c.Client(fmt.Sprintf("worker-%d", i))
			ctx := ctxT(t, 60*time.Second)
			for done := 0; done < perClient; {
				cur, err := cl.Do(ctx, get("counter"))
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				n, _ := strconv.Atoi(string(cur.Value))
				res, err := cl.Do(ctx, kv.Command{Op: kv.OpCAS, Key: "counter", Expect: kv.ExpectValue, ExpectValue: cur.Value, Value: []byte(strconv.Itoa(n + 1))})
				if err != nil {
					t.Errorf("cas: %v", err)
					return
				}
				switch res.Status {
				case kv.StatusOK:
					succeeded.Add(1)
					done++
				case kv.StatusCASFailed:
					conflicts.Add(1)
				default:
					t.Errorf("cas status %s", res.Status)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	wantValue(t, c.Client("check"), "counter", strconv.Itoa(clients*perClient))
	if succeeded.Load() != clients*perClient {
		t.Fatalf("succeeded = %d, want %d", succeeded.Load(), clients*perClient)
	}
	if conflicts.Load() == 0 {
		t.Log("no CAS conflicts occurred; the clients never actually raced")
	} else {
		t.Logf("%d successful increments, %d CAS conflicts retried", succeeded.Load(), conflicts.Load())
	}
}

// ---- retries across a leader change ----------------------------------------

func TestRetryAfterLeaderChangeIsNotAppliedTwice(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	old := c.WaitLeader()
	init := c.Client("init")
	mustDo(t, init, put("balance", "100"))

	// The client's command commits on the old leader...
	alice := c.Client("alice")
	cas := alice.Next(kv.Command{Op: kv.OpCAS, Key: "balance", Expect: kv.ExpectValue, ExpectValue: []byte("100"), Value: []byte("90")})
	first, err := c.Node(old).Propose(ctxT(t, 10*time.Second), cas.Encode())
	if err != nil || first.Status != kv.StatusOK {
		t.Fatalf("first attempt: %+v, %v", first, err)
	}
	// ...but imagine the reply never arrived, and the leader then died.
	c.Crash(old)
	c.WaitLeader(old)

	// The client retries the identical request against the new leader.
	retry, err := alice.Send(ctxT(t, 20*time.Second), cas)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.Status != kv.StatusOK || !retry.Duplicate || retry.Revision != first.Revision {
		t.Fatalf("retry = %+v, want the original success (revision %d) recognised as a duplicate", retry, first.Revision)
	}
	wantValue(t, init, "balance", "90")

	// Reusing the identity for a different operation is refused.
	other := cas
	other.Value = []byte("0")
	if res, err := alice.Send(ctxT(t, 20*time.Second), other); err != nil || res.Status != kv.StatusIdentityReused {
		t.Fatalf("identity reuse = %+v, %v; want IDENTITY_REUSED", res, err)
	}
	wantValue(t, init, "balance", "90")
}

// ---- partitions ------------------------------------------------------------

func TestIsolatedLeaderCannotAcknowledgeReadsOrWrites(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	old := c.WaitLeader()
	cl := c.Client("client")
	mustDo(t, cl, put("k", "before"))
	c.WaitConverged(1)

	// Cut the leader off from both followers. It can still be reached by
	// clients, which is the dangerous case.
	c.Net.Partition(c.IDs, []string{old}, c.Others(old))

	// Reads and writes sent to it must not succeed. Until check-quorum fires
	// it still believes it leads and accepts the command into its log, where
	// it can never commit; afterwards it refuses outright.
	var sawPending, sawRefusal bool
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; !sawRefusal && time.Now().Before(deadline); i++ {
		for _, cmd := range []kv.Command{get("k"), {Op: kv.OpPut, Key: "k", Value: []byte("from-isolated"), ClientID: "stale", Seq: uint64(i + 1)}} {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			res, err := c.Node(old).Propose(ctx, cmd.Encode())
			cancel()
			var nl *node.NotLeaderError
			switch {
			case err == nil:
				t.Fatalf("isolated leader acknowledged %s: %+v", cmd.Op, res)
			case errors.Is(err, node.ErrOutcomeUnknown):
				sawPending = true
			case errors.As(err, &nl):
				sawRefusal = true
			case errors.Is(err, node.ErrOverloaded), errors.Is(err, node.ErrProposalDropped):
			default:
				t.Fatalf("unexpected error from isolated leader: %v", err)
			}
		}
	}
	if !sawRefusal {
		t.Fatalf("isolated leader never stepped down\n%s", c.Describe())
	}
	t.Logf("isolated leader: requests pending without acknowledgement = %v, then refused as not-leader", sawPending)

	// The majority elects a new leader and makes progress.
	fresh := c.WaitLeader(old)
	majority := c.Client("majority")
	if res, err := c.Node(fresh).Propose(ctxT(t, 10*time.Second), majority.Next(put("k", "after")).Encode()); err != nil || res.Status != kv.StatusOK {
		t.Fatalf("write on the majority side: %+v, %v", res, err)
	}

	// After healing, the old leader follows, and its stranded writes are gone.
	c.Net.Heal()
	c.Eventually(20*time.Second, "the old leader to follow the new one", func() bool {
		st := c.Node(old).Status()
		return st.Role == raft.Follower && st.Leader != "" && st.Leader != old
	})
	c.WaitConverged(1)
	wantValue(t, cl, "k", "after")
	c.Eventually(10*time.Second, "the old leader to have no proposals left waiting", func() bool {
		return c.Node(old).Status().Pending == 0
	})
}

func TestStrandedProposalGetsDefiniteAnswerAfterHeal(t *testing.T) {
	c := testcluster.New(t, 3, func(cfg *node.Config) {
		// Without check-quorum the isolated leader keeps its role, which
		// keeps the client waiting until the partition heals.
		cfg.DisableCheckQuorum = true
	})
	old := c.WaitLeader()
	c.Net.Partition(c.IDs, []string{old}, c.Others(old))

	result := make(chan error, 1)
	go func() {
		_, err := c.Node(old).Propose(context.Background(), (&kv.Command{Op: kv.OpPut, Key: "k", Value: []byte("stranded"), ClientID: "s", Seq: 1}).Encode())
		result <- err
	}()
	c.Eventually(5*time.Second, "the proposal to be pending on the isolated leader", func() bool { return c.Node(old).Status().Pending == 1 })

	fresh := c.WaitLeader(old)
	if _, err := c.Node(fresh).Propose(ctxT(t, 10*time.Second), (&kv.Command{Op: kv.OpPut, Key: "k", Value: []byte("winner"), ClientID: "w", Seq: 1}).Encode()); err != nil {
		t.Fatal(err)
	}
	c.Net.Heal()

	// The new leader's entries overwrite the stranded one. The waiting
	// client is told, definitely, that its command did not commit.
	select {
	case err := <-result:
		if !errors.Is(err, node.ErrProposalDropped) {
			t.Fatalf("stranded proposal returned %v, want ErrProposalDropped", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("stranded proposal never returned\n%s", c.Describe())
	}
	wantValue(t, c.Client("r"), "k", "winner")
}

func TestQuorumLossAndRecovery(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	leader := c.WaitLeader()
	cl := c.Client("client")
	mustDo(t, cl, put("k", "v1"))

	for _, id := range c.Others(leader) {
		c.Stop(id)
	}
	// One node of three cannot commit anything.
	res, err := cl.Do(ctxT(t, 400*time.Millisecond), put("k", "v2"))
	if err == nil {
		t.Fatalf("write succeeded without a quorum: %+v", res)
	}
	if !errors.Is(err, node.ErrOutcomeUnknown) {
		t.Fatalf("write without a quorum: %v, want an unknown outcome", err)
	}
	c.Eventually(10*time.Second, "the lone node to give up leadership", func() bool { return c.Leader() == "" })

	for _, id := range c.Others(leader) {
		c.Start(id)
	}
	c.WaitLeader()
	// The client retries the same request, and it is applied exactly once
	// whether or not the first attempt had made it into a log that survived.
	mustDo(t, cl, get("k"))
	wantNext := mustDo(t, cl, put("k", "v3"))
	if wantNext.Status != kv.StatusOK {
		t.Fatalf("write after quorum returned: %s", wantNext.Status)
	}
	wantValue(t, cl, "k", "v3")
}

// ---- crash recovery --------------------------------------------------------

func TestAcknowledgedWritesSurviveCrashOfEveryNode(t *testing.T) {
	c := testcluster.New(t, 3, func(cfg *node.Config) {
		cfg.SnapshotThreshold = 25
		cfg.SnapshotTrailing = 5
	})
	c.WaitLeader()
	cl := c.Client("client")
	want := map[string]string{}
	for i := 0; i < 80; i++ {
		k, v := fmt.Sprintf("key-%d", i%20), fmt.Sprintf("value-%d", i)
		mustDo(t, cl, put(k, v))
		want[k] = v
	}
	last := cl.Next(put("last", "write"))
	if _, err := cl.Send(ctxT(t, 10*time.Second), last); err != nil {
		t.Fatal(err)
	}
	want["last"] = "write"
	c.Eventually(10*time.Second, "a snapshot on some node", func() bool {
		for _, id := range c.IDs {
			if c.Node(id).Status().SnapshotIndex > 0 {
				return true
			}
		}
		return false
	})

	// Power failure on all three at once: unsynced data is gone.
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Start(id)
	}
	c.WaitLeader()

	for k, v := range want {
		wantValue(t, cl, k, v)
	}
	// The de-duplication table came back too.
	if res, err := cl.Send(ctxT(t, 10*time.Second), last); err != nil || !res.Duplicate {
		t.Fatalf("retry after full restart = %+v, %v; want a recognised duplicate", res, err)
	}
}

func TestLeaderCrashDoesNotLoseAcknowledgedWrites(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	cl := c.Client("client")
	want := map[string]string{}
	for round := 0; round < 4; round++ {
		leader := c.WaitLeader()
		for i := 0; i < 10; i++ {
			k, v := fmt.Sprintf("r%d-k%d", round, i), fmt.Sprintf("v%d", i)
			mustDo(t, cl, put(k, v))
			want[k] = v
		}
		c.Crash(leader)
		c.WaitLeader(leader)
		c.Start(leader)
	}
	for k, v := range want {
		wantValue(t, cl, k, v)
	}
	c.WaitConverged(1)
}

// ---- snapshots -------------------------------------------------------------

func TestLaggingFollowerCatchesUpFromSnapshot(t *testing.T) {
	c := testcluster.New(t, 3, func(cfg *node.Config) {
		cfg.SnapshotThreshold = 20
		cfg.SnapshotTrailing = 3
	})
	leader := c.WaitLeader()
	behind := c.Others(leader)[0]
	c.Stop(behind)

	cl := c.Client("client")
	for i := 0; i < 120; i++ {
		mustDo(t, cl, put(fmt.Sprintf("key-%d", i%30), fmt.Sprintf("value-%d", i)))
	}
	leader = c.WaitLeader()
	c.Eventually(10*time.Second, "the leader to compact its log past the stopped follower", func() bool {
		return c.Node(leader).Status().FirstIndex > 50
	})

	c.Start(behind)
	c.Eventually(20*time.Second, "the follower to install a snapshot and catch up", func() bool {
		st, lst := c.Node(behind).Status(), c.Node(c.Leader()).Status()
		return c.Leader() != "" && st.SnapshotIndex > 0 && st.Applied == lst.Applied && st.Keys == lst.Keys
	})
	if st := c.Node(behind).Status(); st.FirstIndex <= 1 {
		t.Fatalf("follower log starts at %d; it replayed the log instead of installing a snapshot", st.FirstIndex)
	}

	// The follower's state is really the leader's: make it the only place
	// the data can come from.
	c.Stop(leader)
	c.WaitLeader(leader)
	for i := 90; i < 120; i++ {
		wantValue(t, cl, fmt.Sprintf("key-%d", i%30), fmt.Sprintf("value-%d", i))
	}
}

func TestSnapshotIsTriggeredByLogSizeToo(t *testing.T) {
	// Far fewer entries than the count threshold, but large ones.
	c := testcluster.New(t, 1, func(cfg *node.Config) {
		cfg.SnapshotThreshold = 1 << 30
		cfg.SnapshotBytes = 20 << 10
	})
	c.WaitLeader()
	cl := c.Client("client")
	value := string(make([]byte, 4<<10))
	for i := 0; i < 12; i++ {
		mustDo(t, cl, put(fmt.Sprintf("big-%d", i), value))
	}
	c.Eventually(10*time.Second, "a snapshot triggered by bytes applied", func() bool {
		return c.Node("n1").Status().SnapshotIndex > 0
	})
	if st := c.Node("n1").Status(); st.SnapshotIndex > 14 {
		t.Fatalf("first snapshot at index %d; 20 KiB of 4 KiB entries should trigger one after about five", st.SnapshotIndex)
	}
}

// ---- overload, cancellation, failure, shutdown -----------------------------

func TestOverloadIsRefusedNotQueuedForever(t *testing.T) {
	c := testcluster.New(t, 3, func(cfg *node.Config) {
		cfg.MaxPendingProposals = 8
		cfg.DisableCheckQuorum = true // keep the isolated node a leader
	})
	leader := c.WaitLeader()
	c.Net.Partition(c.IDs, []string{leader}, c.Others(leader))

	// Nothing can commit now, so proposals accumulate until the bound.
	var overloaded, unknown atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			_, err := c.Node(leader).Propose(ctx, (&kv.Command{Op: kv.OpPut, Key: "k", Value: []byte("v"), ClientID: fmt.Sprintf("c%d", i), Seq: 1}).Encode())
			switch {
			case errors.Is(err, node.ErrOverloaded):
				overloaded.Add(1)
			case errors.Is(err, node.ErrOutcomeUnknown):
				unknown.Add(1)
			default:
				t.Errorf("proposal %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if overloaded.Load() == 0 {
		t.Fatalf("no proposal was refused as overloaded (%d timed out)", unknown.Load())
	}
	if st := c.Node(leader).Status(); st.Pending > 8 {
		t.Fatalf("%d proposals pending, the bound is 8", st.Pending)
	}
	t.Logf("%d refused immediately as overloaded, %d accepted and left pending", overloaded.Load(), unknown.Load())

	// Once the partition heals the node works normally again.
	c.Net.Heal()
	c.WaitLeader()
	mustDo(t, c.Client("after"), put("after", "ok"))
}

func TestCancelledCallersDoNotBlockTheNode(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	leader := c.WaitLeader()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 500; i++ {
		_, err := c.Node(leader).Propose(cancelled, (&kv.Command{Op: kv.OpPut, Key: "k", Value: []byte("v"), ClientID: fmt.Sprintf("c%d", i), Seq: 1}).Encode())
		// Either the node was faster than the cancellation check, or the
		// caller left with an unknown outcome. Never a hang.
		if err != nil && !errors.Is(err, node.ErrOutcomeUnknown) && !errors.Is(err, node.ErrOverloaded) {
			t.Fatalf("proposal with a cancelled context: %v", err)
		}
	}
	mustDo(t, c.Client("after"), put("after", "ok"))
	c.Eventually(10*time.Second, "abandoned proposals to drain", func() bool { return c.Node(c.WaitLeader()).Status().Pending == 0 })
}

func TestStorageFailureStopsTheNodeButNotTheCluster(t *testing.T) {
	c := testcluster.New(t, 3, nil)
	leader := c.WaitLeader()
	cl := c.Client("client")
	mustDo(t, cl, put("k", "v1"))

	// The leader's disk starts failing on its next write.
	fs := c.FS(leader)
	fs.FailAt(fs.Ops()+1, syscall.EIO)
	failed := c.Node(leader)
	_, err := failed.Propose(ctxT(t, 5*time.Second), cl.Next(put("k", "v2")).Encode())
	if err == nil {
		t.Fatal("a write was acknowledged by a node whose disk failed")
	}
	select {
	case <-failed.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("node kept running after a storage failure")
	}
	if st := failed.Status(); st.Failed == nil || st.Role == raft.Leader {
		t.Fatalf("status after failure: failed=%v role=%s", st.Failed, st.Role)
	}
	if _, err := failed.Propose(ctxT(t, time.Second), get("k").Encode()); !errors.Is(err, node.ErrStopped) {
		t.Fatalf("Propose on a failed node = %v, want ErrStopped", err)
	}

	// The other two carry on.
	c.Net.Detach(leader)
	c.WaitLeader(leader)
	mustDo(t, cl, put("k", "v3"))
	wantValue(t, cl, "k", "v3")
}

func TestStopAnswersWaitingCallers(t *testing.T) {
	c := testcluster.New(t, 3, func(cfg *node.Config) { cfg.DisableCheckQuorum = true })
	leader := c.WaitLeader()
	c.Net.Partition(c.IDs, []string{leader}, c.Others(leader))

	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func(i int) {
			_, err := c.Node(leader).Propose(context.Background(), (&kv.Command{Op: kv.OpPut, Key: "k", Value: []byte("v"), ClientID: fmt.Sprintf("c%d", i), Seq: 1}).Encode())
			errs <- err
		}(i)
	}
	nd := c.Node(leader)
	c.Eventually(5*time.Second, "proposals to be pending", func() bool { return nd.Status().Pending == 5 })

	start := time.Now()
	c.Stop(leader)
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Stop took %v", took)
	}
	for i := 0; i < 5; i++ {
		select {
		case err := <-errs:
			// Accepted but unacknowledged: these may yet commit elsewhere,
			// so the only honest answer is "unknown".
			if !errors.Is(err, node.ErrOutcomeUnknown) {
				t.Fatalf("caller got %v, want ErrOutcomeUnknown", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a caller was left waiting after Stop")
		}
	}
	if _, err := nd.Propose(context.Background(), get("k").Encode()); !errors.Is(err, node.ErrStopped) {
		t.Fatalf("Propose after Stop = %v, want ErrStopped", err)
	}
}

func TestRepeatedStartStop(t *testing.T) {
	// goleak in TestMain verifies that none of these cycles leaks a
	// goroutine; here we check the data survives them.
	c := testcluster.New(t, 3, func(cfg *node.Config) { cfg.SnapshotThreshold = 10 })
	cl := c.Client("client")
	for round := 0; round < 6; round++ {
		c.WaitLeader()
		mustDo(t, cl, put(fmt.Sprintf("round-%d", round), "done"))
		victim := c.IDs[round%len(c.IDs)]
		c.Stop(victim)
		c.WaitLeader(victim)
		mustDo(t, cl, put(fmt.Sprintf("round-%d-degraded", round), "done"))
		c.Start(victim)
	}
	c.WaitLeader()
	for round := 0; round < 6; round++ {
		wantValue(t, cl, fmt.Sprintf("round-%d", round), "done")
		wantValue(t, cl, fmt.Sprintf("round-%d-degraded", round), "done")
	}
}
