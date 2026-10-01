// Package testcluster runs several real nodes inside one test process. Each
// node has its own event loop, its own write-ahead log on a crash-modelling
// in-memory filesystem, and talks to the others over an in-memory network
// that tests can partition.
//
// Unlike the deterministic simulator this uses real goroutines and real
// time, so it exercises the concurrency of the node itself. Tests built on
// it wait for conditions with a deadline; they never sleep for a fixed time
// and assume something happened.
package testcluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/faultfs"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/memnet"
)

// ClusterID is the cluster identity every test node uses.
const ClusterID = "test-cluster"

type member struct {
	fs    *faultfs.FS
	store *storage.Store
	node  *node.Node
}

// Cluster is a set of in-process nodes.
type Cluster struct {
	t   testing.TB
	IDs []string
	Net *memnet.Network
	mod func(*node.Config)

	mu      sync.Mutex
	members map[string]*member
}

// New starts an n-node cluster. mod, if not nil, adjusts each node's
// configuration. Everything is shut down when the test ends.
func New(t testing.TB, n int, mod func(*node.Config)) *Cluster {
	t.Helper()
	c := &Cluster{t: t, Net: memnet.New(1), mod: mod, members: map[string]*member{}}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.IDs = append(c.IDs, id)
		c.members[id] = &member{fs: faultfs.New(int64(i))}
	}
	t.Cleanup(c.Close)
	for _, id := range c.IDs {
		c.Start(id)
	}
	return c
}

// Close stops every node and the network.
func (c *Cluster) Close() {
	for _, id := range c.IDs {
		c.Stop(id)
	}
	c.Net.Close()
}

// Start starts a stopped node from whatever is in its data directory.
func (c *Cluster) Start(id string) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m.node != nil {
		return
	}
	store, err := storage.Open(storage.Options{
		Dir: "/data/" + id, ClusterID: ClusterID, NodeID: id, FS: m.fs,
		SegmentSize: 64 << 10, MaxSnapshotBytes: 8 << 20,
	})
	if err != nil {
		c.t.Fatalf("opening storage for %s: %v", id, err)
	}
	cfg := node.Config{
		ID: id, Peers: c.IDs,
		TickInterval: 5 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2,
		SnapshotThreshold: 1 << 30,
	}
	if c.mod != nil {
		c.mod(&cfg)
	}
	nd, err := node.Start(cfg, store, c.Net.Port(id))
	if err != nil {
		store.Close()
		c.t.Fatalf("starting %s: %v", id, err)
	}
	m.store, m.node = store, nd
	c.Net.Attach(id, nd)
}

// Stop shuts a node down cleanly.
func (c *Cluster) Stop(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m.node == nil {
		return
	}
	c.Net.Detach(id)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.node.Stop(ctx); err != nil {
		c.t.Errorf("stopping %s: %v", id, err)
	}
	m.store.Close()
	m.store, m.node = nil, nil
}

// Crash kills a node the way a power failure would: nothing more reaches the
// disk, and whatever was written but not synced may be lost.
func (c *Cluster) Crash(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m.node == nil {
		return
	}
	c.Net.Detach(id)
	m.fs.CrashAt(m.fs.Ops() + 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.node.Stop(ctx); err != nil {
		c.t.Errorf("stopping crashed %s: %v", id, err)
	}
	m.fs.Restart()
	m.store, m.node = nil, nil
}

// FS returns a node's filesystem, for injecting faults.
func (c *Cluster) FS(id string) *faultfs.FS { return c.members[id].fs }

// Node returns a running node, or nil if it is stopped.
func (c *Cluster) Node(id string) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m == nil {
		return nil
	}
	return m.node
}

// Running returns the IDs of nodes that are up.
func (c *Cluster) Running() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, id := range c.IDs {
		if c.members[id].node != nil {
			out = append(out, id)
		}
	}
	return out
}

// Others returns every ID except the given ones.
func (c *Cluster) Others(exclude ...string) []string {
	var out []string
	for _, id := range c.IDs {
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

// Leader returns the running node that claims leadership in the highest
// term, or "" if none does. During a partition an isolated former leader may
// still claim an older term; it is not returned while a newer leader exists.
func (c *Cluster) Leader(exclude ...string) string {
	best, bestTerm := "", uint64(0)
	for _, id := range c.Others(exclude...) {
		nd := c.Node(id)
		if nd == nil {
			continue
		}
		if st := nd.Status(); st.Role == raft.Leader && st.Term >= bestTerm {
			best, bestTerm = id, st.Term
		}
	}
	return best
}

// Eventually polls cond until it holds or the timeout passes.
func (c *Cluster) Eventually(timeout time.Duration, what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out after %v waiting for: %s\n%s", timeout, what, c.Describe())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// WaitLeader waits for a leader that has committed an entry in its own term
// and returns its ID. Nodes in exclude are not considered.
func (c *Cluster) WaitLeader(exclude ...string) string {
	c.t.Helper()
	var leader string
	c.Eventually(20*time.Second, "a leader to be elected", func() bool {
		leader = c.Leader(exclude...)
		if leader == "" {
			return false
		}
		// Applying its own no-op proves the leader reached a quorum.
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err := c.Node(leader).Propose(ctx, (&kv.Command{Op: kv.OpRead, Key: "__probe__"}).Encode())
		return err == nil
	})
	return leader
}

// WaitConverged waits until every running node has applied the same index
// and that index is at least min.
func (c *Cluster) WaitConverged(min uint64) {
	c.t.Helper()
	c.Eventually(20*time.Second, fmt.Sprintf("all running nodes to apply index >= %d", min), func() bool {
		var applied uint64
		for i, id := range c.Running() {
			st := c.Node(id).Status()
			if i == 0 {
				applied = st.Applied
			}
			if st.Applied != applied || st.Applied < min || st.Applied != st.Commit {
				return false
			}
		}
		return true
	})
}

// Describe summarises every node, for failure messages.
func (c *Cluster) Describe() string {
	out := ""
	for _, id := range c.IDs {
		nd := c.Node(id)
		if nd == nil {
			out += id + ": down\n"
			continue
		}
		st := nd.Status()
		out += fmt.Sprintf("%s: %s term=%d leader=%q commit=%d applied=%d log=%d..%d snapshot=%d keys=%d pending=%d failed=%v\n",
			id, st.Role, st.Term, st.Leader, st.Commit, st.Applied, st.FirstIndex, st.LastIndex, st.SnapshotIndex, st.Keys, st.Pending, st.Failed)
	}
	return out
}

// Client issues commands the way kvctl does: it finds the leader, follows
// hints, and retries with the same request identity until it gets a definite
// answer or its deadline passes.
type Client struct {
	c   *Cluster
	ID  string
	seq uint64
	// last is the node the client talked to most recently.
	last string
}

// Client returns a new client with the given identity.
func (c *Cluster) Client(id string) *Client { return &Client{c: c, ID: id} }

// Next stamps cmd with this client's next sequence number. Reads are left
// without an identity.
func (cl *Client) Next(cmd kv.Command) kv.Command {
	if cmd.Op.IsMutation() {
		cl.seq++
		cmd.ClientID, cmd.Seq = cl.ID, cl.seq
	}
	return cmd
}

// Do sends cmd as the client's next request and retries until it succeeds or
// ctx ends.
func (cl *Client) Do(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	return cl.Send(ctx, cl.Next(cmd))
}

// Send sends an already stamped command, retrying where that is safe.
// Because the identity stays the same across attempts, retrying is safe even
// when an earlier attempt's outcome is unknown.
func (cl *Client) Send(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	data := cmd.Encode()
	target := cl.last
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			return kv.Result{}, fmt.Errorf("%w (last error: %v)", node.ErrOutcomeUnknown, lastErr)
		}
		running := cl.c.Running()
		if len(running) == 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		nd := cl.c.Node(target)
		if nd == nil {
			target = running[attempt%len(running)]
			nd = cl.c.Node(target)
			if nd == nil {
				continue
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		res, err := nd.Propose(attemptCtx, data)
		cancel()
		if err == nil {
			cl.last = target
			return res, nil
		}
		lastErr = err
		var nl *node.NotLeaderError
		if errors.As(err, &nl) && nl.Leader != "" && nl.Leader != target {
			target = nl.Leader
			continue
		}
		// No usable hint: try someone else after a short pause.
		target = running[(attempt+1)%len(running)]
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
		}
	}
}
