package transport_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftv1 "github.com/bhattt1/RaftBasedDistributedKey/internal/gen/raft/v1"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/security"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/faultfs"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/testcerts"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/transport"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const clusterID = "transport-test"

// member is one node speaking real gRPC over loopback.
type member struct {
	id    string
	addr  string
	lis   net.Listener
	fs    *faultfs.FS
	store *storage.Store
	node  *node.Node
	tr    *transport.Transport
}

type cluster struct {
	t        *testing.T
	members  map[string]*member
	ids      []string
	addrs    map[string]string
	ca       *testcerts.CA
	nodeMod  func(*node.Config)
	transMod func(*transport.Config)
}

// newCluster reserves a loopback port per node and starts them all. With
// secure set, peers use mutual TLS with certificates from a test CA.
func newCluster(t *testing.T, n int, secure bool, nodeMod func(*node.Config), transMod func(*transport.Config)) *cluster {
	t.Helper()
	c := &cluster{t: t, members: map[string]*member{}, addrs: map[string]string{}, nodeMod: nodeMod, transMod: transMod}
	if secure {
		c.ca = testcerts.NewCA(t, t.TempDir(), "cluster")
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.ids = append(c.ids, id)
		c.addrs[id] = lis.Addr().String()
		c.members[id] = &member{id: id, addr: lis.Addr().String(), lis: lis, fs: faultfs.New(int64(i))}
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

func (c *cluster) tls(id string) (*tls.Config, func(string) *tls.Config) {
	if c.ca == nil {
		return nil, nil
	}
	cert, key := c.ca.Issue(id)
	server, client, err := security.PeerTLS(c.ca.File, cert, key, id)
	if err != nil {
		c.t.Fatal(err)
	}
	return server, client
}

func (c *cluster) start(id string) {
	c.t.Helper()
	m := c.members[id]
	if m.node != nil {
		return
	}
	if m.lis == nil {
		lis, err := net.Listen("tcp", m.addr)
		if err != nil {
			c.t.Fatalf("re-listening on %s: %v", m.addr, err)
		}
		m.lis = lis
	}
	store, err := storage.Open(storage.Options{Dir: "/data/" + id, ClusterID: clusterID, NodeID: id, FS: m.fs, MaxSnapshotBytes: 8 << 20})
	if err != nil {
		c.t.Fatal(err)
	}
	serverTLS, clientTLS := c.tls(id)
	tcfg := transport.Config{ClusterID: clusterID, ID: id, Peers: c.addrs, ServerTLS: serverTLS, ClientTLS: clientTLS, RPCTimeout: 500 * time.Millisecond}
	if c.transMod != nil {
		c.transMod(&tcfg)
	}
	tr, err := transport.New(tcfg)
	if err != nil {
		c.t.Fatal(err)
	}
	ncfg := node.Config{ID: id, Peers: c.ids, TickInterval: 10 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 2, SnapshotThreshold: 1 << 30}
	if c.nodeMod != nil {
		c.nodeMod(&ncfg)
	}
	nd, err := node.Start(ncfg, store, tr)
	if err != nil {
		c.t.Fatal(err)
	}
	m.store, m.node, m.tr = store, nd, tr
	lis := m.lis
	m.lis = nil
	go tr.Serve(lis, nd)
}

func (c *cluster) stop(id string) {
	m := c.members[id]
	if m.node == nil {
		if m.lis != nil {
			m.lis.Close()
			m.lis = nil
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m.node.Stop(ctx)
	m.tr.Close()
	m.store.Close()
	m.node, m.tr, m.store = nil, nil, nil
}

func (c *cluster) eventually(what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *cluster) leader() *member {
	c.t.Helper()
	var found *member
	c.eventually("a leader", func() bool {
		for _, id := range c.ids {
			m := c.members[id]
			if m.node == nil || m.node.Status().Role != raft.Leader {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			_, err := m.node.Propose(ctx, kv.Command{Op: kv.OpRead, Key: "probe"}.Encode())
			cancel()
			if err == nil {
				found = m
				return true
			}
		}
		return false
	})
	return found
}

func (c *cluster) put(key, value string, seq uint64) {
	c.t.Helper()
	cmd := kv.Command{Op: kv.OpPut, Key: key, Value: []byte(value), ClientID: "test", Seq: seq}.Encode()
	c.eventually("a write to commit", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := c.leader().node.Propose(ctx, cmd)
		return err == nil && res.Status == kv.StatusOK
	})
}

func (c *cluster) converged() bool {
	var applied uint64
	for i, id := range c.ids {
		m := c.members[id]
		if m.node == nil {
			continue
		}
		st := m.node.Status()
		if i == 0 {
			applied = st.Applied
		}
		if st.Applied != applied || st.Applied != st.Commit || applied == 0 {
			return false
		}
	}
	return true
}

// ---- replication over real gRPC --------------------------------------------

func TestReplicationOverGRPC(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "plaintext"
		if secure {
			name = "mutual TLS"
		}
		t.Run(name, func(t *testing.T) {
			c := newCluster(t, 3, secure, nil, nil)
			for i := uint64(1); i <= 20; i++ {
				c.put(fmt.Sprintf("key-%d", i), "value", i)
			}
			// A node publishes its status just after it answers clients, so
			// wait for the final state rather than reading it once.
			c.eventually("all three nodes to hold all 20 keys", func() bool {
				for _, id := range c.ids {
					if c.members[id].node.Status().Keys != 20 {
						return false
					}
				}
				return c.converged()
			})

			// Losing the leader: the other two elect a new one over the
			// same transport and keep accepting writes.
			old := c.leader()
			c.stop(old.id)
			c.put("after-failover", "value", 21)
			if l := c.leader(); l.id == old.id {
				t.Fatal("the stopped node is still reported as leader")
			}
		})
	}
}

func TestSnapshotIsStreamedInChunks(t *testing.T) {
	c := newCluster(t, 3, true,
		func(cfg *node.Config) { cfg.SnapshotThreshold, cfg.SnapshotTrailing = 20, 2 },
		// Tiny chunks, so the snapshot takes many messages.
		func(cfg *transport.Config) { cfg.SnapshotChunkBytes = 512 })
	leader := c.leader()
	var behind string
	for _, id := range c.ids {
		if id != leader.id {
			behind = id
			break
		}
	}
	c.stop(behind)

	// About 40 KiB of state, so the transfer spans dozens of chunks.
	value := strings.Repeat("x", 400)
	for i := uint64(1); i <= 100; i++ {
		c.put(fmt.Sprintf("key-%03d", i), value, i)
	}
	c.eventually("the leader to compact past the stopped follower", func() bool {
		st := c.leader().node.Status()
		return st.SnapshotIndex > 0 && st.FirstIndex > 50
	})

	c.start(behind)
	c.eventually("the follower to install the snapshot and catch up", func() bool {
		st := c.members[behind].node.Status()
		return st.SnapshotIndex > 0 && st.Keys == 100 && c.converged()
	})
	if st := c.members[behind].node.Status(); st.FirstIndex <= 1 {
		t.Fatalf("follower log starts at %d: it caught up by replaying entries, not from a snapshot", st.FirstIndex)
	}
	for _, name := range c.members[behind].fs.Names() {
		if strings.HasSuffix(name, ".tmp") {
			t.Fatalf("a temporary snapshot file was left behind: %s", name)
		}
	}
}

// ---- requests that must be refused -----------------------------------------

func dial(t *testing.T, addr string, creds credentials.TransportCredentials) raftv1.RaftClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return raftv1.NewRaftClient(conn)
}

func TestRequestsFromOutsideTheClusterAreRefused(t *testing.T) {
	c := newCluster(t, 3, false, nil, nil)
	leader := c.leader()
	term := leader.node.Status().Term
	client := dial(t, c.members["n1"].addr, insecure.NewCredentials())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tests := []struct {
		name   string
		header *raftv1.Header
		want   codes.Code
	}{
		{"another cluster", &raftv1.Header{ClusterId: "some-other-cluster", From: "n2", To: "n1", Term: term + 100}, codes.FailedPrecondition},
		{"unknown sender", &raftv1.Header{ClusterId: clusterID, From: "n9", To: "n1", Term: term + 100}, codes.PermissionDenied},
		{"sender claims to be the receiver", &raftv1.Header{ClusterId: clusterID, From: "n1", To: "n1", Term: term + 100}, codes.PermissionDenied},
		{"addressed to another node", &raftv1.Header{ClusterId: clusterID, From: "n2", To: "n3", Term: term + 100}, codes.FailedPrecondition},
		{"no header", nil, codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.RequestVote(ctx, &raftv1.VoteRequest{Header: tc.header, LastLogIndex: 1 << 40, LastLogTerm: 1 << 40})
			if status.Code(err) != tc.want {
				t.Fatalf("RequestVote: %v, want code %s", err, tc.want)
			}
			_, err = client.AppendEntries(ctx, &raftv1.AppendEntriesRequest{Header: tc.header})
			if status.Code(err) != tc.want {
				t.Fatalf("AppendEntries: %v, want code %s", err, tc.want)
			}
		})
	}
	// None of those high-term requests reached Raft: the term is unchanged.
	if got := c.members["n1"].node.Status().Term; got != term {
		t.Fatalf("term moved from %d to %d because of refused requests", term, got)
	}
}

func TestCertificateIdentityMustMatchTheClaimedSender(t *testing.T) {
	c := newCluster(t, 3, true, nil, nil)
	c.leader()
	term := c.members["n1"].node.Status().Term

	// n3's own, valid certificate...
	cert, key := c.ca.Issue("n3")
	_, clientTLS, err := security.PeerTLS(c.ca.File, cert, key, "n3")
	if err != nil {
		t.Fatal(err)
	}
	client := dial(t, c.members["n1"].addr, credentials.NewTLS(clientTLS("n1")))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ...used to send a message claiming to come from n2.
	_, err = client.RequestVote(ctx, &raftv1.VoteRequest{Header: &raftv1.Header{ClusterId: clusterID, From: "n2", To: "n1", Term: term + 50}})
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), `certificate identifies "n3"`) {
		t.Fatalf("a member impersonating another member: %v, want PermissionDenied", err)
	}
	// Speaking as itself it is accepted.
	if _, err := client.PreVote(ctx, &raftv1.VoteRequest{Header: &raftv1.Header{ClusterId: clusterID, From: "n3", To: "n1", Term: term + 1}}); err != nil {
		t.Fatalf("n3 speaking as itself: %v", err)
	}
	if got := c.members["n1"].node.Status().Term; got != term {
		t.Fatalf("term moved from %d to %d", term, got)
	}
}

func TestPlaintextClientCannotReachASecurePeer(t *testing.T) {
	c := newCluster(t, 3, true, nil, nil)
	c.leader()
	client := dial(t, c.members["n1"].addr, insecure.NewCredentials())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := client.RequestVote(ctx, &raftv1.VoteRequest{Header: &raftv1.Header{ClusterId: clusterID, From: "n2", To: "n1", Term: 99}})
	if err == nil {
		t.Fatal("a plaintext connection to a TLS peer listener succeeded")
	}
}

func TestOversizedMessageIsRejected(t *testing.T) {
	c := newCluster(t, 3, false, nil, func(cfg *transport.Config) { cfg.MaxMessageBytes = 64 << 10 })
	c.leader()
	client := dial(t, c.members["n1"].addr, insecure.NewCredentials())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	big := &raftv1.AppendEntriesRequest{
		Header:  &raftv1.Header{ClusterId: clusterID, From: "n2", To: "n1", Term: 1},
		Entries: []*raftv1.Entry{{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_COMMAND, Data: make([]byte, 200<<10)}},
	}
	if _, err := client.AppendEntries(ctx, big); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a 200 KiB message against a 64 KiB limit: %v, want ResourceExhausted", err)
	}
}

func TestSendToUnreachablePeerDoesNotBlock(t *testing.T) {
	// Nothing listens on these addresses. Send must return at once however
	// many messages are queued, because the node's event loop calls it.
	tr, err := transport.New(transport.Config{ClusterID: clusterID, ID: "n1",
		Peers: map[string]string{"n1": "127.0.0.1:1", "n2": "127.0.0.1:2", "n3": "127.0.0.1:3"}, QueueSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	start := time.Now()
	for i := 0; i < 10000; i++ {
		tr.Send(raft.Message{Type: raft.MsgApp, From: "n1", To: "n2", Term: 1})
		tr.Send(raft.Message{Type: raft.MsgVote, From: "n1", To: "nobody", Term: 1})
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("20000 sends to unreachable peers took %v", took)
	}
}

func TestTLSSettingsMustComeTogether(t *testing.T) {
	_, err := transport.New(transport.Config{ClusterID: clusterID, ID: "n1", Peers: map[string]string{"n1": "127.0.0.1:1"},
		ServerTLS: &tls.Config{MinVersion: tls.VersionTLS13}})
	if err == nil {
		t.Fatal("a transport with server TLS but plaintext clients was accepted")
	}
}
