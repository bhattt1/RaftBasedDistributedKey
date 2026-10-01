//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/client"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/httpapi"
)

func mustPut(t *testing.T, cl *client.Client, key, value string) httpapi.PutResponse {
	t.Helper()
	resp, err := cl.Put(ctxFor(30*time.Second), key, value)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return resp
}

func wantGet(t *testing.T, cl *client.Client, key, want string) {
	t.Helper()
	got, _, found, err := cl.Get(ctxFor(30*time.Second), key)
	if err != nil || !found || got != want {
		t.Fatalf("get %s = %q (found=%v, err=%v), want %q", key, got, found, err, want)
	}
}

func verify(t *testing.T, cl *client.Client, acked map[string]string) {
	t.Helper()
	for k, v := range acked {
		wantGet(t, cl, k, v)
	}
}

// TestLeaderKilledAndRestarted is the basic failover story with real
// processes: SIGKILL the leader while clients are writing, see the other two
// take over, bring the old leader back, and check that every write a client
// was told succeeded is still there.
func TestLeaderKilledAndRestarted(t *testing.T) {
	c := newProcCluster(t, 3, false)
	admin := c.client("admin")
	first := c.leader(admin)
	w := startWriter(c, "writer")
	c.eventually("some writes before the failure", func() bool { return w.count() >= 20 })

	killedAt := time.Now()
	c.signal(first, syscall.SIGKILL)
	before := w.count()
	c.eventually("writes to resume on the surviving majority", func() bool { return w.count() >= before+20 })
	t.Logf("writes resumed %v after the leader was killed", time.Since(killedAt).Round(10*time.Millisecond))

	second := c.leader(admin)
	if second == first {
		t.Fatal("the killed node is still reported as leader")
	}

	c.start(first)
	c.eventually("the old leader to rejoin as a follower", func() bool {
		st, ok := c.status(admin, first)
		return ok && st.Role == "follower" && st.Leader != nil
	})
	acked := w.finish()
	c.converged(admin)
	verify(t, c.client("verifier"), acked)
	t.Logf("%d acknowledged writes all present after leader failover and rejoin", len(acked))
}

// TestAllNodesKilledAtOnce kills every process with SIGKILL at the same
// instant, mid-write, and restarts them. Nothing acknowledged may be missing.
func TestAllNodesKilledAtOnce(t *testing.T) {
	c := newProcCluster(t, 3, false, "--snapshot-threshold", "200", "--snapshot-trailing", "20")
	admin := c.client("admin")
	c.leader(admin)
	w := startWriter(c, "writer")
	c.eventually("enough writes to have produced a snapshot", func() bool { return w.count() >= 500 })

	for i := range c.nodes {
		c.nodes[i].cmd.Process.Signal(syscall.SIGKILL) // all three, without waiting in between
	}
	for i := range c.nodes {
		c.signal(i, syscall.SIGKILL)
	}
	acked := w.finish()

	for i := range c.nodes {
		c.start(i)
	}
	c.leader(admin)
	c.converged(admin)
	verify(t, c.client("verifier"), acked)

	snapshots := 0
	for i := range c.nodes {
		if st, ok := c.status(admin, i); ok && st.SnapshotIndex > 0 {
			snapshots++
		}
	}
	if snapshots == 0 {
		t.Fatal("no node had taken a snapshot; recovery from snapshot plus log was not exercised")
	}
	t.Logf("%d acknowledged writes all present after SIGKILL of all three nodes; %d nodes recovered from a snapshot", len(acked), snapshots)
}

// TestGracefulRestartKeepsDataAndRetryState stops every node with SIGTERM,
// checks they exit cleanly, restarts them and confirms both the data and the
// de-duplication table came back.
func TestGracefulRestartKeepsDataAndRetryState(t *testing.T) {
	c := newProcCluster(t, 3, false)
	admin := c.client("admin")
	c.leader(admin)
	alice := c.client("alice")
	first := mustPut(t, alice, "account", "100")

	for i := range c.nodes {
		c.signal(i, syscall.SIGTERM)
		if code := c.exitCode(i); code != 0 {
			t.Fatalf("%s exited with code %d after SIGTERM, want 0", c.nodes[i].id, code)
		}
	}
	for i := range c.nodes {
		c.start(i)
	}
	c.leader(admin)
	wantGet(t, admin, "account", "100")

	// The same request again, as a client that never saw the first reply
	// would send it: same identity, same payload.
	again, err := client.New(client.Options{Endpoints: c.urls(), ClientID: "alice", FirstSeq: alice.LastSeq(), AttemptTimeout: 2 * time.Second, MaxAttempts: 40})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	retry := mustPut(t, again, "account", "100")
	if !retry.Duplicate || retry.Revision != first.Revision {
		t.Fatalf("retry after a full restart = %+v, want the original result (revision %d) marked duplicate", retry, first.Revision)
	}
}

// TestFiveNodeClusterToleratesTwoFailures exercises the five-node
// configuration: it keeps working with two nodes down, stops acknowledging
// with three down, and recovers when one returns.
func TestFiveNodeClusterToleratesTwoFailures(t *testing.T) {
	c := newProcCluster(t, 5, false)
	admin := c.client("admin")
	leader := c.leader(admin)
	cl := c.client("client")
	mustPut(t, cl, "k", "all-five")

	// Take down the leader and one follower: three of five remain.
	follower := (leader + 1) % 5
	c.signal(leader, syscall.SIGKILL)
	c.signal(follower, syscall.SIGKILL)
	mustPut(t, cl, "k", "three-of-five")
	wantGet(t, cl, "k", "three-of-five")

	// A third failure leaves two of five: no quorum.
	third := (leader + 2) % 5
	c.signal(third, syscall.SIGKILL)
	short, err := client.New(client.Options{Endpoints: c.urls(), ClientID: "minority", AttemptTimeout: 500 * time.Millisecond, MaxAttempts: 6})
	if err != nil {
		t.Fatal(err)
	}
	defer short.Close()
	if resp, err := short.Put(ctxFor(5*time.Second), "k", "two-of-five"); err == nil {
		t.Fatalf("a write was acknowledged with two of five nodes running: %+v", resp)
	}
	if _, _, _, err := short.Get(ctxFor(5*time.Second), "k"); err == nil {
		t.Fatal("a read was answered with two of five nodes running")
	}
	for i := range c.nodes {
		if c.nodes[i].cmd == nil {
			continue
		}
		// Each survivor is alive but must report that it is not ready.
		c.eventually(c.nodes[i].id+" to report not ready", func() bool {
			resp, err := http.Get(c.nodes[i].url + "/readyz")
			if err != nil {
				return false
			}
			resp.Body.Close()
			live, err := http.Get(c.nodes[i].url + "/healthz")
			if err != nil {
				return false
			}
			live.Body.Close()
			return resp.StatusCode == 503 && live.StatusCode == 200
		})
	}

	c.start(third)
	c.leader(admin)
	// The value is whichever of the last two writes won; the two-of-five
	// write was never acknowledged, so both outcomes are allowed, but the
	// acknowledged three-of-five write must not have been lost before it.
	got, _, found, err := cl.Get(ctxFor(30*time.Second), "k")
	if err != nil || !found || (got != "three-of-five" && got != "two-of-five") {
		t.Fatalf("after quorum returned, k = %q (found=%v, err=%v)", got, found, err)
	}
	c.start(leader)
	c.start(follower)
	c.converged(admin)
}

// TestRestartedFollowerCatchesUpFromSnapshot leaves a follower down while the
// others write enough to compact their logs past it.
func TestRestartedFollowerCatchesUpFromSnapshot(t *testing.T) {
	c := newProcCluster(t, 3, false, "--snapshot-threshold", "100", "--snapshot-trailing", "10")
	admin := c.client("admin")
	leader := c.leader(admin)
	behind := (leader + 1) % 3
	c.signal(behind, syscall.SIGKILL)

	cl := c.client("client")
	for i := 0; i < 400; i++ {
		mustPut(t, cl, fmt.Sprintf("key-%d", i%50), strconv.Itoa(i))
	}
	leader = c.leader(admin)
	c.eventually("the leader to compact its log", func() bool {
		st, ok := c.status(admin, leader)
		return ok && st.SnapshotIndex > 0 && st.FirstIndex > 100
	})

	c.start(behind)
	c.eventually("the follower to install a snapshot", func() bool {
		st, ok := c.status(admin, behind)
		return ok && st.SnapshotIndex > 0 && st.Keys == 50
	})
	c.converged(admin)
	st, _ := c.status(admin, behind)
	if st.FirstIndex <= 1 {
		t.Fatalf("follower's log starts at %d; it replayed the log instead of installing a snapshot", st.FirstIndex)
	}

	// Read through the follower alone after making it the only candidate
	// that has the data: stop the other two, restart one empty-handed is not
	// possible with fixed membership, so instead verify its metrics agree.
	body := httpGet(t, "http://"+c.nodes[behind].adminAddr+"/metrics")
	if !strings.Contains(body, "raftkv_snapshots_installed_total 1") {
		t.Fatalf("follower metrics do not show an installed snapshot:\n%s", grep(body, "raftkv_snapshot"))
	}
	t.Logf("follower %s installed a snapshot at index %d and now holds %d keys", c.nodes[behind].id, st.SnapshotIndex, st.Keys)
}

// TestSecondProcessOnSameDataDirectoryIsRefused checks the directory lock
// with real processes.
func TestSecondProcessOnSameDataDirectoryIsRefused(t *testing.T) {
	c := newProcCluster(t, 1, false)
	admin := c.client("admin")
	c.leader(admin)

	args := c.args(0)
	for i := range args {
		switch args[i] {
		case "--raft-listen", "--http-listen", "--admin-listen":
			args[i+1] = freeAddr(t) // different ports, same data directory
		}
	}
	out, err := exec.Command(filepath.Join(binDir, "kvserver"), args...).CombinedOutput()
	ee, ok := err.(*exec.ExitError)
	if !ok || ee.ExitCode() != 1 {
		t.Fatalf("second process: err=%v, want exit code 1\n%s", err, out)
	}
	if !strings.Contains(string(out), "locked by another process") {
		t.Fatalf("second process did not explain the refusal:\n%s", out)
	}
	// The first process is unaffected.
	mustPut(t, admin, "k", "v")
}

// TestWrongClusterDataDirectoryIsRefused restarts a node against a data
// directory that belongs to a different cluster ID.
func TestWrongClusterDataDirectoryIsRefused(t *testing.T) {
	c := newProcCluster(t, 1, false)
	c.leader(c.client("admin"))
	c.signal(0, syscall.SIGTERM)

	args := c.args(0)
	for i := range args {
		if args[i] == "--cluster-id" {
			args[i+1] = "a-different-cluster"
		}
	}
	out, err := exec.Command(filepath.Join(binDir, "kvserver"), args...).CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 1 || !strings.Contains(string(out), "data directory belongs to cluster") {
		t.Fatalf("err=%v, want exit code 1 naming the directory's cluster\n%s", err, out)
	}
}

// TestCorruptLogStopsTheNodeFromStarting flips a byte in the middle of a
// node's write-ahead log.
func TestCorruptLogStopsTheNodeFromStarting(t *testing.T) {
	c := newProcCluster(t, 3, false)
	admin := c.client("admin")
	leader := c.leader(admin)
	cl := c.client("client")
	for i := 0; i < 50; i++ {
		mustPut(t, cl, fmt.Sprintf("k%d", i), "v")
	}
	victim := (leader + 1) % 3
	c.signal(victim, syscall.SIGTERM)

	segs, _ := filepath.Glob(filepath.Join(c.nodes[victim].dataDir, "wal", "*.log"))
	if len(segs) == 0 {
		t.Fatal("no WAL segment found")
	}
	data, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(segs[0], data, 0o600); err != nil {
		t.Fatal(err)
	}

	c.start(victim)
	select {
	case <-c.nodes[victim].exited:
	case <-time.After(15 * time.Second):
		t.Fatal("a node with a corrupt log kept running")
	}
	c.nodes[victim].cmd = nil
	if code := c.exitCode(victim); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	logText, _ := os.ReadFile(c.nodes[victim].logPath)
	if !strings.Contains(string(logText), "corrupt data") {
		t.Fatalf("the node did not report corruption:\n%s", tail(string(logText), 5))
	}
	// The other two are a quorum and keep serving.
	mustPut(t, cl, "after", "ok")
}

// TestKvctl runs the CLI binary and checks its output and exit codes, which
// scripts depend on.
func TestKvctl(t *testing.T) {
	c := newProcCluster(t, 3, false)
	c.leader(c.client("admin"))

	if out, _, code := c.kvctl("put", "greeting", "hello"); code != 0 || !strings.HasPrefix(out, "OK revision=") {
		t.Fatalf("put: exit %d, %q", code, out)
	}
	if out, _, code := c.kvctl("get", "greeting"); code != 0 || out != "hello" {
		t.Fatalf("get: exit %d, %q", code, out)
	}
	if out, _, code := c.kvctl("--json", "get", "greeting"); code != 0 || !json.Valid([]byte(out)) || !strings.Contains(out, `"value":"hello"`) {
		t.Fatalf("get --json: exit %d, %q", code, out)
	}
	if _, _, code := c.kvctl("get", "missing"); code != 3 {
		t.Fatalf("get of a missing key: exit %d, want 3", code)
	}
	if out, _, code := c.kvctl("cas", "greeting", "hi", "--expect-value", "hello"); code != 0 || !strings.Contains(out, "swapped") {
		t.Fatalf("cas: exit %d, %q", code, out)
	}
	if out, _, code := c.kvctl("cas", "greeting", "nope", "--expect-value", "hello"); code != 4 || !strings.Contains(out, "NOT SWAPPED") {
		t.Fatalf("cas with a stale expectation: exit %d (want 4), %q", code, out)
	}
	if _, _, code := c.kvctl("cas", "fresh", "v", "--expect-absent"); code != 0 {
		t.Fatalf("cas --expect-absent: exit %d", code)
	}
	if _, stderr, code := c.kvctl("cas", "greeting", "x"); code != 2 || !strings.Contains(stderr, "exactly one") {
		t.Fatalf("cas without a condition: exit %d (want 2), %q", code, stderr)
	}
	if _, _, code := c.kvctl("frobnicate"); code != 2 {
		t.Fatalf("unknown command: exit %d, want 2", code)
	}
	if _, stderr, code := c.kvctl("put", strings.Repeat("k", 300), "v"); code != 1 || !strings.Contains(stderr, "INVALID_ARGUMENT") {
		t.Fatalf("put with an oversized key: exit %d (want 1), %q", code, stderr)
	}

	// Repeating a request with an explicit identity is recognised.
	if out, _, code := c.kvctl("put", "once", "v", "--client-id", "script-1", "--seq", "1"); code != 0 || strings.Contains(out, "duplicate") {
		t.Fatalf("first put: exit %d, %q", code, out)
	}
	if out, _, code := c.kvctl("put", "once", "v", "--client-id", "script-1", "--seq", "1"); code != 0 || !strings.Contains(out, "duplicate") {
		t.Fatalf("repeated put: exit %d, %q; want it reported as a duplicate", code, out)
	}
	if _, stderr, code := c.kvctl("put", "once", "DIFFERENT", "--client-id", "script-1", "--seq", "1"); code != 1 || !strings.Contains(stderr, "IDENTITY_REUSED") {
		t.Fatalf("identity reused for another request: exit %d, %q", code, stderr)
	}

	out, _, code := c.kvctl("status")
	if code != 0 || strings.Count(out, "follower") != 2 || strings.Count(out, "leader  ") < 1 {
		t.Fatalf("status: exit %d\n%s", code, out)
	}
	if out, _, code := c.kvctl("delete", "greeting"); code != 0 || !strings.Contains(out, "deleted") {
		t.Fatalf("delete: exit %d, %q", code, out)
	}
}

// TestSecureCluster runs three processes in secure mode: mutual TLS between
// peers, HTTPS for clients, and a bearer token.
func TestSecureCluster(t *testing.T) {
	c := newProcCluster(t, 3, true)
	admin := c.client("admin")
	leader := c.leader(admin)
	mustPut(t, admin, "k", "v")
	wantGet(t, admin, "k", "v")
	if out, _, code := c.kvctl("get", "k"); code != 0 || out != "v" {
		t.Fatalf("kvctl over HTTPS with a token: exit %d, %q", code, out)
	}

	// Replication really crosses the mutually authenticated links.
	c.signal(leader, syscall.SIGKILL)
	c.leader(admin)
	wantGet(t, admin, "k", "v")

	url := c.nodes[(leader+1)%3].url
	t.Run("no token is refused", func(t *testing.T) {
		noToken, err := client.New(client.Options{Endpoints: []string{url}, CACertFile: c.caFile, MaxAttempts: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer noToken.Close()
		if _, _, _, err := noToken.Get(ctxFor(5*time.Second), "k"); !client.IsCode(err, httpapi.CodeUnauthenticated) {
			t.Fatalf("request without a token: %v, want UNAUTHENTICATED", err)
		}
	})
	t.Run("wrong token is refused", func(t *testing.T) {
		bad, err := client.New(client.Options{Endpoints: []string{url}, CACertFile: c.caFile, Token: "not-the-token-not-the-token-xx", MaxAttempts: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer bad.Close()
		if _, err := bad.Put(ctxFor(5*time.Second), "k", "evil"); !client.IsCode(err, httpapi.CodeUnauthenticated) {
			t.Fatalf("request with a wrong token: %v, want UNAUTHENTICATED", err)
		}
		wantGet(t, admin, "k", "v")
	})
	t.Run("untrusted certificate is refused by the client", func(t *testing.T) {
		// A client that does not have the cluster's CA must not talk to it.
		systemRoots, err := client.New(client.Options{Endpoints: []string{url}, Token: c.token, MaxAttempts: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer systemRoots.Close()
		if _, _, _, err := systemRoots.Get(ctxFor(5*time.Second), "k"); !client.IsCode(err, client.CodeUnreachable) {
			t.Fatalf("client without the CA: %v, want a TLS failure", err)
		}
	})
	t.Run("plain HTTP is not served", func(t *testing.T) {
		resp, err := http.Get(strings.Replace(url, "https://", "http://", 1) + "/healthz")
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				t.Fatal("the HTTPS listener answered a plaintext request")
			}
		}
	})
	t.Run("metrics need the token", func(t *testing.T) {
		metricsURL := "http://" + c.nodes[(leader+1)%3].adminAddr + "/metrics"
		resp, err := http.Get(metricsURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatalf("/metrics without a token: %d, want 401", resp.StatusCode)
		}
		req, _ := http.NewRequest("GET", metricsURL, nil)
		req.Header.Set("Authorization", "Bearer "+c.token)
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || !strings.Contains(string(body), "raftkv_raft_term") {
			t.Fatalf("/metrics with the token: %d", resp.StatusCode)
		}
		if strings.Contains(string(body), c.token) {
			t.Fatal("the metrics output contains the token")
		}
	})
	t.Run("secrets do not appear in logs", func(t *testing.T) {
		for _, p := range c.nodes {
			logText, _ := os.ReadFile(p.logPath)
			if strings.Contains(string(logText), c.token) {
				t.Fatalf("%s logged the client token", p.id)
			}
		}
	})
}

// TestServerRefusesToStartWithoutSecurityChoice checks that neither TLS
// settings nor --insecure-dev means no start, with a helpful message.
func TestServerRefusesToStartWithoutSecurityChoice(t *testing.T) {
	out, err := exec.Command(filepath.Join(binDir, "kvserver"),
		"--node-id", "n1", "--peers", "n1=127.0.0.1:7999@http://127.0.0.1:8999", "--data-dir", t.TempDir()).CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 || !strings.Contains(string(out), "--insecure-dev") {
		t.Fatalf("err=%v, want exit code 2 mentioning --insecure-dev\n%s", err, out)
	}
}

func httpGet(t *testing.T, url string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctxBG, 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func grep(text, substr string) string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, substr) && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

func tail(text string, n int) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
