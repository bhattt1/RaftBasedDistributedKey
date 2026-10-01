//go:build integration

// Package integration runs real kvserver processes, each with its own data
// directory on the real filesystem, talking gRPC and HTTP over loopback.
// Nodes are killed with SIGKILL and SIGTERM and restarted.
//
// Run with:  go test -tags integration ./tests/integration/
//
// These tests cannot partition the network; that needs packet filtering and
// lives in the Docker-based scripts under scripts/.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/client"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/httpapi"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/testcerts"
)

var binDir string

// TestMain builds the binaries once for the whole package.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "raftkv-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binDir = dir
	build := exec.Command("go", "build", "-o", dir+string(os.PathSeparator), "../../cmd/kvserver", "../../cmd/kvctl")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building binaries: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type proc struct {
	id        string
	dataDir   string
	raftAddr  string
	httpAddr  string
	adminAddr string
	url       string
	logPath   string
	cmd       *exec.Cmd
	exited    chan struct{}
	exitErr   error
}

type procCluster struct {
	t     *testing.T
	dir   string
	nodes []*proc
	extra []string // extra kvserver flags
	// Secure mode material.
	secure   bool
	caFile   string
	token    string
	tokenLoc string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// newProcCluster starts n server processes. The cluster is torn down, and
// every node's log printed if the test failed, when the test ends.
func newProcCluster(t *testing.T, n int, secure bool, extra ...string) *procCluster {
	t.Helper()
	c := &procCluster{t: t, dir: t.TempDir(), extra: extra, secure: secure}
	scheme := "http"
	if secure {
		scheme = "https"
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		p := &proc{id: id, dataDir: filepath.Join(c.dir, id, "data"), raftAddr: freeAddr(t), httpAddr: freeAddr(t), adminAddr: freeAddr(t),
			logPath: filepath.Join(c.dir, id+".log")}
		p.url = scheme + "://" + p.httpAddr
		c.nodes = append(c.nodes, p)
	}
	if secure {
		ca := testcerts.NewCA(t, c.dir, "cluster")
		c.caFile = ca.File
		c.token = "integration-test-token-0123456789"
		c.tokenLoc = filepath.Join(c.dir, "client.token")
		if err := os.WriteFile(c.tokenLoc, []byte(c.token+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		// Issue writes <dir>/cluster-<id>.pem and .key, which args() refers to.
		for _, p := range c.nodes {
			ca.Issue(p.id)
		}
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.signal(i, syscall.SIGKILL)
		}
		if t.Failed() {
			for _, p := range c.nodes {
				if b, err := os.ReadFile(p.logPath); err == nil {
					lines := strings.Split(strings.TrimSpace(string(b)), "\n")
					if len(lines) > 40 {
						lines = lines[len(lines)-40:]
					}
					t.Logf("---- last log lines of %s ----\n%s", p.id, strings.Join(lines, "\n"))
				}
			}
		}
	})
	for i := range c.nodes {
		c.start(i)
	}
	return c
}

func (c *procCluster) peersFlag() string {
	var parts []string
	for _, p := range c.nodes {
		parts = append(parts, fmt.Sprintf("%s=%s@%s", p.id, p.raftAddr, p.url))
	}
	return strings.Join(parts, ",")
}

func (c *procCluster) args(i int) []string {
	p := c.nodes[i]
	args := []string{
		"--node-id", p.id, "--cluster-id", "integration", "--data-dir", p.dataDir,
		"--raft-listen", p.raftAddr, "--http-listen", p.httpAddr, "--admin-listen", p.adminAddr,
		"--peers", c.peersFlag(),
		// Faster than the defaults so that failover tests finish quickly:
		// elections after 200-400 ms without a leader.
		"--tick-interval", "20ms", "--election-ticks", "10", "--heartbeat-ticks", "2",
		"--shutdown-timeout", "5s",
	}
	if c.secure {
		ca := filepath.Join(c.dir, "cluster-ca.pem")
		args = append(args,
			"--tls-peer-ca", ca,
			"--tls-peer-cert", filepath.Join(c.dir, "cluster-"+p.id+".pem"), "--tls-peer-key", filepath.Join(c.dir, "cluster-"+p.id+".key"),
			"--tls-http-cert", filepath.Join(c.dir, "cluster-"+p.id+".pem"), "--tls-http-key", filepath.Join(c.dir, "cluster-"+p.id+".key"),
			"--client-token-file", c.tokenLoc)
	} else {
		args = append(args, "--insecure-dev")
	}
	return append(args, c.extra...)
}

func (c *procCluster) start(i int) {
	c.t.Helper()
	p := c.nodes[i]
	if p.cmd != nil {
		return
	}
	logFile, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(binDir, "kvserver"), c.args(i)...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		c.t.Fatalf("starting %s: %v", p.id, err)
	}
	p.cmd, p.exited = cmd, make(chan struct{})
	go func(exited chan struct{}) {
		p.exitErr = cmd.Wait()
		logFile.Close()
		close(exited)
	}(p.exited)
}

// signal sends sig to node i and waits for the process to exit.
func (c *procCluster) signal(i int, sig syscall.Signal) {
	p := c.nodes[i]
	if p.cmd == nil {
		return
	}
	p.cmd.Process.Signal(sig)
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		c.t.Errorf("%s did not exit within 15s of %v", p.id, sig)
		p.cmd.Process.Kill()
		<-p.exited
	}
	p.cmd = nil
}

// exitCode returns the exit status of node i's most recent run.
func (c *procCluster) exitCode(i int) int {
	if ee, ok := c.nodes[i].exitErr.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	if c.nodes[i].exitErr != nil {
		return -1
	}
	return 0
}

func (c *procCluster) urls(only ...int) []string {
	var out []string
	for i, p := range c.nodes {
		if len(only) == 0 {
			out = append(out, p.url)
			continue
		}
		for _, j := range only {
			if i == j {
				out = append(out, p.url)
			}
		}
	}
	return out
}

func (c *procCluster) client(id string, only ...int) *client.Client {
	c.t.Helper()
	opts := client.Options{Endpoints: c.urls(only...), ClientID: id, AttemptTimeout: 2 * time.Second, MaxAttempts: 40}
	if c.secure {
		opts.Token, opts.CACertFile = c.token, c.caFile
	}
	cl, err := client.New(opts)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

func (c *procCluster) status(cl *client.Client, i int) (httpapi.StatusResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st, err := cl.Status(ctx, c.nodes[i].url)
	return st, err == nil
}

func (c *procCluster) eventually(what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// leader waits until exactly one running node reports itself leader and a
// write through it succeeds, and returns its index.
func (c *procCluster) leader(cl *client.Client) int {
	c.t.Helper()
	found := -1
	c.eventually("a leader", func() bool {
		found = -1
		for i, p := range c.nodes {
			if p.cmd == nil {
				continue
			}
			if st, ok := c.status(cl, i); ok && st.Role == "leader" {
				if found != -1 {
					return false // two claimants: an election is settling
				}
				found = i
			}
		}
		return found != -1
	})
	return found
}

// converged waits until every running node has applied the same index.
func (c *procCluster) converged(cl *client.Client) {
	c.t.Helper()
	c.eventually("all running nodes to apply the same index", func() bool {
		var applied uint64
		first := true
		for i, p := range c.nodes {
			if p.cmd == nil {
				continue
			}
			st, ok := c.status(cl, i)
			if !ok || st.AppliedIndex != st.CommitIndex {
				return false
			}
			if first {
				applied, first = st.AppliedIndex, false
			} else if st.AppliedIndex != applied {
				return false
			}
		}
		return !first
	})
}

var ctxBG = context.Background()

func ctxFor(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(ctxBG, d)
	_ = cancel // the deadline itself releases the context's resources
	return ctx
}

// kvctl runs the CLI binary against the cluster and returns its output and
// exit code.
func (c *procCluster) kvctl(args ...string) (stdout, stderr string, code int) {
	c.t.Helper()
	full := []string{"--endpoints", strings.Join(c.urls(), ","), "--timeout", "20s", "--attempts", "40"}
	if c.secure {
		full = append(full, "--ca-cert", c.caFile, "--token-file", c.tokenLoc)
	}
	cmd := exec.Command(filepath.Join(binDir, "kvctl"), append(full, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		c.t.Fatalf("running kvctl: %v", err)
	}
	return strings.TrimSpace(out.String()), strings.TrimSpace(errb.String()), code
}

// writer keeps writing unique keys until stopped and records which writes
// were acknowledged.
type writer struct {
	mu    sync.Mutex
	acked map[string]string
	stop  chan struct{}
	done  chan struct{}
}

func startWriter(c *procCluster, id string) *writer {
	w := &writer{acked: map[string]string{}, stop: make(chan struct{}), done: make(chan struct{})}
	cl := c.client(id)
	go func() {
		defer close(w.done)
		for n := 0; ; n++ {
			select {
			case <-w.stop:
				return
			default:
			}
			key, value := fmt.Sprintf("%s-%d", id, n), fmt.Sprintf("value-%d", n)
			ctx, cancel := context.WithTimeout(ctxBG, 10*time.Second)
			_, err := cl.Put(ctx, key, value)
			cancel()
			if err == nil {
				w.mu.Lock()
				w.acked[key] = value
				w.mu.Unlock()
			}
		}
	}()
	return w
}

func (w *writer) finish() map[string]string {
	close(w.stop)
	<-w.done
	return w.acked
}

func (w *writer) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.acked)
}
