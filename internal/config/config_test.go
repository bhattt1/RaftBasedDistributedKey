package config

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const threePeers = "n1=127.0.0.1:7001@http://127.0.0.1:8001,n2=127.0.0.1:7002@http://127.0.0.1:8002,n3=127.0.0.1:7003@http://127.0.0.1:8003"

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func load(t *testing.T, args []string, e map[string]string) (Config, error) {
	t.Helper()
	return Load(args, env(e), io.Discard)
}

func TestPrecedenceFlagsOverEnvironmentOverFileOverDefaults(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node.json")
	os.WriteFile(file, []byte(`{
		"node_id": "n1", "insecure_dev": true,
		"data_dir": "/from/file", "log_level": "warn", "election_ticks": 20, "request_timeout": "9s",
		"peers": [
			{"id": "n1", "raft_addr": "127.0.0.1:7001", "http_addr": "http://127.0.0.1:8001"},
			{"id": "n2", "raft_addr": "127.0.0.1:7002", "http_addr": "http://127.0.0.1:8002"},
			{"id": "n3", "raft_addr": "127.0.0.1:7003", "http_addr": "http://127.0.0.1:8003"}
		]}`), 0o600)

	cfg, err := load(t,
		[]string{"--config", file, "--log-level", "debug"},
		map[string]string{"RAFTKV_LOG_LEVEL": "error", "RAFTKV_DATA_DIR": "/from/env"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("log level = %q, want the flag's value: a flag beats the environment and the file", cfg.LogLevel)
	}
	if cfg.DataDir != "/from/env" {
		t.Errorf("data dir = %q, want the environment's value: the environment beats the file", cfg.DataDir)
	}
	if cfg.ElectionTicks != 20 || cfg.RequestTimeout.D() != 9*time.Second {
		t.Errorf("election ticks = %d, request timeout = %v, want the file's values over the defaults", cfg.ElectionTicks, cfg.RequestTimeout.D())
	}
	if cfg.HeartbeatTicks != 2 || cfg.TickInterval.D() != 100*time.Millisecond || cfg.AdminListen != "127.0.0.1:9000" {
		t.Errorf("unset values did not keep their defaults: %+v", cfg)
	}
	if len(cfg.Peers) != 3 {
		t.Errorf("peers = %+v", cfg.Peers)
	}
}

func TestFlagsAlone(t *testing.T) {
	cfg, err := load(t, []string{"--node-id", "n2", "--peers", threePeers, "--insecure-dev", "--tick-interval", "50ms", "--snapshot-threshold", "500"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != "n2" || !cfg.InsecureDev || cfg.TickInterval.D() != 50*time.Millisecond || cfg.SnapshotThreshold != 500 {
		t.Fatalf("config = %+v", cfg)
	}
	if got := cfg.RaftAddrs()["n3"]; got != "127.0.0.1:7003" {
		t.Fatalf("raft address of n3 = %q", got)
	}
	if got := cfg.HTTPAddrs()["n1"]; got != "http://127.0.0.1:8001" {
		t.Fatalf("client URL of n1 = %q", got)
	}
	if ids := cfg.PeerIDs(); strings.Join(ids, ",") != "n1,n2,n3" {
		t.Fatalf("peer IDs = %v", ids)
	}
}

func TestEnvironmentAlone(t *testing.T) {
	cfg, err := load(t, nil, map[string]string{
		"RAFTKV_NODE_ID": "n3", "RAFTKV_PEERS": threePeers, "RAFTKV_INSECURE_DEV": "true", "RAFTKV_HEARTBEAT_TICKS": "3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != "n3" || cfg.HeartbeatTicks != 3 || !cfg.InsecureDev {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestInvalidConfigurationsAreRejected(t *testing.T) {
	base := []string{"--node-id", "n1", "--peers", threePeers, "--insecure-dev"}
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string // substring of the error
	}{
		{"no node id", []string{"--peers", threePeers, "--insecure-dev"}, nil, "node-id"},
		{"node not among peers", []string{"--node-id", "n9", "--peers", threePeers, "--insecure-dev"}, nil, "does not appear in peers"},
		{"no peers", []string{"--node-id", "n1", "--insecure-dev"}, nil, "peers is required"},
		{"malformed peer", []string{"--node-id", "n1", "--peers", "n1=127.0.0.1:7001", "--insecure-dev"}, nil, "id=raft-host:port@client-url"},
		{"duplicate peer id", []string{"--node-id", "n1", "--peers", "n1=a:1@http://a:2,n1=b:1@http://b:2", "--insecure-dev"}, nil, "listed twice"},
		{"duplicate raft address", []string{"--node-id", "n1", "--peers", "n1=a:1@http://a:2,n2=a:1@http://b:2", "--insecure-dev"}, nil, "used by two peers"},
		{"client URL without scheme", []string{"--node-id", "n1", "--peers", "n1=a:1@a:2", "--insecure-dev"}, nil, "http:// or https://"},
		{"raft address without port", []string{"--node-id", "n1", "--peers", "n1=hostonly@http://a:2", "--insecure-dev"}, nil, "raft address"},
		{"bad node id characters", []string{"--node-id", "n 1", "--peers", threePeers, "--insecure-dev"}, nil, "node-id"},
		{"election not above heartbeat", append(base[:len(base):len(base)], "--election-ticks", "4", "--heartbeat-ticks", "2"), nil, "election-ticks"},
		{"tick too small", append(base[:len(base):len(base)], "--tick-interval", "10us"), nil, "tick-interval"},
		{"bad duration", append(base[:len(base):len(base)], "--request-timeout", "fast"), nil, "request-timeout"},
		{"timeout above its maximum", append(base[:len(base):len(base)], "--request-timeout", "1m", "--max-request-timeout", "10s"), nil, "request-timeout"},
		{"negative number", append(base[:len(base):len(base)], "--snapshot-threshold", "-5"), nil, "snapshot-threshold"},
		{"unknown log level", append(base[:len(base):len(base)], "--log-level", "loud"), nil, "log-level"},
		{"bad value from the environment", base, map[string]string{"RAFTKV_ELECTION_TICKS": "many"}, "RAFTKV_ELECTION_TICKS"},
		{"unknown flag", append(base[:len(base):len(base)], "--no-such-flag"), nil, "no-such-flag"},
		{"stray argument", append(base[:len(base):len(base)], "extra"), nil, "unexpected argument"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil {
				t.Fatal("the configuration was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestConfigFileRejectsUnknownKeys(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node.json")
	os.WriteFile(file, []byte(`{"node_id": "n1", "electoin_ticks": 20}`), 0o600)
	_, err := load(t, []string{"--config", file}, nil)
	if err == nil || !strings.Contains(err.Error(), "electoin_ticks") {
		t.Fatalf("a misspelled key was not reported: %v", err)
	}
}

// ---- security mode ---------------------------------------------------------

func TestRunningWithoutTLSMustBeAskedForExplicitly(t *testing.T) {
	_, err := load(t, []string{"--node-id", "n1", "--peers", threePeers}, nil)
	if err == nil || !strings.Contains(err.Error(), "--insecure-dev") {
		t.Fatalf("a node with no TLS settings started without --insecure-dev: %v", err)
	}
}

func TestSecureModeNeedsEverySetting(t *testing.T) {
	httpsPeers := strings.ReplaceAll(threePeers, "http://", "https://")
	all := map[string]string{
		"--tls-peer-ca": "ca.pem", "--tls-peer-cert": "n1.pem", "--tls-peer-key": "n1.key",
		"--tls-http-cert": "http.pem", "--tls-http-key": "http.key", "--client-token-file": "token",
	}
	build := func(skip string, peers string, extra ...string) []string {
		args := []string{"--node-id", "n1", "--peers", peers}
		for k, v := range all {
			if k != skip {
				args = append(args, k, v)
			}
		}
		return append(args, extra...)
	}

	if _, err := load(t, build("", httpsPeers), nil); err != nil {
		t.Fatalf("complete secure configuration rejected: %v", err)
	}
	for k := range all {
		if _, err := load(t, build(k, httpsPeers), nil); err == nil || !strings.Contains(err.Error(), "secure mode needs all of") {
			t.Errorf("secure configuration without %s was accepted: %v", k, err)
		}
	}
	if _, err := load(t, build("", httpsPeers, "--insecure-dev"), nil); err == nil || !strings.Contains(err.Error(), "choose one mode") {
		t.Errorf("TLS settings combined with --insecure-dev were accepted: %v", err)
	}
	// Leader hints are sent to clients; in secure mode they must not point
	// them at plaintext URLs.
	if _, err := load(t, build("", threePeers), nil); err == nil || !strings.Contains(err.Error(), "https://") {
		t.Errorf("secure mode accepted http:// client URLs: %v", err)
	}
}

func TestDefaultsKeepOperationalEndpointsPrivate(t *testing.T) {
	d := Default()
	if !strings.HasPrefix(d.AdminListen, "127.0.0.1:") {
		t.Errorf("admin listener defaults to %q, want loopback only", d.AdminListen)
	}
	if d.EnablePprof || d.InsecureDev || d.UnsafeNoFsync {
		t.Errorf("unsafe options are on by default: %+v", d)
	}
}
