// Package config loads and validates a server's configuration.
//
// Settings come from four places, each overriding the one before it:
//
//  1. built-in defaults
//  2. a JSON configuration file (--config)
//  3. environment variables (RAFTKV_NODE_ID, RAFTKV_DATA_DIR, ...)
//  4. command-line flags (--node-id, --data-dir, ...)
//
// Every setting has the same name in all three forms: the flag --raft-listen
// is the environment variable RAFTKV_RAFT_LISTEN and the JSON key
// "raft_listen".
package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Duration is a time.Duration that reads from JSON as a string like "100ms".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New(`durations are written as strings, for example "100ms"`)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// D converts to a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Peer is one member of the cluster.
type Peer struct {
	ID string `json:"id"`
	// RaftAddr is the host:port other nodes dial for the peer protocol.
	RaftAddr string `json:"raft_addr"`
	// HTTPAddr is the base URL clients use to reach this node's API. It is
	// handed out as the leader hint, so it must be reachable from where the
	// clients run, not merely from inside a container network.
	HTTPAddr string `json:"http_addr"`
}

// TLS holds certificate and credential paths for secure mode.
type TLS struct {
	// PeerCA signs every node's peer certificate. PeerCert and PeerKey are
	// this node's certificate, whose common name must be the node ID.
	PeerCA   string `json:"peer_ca"`
	PeerCert string `json:"peer_cert"`
	PeerKey  string `json:"peer_key"`
	// HTTPCert and HTTPKey serve the client API over HTTPS.
	HTTPCert string `json:"http_cert"`
	HTTPKey  string `json:"http_key"`
	// ClientTokenFile holds the bearer token clients must present.
	ClientTokenFile string `json:"client_token_file"`
}

// Config is a server's complete configuration.
type Config struct {
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id"`
	DataDir   string `json:"data_dir"`

	RaftListen string `json:"raft_listen"`
	HTTPListen string `json:"http_listen"`
	// AdminListen serves /metrics and, if enabled, pprof. It defaults to
	// loopback and should stay off public interfaces.
	AdminListen string `json:"admin_listen"`

	Peers []Peer `json:"peers"`

	TickInterval   Duration `json:"tick_interval"`
	ElectionTicks  int      `json:"election_ticks"`
	HeartbeatTicks int      `json:"heartbeat_ticks"`

	// RequestTimeout applies to client requests that do not ask for their
	// own; MaxRequestTimeout caps what a client may ask for.
	RequestTimeout    Duration `json:"request_timeout"`
	MaxRequestTimeout Duration `json:"max_request_timeout"`
	ShutdownTimeout   Duration `json:"shutdown_timeout"`

	MaxPendingProposals int    `json:"max_pending_proposals"`
	SnapshotThreshold   uint64 `json:"snapshot_threshold"`
	SnapshotTrailing    uint64 `json:"snapshot_trailing"`
	WALSegmentBytes     int64  `json:"wal_segment_bytes"`

	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`
	// EnablePprof exposes /debug/pprof on the admin listener.
	EnablePprof bool `json:"enable_pprof"`

	TLS TLS `json:"tls"`
	// InsecureDev runs without TLS and without client authentication. It
	// must be set explicitly; a node with no TLS settings and without this
	// flag refuses to start.
	InsecureDev bool `json:"insecure_dev"`
	// UnsafeNoFsync skips fsync of the write-ahead log. Acknowledged writes
	// can be lost on a crash. It exists only so benchmarks can measure what
	// fsync costs.
	UnsafeNoFsync bool `json:"unsafe_no_fsync"`
}

// Default returns the built-in defaults.
func Default() Config {
	return Config{
		ClusterID:           "raft-kv",
		DataDir:             "data",
		RaftListen:          ":7000",
		HTTPListen:          ":8000",
		AdminListen:         "127.0.0.1:9000",
		TickInterval:        Duration(100 * time.Millisecond),
		ElectionTicks:       10,
		HeartbeatTicks:      2,
		RequestTimeout:      Duration(5 * time.Second),
		MaxRequestTimeout:   Duration(30 * time.Second),
		ShutdownTimeout:     Duration(10 * time.Second),
		MaxPendingProposals: 1024,
		SnapshotThreshold:   10000,
		SnapshotTrailing:    1000,
		WALSegmentBytes:     16 << 20,
		LogLevel:            "info",
		LogFormat:           "text",
	}
}

// setting describes one configurable value in its flag/environment form.
type setting struct {
	name  string // flag name; the environment variable is derived from it
	usage string
	set   func(c *Config, v string) error
}

// flagValue collects a flag's raw text. Parsing happens later, through the
// same setter the environment uses, so both forms accept exactly the same
// values. Boolean settings may be given without a value (--insecure-dev).
type flagValue struct {
	text   string
	isBool bool
}

func (f *flagValue) String() string     { return f.text }
func (f *flagValue) Set(s string) error { f.text = s; return nil }
func (f *flagValue) IsBoolFlag() bool   { return f.isBool }

var boolSettings = map[string]bool{"enable-pprof": true, "insecure-dev": true, "unsafe-no-fsync": true}

func envName(flagName string) string {
	return "RAFTKV_" + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

func str(dst func(*Config) *string) func(*Config, string) error {
	return func(c *Config, v string) error { *dst(c) = v; return nil }
}

func dur(dst func(*Config) *Duration) func(*Config, string) error {
	return func(c *Config, v string) error {
		d, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		*dst(c) = Duration(d)
		return nil
	}
}

func num[T int | int64 | uint64](dst func(*Config) *T) func(*Config, string) error {
	return func(c *Config, v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return fmt.Errorf("%q is not a non-negative integer", v)
		}
		*dst(c) = T(n)
		return nil
	}
}

func boolean(dst func(*Config) *bool) func(*Config, string) error {
	return func(c *Config, v string) error {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%q is not true or false", v)
		}
		*dst(c) = b
		return nil
	}
}

var settings = []setting{
	{"node-id", "this node's ID; must appear in --peers", str(func(c *Config) *string { return &c.NodeID })},
	{"cluster-id", "name shared by every node of the cluster", str(func(c *Config) *string { return &c.ClusterID })},
	{"data-dir", "directory for the write-ahead log and snapshots", str(func(c *Config) *string { return &c.DataDir })},
	{"raft-listen", "address for the peer protocol (host:port)", str(func(c *Config) *string { return &c.RaftListen })},
	{"http-listen", "address for the client API (host:port)", str(func(c *Config) *string { return &c.HTTPListen })},
	{"admin-listen", "address for metrics and pprof; empty disables it", str(func(c *Config) *string { return &c.AdminListen })},
	{"peers", "all members as id=raft-host:port@client-url, comma separated", func(c *Config, v string) error {
		peers, err := ParsePeers(v)
		c.Peers = peers
		return err
	}},
	{"tick-interval", "length of one Raft tick", dur(func(c *Config) *Duration { return &c.TickInterval })},
	{"election-ticks", "base election timeout in ticks; the real timeout is 1-2x this", num(func(c *Config) *int { return &c.ElectionTicks })},
	{"heartbeat-ticks", "leader heartbeat interval in ticks", num(func(c *Config) *int { return &c.HeartbeatTicks })},
	{"request-timeout", "default deadline for a client request", dur(func(c *Config) *Duration { return &c.RequestTimeout })},
	{"max-request-timeout", "longest deadline a client may ask for", dur(func(c *Config) *Duration { return &c.MaxRequestTimeout })},
	{"shutdown-timeout", "how long a graceful shutdown may take", dur(func(c *Config) *Duration { return &c.ShutdownTimeout })},
	{"max-pending-proposals", "bound on requests queued or waiting to commit", num(func(c *Config) *int { return &c.MaxPendingProposals })},
	{"snapshot-threshold", "applied entries between snapshots", num(func(c *Config) *uint64 { return &c.SnapshotThreshold })},
	{"snapshot-trailing", "log entries kept in memory behind a snapshot", num(func(c *Config) *uint64 { return &c.SnapshotTrailing })},
	{"wal-segment-bytes", "size at which a WAL segment is closed", num(func(c *Config) *int64 { return &c.WALSegmentBytes })},
	{"log-level", "debug, info, warn or error", str(func(c *Config) *string { return &c.LogLevel })},
	{"log-format", "text or json", str(func(c *Config) *string { return &c.LogFormat })},
	{"enable-pprof", "serve /debug/pprof on the admin listener", boolean(func(c *Config) *bool { return &c.EnablePprof })},
	{"tls-peer-ca", "CA certificate that signs peer certificates", str(func(c *Config) *string { return &c.TLS.PeerCA })},
	{"tls-peer-cert", "this node's peer certificate (common name = node ID)", str(func(c *Config) *string { return &c.TLS.PeerCert })},
	{"tls-peer-key", "private key for --tls-peer-cert", str(func(c *Config) *string { return &c.TLS.PeerKey })},
	{"tls-http-cert", "certificate for the HTTPS client API", str(func(c *Config) *string { return &c.TLS.HTTPCert })},
	{"tls-http-key", "private key for --tls-http-cert", str(func(c *Config) *string { return &c.TLS.HTTPKey })},
	{"client-token-file", "file holding the bearer token clients must send", str(func(c *Config) *string { return &c.TLS.ClientTokenFile })},
	{"insecure-dev", "run without TLS or authentication (local development only)", boolean(func(c *Config) *bool { return &c.InsecureDev })},
	{"unsafe-no-fsync", "skip fsync; acknowledged writes can be lost (benchmarks only)", boolean(func(c *Config) *bool { return &c.UnsafeNoFsync })},
}

// ParsePeers parses "n1=host:7000@http://host:8000,n2=...".
func ParsePeers(s string) ([]Peer, error) {
	var peers []Peer
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, rest, ok := strings.Cut(part, "=")
		raftAddr, httpAddr, ok2 := strings.Cut(rest, "@")
		if !ok || !ok2 {
			return nil, fmt.Errorf("peer %q is not in the form id=raft-host:port@client-url", part)
		}
		peers = append(peers, Peer{ID: id, RaftAddr: raftAddr, HTTPAddr: httpAddr})
	}
	return peers, nil
}

// Load builds the configuration from defaults, an optional file, the
// environment and the command line, in that order, and validates it.
// getenv is os.Getenv in production; tests pass their own.
func Load(args []string, getenv func(string) string, stderr io.Writer) (Config, error) {
	cfg := Default()

	fs := flag.NewFlagSet("kvserver", flag.ContinueOnError)
	fs.SetOutput(stderr)
	file := fs.String("config", "", "path to a JSON configuration file")
	values := map[string]*flagValue{}
	for _, s := range settings {
		values[s.name] = &flagValue{isBool: boolSettings[s.name]}
		fs.Var(values[s.name], s.name, s.usage+" (env "+envName(s.name)+")")
	}
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: kvserver [flags]\n\nEvery flag can also be set in the config file or through the environment.\nPrecedence: flags, then environment, then config file, then defaults.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	path := *file
	if path == "" {
		path = getenv("RAFTKV_CONFIG")
	}
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return cfg, fmt.Errorf("config file: %w", err)
		}
		dec := json.NewDecoder(f)
		// A misspelled key is far more likely than a deliberate extra one.
		dec.DisallowUnknownFields()
		err = dec.Decode(&cfg)
		f.Close()
		if err != nil {
			return cfg, fmt.Errorf("config file %s: %w", path, err)
		}
	}
	for _, s := range settings {
		if v := getenv(envName(s.name)); v != "" {
			if err := s.set(&cfg, v); err != nil {
				return cfg, fmt.Errorf("%s: %w", envName(s.name), err)
			}
		}
	}
	var flagErr error
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" || flagErr != nil {
			return
		}
		for _, s := range settings {
			if s.name == f.Name {
				if err := s.set(&cfg, values[s.name].text); err != nil {
					flagErr = fmt.Errorf("--%s: %w", s.name, err)
				}
			}
		}
	})
	if flagErr != nil {
		return cfg, flagErr
	}
	return cfg, cfg.Validate()
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

// Validate reports the first problem with the configuration.
func (c *Config) Validate() error {
	if !idPattern.MatchString(c.NodeID) {
		return fmt.Errorf("node-id %q: must be 1-64 letters, digits, '.', '_' or '-'", c.NodeID)
	}
	if !idPattern.MatchString(c.ClusterID) {
		return fmt.Errorf("cluster-id %q: must be 1-64 letters, digits, '.', '_' or '-'", c.ClusterID)
	}
	if c.DataDir == "" {
		return errors.New("data-dir is required")
	}
	for name, addr := range map[string]string{"raft-listen": c.RaftListen, "http-listen": c.HTTPListen} {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return fmt.Errorf("%s %q: %v", name, addr, err)
		}
	}
	if c.AdminListen != "" {
		if _, _, err := net.SplitHostPort(c.AdminListen); err != nil {
			return fmt.Errorf("admin-listen %q: %v", c.AdminListen, err)
		}
	}

	if len(c.Peers) == 0 {
		return errors.New("peers is required and must list every member, including this node")
	}
	seenID, seenRaft := map[string]bool{}, map[string]bool{}
	self := false
	for _, p := range c.Peers {
		if !idPattern.MatchString(p.ID) {
			return fmt.Errorf("peer id %q: must be 1-64 letters, digits, '.', '_' or '-'", p.ID)
		}
		if seenID[p.ID] {
			return fmt.Errorf("peer %q is listed twice", p.ID)
		}
		seenID[p.ID] = true
		if _, _, err := net.SplitHostPort(p.RaftAddr); err != nil {
			return fmt.Errorf("peer %s raft address %q: %v", p.ID, p.RaftAddr, err)
		}
		if seenRaft[p.RaftAddr] {
			return fmt.Errorf("raft address %q is used by two peers", p.RaftAddr)
		}
		seenRaft[p.RaftAddr] = true
		u, err := url.Parse(p.HTTPAddr)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("peer %s client URL %q: must be an http:// or https:// URL", p.ID, p.HTTPAddr)
		}
		self = self || p.ID == c.NodeID
	}
	if !self {
		return fmt.Errorf("node-id %q does not appear in peers", c.NodeID)
	}

	switch {
	case c.TickInterval.D() < time.Millisecond:
		return errors.New("tick-interval must be at least 1ms")
	case c.HeartbeatTicks < 1:
		return errors.New("heartbeat-ticks must be at least 1")
	case c.ElectionTicks < 2*c.HeartbeatTicks+1:
		return fmt.Errorf("election-ticks (%d) must be more than twice heartbeat-ticks (%d), or followers time out between heartbeats", c.ElectionTicks, c.HeartbeatTicks)
	case c.RequestTimeout.D() <= 0 || c.MaxRequestTimeout.D() < c.RequestTimeout.D():
		return errors.New("request-timeout must be positive and no larger than max-request-timeout")
	case c.ShutdownTimeout.D() <= 0:
		return errors.New("shutdown-timeout must be positive")
	case c.MaxPendingProposals < 1:
		return errors.New("max-pending-proposals must be at least 1")
	case c.SnapshotThreshold < 1:
		return errors.New("snapshot-threshold must be at least 1")
	case c.WALSegmentBytes < 64<<10:
		return errors.New("wal-segment-bytes must be at least 65536")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log-level %q: must be debug, info, warn or error", c.LogLevel)
	}
	if c.LogFormat != "text" && c.LogFormat != "json" {
		return fmt.Errorf("log-format %q: must be text or json", c.LogFormat)
	}
	return c.validateSecurity()
}

func (c *Config) validateSecurity() error {
	t := c.TLS
	set := 0
	for _, v := range []string{t.PeerCA, t.PeerCert, t.PeerKey, t.HTTPCert, t.HTTPKey, t.ClientTokenFile} {
		if v != "" {
			set++
		}
	}
	switch {
	case c.InsecureDev && set > 0:
		return errors.New("insecure-dev cannot be combined with TLS or token settings; choose one mode")
	case c.InsecureDev:
		return nil
	case set == 0:
		return errors.New("no TLS settings given: configure tls-peer-ca, tls-peer-cert, tls-peer-key, tls-http-cert, tls-http-key and client-token-file, or pass --insecure-dev for local development")
	case set < 6:
		return errors.New("secure mode needs all of tls-peer-ca, tls-peer-cert, tls-peer-key, tls-http-cert, tls-http-key and client-token-file")
	}
	for _, p := range c.Peers {
		if !strings.HasPrefix(p.HTTPAddr, "https://") {
			return fmt.Errorf("peer %s client URL %q: must be https:// in secure mode, because it is handed to clients as the leader hint", p.ID, p.HTTPAddr)
		}
	}
	return nil
}

// PeerIDs returns every member's ID, sorted.
func (c *Config) PeerIDs() []string {
	ids := make([]string, 0, len(c.Peers))
	for _, p := range c.Peers {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	return ids
}

// RaftAddrs maps every member's ID to its peer-protocol address.
func (c *Config) RaftAddrs() map[string]string {
	out := make(map[string]string, len(c.Peers))
	for _, p := range c.Peers {
		out[p.ID] = p.RaftAddr
	}
	return out
}

// HTTPAddrs maps every member's ID to its client URL.
func (c *Config) HTTPAddrs() map[string]string {
	out := make(map[string]string, len(c.Peers))
	for _, p := range c.Peers {
		out[p.ID] = strings.TrimRight(p.HTTPAddr, "/")
	}
	return out
}
