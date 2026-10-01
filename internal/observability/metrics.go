// Package observability provides the node's Prometheus metrics and its
// structured logger.
//
// Metric labels are restricted to small, fixed sets: an operation name, an
// outcome, a peer ID from the static membership. Keys, values, client IDs
// and request IDs never become labels, since each distinct label value is a
// separate time series held in memory for the life of the process.
package observability

import (
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// NewLogger builds the process logger. Log lines carry request metadata
// (operation, status, duration) but never keys' values, tokens or other
// credentials.
func NewLogger(w io.Writer, level, format string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

// latencyBuckets cover 100 microseconds to about 13 seconds.
var latencyBuckets = prometheus.ExponentialBuckets(0.0001, 2, 18)

// Metrics holds every metric the server exports. It implements the observer
// interfaces of the node, the transport and the HTTP API.
type Metrics struct {
	Registry *prometheus.Registry

	proposals        *prometheus.HistogramVec
	walSync          prometheus.Histogram
	applyBatch       prometheus.Histogram
	appliedEntries   prometheus.Counter
	leaderChanges    prometheus.Counter
	rejected         *prometheus.CounterVec
	rpcFailures      *prometheus.CounterVec
	messagesDropped  *prometheus.CounterVec
	snapshotsCreated *prometheus.CounterVec
	snapshotDuration prometheus.Histogram
	snapshotBytes    prometheus.Gauge
	snapshotsInstall prometheus.Counter
	snapshotsSent    *prometheus.CounterVec
	httpRequests     *prometheus.CounterVec
	httpDuration     *prometheus.HistogramVec
}

// NewMetrics creates the metrics and registers them, together with the Go
// runtime and process collectors, in a registry of their own.
func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		proposals: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "raftkv_proposal_duration_seconds", Buckets: latencyBuckets,
			Help: "Time from a command being handed to the node until it is committed and applied, or fails, by outcome.",
		}, []string{"outcome"}),
		walSync: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "raftkv_wal_fsync_duration_seconds", Buckets: latencyBuckets,
			Help: "Duration of each fsync of the write-ahead log.",
		}),
		applyBatch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "raftkv_apply_batch_duration_seconds", Buckets: latencyBuckets,
			Help: "Time spent applying one batch of committed entries to the state machine.",
		}),
		appliedEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_applied_entries_total", Help: "Log entries applied to the state machine.",
		}),
		leaderChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_raft_leader_changes_total", Help: "Times this node's view of the leader changed.",
		}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_rejected_total", Help: "Work refused because a bound was reached, by reason.",
		}, []string{"reason"}),
		rpcFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_peer_rpc_failures_total", Help: "Outgoing peer RPCs that failed, by RPC.",
		}, []string{"rpc"}),
		messagesDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_peer_messages_dropped_total", Help: "Outgoing peer messages dropped before sending, by reason.",
		}, []string{"reason"}),
		snapshotsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_snapshots_created_total", Help: "Local snapshots attempted, by result.",
		}, []string{"result"}),
		snapshotDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "raftkv_snapshot_create_duration_seconds", Buckets: latencyBuckets,
			Help: "Time to serialise and durably write a local snapshot.",
		}),
		snapshotBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "raftkv_snapshot_last_size_bytes", Help: "Payload size of the most recent local snapshot.",
		}),
		snapshotsInstall: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_snapshots_installed_total", Help: "Snapshots received from a leader and installed.",
		}),
		snapshotsSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_snapshots_sent_total", Help: "Snapshot transfers to followers, by result.",
		}, []string{"result"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_http_requests_total", Help: "Client API requests, by operation and HTTP status code.",
		}, []string{"op", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "raftkv_http_request_duration_seconds", Buckets: latencyBuckets,
			Help: "Client API request duration, by operation.",
		}, []string{"op"}),
	}
	m.Registry.MustRegister(
		m.proposals, m.walSync, m.applyBatch, m.appliedEntries, m.leaderChanges, m.rejected,
		m.rpcFailures, m.messagesDropped, m.snapshotsCreated, m.snapshotDuration, m.snapshotBytes,
		m.snapshotsInstall, m.snapshotsSent, m.httpRequests, m.httpDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// ---- node.Observer ---------------------------------------------------------

func (m *Metrics) ProposalDone(outcome string, d time.Duration) {
	m.proposals.WithLabelValues(outcome).Observe(d.Seconds())
}

func (m *Metrics) Applied(entries int, d time.Duration) {
	m.appliedEntries.Add(float64(entries))
	m.applyBatch.Observe(d.Seconds())
}

func (m *Metrics) LeaderChanged() { m.leaderChanges.Inc() }

func (m *Metrics) SnapshotCreated(bytes int, d time.Duration, err error) {
	m.snapshotsCreated.WithLabelValues(result(err)).Inc()
	if err == nil {
		m.snapshotDuration.Observe(d.Seconds())
		m.snapshotBytes.Set(float64(bytes))
	}
}

func (m *Metrics) SnapshotInstalled() { m.snapshotsInstall.Inc() }

func (m *Metrics) Rejected(reason string) { m.rejected.WithLabelValues(reason).Inc() }

// ---- transport.Observer ----------------------------------------------------

func (m *Metrics) RPCFailed(rpc string) { m.rpcFailures.WithLabelValues(rpc).Inc() }

func (m *Metrics) MessageDropped(reason string) { m.messagesDropped.WithLabelValues(reason).Inc() }

func (m *Metrics) SnapshotSent(_ int64, _ time.Duration, err error) {
	m.snapshotsSent.WithLabelValues(result(err)).Inc()
}

// ---- storage and HTTP ------------------------------------------------------

// WALSync records one fsync of the write-ahead log.
func (m *Metrics) WALSync(d time.Duration) { m.walSync.Observe(d.Seconds()) }

// HTTPRequest records one client API request.
func (m *Metrics) HTTPRequest(op string, code int, d time.Duration) {
	m.httpRequests.WithLabelValues(op, strconv.Itoa(code)).Inc()
	m.httpDuration.WithLabelValues(op).Observe(d.Seconds())
}

func result(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

// ---- state gauges ----------------------------------------------------------

// stateCollector reports the node's current state. It reads the node's
// published status at scrape time instead of being updated on every change,
// so the event loop does no metric work for these.
type stateCollector struct {
	status func() node.Status

	term, isLeader, hasLeader, commit, applied, last, first, snapshot *prometheus.Desc
	keys, stateBytes, sessions, pending, healthy                      *prometheus.Desc
	peerLag, peerActive                                               *prometheus.Desc
}

// RegisterState exports gauges derived from the node's status.
func (m *Metrics) RegisterState(status func() node.Status) {
	d := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc("raftkv_"+name, help, labels, nil)
	}
	m.Registry.MustRegister(&stateCollector{
		status:     status,
		term:       d("raft_term", "Current Raft term."),
		isLeader:   d("raft_is_leader", "1 if this node is the leader, otherwise 0."),
		hasLeader:  d("raft_has_leader", "1 if this node knows a leader, otherwise 0."),
		commit:     d("raft_commit_index", "Highest log index known to be committed."),
		applied:    d("raft_applied_index", "Highest log index applied to the state machine."),
		last:       d("raft_last_index", "Index of the last entry in the log."),
		first:      d("raft_first_index", "Index of the first entry still in the in-memory log."),
		snapshot:   d("snapshot_index", "Log index covered by the newest snapshot."),
		keys:       d("kv_keys", "Number of keys stored."),
		stateBytes: d("kv_state_bytes", "Total bytes of keys and values stored."),
		sessions:   d("kv_sessions", "Client sessions held for retry de-duplication."),
		pending:    d("pending_proposals", "Proposals accepted and waiting to commit."),
		healthy:    d("healthy", "0 after a storage failure has stopped the node, otherwise 1."),
		peerLag:    d("replication_lag_entries", "On the leader: entries the follower is behind the leader's log.", "peer"),
		peerActive: d("peer_recently_active", "On the leader: 1 if the follower answered since the last quorum check.", "peer"),
	})
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.term, c.isLeader, c.hasLeader, c.commit, c.applied, c.last, c.first,
		c.snapshot, c.keys, c.stateBytes, c.sessions, c.pending, c.healthy, c.peerLag, c.peerActive} {
		ch <- d
	}
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.status()
	g := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	b := func(v bool) float64 {
		if v {
			return 1
		}
		return 0
	}
	g(c.term, float64(st.Term))
	g(c.isLeader, b(st.Role == raft.Leader))
	g(c.hasLeader, b(st.Leader != ""))
	g(c.commit, float64(st.Commit))
	g(c.applied, float64(st.Applied))
	g(c.last, float64(st.LastIndex))
	g(c.first, float64(st.FirstIndex))
	g(c.snapshot, float64(st.SnapshotIndex))
	g(c.keys, float64(st.Keys))
	g(c.stateBytes, float64(st.StateBytes))
	g(c.sessions, float64(st.Sessions))
	g(c.pending, float64(st.Pending))
	g(c.healthy, b(st.Failed == nil))
	for _, p := range st.Peers {
		lag := float64(0)
		if st.LastIndex > p.Match {
			lag = float64(st.LastIndex - p.Match)
		}
		g(c.peerLag, lag, p.ID)
		g(c.peerActive, b(p.RecentActive), p.ID)
	}
}
