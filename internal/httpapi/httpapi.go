// Package httpapi is the client-facing HTTP API.
//
// A handler here never touches the key-value data. It validates the request,
// encodes it as a command, hands it to the node, and waits for the node to
// report what happened when that command was applied from the log. Reads go
// through the log like writes; that is what keeps an out-of-date leader from
// answering them.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/security"
)

// Node is what the API needs from the node. *node.Node implements it.
type Node interface {
	Propose(ctx context.Context, cmd []byte) (kv.Result, error)
	Status() node.Status
}

// Options configures the API.
type Options struct {
	NodeID    string
	ClusterID string
	// PeerURLs maps node IDs to the client URLs handed out as leader hints.
	PeerURLs map[string]string
	Limits   kv.Limits
	// DefaultTimeout applies when a request does not set ?timeout=;
	// MaxTimeout caps what a request may ask for.
	DefaultTimeout time.Duration
	MaxTimeout     time.Duration
	// Token, when not empty, is the bearer token required on /v1/ requests.
	Token  string
	Logger *slog.Logger
	// Observe, if set, is called once per request with a fixed operation
	// name, the status code and the duration.
	Observe func(op string, code int, d time.Duration)
}

// Header names for the request identity of a mutation.
const (
	HeaderClientID = "X-Client-Id"
	HeaderSeq      = "X-Request-Seq"
	// HeaderLeader carries the leader's URL on a NOT_LEADER response.
	HeaderLeader = "X-Raft-Leader"
)

// maxBodyBytes bounds a request body. A value may be 64 KiB and JSON
// escaping can expand a byte to six, so this leaves room for the largest
// legal CAS (two values) and nothing more.
const maxBodyBytes = 1 << 20

// Error codes returned in the JSON error body. They are stable: clients
// should branch on these, not on messages.
const (
	CodeInvalidArgument  = "INVALID_ARGUMENT"
	CodeUnauthenticated  = "UNAUTHENTICATED"
	CodeNotFound         = "NOT_FOUND"
	CodeKeyNotFound      = "KEY_NOT_FOUND"
	CodeCASFailed        = "CAS_FAILED"
	CodeIdentityReused   = "IDENTITY_REUSED"
	CodeStaleSequence    = "STALE_SEQUENCE"
	CodePayloadTooLarge  = "PAYLOAD_TOO_LARGE"
	CodeNotLeader        = "NOT_LEADER"
	CodeNoLeader         = "NO_LEADER"
	CodeOverloaded       = "OVERLOADED"
	CodeLeadershipLost   = "LEADERSHIP_LOST"
	CodeTimeout          = "TIMEOUT"
	CodeShuttingDown     = "SHUTTING_DOWN"
	CodeCapacityExceeded = "CAPACITY_EXCEEDED"
	CodeInternal         = "INTERNAL"
)

// Outcome values tell a client whether its request can have taken effect.
const (
	// OutcomeNotApplied: the request definitely changed nothing.
	OutcomeNotApplied = "not_applied"
	// OutcomeUnknown: the request may or may not have taken effect, now or
	// later. Retry with the same request identity to find out safely.
	OutcomeUnknown = "unknown"
)

// LeaderHint tells a client where the leader probably is.
type LeaderHint struct {
	ID  string `json:"id"`
	URL string `json:"url,omitempty"`
}

// Error is the JSON error object.
type Error struct {
	Code      string      `json:"code"`
	Message   string      `json:"message"`
	Outcome   string      `json:"outcome"`
	Retryable bool        `json:"retryable"`
	Leader    *LeaderHint `json:"leader,omitempty"`
	// Exists and Revision describe the key's current state on CAS_FAILED.
	Exists   *bool   `json:"exists,omitempty"`
	Revision *uint64 `json:"revision,omitempty"`
}

// ErrorBody is the body of every non-2xx response.
type ErrorBody struct {
	Error Error `json:"error"`
}

// Response bodies.
type (
	GetResponse struct {
		Key      string `json:"key"`
		Value    string `json:"value"`
		Revision uint64 `json:"revision"`
	}
	PutResponse struct {
		Key      string `json:"key"`
		Revision uint64 `json:"revision"`
		// Duplicate is true when this was recognised as a retry and the
		// result of the original request is being returned.
		Duplicate bool `json:"duplicate"`
	}
	DeleteResponse struct {
		Key       string `json:"key"`
		Existed   bool   `json:"existed"`
		Duplicate bool   `json:"duplicate"`
	}
	CASResponse struct {
		Key       string `json:"key"`
		Swapped   bool   `json:"swapped"`
		Revision  uint64 `json:"revision"`
		Duplicate bool   `json:"duplicate"`
	}
	PutRequest struct {
		Value *string `json:"value"`
	}
	CASRequest struct {
		Value          *string `json:"value"`
		ExpectAbsent   bool    `json:"expect_absent,omitempty"`
		ExpectValue    *string `json:"expect_value,omitempty"`
		ExpectRevision *uint64 `json:"expect_revision,omitempty"`
	}
	PeerStatus struct {
		ID           string `json:"id"`
		Match        uint64 `json:"match_index"`
		Next         uint64 `json:"next_index"`
		Lag          uint64 `json:"lag"`
		Snapshotting bool   `json:"snapshotting"`
		RecentActive bool   `json:"recently_active"`
	}
	StatusResponse struct {
		NodeID        string       `json:"node_id"`
		ClusterID     string       `json:"cluster_id"`
		Role          string       `json:"role"`
		Term          uint64       `json:"term"`
		Leader        *LeaderHint  `json:"leader"`
		CommitIndex   uint64       `json:"commit_index"`
		AppliedIndex  uint64       `json:"applied_index"`
		FirstIndex    uint64       `json:"first_index"`
		LastIndex     uint64       `json:"last_index"`
		SnapshotIndex uint64       `json:"snapshot_index"`
		Keys          int          `json:"keys"`
		StateBytes    int64        `json:"state_bytes"`
		Sessions      int          `json:"sessions"`
		Pending       int          `json:"pending_proposals"`
		Healthy       bool         `json:"healthy"`
		Peers         []PeerStatus `json:"peers,omitempty"`
	}
)

// API serves the client endpoints.
type API struct {
	node Node
	opts Options
	log  *slog.Logger
}

// New creates the API.
func New(n Node, opts Options) *API {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Limits == (kv.Limits{}) {
		opts.Limits = kv.DefaultLimits
	}
	if opts.DefaultTimeout <= 0 {
		opts.DefaultTimeout = 5 * time.Second
	}
	if opts.MaxTimeout < opts.DefaultTimeout {
		opts.MaxTimeout = opts.DefaultTimeout
	}
	return &API{node: n, opts: opts, log: opts.Logger}
}

// Handler returns the client API handler.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", a.wrap("healthz", false, a.healthz))
	mux.Handle("GET /readyz", a.wrap("readyz", false, a.readyz))
	mux.Handle("GET /v1/status", a.wrap("status", true, a.status))
	mux.Handle("GET /v1/kv/{key}", a.wrap("get", true, a.get))
	mux.Handle("PUT /v1/kv/{key}", a.wrap("put", true, a.put))
	mux.Handle("DELETE /v1/kv/{key}", a.wrap("delete", true, a.delete))
	mux.Handle("POST /v1/kv/{key}/cas", a.wrap("cas", true, a.cas))
	mux.Handle("/", a.wrap("other", false, func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, Error{Code: CodeNotFound, Message: "no such endpoint", Outcome: OutcomeNotApplied})
	}))
	return mux
}

// AdminHandler returns the operational handler: metrics, liveness and,
// optionally, pprof. It belongs on a listener that is not exposed to
// clients. When a token is configured it is required here too.
func (a *API) AdminHandler(reg *prometheus.Registry, enablePprof bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", a.wrap("healthz", false, a.healthz))
	mux.Handle("GET /metrics", a.auth(promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))
	if enablePprof {
		mux.Handle("/debug/pprof/", a.auth(http.HandlerFunc(pprof.Index)))
		mux.Handle("/debug/pprof/cmdline", a.auth(http.HandlerFunc(pprof.Cmdline)))
		mux.Handle("/debug/pprof/profile", a.auth(http.HandlerFunc(pprof.Profile)))
		mux.Handle("/debug/pprof/symbol", a.auth(http.HandlerFunc(pprof.Symbol)))
		mux.Handle("/debug/pprof/trace", a.auth(http.HandlerFunc(pprof.Trace)))
	}
	return mux
}

// ---- middleware ------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// wrap adds authentication (when required), metrics and a debug log line.
// The log line carries the operation and outcome but not the key's value.
func (a *API) wrap(op string, needAuth bool, h http.HandlerFunc) http.Handler {
	var inner http.Handler = h
	if needAuth {
		inner = a.auth(h)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		inner.ServeHTTP(rec, r)
		took := time.Since(start)
		if a.opts.Observe != nil {
			a.opts.Observe(op, rec.code, took)
		}
		a.log.Debug("request", "op", op, "method", r.Method, "status", rec.code, "took", took)
	})
}

func (a *API) auth(next http.Handler) http.Handler {
	if a.opts.Token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !security.TokenMatches(presented, a.opts.Token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="raft-kv"`)
			writeError(w, http.StatusUnauthorized, Error{Code: CodeUnauthenticated, Message: "missing or invalid bearer token", Outcome: OutcomeNotApplied})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ---- health ----------------------------------------------------------------

// healthz is liveness: the process is up and its event loop has not been
// stopped by a storage failure. It says nothing about the rest of the
// cluster.
func (a *API) healthz(w http.ResponseWriter, _ *http.Request) {
	if err := a.node.Status().Failed; err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "failed", "reason": "storage failure; see the node's log"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is readiness: this node currently knows a leader, so a request sent
// here can either be served or redirected. A live node that is partitioned
// away from the cluster, or whose cluster has no quorum, is not ready.
func (a *API) readyz(w http.ResponseWriter, _ *http.Request) {
	st := a.node.Status()
	switch {
	case st.Failed != nil:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": "node has failed"})
	case st.Leader == "":
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": "no leader known"})
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "role": st.Role.String(), "leader": st.Leader})
	}
}

func (a *API) status(w http.ResponseWriter, _ *http.Request) {
	st := a.node.Status()
	resp := StatusResponse{
		NodeID: a.opts.NodeID, ClusterID: a.opts.ClusterID,
		Role: st.Role.String(), Term: st.Term, Leader: a.leaderHint(st.Leader),
		CommitIndex: st.Commit, AppliedIndex: st.Applied, FirstIndex: st.FirstIndex, LastIndex: st.LastIndex,
		SnapshotIndex: st.SnapshotIndex, Keys: st.Keys, StateBytes: st.StateBytes, Sessions: st.Sessions,
		Pending: st.Pending, Healthy: st.Failed == nil,
	}
	for _, p := range st.Peers {
		ps := PeerStatus{ID: p.ID, Match: p.Match, Next: p.Next, Snapshotting: p.Snapshotting, RecentActive: p.RecentActive}
		if st.LastIndex > p.Match {
			ps.Lag = st.LastIndex - p.Match
		}
		resp.Peers = append(resp.Peers, ps)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) leaderHint(id string) *LeaderHint {
	if id == "" {
		return nil
	}
	return &LeaderHint{ID: id, URL: a.opts.PeerURLs[id]}
}

// ---- key-value operations --------------------------------------------------

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	a.run(w, r, kv.Command{Op: kv.OpRead, Key: key}, func(res kv.Result) {
		writeJSON(w, http.StatusOK, GetResponse{Key: key, Value: string(res.Value), Revision: res.Revision})
	})
}

func (a *API) put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var body PutRequest
	if !decodeBody(w, r, &body) {
		return
	}
	if body.Value == nil {
		invalid(w, `the body must be a JSON object with a "value" string`)
		return
	}
	a.run(w, r, kv.Command{Op: kv.OpPut, Key: key, Value: []byte(*body.Value)}, func(res kv.Result) {
		writeJSON(w, http.StatusOK, PutResponse{Key: key, Revision: res.Revision, Duplicate: res.Duplicate})
	})
}

func (a *API) delete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	a.run(w, r, kv.Command{Op: kv.OpDelete, Key: key}, func(res kv.Result) {
		writeJSON(w, http.StatusOK, DeleteResponse{Key: key, Existed: res.Existed, Duplicate: res.Duplicate})
	})
}

func (a *API) cas(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var body CASRequest
	if !decodeBody(w, r, &body) {
		return
	}
	if body.Value == nil {
		invalid(w, `the body must include the new "value"`)
		return
	}
	cmd := kv.Command{Op: kv.OpCAS, Key: key, Value: []byte(*body.Value)}
	conditions := 0
	if body.ExpectAbsent {
		conditions++
		cmd.Expect = kv.ExpectAbsent
	}
	if body.ExpectValue != nil {
		conditions++
		cmd.Expect, cmd.ExpectValue = kv.ExpectValue, []byte(*body.ExpectValue)
	}
	if body.ExpectRevision != nil {
		conditions++
		cmd.Expect, cmd.ExpectRevision = kv.ExpectRevision, *body.ExpectRevision
	}
	if conditions != 1 {
		invalid(w, `give exactly one of "expect_absent": true, "expect_value" or "expect_revision"`)
		return
	}
	a.run(w, r, cmd, func(res kv.Result) {
		writeJSON(w, http.StatusOK, CASResponse{Key: key, Swapped: true, Revision: res.Revision, Duplicate: res.Duplicate})
	})
}

// run is the common path of every operation: identity, validation, deadline,
// proposal, and translation of the outcome.
func (a *API) run(w http.ResponseWriter, r *http.Request, cmd kv.Command, ok func(kv.Result)) {
	if cmd.Op.IsMutation() {
		if !a.setIdentity(w, r, &cmd) {
			return
		}
	}
	if err := cmd.Validate(a.opts.Limits); err != nil {
		invalid(w, strings.TrimPrefix(err.Error(), kv.ErrInvalid.Error()+": "))
		return
	}
	timeout := a.opts.DefaultTimeout
	if v := r.URL.Query().Get("timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			invalid(w, "timeout must be a positive duration, for example 2s")
			return
		}
		// A request may shorten its deadline freely but cannot hold the
		// server longer than the configured maximum.
		timeout = min(d, a.opts.MaxTimeout)
	}
	// The request context ends if the client disconnects, so an abandoned
	// request stops waiting. It cannot be un-proposed, though: see the
	// TIMEOUT error below.
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	res, err := a.node.Propose(ctx, cmd.Encode())
	if err != nil {
		a.writeProposeError(w, err)
		return
	}
	switch res.Status {
	case kv.StatusOK:
		ok(res)
	case kv.StatusNotFound:
		writeError(w, http.StatusNotFound, Error{Code: CodeKeyNotFound, Message: "the key does not exist", Outcome: OutcomeNotApplied})
	case kv.StatusCASFailed:
		writeError(w, http.StatusConflict, Error{Code: CodeCASFailed, Message: "the condition did not hold; nothing was changed",
			Outcome: OutcomeNotApplied, Exists: &res.Existed, Revision: &res.Revision})
	case kv.StatusIdentityReused:
		writeError(w, http.StatusConflict, Error{Code: CodeIdentityReused, Outcome: OutcomeNotApplied,
			Message: "this client ID and sequence number were already used for a different request"})
	case kv.StatusStaleSequence:
		writeError(w, http.StatusConflict, Error{Code: CodeStaleSequence, Outcome: OutcomeNotApplied,
			Message: "this client has already sent a request with a higher sequence number; the result of the older one is no longer kept"})
	case kv.StatusCapacityExceeded:
		writeError(w, http.StatusInsufficientStorage, Error{Code: CodeCapacityExceeded, Outcome: OutcomeNotApplied,
			Message: fmt.Sprintf("storing this value would exceed the store's size limit of %d bytes", a.opts.Limits.MaxStateBytes)})
	default:
		writeError(w, http.StatusBadRequest, Error{Code: CodeInvalidArgument, Message: "the command was rejected by the state machine", Outcome: OutcomeNotApplied})
	}
}

// setIdentity reads the request identity of a mutation. A client that wants
// safe retries sends its own; otherwise the server makes one up, which makes
// this single request well-formed but gives the client nothing to retry with.
func (a *API) setIdentity(w http.ResponseWriter, r *http.Request, cmd *kv.Command) bool {
	id, seq := r.Header.Get(HeaderClientID), r.Header.Get(HeaderSeq)
	switch {
	case id == "" && seq == "":
		var b [9]byte
		if _, err := rand.Read(b[:]); err != nil {
			writeError(w, http.StatusInternalServerError, Error{Code: CodeInternal, Message: "could not generate a request identity", Outcome: OutcomeNotApplied})
			return false
		}
		cmd.ClientID, cmd.Seq = "anon-"+hex.EncodeToString(b[:]), 1
	case id == "" || seq == "":
		invalid(w, HeaderClientID+" and "+HeaderSeq+" must be sent together")
		return false
	default:
		n, err := strconv.ParseUint(seq, 10, 64)
		if err != nil || n == 0 {
			invalid(w, HeaderSeq+" must be a positive integer")
			return false
		}
		cmd.ClientID, cmd.Seq = id, n
	}
	w.Header().Set(HeaderClientID, cmd.ClientID)
	w.Header().Set(HeaderSeq, strconv.FormatUint(cmd.Seq, 10))
	return true
}

// writeProposeError maps the node's errors onto responses. The important
// part is the outcome field: it never claims a request failed when it might
// still take effect.
func (a *API) writeProposeError(w http.ResponseWriter, err error) {
	var nl *node.NotLeaderError
	switch {
	case errors.As(err, &nl) && nl.Leader != "":
		hint := a.leaderHint(nl.Leader)
		if hint.URL != "" {
			w.Header().Set(HeaderLeader, hint.URL)
		}
		writeError(w, http.StatusMisdirectedRequest, Error{Code: CodeNotLeader, Outcome: OutcomeNotApplied, Retryable: true, Leader: hint,
			Message: "this node is not the leader; the hint may be out of date"})
	case errors.As(err, &nl):
		writeError(w, http.StatusServiceUnavailable, Error{Code: CodeNoLeader, Outcome: OutcomeNotApplied, Retryable: true,
			Message: "this node knows no leader: an election is in progress, this node is cut off, or the cluster has lost its quorum"})
	case errors.Is(err, node.ErrOverloaded):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, Error{Code: CodeOverloaded, Outcome: OutcomeNotApplied, Retryable: true,
			Message: "too many requests are waiting; try again shortly"})
	case errors.Is(err, node.ErrProposalDropped):
		writeError(w, http.StatusServiceUnavailable, Error{Code: CodeLeadershipLost, Outcome: OutcomeNotApplied, Retryable: true,
			Message: "leadership changed before the request committed; it was not applied"})
	case errors.Is(err, node.ErrOutcomeUnknown):
		writeError(w, http.StatusGatewayTimeout, Error{Code: CodeTimeout, Outcome: OutcomeUnknown, Retryable: true,
			Message: "the request was submitted but did not complete in time; it may still take effect. Retry with the same " + HeaderClientID + " and " + HeaderSeq + " to get its result without applying it twice"})
	case errors.Is(err, node.ErrStopped):
		writeError(w, http.StatusServiceUnavailable, Error{Code: CodeShuttingDown, Outcome: OutcomeNotApplied, Retryable: true,
			Message: "this node is shutting down"})
	default:
		a.log.Error("unexpected proposal error", "error", err)
		writeError(w, http.StatusInternalServerError, Error{Code: CodeInternal, Outcome: OutcomeUnknown, Message: "internal error"})
	}
}

// ---- helpers ---------------------------------------------------------------

func decodeBody(w http.ResponseWriter, r *http.Request, into any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, Error{Code: CodePayloadTooLarge, Outcome: OutcomeNotApplied,
				Message: fmt.Sprintf("the request body exceeds %d bytes", maxBodyBytes)})
			return false
		}
		invalid(w, "the body is not valid JSON for this endpoint: "+err.Error())
		return false
	}
	if dec.More() {
		invalid(w, "the body must contain a single JSON object")
		return false
	}
	return true
}

func invalid(w http.ResponseWriter, msg string) {
	writeError(w, http.StatusBadRequest, Error{Code: CodeInvalidArgument, Message: msg, Outcome: OutcomeNotApplied})
}

func writeError(w http.ResponseWriter, status int, e Error) {
	writeJSON(w, status, ErrorBody{Error: e})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
