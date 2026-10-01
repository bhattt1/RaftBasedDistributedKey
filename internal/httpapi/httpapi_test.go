package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// fakeNode stands in for the node. It records the commands it is given and
// answers with whatever the test scripted.
type fakeNode struct {
	mu       sync.Mutex
	commands []kv.Command
	propose  func(ctx context.Context, cmd kv.Command) (kv.Result, error)
	status   node.Status
}

func (f *fakeNode) Propose(ctx context.Context, data []byte) (kv.Result, error) {
	cmd, err := kv.Decode(data)
	if err != nil {
		return kv.Result{}, fmt.Errorf("handler proposed an undecodable command: %w", err)
	}
	f.mu.Lock()
	f.commands = append(f.commands, cmd)
	f.mu.Unlock()
	if f.propose != nil {
		return f.propose(ctx, cmd)
	}
	return kv.Result{Revision: 7}, nil
}

func (f *fakeNode) Status() node.Status { return f.status }

func (f *fakeNode) last(t *testing.T) kv.Command {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) == 0 {
		t.Fatal("no command reached the node")
	}
	return f.commands[len(f.commands)-1]
}

func (f *fakeNode) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.commands)
}

func newAPI(f *fakeNode, mod func(*Options)) http.Handler {
	opts := Options{
		NodeID: "n1", ClusterID: "c",
		PeerURLs:       map[string]string{"n1": "http://n1.example:8001", "n2": "http://n2.example:8002"},
		DefaultTimeout: 2 * time.Second, MaxTimeout: 5 * time.Second,
	}
	if mod != nil {
		mod(&opts)
	}
	return New(f, opts).Handler()
}

type response struct {
	code   int
	header http.Header
	body   []byte
}

func (r response) errorBody(t *testing.T) Error {
	t.Helper()
	var eb ErrorBody
	if err := json.Unmarshal(r.body, &eb); err != nil || eb.Error.Code == "" {
		t.Fatalf("status %d: body is not a JSON error object: %s", r.code, r.body)
	}
	return eb.Error
}

func call(h http.Handler, method, target, body string, headers ...string) response {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return response{code: rec.Code, header: rec.Header(), body: rec.Body.Bytes()}
}

// ---- requests become the right commands ------------------------------------

func TestRequestsAreEncodedAsCommands(t *testing.T) {
	f := &fakeNode{}
	h := newAPI(f, nil)
	id := []string{HeaderClientID, "alice", HeaderSeq, "42"}

	if r := call(h, "PUT", "/v1/kv/greeting", `{"value":"hello"}`, id...); r.code != 200 {
		t.Fatalf("PUT: %d %s", r.code, r.body)
	}
	want := kv.Command{Op: kv.OpPut, Key: "greeting", Value: []byte("hello"), ClientID: "alice", Seq: 42}
	if got := f.last(t); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("PUT became %+v, want %+v", got, want)
	}

	call(h, "GET", "/v1/kv/greeting", "")
	if got := f.last(t); got.Op != kv.OpRead || got.Key != "greeting" || got.ClientID != "" || got.Seq != 0 {
		t.Fatalf("GET became %+v, want a read with no identity", got)
	}

	call(h, "DELETE", "/v1/kv/greeting", "", id...)
	if got := f.last(t); got.Op != kv.OpDelete || got.Key != "greeting" || got.Seq != 42 {
		t.Fatalf("DELETE became %+v", got)
	}

	for _, tc := range []struct {
		body string
		want kv.Command
	}{
		{`{"value":"v","expect_absent":true}`, kv.Command{Op: kv.OpCAS, Expect: kv.ExpectAbsent}},
		{`{"value":"v","expect_value":"old"}`, kv.Command{Op: kv.OpCAS, Expect: kv.ExpectValue, ExpectValue: []byte("old")}},
		{`{"value":"v","expect_value":""}`, kv.Command{Op: kv.OpCAS, Expect: kv.ExpectValue}},
		{`{"value":"v","expect_revision":9}`, kv.Command{Op: kv.OpCAS, Expect: kv.ExpectRevision, ExpectRevision: 9}},
	} {
		if r := call(h, "POST", "/v1/kv/k/cas", tc.body, id...); r.code != 200 {
			t.Fatalf("CAS %s: %d %s", tc.body, r.code, r.body)
		}
		got := f.last(t)
		if got.Op != tc.want.Op || got.Expect != tc.want.Expect || string(got.ExpectValue) != string(tc.want.ExpectValue) || got.ExpectRevision != tc.want.ExpectRevision || string(got.Value) != "v" {
			t.Fatalf("CAS %s became %+v", tc.body, got)
		}
	}
}

func TestKeysAreUnescapedFromThePath(t *testing.T) {
	f := &fakeNode{}
	h := newAPI(f, nil)
	if r := call(h, "GET", "/v1/kv/users%2F42%20x", ""); r.code != 200 {
		t.Fatalf("GET with an escaped key: %d %s", r.code, r.body)
	}
	if got := f.last(t).Key; got != "users/42 x" {
		t.Fatalf("key = %q, want %q", got, "users/42 x")
	}
}

func TestEmptyValueIsAccepted(t *testing.T) {
	f := &fakeNode{}
	if r := call(newAPI(f, nil), "PUT", "/v1/kv/k", `{"value":""}`); r.code != 200 {
		t.Fatalf("PUT of an empty value: %d %s", r.code, r.body)
	}
	if got := f.last(t); got.Op != kv.OpPut || len(got.Value) != 0 {
		t.Fatalf("command = %+v", got)
	}
}

func TestMutationWithoutIdentityGetsAGeneratedOne(t *testing.T) {
	f := &fakeNode{}
	h := newAPI(f, nil)
	r1 := call(h, "PUT", "/v1/kv/k", `{"value":"v"}`)
	r2 := call(h, "PUT", "/v1/kv/k", `{"value":"v"}`)
	id1, id2 := r1.header.Get(HeaderClientID), r2.header.Get(HeaderClientID)
	if id1 == "" || id1 == id2 {
		t.Fatalf("generated identities %q and %q, want two different non-empty values", id1, id2)
	}
	if got := f.last(t); got.ClientID != id2 || got.Seq != 1 {
		t.Fatalf("command identity = (%q, %d), want (%q, 1)", got.ClientID, got.Seq, id2)
	}
	// The identity the client did send is echoed back.
	r3 := call(h, "PUT", "/v1/kv/k", `{"value":"v"}`, HeaderClientID, "alice", HeaderSeq, "3")
	if r3.header.Get(HeaderClientID) != "alice" || r3.header.Get(HeaderSeq) != "3" {
		t.Fatalf("identity not echoed: %v", r3.header)
	}
}

// ---- validation happens before anything is proposed ------------------------

func TestInvalidRequestsNeverReachTheNode(t *testing.T) {
	long := strings.Repeat("k", kv.DefaultLimits.MaxKeyBytes+1)
	bigValue := strings.Repeat("v", kv.DefaultLimits.MaxValueBytes+1)
	tests := []struct {
		name, method, target, body string
		headers                    []string
		wantCode                   int
		wantErr                    string
	}{
		{"key too long", "GET", "/v1/kv/" + long, "", nil, 400, CodeInvalidArgument},
		{"key with control character", "GET", "/v1/kv/a%0Ab", "", nil, 400, CodeInvalidArgument},
		{"put without body", "PUT", "/v1/kv/k", "", nil, 400, CodeInvalidArgument},
		{"put with malformed JSON", "PUT", "/v1/kv/k", `{"value":`, nil, 400, CodeInvalidArgument},
		{"put without value", "PUT", "/v1/kv/k", `{}`, nil, 400, CodeInvalidArgument},
		{"put with non-string value", "PUT", "/v1/kv/k", `{"value":5}`, nil, 400, CodeInvalidArgument},
		{"put with unknown field", "PUT", "/v1/kv/k", `{"value":"v","ttl":5}`, nil, 400, CodeInvalidArgument},
		{"put with two JSON objects", "PUT", "/v1/kv/k", `{"value":"v"}{"value":"w"}`, nil, 400, CodeInvalidArgument},
		{"value too large", "PUT", "/v1/kv/k", `{"value":"` + bigValue + `"}`, nil, 400, CodeInvalidArgument},
		{"body too large", "PUT", "/v1/kv/k", `{"value":"` + strings.Repeat("v", maxBodyBytes) + `"}`, nil, 413, CodePayloadTooLarge},
		{"cas without condition", "POST", "/v1/kv/k/cas", `{"value":"v"}`, nil, 400, CodeInvalidArgument},
		{"cas with two conditions", "POST", "/v1/kv/k/cas", `{"value":"v","expect_absent":true,"expect_value":"x"}`, nil, 400, CodeInvalidArgument},
		{"cas without new value", "POST", "/v1/kv/k/cas", `{"expect_absent":true}`, nil, 400, CodeInvalidArgument},
		{"cas with revision zero", "POST", "/v1/kv/k/cas", `{"value":"v","expect_revision":0}`, nil, 400, CodeInvalidArgument},
		{"client id without sequence", "PUT", "/v1/kv/k", `{"value":"v"}`, []string{HeaderClientID, "a"}, 400, CodeInvalidArgument},
		{"sequence without client id", "PUT", "/v1/kv/k", `{"value":"v"}`, []string{HeaderSeq, "1"}, 400, CodeInvalidArgument},
		{"sequence zero", "PUT", "/v1/kv/k", `{"value":"v"}`, []string{HeaderClientID, "a", HeaderSeq, "0"}, 400, CodeInvalidArgument},
		{"sequence not a number", "PUT", "/v1/kv/k", `{"value":"v"}`, []string{HeaderClientID, "a", HeaderSeq, "x"}, 400, CodeInvalidArgument},
		{"client id too long", "PUT", "/v1/kv/k", `{"value":"v"}`, []string{HeaderClientID, strings.Repeat("a", 65), HeaderSeq, "1"}, 400, CodeInvalidArgument},
		{"timeout not a duration", "GET", "/v1/kv/k?timeout=soon", "", nil, 400, CodeInvalidArgument},
		{"timeout negative", "GET", "/v1/kv/k?timeout=-1s", "", nil, 400, CodeInvalidArgument},
		{"unknown endpoint", "GET", "/v2/anything", "", nil, 404, CodeNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeNode{}
			r := call(newAPI(f, nil), tc.method, tc.target, tc.body, tc.headers...)
			if r.code != tc.wantCode {
				t.Fatalf("status = %d, want %d: %s", r.code, tc.wantCode, r.body)
			}
			if e := r.errorBody(t); e.Code != tc.wantErr || e.Outcome != OutcomeNotApplied {
				t.Fatalf("error = %+v, want code %s with outcome not_applied", e, tc.wantErr)
			}
			if f.count() != 0 {
				t.Fatalf("an invalid request was proposed: %+v", f.commands)
			}
		})
	}
}

// ---- outcomes are reported accurately ---------------------------------------

func TestNodeErrorsMapToDistinctResponses(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantStatus    int
		wantCode      string
		wantOutcome   string
		wantRetryable bool
		wantLeader    string
	}{
		{"not leader, leader known", &node.NotLeaderError{Leader: "n2"}, 421, CodeNotLeader, OutcomeNotApplied, true, "http://n2.example:8002"},
		{"not leader, no leader known", &node.NotLeaderError{}, 503, CodeNoLeader, OutcomeNotApplied, true, ""},
		{"overloaded", node.ErrOverloaded, 429, CodeOverloaded, OutcomeNotApplied, true, ""},
		{"leadership lost", node.ErrProposalDropped, 503, CodeLeadershipLost, OutcomeNotApplied, true, ""},
		{"deadline after submission", fmt.Errorf("%w: deadline", node.ErrOutcomeUnknown), 504, CodeTimeout, OutcomeUnknown, true, ""},
		{"shutting down", node.ErrStopped, 503, CodeShuttingDown, OutcomeNotApplied, true, ""},
		{"unexpected", errors.New("boom"), 500, CodeInternal, OutcomeUnknown, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeNode{propose: func(context.Context, kv.Command) (kv.Result, error) { return kv.Result{}, tc.err }}
			r := call(newAPI(f, nil), "PUT", "/v1/kv/k", `{"value":"v"}`)
			e := r.errorBody(t)
			if r.code != tc.wantStatus || e.Code != tc.wantCode || e.Outcome != tc.wantOutcome || e.Retryable != tc.wantRetryable {
				t.Fatalf("got status %d, %+v; want %d %s outcome=%s retryable=%v", r.code, e, tc.wantStatus, tc.wantCode, tc.wantOutcome, tc.wantRetryable)
			}
			if tc.wantLeader != "" {
				if e.Leader == nil || e.Leader.ID != "n2" || e.Leader.URL != tc.wantLeader || r.header.Get(HeaderLeader) != tc.wantLeader {
					t.Fatalf("leader hint = %+v, header %q; want %s", e.Leader, r.header.Get(HeaderLeader), tc.wantLeader)
				}
			} else if e.Leader != nil {
				t.Fatalf("unexpected leader hint %+v", e.Leader)
			}
			if tc.wantCode == CodeOverloaded && r.header.Get("Retry-After") == "" {
				t.Fatal("OVERLOADED without a Retry-After header")
			}
		})
	}
}

func TestStateMachineResultsMapToResponses(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		body       string
		result     kv.Result
		wantStatus int
		wantCode   string
		check      func(t *testing.T, r response)
	}{
		{"get found", "GET", "/v1/kv/k", "", kv.Result{Value: []byte("v"), Revision: 5}, 200, "", func(t *testing.T, r response) {
			var g GetResponse
			json.Unmarshal(r.body, &g)
			if g != (GetResponse{Key: "k", Value: "v", Revision: 5}) {
				t.Fatalf("body = %s", r.body)
			}
		}},
		{"get missing", "GET", "/v1/kv/k", "", kv.Result{Status: kv.StatusNotFound}, 404, CodeKeyNotFound, nil},
		{"put duplicate", "PUT", "/v1/kv/k", `{"value":"v"}`, kv.Result{Revision: 5, Duplicate: true}, 200, "", func(t *testing.T, r response) {
			var p PutResponse
			json.Unmarshal(r.body, &p)
			if p != (PutResponse{Key: "k", Revision: 5, Duplicate: true}) {
				t.Fatalf("body = %s", r.body)
			}
		}},
		{"delete of a missing key succeeds", "DELETE", "/v1/kv/k", "", kv.Result{}, 200, "", func(t *testing.T, r response) {
			var d DeleteResponse
			json.Unmarshal(r.body, &d)
			if d.Existed {
				t.Fatalf("body = %s", r.body)
			}
		}},
		{"cas swapped", "POST", "/v1/kv/k/cas", `{"value":"v","expect_absent":true}`, kv.Result{Revision: 9}, 200, "", func(t *testing.T, r response) {
			var c CASResponse
			json.Unmarshal(r.body, &c)
			if !c.Swapped || c.Revision != 9 {
				t.Fatalf("body = %s", r.body)
			}
		}},
		{"cas failed", "POST", "/v1/kv/k/cas", `{"value":"v","expect_absent":true}`, kv.Result{Status: kv.StatusCASFailed, Existed: true, Revision: 4}, 409, CodeCASFailed, func(t *testing.T, r response) {
			e := r.errorBody(t)
			if e.Exists == nil || !*e.Exists || e.Revision == nil || *e.Revision != 4 {
				t.Fatalf("a failed CAS must report the key's current state: %s", r.body)
			}
		}},
		{"identity reused", "PUT", "/v1/kv/k", `{"value":"v"}`, kv.Result{Status: kv.StatusIdentityReused}, 409, CodeIdentityReused, nil},
		{"stale sequence", "PUT", "/v1/kv/k", `{"value":"v"}`, kv.Result{Status: kv.StatusStaleSequence}, 409, CodeStaleSequence, nil},
		{"capacity exceeded", "PUT", "/v1/kv/k", `{"value":"v"}`, kv.Result{Status: kv.StatusCapacityExceeded}, 507, CodeCapacityExceeded, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeNode{propose: func(context.Context, kv.Command) (kv.Result, error) { return tc.result, nil }}
			r := call(newAPI(f, nil), tc.method, tc.target, tc.body)
			if r.code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", r.code, tc.wantStatus, r.body)
			}
			if tc.wantCode != "" {
				if e := r.errorBody(t); e.Code != tc.wantCode {
					t.Fatalf("code = %s, want %s", e.Code, tc.wantCode)
				}
			}
			if tc.check != nil {
				tc.check(t, r)
			}
			if ct := r.header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q", ct)
			}
		})
	}
}

// ---- deadlines -------------------------------------------------------------

func TestDeadlineIsEnforcedAndReportedAsUnknown(t *testing.T) {
	var gotDeadline time.Duration
	f := &fakeNode{propose: func(ctx context.Context, _ kv.Command) (kv.Result, error) {
		d, _ := ctx.Deadline()
		gotDeadline = time.Until(d)
		<-ctx.Done() // a command stuck waiting for a quorum
		return kv.Result{}, fmt.Errorf("%w: %v", node.ErrOutcomeUnknown, ctx.Err())
	}}
	h := newAPI(f, nil)
	start := time.Now()
	r := call(h, "PUT", "/v1/kv/k?timeout=80ms", `{"value":"v"}`)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("request took %v with an 80ms deadline", took)
	}
	if gotDeadline <= 0 || gotDeadline > 80*time.Millisecond {
		t.Fatalf("node saw a deadline %v away, want at most 80ms", gotDeadline)
	}
	e := r.errorBody(t)
	if r.code != 504 || e.Code != CodeTimeout || e.Outcome != OutcomeUnknown {
		t.Fatalf("status %d, %+v; want 504 TIMEOUT with outcome unknown", r.code, e)
	}
	if !strings.Contains(e.Message, "may still take effect") {
		t.Fatalf("the message must not imply the write failed: %q", e.Message)
	}
}

func TestRequestedTimeoutIsCappedByTheServer(t *testing.T) {
	var gotDeadline time.Duration
	f := &fakeNode{propose: func(ctx context.Context, _ kv.Command) (kv.Result, error) {
		d, _ := ctx.Deadline()
		gotDeadline = time.Until(d)
		return kv.Result{}, nil
	}}
	h := newAPI(f, nil) // MaxTimeout is 5s
	if r := call(h, "PUT", "/v1/kv/k?timeout=10m", `{"value":"v"}`); r.code != 200 {
		t.Fatalf("status %d: %s", r.code, r.body)
	}
	if gotDeadline > 5*time.Second || gotDeadline < 4*time.Second {
		t.Fatalf("deadline %v away, want it capped at the 5s maximum", gotDeadline)
	}
	call(h, "PUT", "/v1/kv/k", `{"value":"v"}`)
	if gotDeadline > 2*time.Second || gotDeadline < time.Second {
		t.Fatalf("deadline %v away, want the 2s default", gotDeadline)
	}
}

func TestClientDisconnectCancelsTheWait(t *testing.T) {
	cancelled := make(chan struct{})
	f := &fakeNode{propose: func(ctx context.Context, _ kv.Command) (kv.Result, error) {
		<-ctx.Done()
		close(cancelled)
		return kv.Result{}, fmt.Errorf("%w: %v", node.ErrOutcomeUnknown, ctx.Err())
	}}
	srv := httptest.NewServer(newAPI(f, func(o *Options) { o.DefaultTimeout, o.MaxTimeout = time.Minute, time.Minute }))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "PUT", srv.URL+"/v1/kv/k", strings.NewReader(`{"value":"v"}`))
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel() // the client goes away
	}()
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("expected the client request to be cancelled")
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler kept waiting after its client disconnected")
	}
}

// ---- authentication --------------------------------------------------------

func TestBearerTokenProtectsDataEndpoints(t *testing.T) {
	const token = "a-long-enough-secret-token-value"
	f := &fakeNode{}
	h := newAPI(f, func(o *Options) { o.Token = token })

	for _, tc := range []struct {
		name   string
		header []string
		want   int
	}{
		{"no token", nil, 401},
		{"wrong token", []string{"Authorization", "Bearer wrong"}, 401},
		{"wrong scheme", []string{"Authorization", "Basic " + token}, 401},
		{"token as prefix of a longer value", []string{"Authorization", "Bearer " + token + "x"}, 401},
		{"correct token", []string{"Authorization", "Bearer " + token}, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := f.count()
			for _, target := range []string{"/v1/kv/k", "/v1/status"} {
				r := call(h, "GET", target, "", tc.header...)
				if r.code != tc.want {
					t.Fatalf("GET %s: status %d, want %d", target, r.code, tc.want)
				}
				if tc.want == 401 {
					if e := r.errorBody(t); e.Code != CodeUnauthenticated {
						t.Fatalf("code = %s", e.Code)
					}
					if strings.Contains(string(r.body), token) {
						t.Fatal("the response leaks the token")
					}
				}
			}
			if tc.want == 401 && f.count() != before {
				t.Fatal("an unauthenticated request was proposed")
			}
		})
	}
	// Health checks stay open so an orchestrator can probe without secrets.
	for _, target := range []string{"/healthz", "/readyz"} {
		if r := call(h, "GET", target, ""); r.code == 401 {
			t.Fatalf("%s requires a token", target)
		}
	}
}

func TestAdminEndpointsRequireTheTokenAndPprofIsOptIn(t *testing.T) {
	const token = "a-long-enough-secret-token-value"
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_metric", Help: "x"}))
	api := New(&fakeNode{}, Options{Token: token})

	admin := api.AdminHandler(reg, false)
	if r := call(admin, "GET", "/metrics", ""); r.code != 401 {
		t.Fatalf("/metrics without a token: %d", r.code)
	}
	r := call(admin, "GET", "/metrics", "", "Authorization", "Bearer "+token)
	if r.code != 200 || !strings.Contains(string(r.body), "test_metric") {
		t.Fatalf("/metrics with the token: %d %s", r.code, r.body)
	}
	if r := call(admin, "GET", "/debug/pprof/", "", "Authorization", "Bearer "+token); r.code != 404 {
		t.Fatalf("pprof is reachable although it was not enabled: %d", r.code)
	}

	withPprof := api.AdminHandler(reg, true)
	if r := call(withPprof, "GET", "/debug/pprof/", ""); r.code != 401 {
		t.Fatalf("pprof without a token: %d", r.code)
	}
	if r := call(withPprof, "GET", "/debug/pprof/", "", "Authorization", "Bearer "+token); r.code != 200 {
		t.Fatalf("pprof with the token: %d", r.code)
	}
	// The client-facing handler never serves operational endpoints.
	client := api.Handler()
	for _, target := range []string{"/metrics", "/debug/pprof/"} {
		if r := call(client, "GET", target, "", "Authorization", "Bearer "+token); r.code != 404 {
			t.Fatalf("%s is served on the client listener: %d", target, r.code)
		}
	}
}

// ---- health and status -----------------------------------------------------

func TestLivenessAndReadinessAreDifferentQuestions(t *testing.T) {
	tests := []struct {
		name       string
		status     node.Status
		wantHealth int
		wantReady  int
	}{
		{"leader", node.Status{Status: raft.Status{Role: raft.Leader, Leader: "n1"}}, 200, 200},
		{"follower with a leader", node.Status{Status: raft.Status{Role: raft.Follower, Leader: "n2"}}, 200, 200},
		// Alive, but cut off or without a quorum: it cannot serve.
		{"no leader known", node.Status{Status: raft.Status{Role: raft.PreCandidate}}, 200, 503},
		{"storage failed", node.Status{Failed: errors.New("fsync: input/output error")}, 503, 503},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newAPI(&fakeNode{status: tc.status}, nil)
			if r := call(h, "GET", "/healthz", ""); r.code != tc.wantHealth {
				t.Fatalf("/healthz = %d, want %d", r.code, tc.wantHealth)
			}
			r := call(h, "GET", "/readyz", "")
			if r.code != tc.wantReady {
				t.Fatalf("/readyz = %d, want %d", r.code, tc.wantReady)
			}
			if strings.Contains(string(r.body), "input/output") {
				t.Fatal("health output exposes internal error details")
			}
		})
	}
}

func TestStatusReportsReplicationProgress(t *testing.T) {
	f := &fakeNode{status: node.Status{
		Status: raft.Status{Role: raft.Leader, Term: 4, Leader: "n1", Commit: 90, Applied: 90, FirstIndex: 50, LastIndex: 100,
			Peers: []raft.PeerStatus{{ID: "n2", Match: 100, Next: 101, RecentActive: true}, {ID: "n3", Match: 40, Next: 41, Snapshotting: true}}},
		SnapshotIndex: 60, Keys: 12, StateBytes: 345, Sessions: 3, Pending: 2,
	}}
	r := call(newAPI(f, nil), "GET", "/v1/status", "")
	var st StatusResponse
	if err := json.Unmarshal(r.body, &st); err != nil || r.code != 200 {
		t.Fatalf("status %d: %s", r.code, r.body)
	}
	if st.Role != "leader" || st.Term != 4 || st.Leader == nil || st.Leader.URL != "http://n1.example:8001" || st.SnapshotIndex != 60 || !st.Healthy {
		t.Fatalf("status = %+v", st)
	}
	if len(st.Peers) != 2 || st.Peers[0].Lag != 0 || st.Peers[1].Lag != 60 || !st.Peers[1].Snapshotting {
		t.Fatalf("peers = %+v, want n2 caught up and n3 60 entries behind and snapshotting", st.Peers)
	}
}

func TestMetricLabelsComeFromAFixedSet(t *testing.T) {
	seen := map[string]bool{}
	var mu sync.Mutex
	h := newAPI(&fakeNode{}, func(o *Options) {
		o.Observe = func(op string, _ int, _ time.Duration) {
			mu.Lock()
			seen[op] = true
			mu.Unlock()
		}
	})
	for i := 0; i < 50; i++ {
		call(h, "GET", fmt.Sprintf("/v1/kv/key-%d", i), "")
		call(h, "PUT", fmt.Sprintf("/v1/kv/key-%d", i), `{"value":"v"}`, HeaderClientID, fmt.Sprintf("client-%d", i), HeaderSeq, "1")
		call(h, "GET", fmt.Sprintf("/random/path/%d", i), "")
	}
	// 150 requests over 50 keys, 50 clients and 50 junk paths must not
	// produce more label values than there are operations.
	if len(seen) != 3 || !seen["get"] || !seen["put"] || !seen["other"] {
		t.Fatalf("operation labels = %v, want exactly get, put and other", seen)
	}
}
