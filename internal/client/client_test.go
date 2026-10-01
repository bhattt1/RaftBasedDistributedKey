package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/httpapi"
)

// seen records what a fake server received for one request.
type seen struct {
	method, path, clientID, seq, auth string
}

// fakeServer answers each request with the next scripted reply; once the
// script is exhausted it repeats the last one.
type fakeServer struct {
	*httptest.Server
	mu      sync.Mutex
	got     []seen
	replies []func(w http.ResponseWriter, r *http.Request)
}

func newFake(t *testing.T, replies ...func(http.ResponseWriter, *http.Request)) *fakeServer {
	f := &fakeServer{replies: replies}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		i := len(f.got)
		f.got = append(f.got, seen{r.Method, r.URL.Path, r.Header.Get(httpapi.HeaderClientID), r.Header.Get(httpapi.HeaderSeq), r.Header.Get("Authorization")})
		reply := f.replies[min(i, len(f.replies)-1)]
		f.mu.Unlock()
		reply(w, r)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeServer) requests() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seen(nil), f.got...)
}

func ok(body any) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(body)
	}
}

func fail(status int, e httpapi.Error) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(httpapi.ErrorBody{Error: e})
	}
}

// hang accepts a request and never answers it. The body is drained first:
// the server only notices that a client has gone away once it has nothing
// left to read from the connection.
func hang(w http.ResponseWriter, r *http.Request) {
	io.Copy(io.Discard, r.Body)
	<-r.Context().Done()
}

func mustClient(t *testing.T, opts Options) *Client {
	t.Helper()
	if opts.AttemptTimeout == 0 {
		opts.AttemptTimeout = 300 * time.Millisecond
	}
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

var bg = context.Background()

// ---- leader discovery ------------------------------------------------------

func TestFollowsLeaderHintAndRemembersIt(t *testing.T) {
	leader := newFake(t, ok(httpapi.PutResponse{Key: "k", Revision: 5}))
	follower := newFake(t, fail(421, httpapi.Error{Code: httpapi.CodeNotLeader, Outcome: httpapi.OutcomeNotApplied, Leader: &httpapi.LeaderHint{ID: "n2", URL: leader.URL}}))
	c := mustClient(t, Options{Endpoints: []string{follower.URL}, ClientID: "alice"})

	resp, err := c.Put(bg, "k", "v")
	if err != nil || resp.Revision != 5 {
		t.Fatalf("Put = %+v, %v", resp, err)
	}
	// The leader was not in the endpoint list; the hint alone led there.
	if n := len(leader.requests()); n != 1 {
		t.Fatalf("leader saw %d requests, want 1", n)
	}
	// The next request goes straight to the leader.
	if _, err := c.Put(bg, "k", "v2"); err != nil {
		t.Fatal(err)
	}
	if got := len(follower.requests()); got != 1 {
		t.Fatalf("follower saw %d requests, want 1: the client should remember the leader", got)
	}
}

func TestStaleHintDoesNotLoopForever(t *testing.T) {
	// Two nodes each insist the other is the leader, as can happen briefly
	// around an election. The client must give up after a bounded number of
	// attempts.
	var a, b *fakeServer
	a = newFake(t, func(w http.ResponseWriter, r *http.Request) {
		fail(421, httpapi.Error{Code: httpapi.CodeNotLeader, Leader: &httpapi.LeaderHint{ID: "b", URL: b.URL}})(w, r)
	})
	b = newFake(t, func(w http.ResponseWriter, r *http.Request) {
		fail(421, httpapi.Error{Code: httpapi.CodeNotLeader, Leader: &httpapi.LeaderHint{ID: "a", URL: a.URL}})(w, r)
	})
	c := mustClient(t, Options{Endpoints: []string{a.URL, b.URL}, MaxAttempts: 6})
	_, err := c.Put(bg, "k", "v")
	if !IsCode(err, httpapi.CodeNotLeader) {
		t.Fatalf("err = %v, want NOT_LEADER after exhausting attempts", err)
	}
	if total := len(a.requests()) + len(b.requests()); total != 6 {
		t.Fatalf("%d attempts were made, the limit was 6", total)
	}
	if e := err.(*Error); e.Outcome != httpapi.OutcomeNotApplied || e.Attempts != 6 {
		t.Fatalf("error = %+v, want outcome not_applied after 6 attempts", e)
	}
}

// ---- retries keep the request identity -------------------------------------

func TestRetriesReuseTheSameIdentity(t *testing.T) {
	srv := newFake(t,
		fail(503, httpapi.Error{Code: httpapi.CodeNoLeader, Outcome: httpapi.OutcomeNotApplied}),
		fail(504, httpapi.Error{Code: httpapi.CodeTimeout, Outcome: httpapi.OutcomeUnknown}),
		fail(429, httpapi.Error{Code: httpapi.CodeOverloaded, Outcome: httpapi.OutcomeNotApplied}),
		ok(httpapi.PutResponse{Key: "k", Revision: 9, Duplicate: true}),
	)
	c := mustClient(t, Options{Endpoints: []string{srv.URL}, ClientID: "alice", Token: "secret-token"})
	resp, err := c.Put(bg, "k", "v")
	if err != nil || !resp.Duplicate {
		t.Fatalf("Put = %+v, %v", resp, err)
	}
	reqs := srv.requests()
	if len(reqs) != 4 {
		t.Fatalf("%d attempts, want 4", len(reqs))
	}
	for i, r := range reqs {
		if r.clientID != "alice" || r.seq != "1" {
			t.Fatalf("attempt %d carried identity (%q, %q); every attempt must carry (alice, 1)", i+1, r.clientID, r.seq)
		}
		if r.auth != "Bearer secret-token" {
			t.Fatalf("attempt %d carried Authorization %q", i+1, r.auth)
		}
	}
	// A new logical request gets the next sequence number.
	c.Put(bg, "k", "v2")
	if got := srv.requests()[4].seq; got != "2" {
		t.Fatalf("second request used sequence %s, want 2", got)
	}
}

func TestReadsCarryNoIdentity(t *testing.T) {
	srv := newFake(t, ok(httpapi.GetResponse{Key: "k", Value: "v", Revision: 3}))
	c := mustClient(t, Options{Endpoints: []string{srv.URL}})
	value, rev, found, err := c.Get(bg, "k")
	if err != nil || !found || value != "v" || rev != 3 {
		t.Fatalf("Get = %q, %d, %v, %v", value, rev, found, err)
	}
	if r := srv.requests()[0]; r.clientID != "" || r.seq != "" {
		t.Fatalf("a read carried identity (%q, %q)", r.clientID, r.seq)
	}
	if c.LastSeq() != 0 {
		t.Fatalf("a read consumed sequence number %d", c.LastSeq())
	}
}

func TestResumingWithAnExplicitSequenceNumber(t *testing.T) {
	srv := newFake(t, ok(httpapi.PutResponse{Key: "k", Revision: 9, Duplicate: true}))
	c := mustClient(t, Options{Endpoints: []string{srv.URL}, ClientID: "alice", FirstSeq: 17})
	if _, err := c.Put(bg, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if got := srv.requests()[0].seq; got != "17" {
		t.Fatalf("sequence = %s, want 17", got)
	}
}

// ---- definite answers are not retried --------------------------------------

func TestDefiniteAnswersAreReturnedAtOnce(t *testing.T) {
	exists := true
	rev := uint64(4)
	for _, tc := range []struct {
		name   string
		status int
		e      httpapi.Error
	}{
		{"invalid argument", 400, httpapi.Error{Code: httpapi.CodeInvalidArgument, Outcome: httpapi.OutcomeNotApplied}},
		{"unauthenticated", 401, httpapi.Error{Code: httpapi.CodeUnauthenticated, Outcome: httpapi.OutcomeNotApplied}},
		{"identity reused", 409, httpapi.Error{Code: httpapi.CodeIdentityReused, Outcome: httpapi.OutcomeNotApplied}},
		{"stale sequence", 409, httpapi.Error{Code: httpapi.CodeStaleSequence, Outcome: httpapi.OutcomeNotApplied}},
		{"capacity exceeded", 507, httpapi.Error{Code: httpapi.CodeCapacityExceeded, Outcome: httpapi.OutcomeNotApplied}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFake(t, fail(tc.status, tc.e))
			c := mustClient(t, Options{Endpoints: []string{srv.URL}})
			_, err := c.Put(bg, "k", "v")
			if !IsCode(err, tc.e.Code) {
				t.Fatalf("err = %v, want %s", err, tc.e.Code)
			}
			if n := len(srv.requests()); n != 1 {
				t.Fatalf("%d attempts for a definite answer, want 1", n)
			}
		})
	}

	t.Run("failed CAS is a result, not an error", func(t *testing.T) {
		srv := newFake(t, fail(409, httpapi.Error{Code: httpapi.CodeCASFailed, Outcome: httpapi.OutcomeNotApplied, Exists: &exists, Revision: &rev}))
		c := mustClient(t, Options{Endpoints: []string{srv.URL}})
		swapped, _, cur, err := c.CAS(bg, "k", IfValue("old"), "new")
		if err != nil || swapped || !cur.Exists || cur.Revision != 4 {
			t.Fatalf("CAS = swapped %v, current %+v, err %v", swapped, cur, err)
		}
		if n := len(srv.requests()); n != 1 {
			t.Fatalf("%d attempts, want 1", n)
		}
	})
	t.Run("missing key is a result, not an error", func(t *testing.T) {
		srv := newFake(t, fail(404, httpapi.Error{Code: httpapi.CodeKeyNotFound, Outcome: httpapi.OutcomeNotApplied}))
		c := mustClient(t, Options{Endpoints: []string{srv.URL}})
		_, _, found, err := c.Get(bg, "k")
		if err != nil || found {
			t.Fatalf("Get = found %v, err %v", found, err)
		}
	})
}

// ---- what the client claims about the outcome ------------------------------

func TestOutcomeIsUnknownOnceAnAttemptMayHaveLanded(t *testing.T) {
	// First attempt: the server accepts the request and never answers.
	// Later attempts are cleanly refused. The write may have happened in the
	// first attempt, so the client must not report "not applied".
	srv := newFake(t, hang, fail(503, httpapi.Error{Code: httpapi.CodeNoLeader, Outcome: httpapi.OutcomeNotApplied}))
	c := mustClient(t, Options{Endpoints: []string{srv.URL}, MaxAttempts: 3, AttemptTimeout: 100 * time.Millisecond})
	_, err := c.Put(bg, "k", "v")
	e, isErr := err.(*Error)
	if !isErr || e.Outcome != httpapi.OutcomeUnknown {
		t.Fatalf("err = %v, want an error with outcome unknown", err)
	}
}

func TestOutcomeIsNotAppliedWhenEveryAttemptWasRefused(t *testing.T) {
	srv := newFake(t, fail(503, httpapi.Error{Code: httpapi.CodeNoLeader, Outcome: httpapi.OutcomeNotApplied}))
	c := mustClient(t, Options{Endpoints: []string{srv.URL}, MaxAttempts: 3})
	_, err := c.Put(bg, "k", "v")
	e, isErr := err.(*Error)
	if !isErr || e.Outcome != httpapi.OutcomeNotApplied || e.Code != httpapi.CodeNoLeader {
		t.Fatalf("err = %v, want NO_LEADER with outcome not_applied", err)
	}
}

func TestUnreachableServersAreReported(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	c := mustClient(t, Options{Endpoints: []string{url}, MaxAttempts: 2})
	_, _, _, err := c.Get(bg, "k")
	if !IsCode(err, CodeUnreachable) {
		t.Fatalf("err = %v, want UNREACHABLE", err)
	}
	// A read that never succeeded changed nothing, whatever happened.
	if e := err.(*Error); e.Outcome != httpapi.OutcomeNotApplied {
		t.Fatalf("outcome = %s", e.Outcome)
	}
}

func TestConnectionRefusedIsNotAmbiguous(t *testing.T) {
	// Nothing listens here, so a write can not have been delivered.
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	c := mustClient(t, Options{Endpoints: []string{url}, MaxAttempts: 2})
	_, err := c.Put(bg, "k", "v")
	e, isErr := err.(*Error)
	if !isErr || e.Code != CodeUnreachable || e.Outcome != httpapi.OutcomeNotApplied {
		t.Fatalf("err = %v, want UNREACHABLE with outcome not_applied", err)
	}
}

func TestHintToAnUnreachableLeaderIsNotFollowedTwice(t *testing.T) {
	// The old leader has just died. The follower does not know yet and keeps
	// pointing at it. The client must not bounce between the two; it should
	// back off and ask the follower again until the election is over.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	follower := newFake(t,
		fail(421, httpapi.Error{Code: httpapi.CodeNotLeader, Leader: &httpapi.LeaderHint{ID: "old", URL: deadURL}}),
		fail(421, httpapi.Error{Code: httpapi.CodeNotLeader, Leader: &httpapi.LeaderHint{ID: "old", URL: deadURL}}),
		fail(421, httpapi.Error{Code: httpapi.CodeNotLeader, Leader: &httpapi.LeaderHint{ID: "old", URL: deadURL}}),
		ok(httpapi.PutResponse{Key: "k", Revision: 3}), // by now it has been elected itself
	)
	c := mustClient(t, Options{Endpoints: []string{follower.URL}, MaxAttempts: 6})
	if _, err := c.Put(bg, "k", "v"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Attempts: follower, dead (once), follower, follower, follower = 5.
	// Following the stale hint each time would have needed 7.
	if got := len(follower.requests()); got != 4 {
		t.Fatalf("follower saw %d requests, want 4", got)
	}
}

func TestContextDeadlineBoundsTheWholeRequest(t *testing.T) {
	srv := newFake(t, fail(503, httpapi.Error{Code: httpapi.CodeNoLeader, Outcome: httpapi.OutcomeNotApplied}))
	c := mustClient(t, Options{Endpoints: []string{srv.URL}, MaxAttempts: 1000})
	ctx, cancel := context.WithTimeout(bg, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Put(ctx, "k", "v")
	if err == nil {
		t.Fatal("expected an error")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("request took %v with a 300ms deadline", took)
	}
	// Backoff means far fewer than one attempt per millisecond.
	if n := len(srv.requests()); n > 20 {
		t.Fatalf("%d attempts in 300ms: the client is not backing off", n)
	}
}

func TestEndpointsAreTriedInTurnWhenNoLeaderIsKnown(t *testing.T) {
	down := newFake(t, fail(503, httpapi.Error{Code: httpapi.CodeNoLeader, Outcome: httpapi.OutcomeNotApplied}))
	up := newFake(t, ok(httpapi.DeleteResponse{Key: "k", Existed: true}))
	c := mustClient(t, Options{Endpoints: []string{down.URL, up.URL}})
	resp, err := c.Delete(bg, "k")
	if err != nil || !resp.Existed {
		t.Fatalf("Delete = %+v, %v", resp, err)
	}
	if len(down.requests()) != 1 || len(up.requests()) != 1 {
		t.Fatalf("attempts: first endpoint %d, second %d; want one each", len(down.requests()), len(up.requests()))
	}
}

func TestKeysAreEscapedInThePath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		ok(httpapi.GetResponse{})(w, r)
	}))
	defer srv.Close()
	c := mustClient(t, Options{Endpoints: []string{srv.URL}})
	c.Get(bg, "users/42 x?")
	if gotPath != "/v1/kv/users%2F42%20x%3F" {
		t.Fatalf("path = %s", gotPath)
	}
}

func TestNewValidatesEndpoints(t *testing.T) {
	for _, eps := range [][]string{nil, {"localhost:8001"}, {"ftp://host"}, {"http://"}} {
		if _, err := New(Options{Endpoints: eps}); err == nil {
			t.Errorf("endpoints %v were accepted", eps)
		}
	}
	c, err := New(Options{Endpoints: []string{"http://host:1/"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.ID() == "" || c.Endpoints()[0] != "http://host:1" {
		t.Fatalf("id %q, endpoints %v", c.ID(), c.Endpoints())
	}
}
