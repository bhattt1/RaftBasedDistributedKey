// Package client is a Go client for the raft-kv HTTP API. kvctl and kvbench
// are built on it, and so are the integration tests.
//
// A Client represents one logical client with one identity. It numbers its
// mutations 1, 2, 3, ... and must be used for one request at a time; that is
// the contract the server's retry de-duplication relies on. Concurrent
// workers each need their own Client.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/httpapi"
)

// Options configures a Client.
type Options struct {
	// Endpoints are base URLs of cluster members, e.g. http://127.0.0.1:8001.
	Endpoints []string
	// Token is the bearer token for secure clusters.
	Token string
	// CACertFile verifies the servers' HTTPS certificates. Empty uses the
	// system roots.
	CACertFile string
	// ClientID identifies this client for retry de-duplication. Empty
	// generates a random one.
	ClientID string
	// FirstSeq is the sequence number of the first mutation. Zero means 1.
	// A caller resuming an interrupted request passes the number it used.
	FirstSeq uint64
	// AttemptTimeout bounds one HTTP attempt. Zero means 3 seconds.
	AttemptTimeout time.Duration
	// MaxAttempts bounds how many attempts one request may make. Zero means 8.
	MaxAttempts int
	// HTTPClient overrides the HTTP client; tests use it.
	HTTPClient *http.Client
}

// Client talks to a raft-kv cluster.
type Client struct {
	opts Options
	http *http.Client

	mu     sync.Mutex
	seq    uint64
	leader string // endpoint that last served a request or was hinted
	next   int    // rotation through endpoints when no leader is known
}

// Error is a failed request. Code and Outcome come from the server's error
// body when there was one; for transport failures Code is "UNREACHABLE".
type Error struct {
	Code    string
	Message string
	// Outcome is "not_applied" when the request definitely had no effect
	// and "unknown" when it may have, or may yet.
	Outcome  string
	Status   int
	Attempts int
	// Exists and Revision are set on CAS_FAILED.
	Exists   bool
	Revision uint64
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (outcome: %s, attempts: %d)", e.Code, e.Message, e.Outcome, e.Attempts)
}

// CodeUnreachable is the Error code used when no server could be reached.
const CodeUnreachable = "UNREACHABLE"

// IsCode reports whether err is an *Error with the given code.
func IsCode(err error, code string) bool {
	var e *Error
	return errors.As(err, &e) && e.Code == code
}

// New creates a client.
func New(opts Options) (*Client, error) {
	if len(opts.Endpoints) == 0 {
		return nil, errors.New("client: at least one endpoint is required")
	}
	for i, e := range opts.Endpoints {
		u, err := url.Parse(e)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("client: endpoint %q is not an http:// or https:// URL", e)
		}
		opts.Endpoints[i] = strings.TrimRight(e, "/")
	}
	if opts.ClientID == "" {
		var b [9]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		opts.ClientID = "c-" + hex.EncodeToString(b[:])
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 3 * time.Second
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 8
	}
	c := &Client{opts: opts, http: opts.HTTPClient}
	if opts.FirstSeq > 0 {
		c.seq = opts.FirstSeq - 1
	}
	if c.http == nil {
		tr := &http.Transport{MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second}
		if opts.CACertFile != "" {
			pem, err := os.ReadFile(opts.CACertFile)
			if err != nil {
				return nil, fmt.Errorf("client: CA certificate: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("client: %s contains no certificates", opts.CACertFile)
			}
			tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		}
		c.http = &http.Client{Transport: tr}
	}
	return c, nil
}

// ID returns the client's identity.
func (c *Client) ID() string { return c.opts.ClientID }

// LastSeq returns the sequence number of the most recent mutation.
func (c *Client) LastSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seq
}

// Close releases idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// ---- operations ------------------------------------------------------------

// Get reads a key. found is false when the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (value string, revision uint64, found bool, err error) {
	var resp httpapi.GetResponse
	err = c.do(ctx, http.MethodGet, keyPath(key), nil, 0, &resp)
	if IsCode(err, httpapi.CodeKeyNotFound) {
		return "", 0, false, nil
	}
	return resp.Value, resp.Revision, err == nil, err
}

// Put stores a value.
func (c *Client) Put(ctx context.Context, key, value string) (httpapi.PutResponse, error) {
	var resp httpapi.PutResponse
	err := c.do(ctx, http.MethodPut, keyPath(key), httpapi.PutRequest{Value: &value}, c.nextSeq(), &resp)
	return resp, err
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) (httpapi.DeleteResponse, error) {
	var resp httpapi.DeleteResponse
	err := c.do(ctx, http.MethodDelete, keyPath(key), nil, c.nextSeq(), &resp)
	return resp, err
}

// Condition is the expectation of a compare-and-swap. Exactly one of the
// constructors below builds it.
type Condition struct{ req httpapi.CASRequest }

// IfAbsent succeeds only if the key does not exist.
func IfAbsent() Condition { return Condition{httpapi.CASRequest{ExpectAbsent: true}} }

// IfValue succeeds only if the key currently has this value.
func IfValue(v string) Condition { return Condition{httpapi.CASRequest{ExpectValue: &v}} }

// IfRevision succeeds only if the key is currently at this revision.
func IfRevision(r uint64) Condition { return Condition{httpapi.CASRequest{ExpectRevision: &r}} }

// CAS writes value if the condition holds. When it does not, swapped is false
// and err is nil; cur describes the key as the server saw it.
func (c *Client) CAS(ctx context.Context, key string, cond Condition, value string) (swapped bool, revision uint64, cur CurrentState, err error) {
	req := cond.req
	req.Value = &value
	var resp httpapi.CASResponse
	err = c.do(ctx, http.MethodPost, keyPath(key)+"/cas", req, c.nextSeq(), &resp)
	var e *Error
	if errors.As(err, &e) && e.Code == httpapi.CodeCASFailed {
		return false, 0, CurrentState{Exists: e.Exists, Revision: e.Revision}, nil
	}
	return err == nil, resp.Revision, CurrentState{}, err
}

// CurrentState is what a failed CAS reports about the key.
type CurrentState struct {
	Exists   bool
	Revision uint64
}

// Status fetches one node's status. It does not follow leader hints.
func (c *Client) Status(ctx context.Context, endpoint string) (httpapi.StatusResponse, error) {
	var resp httpapi.StatusResponse
	ctx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
	defer cancel()
	status, body, err := c.attempt(ctx, strings.TrimRight(endpoint, "/"), http.MethodGet, "/v1/status", nil, 0)
	if err != nil {
		return resp, &Error{Code: CodeUnreachable, Message: err.Error(), Outcome: httpapi.OutcomeNotApplied, Attempts: 1}
	}
	if status != http.StatusOK {
		return resp, serverError(status, body, 1)
	}
	return resp, json.Unmarshal(body, &resp)
}

// Endpoints returns the configured endpoints.
func (c *Client) Endpoints() []string { return append([]string(nil), c.opts.Endpoints...) }

func keyPath(key string) string { return "/v1/kv/" + url.PathEscape(key) }

func (c *Client) nextSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return c.seq
}

// ---- the retry loop --------------------------------------------------------

// do sends one logical request, retrying where that is safe.
//
// It retries when the server says the request was not applied and may be
// retried (not the leader, no leader yet, overloaded, leadership lost), and
// also when the outcome is unknown (a timeout, a broken connection). The
// second kind is safe only because every attempt carries the same request
// identity: if an earlier attempt did take effect, the server recognises the
// retry and returns the original result instead of applying it again. Reads
// carry no identity and need none, since repeating a read changes nothing.
//
// It never retries a definite answer such as CAS_FAILED or INVALID_ARGUMENT.
func (c *Client) do(ctx context.Context, method, path string, body any, seq uint64, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	// Once any attempt may have reached a server, a later failure cannot be
	// reported as "not applied".
	ambiguous := false
	var last *Error
	backoff := 25 * time.Millisecond
	// Endpoints that could not be reached during this request. A follower
	// that has not yet noticed its leader is gone keeps hinting at it;
	// following that hint again would only burn an attempt.
	unreachable := map[string]bool{}

	for attempt := 1; attempt <= c.opts.MaxAttempts; attempt++ {
		endpoint := c.target()
		actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		status, respBody, err := c.attempt(actx, endpoint, method, path, payload, seq)
		cancel()

		wait := true
		switch {
		case err != nil:
			// A connection that could not be opened carried nothing. Any
			// other failure may have happened after the request arrived, so
			// we cannot tell whether it was received.
			ambiguous = ambiguous || (method != http.MethodGet && !neverSent(err))
			last = &Error{Code: CodeUnreachable, Message: err.Error()}
			unreachable[endpoint] = true
			c.forget(endpoint)
		case status >= 200 && status < 300:
			c.remember(endpoint)
			if out == nil {
				return nil
			}
			return json.Unmarshal(respBody, out)
		default:
			last = serverError(status, respBody, attempt)
			switch last.Code {
			case httpapi.CodeNotLeader:
				var eb httpapi.ErrorBody
				json.Unmarshal(respBody, &eb)
				hint := ""
				if eb.Error.Leader != nil {
					hint = strings.TrimRight(eb.Error.Leader.URL, "/")
				}
				if hint != "" && hint != endpoint && !unreachable[hint] {
					c.remember(hint)
					wait = false // follow the hint straight away
				} else {
					// No hint, or a hint to a node we already know is down:
					// the cluster is probably electing. Back off.
					c.forget(endpoint)
				}
			case httpapi.CodeNoLeader, httpapi.CodeOverloaded, httpapi.CodeLeadershipLost, httpapi.CodeShuttingDown:
				c.forget(endpoint)
			case httpapi.CodeTimeout:
				ambiguous = ambiguous || method != http.MethodGet
				c.forget(endpoint)
			default:
				// A definite answer. Report it as it is.
				last.Attempts = attempt
				return last
			}
		}

		if ctx.Err() != nil || attempt == c.opts.MaxAttempts {
			last.Attempts = attempt
			break
		}
		if wait {
			// Exponential backoff with jitter, so that many clients that
			// lost the same leader do not all return at the same instant.
			sleep := backoff/2 + time.Duration(mrand.Int64N(int64(backoff)))
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				last.Attempts = attempt
				return c.final(last, ambiguous)
			}
			if backoff < time.Second {
				backoff *= 2
			}
		}
	}
	return c.final(last, ambiguous)
}

// neverSent reports whether err shows the request never left this machine:
// the TCP connection could not be established at all.
func neverSent(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

func (c *Client) final(last *Error, ambiguous bool) error {
	if last == nil {
		last = &Error{Code: CodeUnreachable, Message: "no attempt was made"}
	}
	last.Outcome = httpapi.OutcomeNotApplied
	if ambiguous {
		last.Outcome = httpapi.OutcomeUnknown
	}
	return last
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	e := c.opts.Endpoints[c.next%len(c.opts.Endpoints)]
	c.next++
	return e
}

func (c *Client) remember(endpoint string) {
	c.mu.Lock()
	c.leader = endpoint
	c.mu.Unlock()
}

func (c *Client) forget(endpoint string) {
	c.mu.Lock()
	if c.leader == endpoint {
		c.leader = ""
	}
	c.mu.Unlock()
}

func (c *Client) attempt(ctx context.Context, endpoint, method, path string, payload []byte, seq uint64) (int, []byte, error) {
	// Ask the server to give up slightly before we do, so that its answer
	// ("timeout, outcome unknown") has time to arrive.
	if deadline, ok := ctx.Deadline(); ok {
		if left := time.Until(deadline) - 100*time.Millisecond; left > 50*time.Millisecond && !strings.Contains(path, "/status") {
			path += "?timeout=" + url.QueryEscape(left.Round(time.Millisecond).String())
		}
	}
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.opts.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.opts.Token)
	}
	if seq != 0 {
		req.Header.Set(httpapi.HeaderClientID, c.opts.ClientID)
		req.Header.Set(httpapi.HeaderSeq, strconv.FormatUint(seq, 10))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func serverError(status int, body []byte, attempts int) *Error {
	var eb httpapi.ErrorBody
	if err := json.Unmarshal(body, &eb); err != nil || eb.Error.Code == "" {
		return &Error{Code: "HTTP_" + strconv.Itoa(status), Message: strings.TrimSpace(string(body)), Outcome: httpapi.OutcomeUnknown, Status: status, Attempts: attempts}
	}
	e := &Error{Code: eb.Error.Code, Message: eb.Error.Message, Outcome: eb.Error.Outcome, Status: status, Attempts: attempts}
	if eb.Error.Exists != nil {
		e.Exists = *eb.Error.Exists
	}
	if eb.Error.Revision != nil {
		e.Revision = *eb.Error.Revision
	}
	return e
}
