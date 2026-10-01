// Command kvctl is a small client for a raft-kv cluster.
//
//	kvctl put greeting hello
//	kvctl get greeting
//	kvctl cas greeting hi --expect-value hello
//	kvctl delete greeting
//	kvctl status
//
// It finds the leader by itself: a node that is not the leader answers with a
// hint, and kvctl follows it. Retries reuse the same request identity, so a
// request that timed out is never applied twice.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/client"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/httpapi"
)

// Exit codes. Scripts rely on these, so they are part of the interface.
const (
	exitOK       = 0
	exitError    = 1 // the request definitely did not take effect
	exitUsage    = 2
	exitNotFound = 3 // get: the key does not exist
	exitCASFail  = 4 // cas: the condition did not hold
	exitUnknown  = 5 // the request may or may not have taken effect
)

const usage = `Usage: kvctl [flags] <command> [arguments]

Commands:
  put <key> <value>         store a value
  get <key>                 read a value
  delete <key>              remove a key
  cas <key> <new-value>     write only if a condition holds; needs exactly one of
                            --expect-value, --expect-revision or --expect-absent
  status                    show every endpoint's view of the cluster

Flags may appear before or after the command.

Exit codes: 0 success, 1 failed (nothing was changed), 2 usage error,
3 key not found, 4 CAS condition did not hold, 5 outcome unknown.

Flags:
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

type flags struct {
	endpoints      string
	tokenFile      string
	caCert         string
	timeout        time.Duration
	attempts       int
	clientID       string
	seq            uint64
	jsonOut        bool
	expectValue    string
	expectRevision uint64
	expectAbsent   bool
}

func run(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	var f flags
	fs := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&f.endpoints, "endpoints", envOr(getenv, "RAFTKV_ENDPOINTS", "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"),
		"comma-separated node URLs (env RAFTKV_ENDPOINTS)")
	fs.StringVar(&f.tokenFile, "token-file", getenv("RAFTKV_TOKEN_FILE"), "file holding the bearer token (env RAFTKV_TOKEN_FILE)")
	fs.StringVar(&f.caCert, "ca-cert", getenv("RAFTKV_CA_CERT"), "CA certificate for HTTPS endpoints (env RAFTKV_CA_CERT)")
	fs.DurationVar(&f.timeout, "timeout", 10*time.Second, "overall deadline for the command, across all attempts")
	fs.IntVar(&f.attempts, "attempts", 20, "maximum number of attempts; --timeout usually ends the command first")
	fs.StringVar(&f.clientID, "client-id", "", "request identity; set it, with --seq, to safely repeat an earlier invocation")
	fs.Uint64Var(&f.seq, "seq", 0, "sequence number of the request (with --client-id)")
	fs.BoolVar(&f.jsonOut, "json", false, "print machine-readable JSON")
	fs.StringVar(&f.expectValue, "expect-value", "", "cas: succeed only if the key has this value")
	fs.Uint64Var(&f.expectRevision, "expect-revision", 0, "cas: succeed only if the key is at this revision")
	fs.BoolVar(&f.expectAbsent, "expect-absent", false, "cas: succeed only if the key does not exist")
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}

	// Accept flags on either side of the positional arguments.
	var positional []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return exitOK
			}
			return exitUsage
		}
		args = fs.Args()
		if len(args) > 0 {
			positional = append(positional, args[0])
			args = args[1:]
		}
	}
	set := map[string]bool{}
	fs.Visit(func(fl *flag.Flag) { set[fl.Name] = true })

	if len(positional) == 0 {
		fs.Usage()
		return exitUsage
	}
	cmd, rest := positional[0], positional[1:]
	want := map[string]int{"put": 2, "get": 1, "delete": 1, "cas": 2, "status": 0}
	n, known := want[cmd]
	if !known {
		fmt.Fprintf(stderr, "kvctl: unknown command %q\n\n", cmd)
		fs.Usage()
		return exitUsage
	}
	if len(rest) != n {
		fmt.Fprintf(stderr, "kvctl: %s takes %d argument(s), got %d\n", cmd, n, len(rest))
		return exitUsage
	}
	if set["client-id"] != set["seq"] {
		fmt.Fprintln(stderr, "kvctl: --client-id and --seq must be given together")
		return exitUsage
	}

	opts := client.Options{
		Endpoints:      strings.Split(f.endpoints, ","),
		CACertFile:     f.caCert,
		ClientID:       f.clientID,
		FirstSeq:       f.seq,
		MaxAttempts:    f.attempts,
		AttemptTimeout: min(f.timeout, 3*time.Second),
	}
	if f.tokenFile != "" {
		b, err := os.ReadFile(f.tokenFile)
		if err != nil {
			fmt.Fprintln(stderr, "kvctl: reading token:", err)
			return exitUsage
		}
		opts.Token = strings.TrimSpace(string(b))
	}
	c, err := client.New(opts)
	if err != nil {
		fmt.Fprintln(stderr, "kvctl:", err)
		return exitUsage
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()

	out := printer{w: stdout, json: f.jsonOut}
	switch cmd {
	case "put":
		resp, err := c.Put(ctx, rest[0], rest[1])
		if err != nil {
			return fail(stderr, c, err)
		}
		out.print(resp, "OK revision=%d%s\n", resp.Revision, dup(resp.Duplicate))
	case "get":
		value, rev, found, err := c.Get(ctx, rest[0])
		if err != nil {
			return fail(stderr, c, err)
		}
		if !found {
			out.print(map[string]any{"key": rest[0], "found": false}, "(not found)\n")
			return exitNotFound
		}
		if f.jsonOut {
			out.print(httpapi.GetResponse{Key: rest[0], Value: value, Revision: rev}, "")
		} else {
			fmt.Fprintln(stdout, value)
		}
	case "delete":
		resp, err := c.Delete(ctx, rest[0])
		if err != nil {
			return fail(stderr, c, err)
		}
		if resp.Existed {
			out.print(resp, "OK deleted%s\n", dup(resp.Duplicate))
		} else {
			out.print(resp, "OK key did not exist%s\n", dup(resp.Duplicate))
		}
	case "cas":
		conds := 0
		var cond client.Condition
		if set["expect-value"] {
			conds, cond = conds+1, client.IfValue(f.expectValue)
		}
		if set["expect-revision"] {
			conds, cond = conds+1, client.IfRevision(f.expectRevision)
		}
		if f.expectAbsent {
			conds, cond = conds+1, client.IfAbsent()
		}
		if conds != 1 {
			fmt.Fprintln(stderr, "kvctl: cas needs exactly one of --expect-value, --expect-revision or --expect-absent")
			return exitUsage
		}
		swapped, rev, cur, err := c.CAS(ctx, rest[0], cond, rest[1])
		if err != nil {
			return fail(stderr, c, err)
		}
		if !swapped {
			out.print(map[string]any{"key": rest[0], "swapped": false, "exists": cur.Exists, "revision": cur.Revision},
				"NOT SWAPPED: the condition did not hold (key exists=%v, revision=%d)\n", cur.Exists, cur.Revision)
			return exitCASFail
		}
		out.print(httpapi.CASResponse{Key: rest[0], Swapped: true, Revision: rev}, "OK swapped revision=%d\n", rev)
	case "status":
		return status(ctx, c, out, stdout)
	}
	return exitOK
}

func envOr(getenv func(string) string, key, def string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return def
}

func dup(d bool) string {
	if d {
		return " (duplicate: this request had already been applied; the original result is shown)"
	}
	return ""
}

type printer struct {
	w    io.Writer
	json bool
}

func (p printer) print(v any, format string, args ...any) {
	if p.json {
		json.NewEncoder(p.w).Encode(v)
		return
	}
	fmt.Fprintf(p.w, format, args...)
}

// fail reports an error and picks the exit code. The distinction that
// matters to a caller is whether the request can have taken effect.
func fail(stderr io.Writer, c *client.Client, err error) int {
	var e *client.Error
	if !errors.As(err, &e) {
		fmt.Fprintln(stderr, "kvctl:", err)
		return exitError
	}
	fmt.Fprintf(stderr, "kvctl: %s: %s\n", e.Code, e.Message)
	if e.Outcome == httpapi.OutcomeUnknown {
		fmt.Fprintf(stderr, "The request may or may not have taken effect. To find out without applying it twice, repeat the command with:\n  --client-id %s --seq %d\n", c.ID(), max(c.LastSeq(), 1))
		return exitUnknown
	}
	fmt.Fprintf(stderr, "The request was not applied (after %d attempt(s)).\n", e.Attempts)
	return exitError
}

func status(ctx context.Context, c *client.Client, out printer, stdout io.Writer) int {
	code := exitOK
	var all []any
	if !out.json {
		fmt.Fprintf(stdout, "%-28s %-6s %-10s %-6s %-8s %8s %8s %8s %6s\n", "ENDPOINT", "NODE", "ROLE", "TERM", "LEADER", "COMMIT", "APPLIED", "SNAPSHOT", "KEYS")
	}
	for _, ep := range c.Endpoints() {
		st, err := c.Status(ctx, ep)
		if err != nil {
			code = exitError
			if out.json {
				all = append(all, map[string]any{"endpoint": ep, "error": err.Error()})
			} else {
				fmt.Fprintf(stdout, "%-28s unreachable: %v\n", ep, err)
			}
			continue
		}
		if out.json {
			all = append(all, map[string]any{"endpoint": ep, "status": st})
			continue
		}
		leader := "-"
		if st.Leader != nil {
			leader = st.Leader.ID
		}
		fmt.Fprintf(stdout, "%-28s %-6s %-10s %-6d %-8s %8d %8d %8d %6d\n", ep, st.NodeID, st.Role, st.Term, leader, st.CommitIndex, st.AppliedIndex, st.SnapshotIndex, st.Keys)
	}
	if out.json {
		json.NewEncoder(stdout).Encode(all)
	}
	return code
}
