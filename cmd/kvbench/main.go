// Command kvbench is a load generator for raft-kv.
//
// It is a closed-loop generator: each of the --clients workers sends one
// request, waits for the answer, and only then sends the next. That choice
// matters for reading the results. A closed loop measures how fast the system
// serves a fixed number of patient clients; when the system slows down the
// clients slow down with it, so the offered load falls and queues never
// build. It therefore understates the latency that clients arriving at a
// fixed rate would see during a stall (the "coordinated omission" effect).
// The numbers answer "what throughput and latency do N concurrent clients
// get", not "what happens at X requests per second".
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/client"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/httpapi"
)

type options struct {
	endpoints    string
	tokenFile    string
	caCert       string
	workload     string
	clients      int
	duration     time.Duration
	warmup       time.Duration
	runs         int
	keys         int
	valueBytes   int
	readFraction float64
	attempts     int
	attemptTO    time.Duration
	output       string
	label        string
	seed         int64
	meta         metaFlag
}

type metaFlag map[string]string

func (m metaFlag) String() string { return fmt.Sprint(map[string]string(m)) }
func (m metaFlag) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return errors.New("expected key=value")
	}
	m[k] = val
	return nil
}

// sample is one completed request.
type sample struct {
	at      time.Duration // completion time since the measured phase began
	latency time.Duration
	op      string
	result  string // "ok", "cas_conflict", or an error code
}

// Latency is a latency summary in milliseconds.
type Latency struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// RunResult is the outcome of one measured run.
type RunResult struct {
	Run             int     `json:"run"`
	DurationSeconds float64 `json:"duration_seconds"`
	// Attempted counts every request sent in the measured phase; Completed
	// counts those that got a definite, successful answer.
	Attempted      int                `json:"attempted"`
	Completed      int                `json:"completed"`
	Throughput     float64            `json:"throughput_ops_per_second"`
	Latency        Latency            `json:"latency"`
	LatencyByOp    map[string]Latency `json:"latency_by_op"`
	Results        map[string]int     `json:"results"`
	UnknownOutcome int                `json:"outcome_unknown"`
	// LongestGapMs is the longest stretch with no successful request. With
	// a healthy cluster it is near the latency; during a failover it is the
	// interruption clients saw.
	LongestGapMs float64 `json:"longest_gap_without_success_ms"`
	// Timeline counts successes and failures per 100 ms bucket.
	Timeline []Bucket `json:"timeline,omitempty"`
	// LeaderBefore and LeaderAfter show whether leadership moved mid-run.
	LeaderBefore string `json:"leader_before"`
	LeaderAfter  string `json:"leader_after"`
}

// Bucket is one 100 ms slice of a run.
type Bucket struct {
	AtMs   int `json:"at_ms"`
	OK     int `json:"ok"`
	Failed int `json:"failed"`
}

// Report is the file kvbench writes.
type Report struct {
	Label       string            `json:"label"`
	StartedAt   string            `json:"started_at"`
	Workload    map[string]any    `json:"workload"`
	Environment map[string]string `json:"environment"`
	Meta        map[string]string `json:"meta"`
	Runs        []RunResult       `json:"runs"`
	Summary     map[string]any    `json:"summary"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	o := options{meta: metaFlag{}}
	fs := flag.NewFlagSet("kvbench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.endpoints, "endpoints", "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003", "comma-separated node URLs")
	fs.StringVar(&o.tokenFile, "token-file", "", "file holding the bearer token")
	fs.StringVar(&o.caCert, "ca-cert", "", "CA certificate for HTTPS endpoints")
	fs.StringVar(&o.workload, "workload", "mixed", "write, read, mixed or cas")
	fs.IntVar(&o.clients, "clients", 16, "concurrent closed-loop clients")
	fs.DurationVar(&o.duration, "duration", 20*time.Second, "measured duration of each run")
	fs.DurationVar(&o.warmup, "warmup", 5*time.Second, "unmeasured warm-up before each run")
	fs.IntVar(&o.runs, "runs", 3, "number of runs")
	fs.IntVar(&o.keys, "keys", 1000, "size of the key space (for cas: number of contended counters)")
	fs.IntVar(&o.valueBytes, "value-bytes", 128, "value size in bytes")
	fs.Float64Var(&o.readFraction, "read-fraction", 0.9, "mixed: fraction of requests that are reads")
	fs.IntVar(&o.attempts, "attempts", 4, "maximum attempts per request, including the first")
	fs.DurationVar(&o.attemptTO, "attempt-timeout", 3*time.Second, "deadline of one attempt")
	fs.StringVar(&o.output, "output", "", "write the JSON report to this file")
	fs.StringVar(&o.label, "label", "", "name for this benchmark in the report")
	fs.Int64Var(&o.seed, "seed", 1, "seed for key selection")
	fs.Var(o.meta, "meta", "extra key=value recorded in the report (repeatable)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	switch o.workload {
	case "write", "read", "mixed", "cas":
	default:
		fmt.Fprintf(stderr, "kvbench: unknown workload %q\n", o.workload)
		return 2
	}
	if o.clients < 1 || o.keys < 1 || o.runs < 1 || o.valueBytes < 0 || o.duration <= 0 {
		fmt.Fprintln(stderr, "kvbench: clients, keys and runs must be positive")
		return 2
	}
	token := ""
	if o.tokenFile != "" {
		b, err := os.ReadFile(o.tokenFile)
		if err != nil {
			fmt.Fprintln(stderr, "kvbench:", err)
			return 2
		}
		token = strings.TrimSpace(string(b))
	}
	newClient := func(id string) (*client.Client, error) {
		return client.New(client.Options{
			Endpoints: strings.Split(o.endpoints, ","), Token: token, CACertFile: o.caCert,
			ClientID: id, MaxAttempts: o.attempts, AttemptTimeout: o.attemptTO,
		})
	}

	stamp := time.Now().UTC()
	admin, err := newClient(fmt.Sprintf("bench-admin-%d", stamp.UnixNano()))
	if err != nil {
		fmt.Fprintln(stderr, "kvbench:", err)
		return 2
	}
	defer admin.Close()

	if err := preload(admin, o); err != nil {
		fmt.Fprintln(stderr, "kvbench: preloading keys:", err)
		return 1
	}

	report := Report{
		Label:     o.label,
		StartedAt: stamp.Format(time.RFC3339),
		Workload: map[string]any{
			"name": o.workload, "clients": o.clients, "keys": o.keys, "value_bytes": o.valueBytes,
			"read_fraction": o.readFraction, "duration": o.duration.String(), "warmup": o.warmup.String(),
			"runs": o.runs, "max_attempts": o.attempts, "attempt_timeout": o.attemptTO.String(),
			"generator": "closed loop: each client waits for a response before sending its next request",
			"endpoints": len(strings.Split(o.endpoints, ",")),
		},
		Environment: environment(),
		Meta:        o.meta,
	}

	for r := 1; r <= o.runs; r++ {
		res, err := oneRun(o, r, stamp, newClient, admin)
		if err != nil {
			fmt.Fprintln(stderr, "kvbench:", err)
			return 1
		}
		report.Runs = append(report.Runs, res)
		fmt.Fprintf(stdout, "run %d: %8.0f ops/s  p50 %6.2f ms  p95 %6.2f ms  p99 %6.2f ms  max %7.2f ms  completed %d/%d  unknown %d  longest gap %.0f ms  leader %s->%s\n",
			r, res.Throughput, res.Latency.P50, res.Latency.P95, res.Latency.P99, res.Latency.Max,
			res.Completed, res.Attempted, res.UnknownOutcome, res.LongestGapMs, res.LeaderBefore, res.LeaderAfter)
	}
	report.Summary = summarise(report.Runs)
	fmt.Fprintf(stdout, "summary over %d run(s): throughput mean %.0f ops/s (min %.0f, max %.0f, stddev %.0f); p50 mean %.2f ms; p99 mean %.2f ms\n",
		o.runs, report.Summary["throughput_mean"], report.Summary["throughput_min"], report.Summary["throughput_max"],
		report.Summary["throughput_stddev"], report.Summary["p50_mean_ms"], report.Summary["p99_mean_ms"])

	if o.output != "" {
		data, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(o.output, append(data, '\n'), 0o644); err != nil {
			fmt.Fprintln(stderr, "kvbench:", err)
			return 1
		}
		fmt.Fprintln(stdout, "report written to", o.output)
	}
	return 0
}

func keyName(i int) string { return "bench/k" + strconv.Itoa(i) }

// preload makes sure every key a read or CAS will touch exists.
func preload(c *client.Client, o options) error {
	if o.workload == "write" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	value := strings.Repeat("x", o.valueBytes)
	if o.workload == "cas" {
		value = "0"
	}
	for i := 0; i < o.keys; i++ {
		if _, err := c.Put(ctx, keyName(i), value); err != nil {
			return err
		}
	}
	return nil
}

func leaderOf(c *client.Client) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, ep := range c.Endpoints() {
		if st, err := c.Status(ctx, ep); err == nil && st.Role == "leader" {
			return st.NodeID
		}
	}
	return "unknown"
}

func oneRun(o options, run int, stamp time.Time, newClient func(string) (*client.Client, error), admin *client.Client) (RunResult, error) {
	res := RunResult{Run: run, LeaderBefore: leaderOf(admin)}
	value := strings.Repeat("v", o.valueBytes)

	start := time.Now()
	measureFrom := start.Add(o.warmup)
	end := measureFrom.Add(o.duration)
	ctx, cancel := context.WithDeadline(context.Background(), end.Add(o.attemptTO))
	defer cancel()

	samples := make([][]sample, o.clients)
	var wg sync.WaitGroup
	for w := 0; w < o.clients; w++ {
		// One Client per worker: each is one logical client with its own
		// identity and its own sequence of request numbers.
		c, err := newClient(fmt.Sprintf("bench-%d-r%d-w%d", stamp.UnixNano(), run, w))
		if err != nil {
			return res, err
		}
		wg.Add(1)
		go func(w int, c *client.Client) {
			defer wg.Done()
			defer c.Close()
			rng := rand.New(rand.NewSource(o.seed + int64(run)*1000 + int64(w)))
			for time.Now().Before(end) {
				key := keyName(rng.Intn(o.keys))
				began := time.Now()
				op, result := doOp(ctx, c, o, rng, key, value)
				done := time.Now()
				if began.Before(measureFrom) {
					continue // warm-up
				}
				samples[w] = append(samples[w], sample{at: done.Sub(measureFrom), latency: done.Sub(began), op: op, result: result})
			}
		}(w, c)
	}
	wg.Wait()
	res.LeaderAfter = leaderOf(admin)

	var all []sample
	for _, s := range samples {
		all = append(all, s...)
	}
	res.DurationSeconds = o.duration.Seconds()
	res.Attempted = len(all)
	res.Results = map[string]int{}
	byOp := map[string][]time.Duration{}
	var okLat []time.Duration
	var okTimes []time.Duration
	buckets := make([]Bucket, int(o.duration/(100*time.Millisecond))+1)
	for i := range buckets {
		buckets[i].AtMs = i * 100
	}
	for _, s := range all {
		res.Results[s.result]++
		b := int(s.at / (100 * time.Millisecond))
		if b >= len(buckets) {
			b = len(buckets) - 1
		}
		// A CAS that loses a race still got a correct, timely answer.
		if s.result == "ok" || s.result == "cas_conflict" {
			okLat = append(okLat, s.latency)
			okTimes = append(okTimes, s.at)
			byOp[s.op] = append(byOp[s.op], s.latency)
			buckets[b].OK++
		} else {
			buckets[b].Failed++
			if s.result == "unknown" {
				res.UnknownOutcome++
			}
		}
	}
	res.Completed = len(okLat)
	res.Throughput = float64(res.Completed) / o.duration.Seconds()
	res.Latency = summarize(okLat)
	res.LatencyByOp = map[string]Latency{}
	for op, l := range byOp {
		res.LatencyByOp[op] = summarize(l)
	}
	res.LongestGapMs = longestGap(okTimes, o.duration)
	res.Timeline = buckets
	return res, nil
}

// doOp performs one request and classifies its result.
func doOp(ctx context.Context, c *client.Client, o options, rng *rand.Rand, key, value string) (op, result string) {
	var err error
	switch {
	case o.workload == "read" || (o.workload == "mixed" && rng.Float64() < o.readFraction):
		op = "get"
		_, _, _, err = c.Get(ctx, key)
	case o.workload == "cas":
		// Read a counter, then try to replace it conditionally. Several
		// clients race on few keys, so some attempts lose.
		op = "cas"
		var cur string
		var rev uint64
		var found bool
		if cur, rev, found, err = c.Get(ctx, key); err == nil && found {
			n, _ := strconv.Atoi(cur)
			var swapped bool
			if swapped, _, _, err = c.CAS(ctx, key, client.IfRevision(rev), strconv.Itoa(n+1)); err == nil && !swapped {
				return op, "cas_conflict"
			}
		}
	default:
		op = "put"
		_, err = c.Put(ctx, key, value)
	}
	if err == nil {
		return op, "ok"
	}
	var e *client.Error
	if errors.As(err, &e) {
		if e.Outcome == httpapi.OutcomeUnknown {
			return op, "unknown"
		}
		return op, e.Code
	}
	return op, "error"
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func summarize(l []time.Duration) Latency {
	if len(l) == 0 {
		return Latency{}
	}
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	var sum time.Duration
	for _, d := range l {
		sum += d
	}
	pct := func(p float64) float64 {
		// Nearest-rank percentile.
		i := int(math.Ceil(p*float64(len(l)))) - 1
		return ms(l[max(0, min(i, len(l)-1))])
	}
	return Latency{Count: len(l), Mean: ms(sum) / float64(len(l)), P50: pct(0.50), P95: pct(0.95), P99: pct(0.99), Max: ms(l[len(l)-1])}
}

func longestGap(okTimes []time.Duration, total time.Duration) float64 {
	sort.Slice(okTimes, func(i, j int) bool { return okTimes[i] < okTimes[j] })
	prev, longest := time.Duration(0), time.Duration(0)
	for _, t := range okTimes {
		if t-prev > longest {
			longest = t - prev
		}
		prev = t
	}
	if total-prev > longest {
		longest = total - prev
	}
	return ms(longest)
}

func summarise(runs []RunResult) map[string]any {
	var tput, p50, p99 []float64
	for _, r := range runs {
		tput, p50, p99 = append(tput, r.Throughput), append(p50, r.Latency.P50), append(p99, r.Latency.P99)
	}
	mean := func(v []float64) float64 {
		s := 0.0
		for _, x := range v {
			s += x
		}
		return s / float64(len(v))
	}
	mn, mx, m := tput[0], tput[0], mean(tput)
	variance := 0.0
	for _, x := range tput {
		mn, mx = math.Min(mn, x), math.Max(mx, x)
		variance += (x - m) * (x - m)
	}
	return map[string]any{
		"throughput_mean": m, "throughput_min": mn, "throughput_max": mx,
		"throughput_stddev": math.Sqrt(variance / float64(len(tput))),
		"p50_mean_ms":       mean(p50), "p99_mean_ms": mean(p99),
	}
}

// environment records where the load generator ran. The servers may be
// elsewhere; the benchmark scripts add their details through --meta.
func environment() map[string]string {
	env := map[string]string{
		"go_version": runtime.Version(),
		"os":         runtime.GOOS,
		"arch":       runtime.GOARCH,
		"cpus":       strconv.Itoa(runtime.NumCPU()),
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if rest, ok := strings.CutPrefix(line, "MemTotal:"); ok {
				env["mem_total"] = strings.TrimSpace(rest)
			}
		}
	}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "model name") {
				if _, v, ok := strings.Cut(line, ":"); ok {
					env["cpu_model"] = strings.TrimSpace(v)
					break
				}
			}
		}
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		env["kernel"] = strings.TrimSpace(string(b))
	}
	return env
}
