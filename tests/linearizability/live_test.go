package linearizability

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/node"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/testcluster"
)

// TestLiveClusterHistoriesAreLinearizable runs real nodes (real event loops,
// real write-ahead logs on a crash-modelling filesystem, real goroutines)
// while a nemesis partitions and crashes them, and checks what concurrent
// clients observed.
//
// Unlike the simulated test this one is not reproducible from a seed: thread
// scheduling differs between runs. It complements the simulation by
// exercising the node's own concurrency. Timestamps come from one monotonic
// clock in this process, read immediately before each request is sent and
// immediately after its answer arrives, so the recorded intervals contain
// the true ones.
func TestLiveClusterHistoriesAreLinearizable(t *testing.T) {
	duration := 6 * time.Second
	if v := os.Getenv("LIN_LIVE_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("bad LIN_LIVE_DURATION: %v", err)
		}
		duration = d
	}
	if testing.Short() {
		duration = 2 * time.Second
	}

	c := testcluster.New(t, 5, func(cfg *node.Config) {
		cfg.SnapshotThreshold, cfg.SnapshotTrailing = 150, 20
	})
	c.WaitLeader()

	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	var mu sync.Mutex
	var history []porcupine.Operation
	var completed, unknown atomic.Int64

	ctx, stop := context.WithTimeout(context.Background(), duration)
	defer stop()

	// ---- clients -----------------------------------------------------------
	var clients sync.WaitGroup
	for i := 0; i < 6; i++ {
		clients.Add(1)
		go func(i int) {
			defer clients.Done()
			rng := rand.New(rand.NewSource(int64(i) + 1))
			cl := c.Client(fmt.Sprintf("live-%d", i))
			lastRead := map[string]string{}
			for n := 0; ctx.Err() == nil; n++ {
				key := simKeys[rng.Intn(len(simKeys))]
				value := fmt.Sprintf("live-%d-%d", i, n)
				var in Input
				cmd := kv.Command{Key: key}
				switch p := rng.Intn(100); {
				case p < 35:
					in, cmd.Op = Input{Op: kv.OpRead, Key: key}, kv.OpRead
				case p < 65:
					in, cmd.Op, cmd.Value = Input{Op: kv.OpPut, Key: key, Value: value}, kv.OpPut, []byte(value)
				case p < 90:
					cmd.Op, cmd.Value = kv.OpCAS, []byte(value)
					if last, ok := lastRead[key]; ok {
						in = Input{Op: kv.OpCAS, Key: key, ExpectValue: last, Value: value}
						cmd.Expect, cmd.ExpectValue = kv.ExpectValue, []byte(last)
					} else {
						in = Input{Op: kv.OpCAS, Key: key, ExpectAbsent: true, Value: value}
						cmd.Expect = kv.ExpectAbsent
					}
				default:
					in, cmd.Op = Input{Op: kv.OpDelete, Key: key}, kv.OpDelete
				}

				// One logical operation, however many attempts it takes.
				opCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				call := now()
				res, err := cl.Do(opCtx, cmd)
				ret := now()
				cancel()

				op := porcupine.Operation{ClientId: i, Input: in, Call: call}
				switch {
				case err == nil && (res.Status == kv.StatusOK || res.Status == kv.StatusNotFound || res.Status == kv.StatusCASFailed):
					op.Output, op.Return = OutputOf(res), ret
					completed.Add(1)
					if in.Op == kv.OpRead {
						if res.Status == kv.StatusOK {
							lastRead[key] = string(res.Value)
						} else {
							delete(lastRead, key)
						}
					}
				case err == nil:
					t.Errorf("client %d: unexpected status %s for %s", i, res.Status, Describe(in, Output{}))
					return
				case errors.Is(err, node.ErrOutcomeUnknown):
					// The client gave up. Whether the operation happened, or
					// will yet happen, is not known.
					op.Output, op.Return = Output{Unknown: true}, Infinity
					unknown.Add(1)
				default:
					t.Errorf("client %d: %v", i, err)
					return
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
			}
		}(i)
	}

	// ---- nemesis -----------------------------------------------------------
	var faults sync.WaitGroup
	var injected atomic.Int64
	faults.Add(1)
	go func() {
		defer faults.Done()
		rng := rand.New(rand.NewSource(99))
		down := map[string]bool{}
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(100+rng.Intn(250)) * time.Millisecond):
			}
			injected.Add(1)
			switch rng.Intn(6) {
			case 0: // cut the leader off from everyone else
				if l := c.Leader(); l != "" {
					c.Net.Partition(c.IDs, []string{l}, c.Others(l))
				}
			case 1: // split into a majority and a minority at random
				ids := append([]string(nil), c.IDs...)
				rng.Shuffle(len(ids), func(a, b int) { ids[a], ids[b] = ids[b], ids[a] })
				c.Net.Partition(c.IDs, ids[:2], ids[2:])
			case 2: // power-fail a node (never more than two at once)
				if len(down) < 2 {
					id := c.IDs[rng.Intn(len(c.IDs))]
					if !down[id] {
						c.Crash(id)
						down[id] = true
					}
				}
			case 3: // bring one back
				for id := range down {
					c.Start(id)
					delete(down, id)
					break
				}
			case 4: // one-way link failure
				c.Net.BlockLink(c.IDs[rng.Intn(len(c.IDs))], c.IDs[rng.Intn(len(c.IDs))])
			default:
				c.Net.Heal()
			}
		}
	}()

	faults.Wait()
	c.Net.Heal()
	for _, id := range c.IDs {
		c.Start(id)
	}
	clients.Wait()
	if t.Failed() {
		return
	}

	// The cluster must come back and agree once the faults stop.
	c.WaitLeader()
	c.WaitConverged(1)

	began := time.Now()
	res := Check(history, 2*time.Minute)
	t.Logf("%d operations in %v under %d injected faults: %d completed, %d with unknown outcome; checked in %v",
		len(history), duration, injected.Load(), completed.Load(), unknown.Load(), time.Since(began).Round(time.Millisecond))
	switch res.Verdict {
	case porcupine.Ok:
	case porcupine.Unknown:
		t.Fatalf("the checker timed out on %d operations; the result is inconclusive, not a pass", len(history))
	default:
		t.Fatalf("history is NOT linearizable\n%s\n%s", res.Explanation, c.Describe())
	}
	if completed.Load() < 100 {
		t.Fatalf("only %d operations completed; the run exercised too little to mean anything", completed.Load())
	}
}
