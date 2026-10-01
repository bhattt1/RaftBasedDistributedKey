package storage_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/faultfs"
)

// The crash test runs a random workload against the store and kills the
// "process" at every single filesystem operation in turn. After each crash it
// simulates a reboot (unsynced data is lost, partly kept or zeroed; unsynced
// directory changes are undone), reopens the store and checks that what came
// back is a state the node could legitimately have been in:
//
//   - everything acknowledged before the crash is present, and
//   - the operation that was in flight is either absent, complete, or cut at
//     a record boundary in the documented order (hard state, then entries).
//
// The expected states come from the small independent model below, not from
// the store's own replay code.

type model struct {
	hs      raft.HardState
	commit  uint64
	snap    raft.SnapshotMeta
	base    uint64 // index before entries[0]
	entries []raft.Entry
}

func (m model) clone() model {
	m.entries = append([]raft.Entry(nil), m.entries...)
	return m
}

func (m model) last() uint64 { return m.base + uint64(len(m.entries)) }

func (m model) term(i uint64) (uint64, bool) {
	if i <= m.base || i > m.last() {
		return 0, false
	}
	return m.entries[i-m.base-1].Term, true
}

// put stores one entry, replacing the suffix if the index is not new.
func (m *model) put(e raft.Entry) {
	m.entries = append(m.entries[:e.Index-m.base-1:e.Index-m.base-1], e)
}

// installSnapshot applies the Raft rule for an installed snapshot and
// reports whether the log after the snapshot point was kept.
func (m *model) installSnapshot(meta raft.SnapshotMeta) (keepLog bool) {
	t, ok := m.term(meta.Index)
	keepLog = ok && t == meta.Term
	if keepLog {
		m.entries = append([]raft.Entry(nil), m.entries[meta.Index-m.base:]...)
	} else {
		m.entries = nil
	}
	m.base, m.snap = meta.Index, meta
	if m.commit < meta.Index {
		m.commit = meta.Index
	}
	return keepLog
}

// visible is what recovery reports for a model state: entries after the
// newest snapshot.
type visible struct {
	hs      raft.HardState
	snap    raft.SnapshotMeta
	entries []raft.Entry
}

func (m model) visible() visible {
	v := visible{hs: m.hs, snap: m.snap}
	for _, e := range m.entries {
		if e.Index > m.snap.Index {
			v.entries = append(v.entries, e)
		}
	}
	return v
}

// op is one step of the workload. apply performs it on the store; states
// lists, in order, every model state a crash during the op may leave behind
// (the first is "nothing happened", the last is "completed").
type op struct {
	name   string
	apply  func(s *storage.Store) error
	states []model
}

// workload builds a deterministic sequence of operations from a seed.
func workload(t *testing.T, seed int64, steps int) []op {
	rng := rand.New(rand.NewSource(seed))
	m := model{}
	var ops []op
	for len(ops) < steps {
		before := m.clone()
		switch r := rng.Intn(100); {
		case r < 70: // a Ready: optional hard state, entries, commit
			var hs *raft.HardState
			states := []model{before.clone()}
			if m.hs.Term == 0 || rng.Intn(4) == 0 {
				next := raft.HardState{Term: m.hs.Term + 1 + uint64(rng.Intn(2)), Vote: fmt.Sprintf("n%d", 1+rng.Intn(3))}
				hs = &next
				m.hs = next
				states = append(states, m.clone())
			}
			// Either extend the log or, sometimes, replace an uncommitted
			// suffix the way a follower does after a leader change.
			from := m.last() + 1
			if rng.Intn(5) == 0 && m.last() > max(m.commit, m.base) {
				from = max(m.commit, m.base) + 1 + uint64(rng.Int63n(int64(m.last()-max(m.commit, m.base))))
			}
			var ents []raft.Entry
			for i, n := uint64(0), uint64(rng.Intn(4)); i < n; i++ {
				e := raft.Entry{Index: from + i, Term: m.hs.Term, Type: raft.EntryCommand,
					Data: []byte(fmt.Sprintf("s%d-op%d-%d", seed, len(ops), i))}
				ents = append(ents, e)
				m.put(e)
				states = append(states, m.clone())
			}
			if m.last() > m.commit && rng.Intn(2) == 0 {
				m.commit += 1 + uint64(rng.Int63n(int64(m.last()-m.commit)))
				states[len(states)-1].commit = m.commit
			}
			commit := m.commit
			if hs == nil && len(ents) == 0 {
				continue
			}
			ops = append(ops, op{name: "save", states: states, apply: func(s *storage.Store) error {
				return s.Save(hs, ents, commit, true)
			}})

		case r < 85: // local snapshot of applied state, then log compaction
			if m.commit <= m.snap.Index {
				continue
			}
			idx := m.snap.Index + 1 + uint64(rng.Int63n(int64(m.commit-m.snap.Index)))
			term, ok := m.term(idx)
			if !ok {
				continue
			}
			meta := raft.SnapshotMeta{Index: idx, Term: term}
			m.snap = meta
			payload := []byte(fmt.Sprintf("state@%d", idx))
			ops = append(ops, op{name: "local snapshot", states: []model{before, m.clone()}, apply: func(s *storage.Store) error {
				if err := s.CreateSnapshot(meta, payload); err != nil {
					return err
				}
				return s.Compact(meta.Index)
			}})

		default: // snapshot installed from the leader
			if m.hs.Term == 0 {
				continue
			}
			meta := raft.SnapshotMeta{Index: m.commit + 1 + uint64(rng.Intn(6)), Term: m.hs.Term}
			if t, ok := m.term(meta.Index); ok && rng.Intn(2) == 0 {
				meta.Term = t // the follower's log agrees with the snapshot
			}
			keepLog := m.installSnapshot(meta)
			data := snapshotBytes(t, testCluster, meta, []byte(fmt.Sprintf("leader-state@%d", meta.Index)))
			ops = append(ops, op{name: "install snapshot", states: []model{before, m.clone()}, apply: func(s *storage.Store) error {
				in, err := s.NewIncomingSnapshot()
				if err != nil {
					return err
				}
				defer in.Discard()
				if _, err := in.Write(data); err != nil {
					return err
				}
				if _, err := in.Finish(); err != nil {
					return err
				}
				return s.InstallSnapshot(in, keepLog)
			}})
		}
	}
	return ops
}

// run executes the workload until an operation fails. It returns the index
// of the operation that failed (len(ops) if none did).
func run(fs *faultfs.FS, ops []op) (failed int, s *storage.Store, err error) {
	s, err = storage.Open(options(fs))
	if err != nil {
		return 0, nil, err
	}
	for i, o := range ops {
		if err := o.apply(s); err != nil {
			return i, s, err
		}
	}
	return len(ops), s, nil
}

func TestCrashAtEveryFilesystemOperation(t *testing.T) {
	// STORAGE_CRASH_SEEDS=N widens the sweep; STORAGE_CRASH_SEED=S replays
	// one workload.
	count := 6
	if v, err := strconv.Atoi(os.Getenv("STORAGE_CRASH_SEEDS")); err == nil && v > 0 {
		count = v
	}
	if testing.Short() {
		count = 1
	}
	var seeds []int64
	for i := 1; i <= count; i++ {
		seeds = append(seeds, int64(i))
	}
	if v, err := strconv.ParseInt(os.Getenv("STORAGE_CRASH_SEED"), 10, 64); err == nil {
		seeds = []int64{v}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ops := workload(t, seed, 45)

			// A clean run tells us how many operations there are to crash at.
			clean := faultfs.New(seed)
			failed, s, err := run(clean, ops)
			if err != nil {
				t.Fatalf("clean run failed at op %d (%s): %v", failed, ops[failed].name, err)
			}
			total := clean.Ops()
			s.Close()
			if total < 100 {
				t.Fatalf("workload only performs %d filesystem operations", total)
			}

			for crashAt := 1; crashAt <= total; crashAt++ {
				fs := faultfs.New(seed*100000 + int64(crashAt))
				fs.CrashAt(crashAt)
				failed, _, err := run(fs, ops)
				// A crash inside a best-effort cleanup (removing a superseded
				// snapshot) is not reported to the caller, so the workload
				// can also end without an error.
				if err == nil && !fs.Crashed() {
					t.Fatalf("crash point %d was never reached", crashAt)
				}
				fs.Restart()

				s, err := storage.Open(options(fs))
				if err != nil {
					t.Fatalf("crash at fs op %d (during workload op %d, %s): reopen failed: %v\n%s", crashAt, failed, opName(ops, failed), err, dumpWAL(fs))
				}
				got := s.InitialState()

				// Before the first op completes, the state is empty.
				allowed := []model{{}}
				if failed < len(ops) {
					allowed = ops[failed].states
				}
				if failed == len(ops) {
					allowed = []model{ops[len(ops)-1].states[len(ops[len(ops)-1].states)-1]}
				}
				if !matchesAny(got, allowed) {
					t.Fatalf("crash at fs op %d (during workload op %d, %s):\nrecovered hs=%+v snap=%+v entries=%s commit=%d\nallowed states:\n%s",
						crashAt, failed, opName(ops, failed), got.HardState, got.Snapshot, describe(got.Entries), got.Commit, describeModels(allowed))
				}
				attempted := allowed[len(allowed)-1]
				if got.Commit > attempted.commit || got.Commit < got.Snapshot.Index {
					t.Fatalf("crash at fs op %d: recovered commit %d outside [%d, %d]", crashAt, got.Commit, got.Snapshot.Index, attempted.commit)
				}

				// The recovered store must accept writes and keep them.
				next := got.Snapshot.Index + uint64(len(got.Entries)) + 1
				hs := raft.HardState{Term: got.HardState.Term + 1, Vote: "n1"}
				extra := raft.Entry{Index: next, Term: hs.Term, Type: raft.EntryCommand, Data: []byte("after-crash")}
				if err := s.Save(&hs, []raft.Entry{extra}, got.Commit, true); err != nil {
					t.Fatalf("crash at fs op %d: Save after recovery: %v", crashAt, err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s, err = storage.Open(options(fs))
				if err != nil {
					t.Fatalf("crash at fs op %d: second reopen failed: %v", crashAt, err)
				}
				again := s.InitialState()
				want := append(append([]raft.Entry(nil), got.Entries...), extra)
				if again.HardState != hs || !reflect.DeepEqual(again.Entries, want) {
					t.Fatalf("crash at fs op %d: write after recovery was not kept", crashAt)
				}
				s.Close()
			}
			t.Logf("seed %d: verified recovery from a crash at each of %d filesystem operations across %d workload steps", seed, total, len(ops))
		})
	}
}

// dumpWAL renders every record of every segment, for failure messages.
func dumpWAL(fs *faultfs.FS) string {
	out := ""
	for _, name := range fs.Names() {
		data, _ := fs.ReadFile(name)
		out += fmt.Sprintf("%s (%d bytes)\n", name, len(data))
		if !strings.Contains(name, "/wal/") || len(data) < 8 {
			continue
		}
		for off := 8; off+13 <= len(data); {
			typ := data[off]
			n := int(binary.LittleEndian.Uint32(data[off+1:]))
			if off+13+n > len(data) {
				out += fmt.Sprintf("  @%d incomplete record type %d len %d\n", off, typ, n)
				break
			}
			p := data[off+13 : off+13+n]
			switch {
			case typ == 1 && n >= 8:
				out += fmt.Sprintf("  @%d hardstate term=%d vote=%s\n", off, binary.LittleEndian.Uint64(p), p[8:])
			case typ == 2 && n >= 16:
				out += fmt.Sprintf("  @%d entry %d/t%d\n", off, binary.LittleEndian.Uint64(p), binary.LittleEndian.Uint64(p[8:]))
			case typ == 3 && n == 8:
				out += fmt.Sprintf("  @%d commit %d\n", off, binary.LittleEndian.Uint64(p))
			case typ == 4 && n == 16:
				out += fmt.Sprintf("  @%d snapshot marker %d/t%d\n", off, binary.LittleEndian.Uint64(p), binary.LittleEndian.Uint64(p[8:]))
			default:
				out += fmt.Sprintf("  @%d type %d len %d\n", off, typ, n)
			}
			off += 13 + n
		}
	}
	return out
}

func opName(ops []op, i int) string {
	if i >= len(ops) {
		return "after the workload"
	}
	return ops[i].name
}

func matchesAny(got raft.InitialState, allowed []model) bool {
	for _, m := range allowed {
		v := m.visible()
		if got.HardState == v.hs && got.Snapshot == v.snap && sameEntries(got.Entries, v.entries) {
			return true
		}
	}
	return false
}

func sameEntries(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	return len(a) == 0 || reflect.DeepEqual(a, b)
}

func describe(es []raft.Entry) string {
	out := "["
	for i, e := range es {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%d/t%d", e.Index, e.Term)
	}
	return out + "]"
}

func describeModels(ms []model) string {
	out := ""
	for _, m := range ms {
		v := m.visible()
		out += fmt.Sprintf("  hs=%+v snap=%+v entries=%s commit=%d\n", v.hs, v.snap, describe(v.entries), m.commit)
	}
	return out
}

// TestRepeatedCrashes keeps one data directory alive across many crashes at
// random points, so that recovery is exercised on state that earlier
// recoveries produced.
func TestRepeatedCrashes(t *testing.T) {
	seeds := int64(40)
	if v, err := strconv.Atoi(os.Getenv("STORAGE_CRASH_SEEDS")); err == nil && v > 0 {
		seeds = int64(v)
	}
	for seed := int64(1); seed <= seeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		fs := faultfs.New(seed)
		acked := map[uint64]raft.Entry{} // index -> entry acknowledged there
		var term uint64

		for round := 0; round < 30; round++ {
			s, err := storage.Open(options(fs))
			if err != nil {
				t.Fatalf("seed %d round %d: Open: %v", seed, round, err)
			}
			st := s.InitialState()
			// Every acknowledged entry beyond the snapshot must be back.
			have := map[uint64]raft.Entry{}
			for _, e := range st.Entries {
				have[e.Index] = e
			}
			for idx, want := range acked {
				if idx > st.Snapshot.Index && !reflect.DeepEqual(have[idx], want) {
					t.Fatalf("seed %d round %d: acknowledged entry %d was lost or changed (have %+v)", seed, round, idx, have[idx])
				}
			}
			if st.HardState.Term < term {
				t.Fatalf("seed %d round %d: term went back from %d to %d", seed, round, term, st.HardState.Term)
			}
			// Entries that came back without ever being acknowledged are
			// legitimate (the write landed, the reply did not); adopt them.
			for idx := range acked {
				delete(acked, idx)
			}
			for _, e := range st.Entries {
				acked[e.Index] = e
			}
			term = st.HardState.Term
			last := st.Snapshot.Index + uint64(len(st.Entries))
			commit := st.Commit
			snap := st.Snapshot

			fs.CrashAt(fs.Ops() + 1 + rng.Intn(60))
			for {
				term++
				hs := raft.HardState{Term: term, Vote: "n1"}
				batch := entries(term, last+1, last+uint64(1+rng.Intn(3)))
				if err := s.Save(&hs, batch, commit, true); err != nil {
					term-- // not acknowledged; the next round learns the truth
					break
				}
				for _, e := range batch {
					acked[e.Index] = e
				}
				last += uint64(len(batch))
				commit = last
				if rng.Intn(4) == 0 && last > snap.Index+3 {
					meta := raft.SnapshotMeta{Index: last - 2, Term: acked[last-2].Term}
					if s.CreateSnapshot(meta, []byte("state")) != nil || s.Compact(meta.Index) != nil {
						break
					}
					snap = meta
				}
			}
			if !fs.Crashed() {
				t.Fatalf("seed %d round %d: workload stopped without a crash", seed, round)
			}
			fs.Restart()
		}
	}
}
