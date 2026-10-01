package kv

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// harness applies commands at increasing log indexes, like the apply loop.
type harness struct {
	t     *testing.T
	s     *Store
	index uint64
	seq   map[string]uint64
}

func newHarness(t *testing.T, l Limits) *harness {
	return &harness{t: t, s: New(l), seq: map[string]uint64{}}
}

func (h *harness) apply(c Command) Result {
	h.t.Helper()
	h.index++
	return h.s.Apply(h.index, c.Encode())
}

// do applies a mutation as the next request of the given client.
func (h *harness) do(client string, c Command) Result {
	h.t.Helper()
	h.seq[client]++
	c.ClientID, c.Seq = client, h.seq[client]
	return h.apply(c)
}

func (h *harness) put(key, value string) Result {
	return h.do("c1", Command{Op: OpPut, Key: key, Value: []byte(value)})
}

func (h *harness) get(key string) Result {
	return h.apply(Command{Op: OpRead, Key: key})
}

func (h *harness) wantValue(key, value string) {
	h.t.Helper()
	res := h.get(key)
	if res.Status != StatusOK || string(res.Value) != value {
		h.t.Fatalf("get %q = (%s, %q), want %q", key, res.Status, res.Value, value)
	}
}

func (h *harness) wantMissing(key string) {
	h.t.Helper()
	if res := h.get(key); res.Status != StatusNotFound {
		h.t.Fatalf("get %q = %s, want NOT_FOUND", key, res.Status)
	}
}

// ---- basic semantics -------------------------------------------------------

func TestPutGetDelete(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	h.wantMissing("a")

	res := h.put("a", "1")
	if res.Status != StatusOK || res.Revision != h.index {
		t.Fatalf("put = %+v, want OK with revision %d", res, h.index)
	}
	h.wantValue("a", "1")

	del := h.do("c1", Command{Op: OpDelete, Key: "a"})
	if del.Status != StatusOK || !del.Existed {
		t.Fatalf("delete = %+v, want OK with Existed", del)
	}
	h.wantMissing("a")

	// Deleting something that is not there succeeds and says so.
	del = h.do("c1", Command{Op: OpDelete, Key: "a"})
	if del.Status != StatusOK || del.Existed {
		t.Fatalf("delete of a missing key = %+v, want OK without Existed", del)
	}
}

func TestEmptyValueIsNotMissing(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	h.put("a", "")
	res := h.get("a")
	if res.Status != StatusOK || len(res.Value) != 0 || res.Revision == 0 {
		t.Fatalf("get of an empty value = %+v, want OK with an empty value", res)
	}
	// An empty value is a value: "expect absent" must fail against it.
	cas := h.do("c1", Command{Op: OpCAS, Key: "a", Expect: ExpectAbsent, Value: []byte("x")})
	if cas.Status != StatusCASFailed || !cas.Existed {
		t.Fatalf("CAS expecting absence against an empty value = %+v, want CAS_FAILED", cas)
	}
	// And it can be matched by value.
	cas = h.do("c1", Command{Op: OpCAS, Key: "a", Expect: ExpectValue, ExpectValue: nil, Value: []byte("x")})
	if cas.Status != StatusOK {
		t.Fatalf("CAS expecting the empty value = %+v, want OK", cas)
	}
}

func TestRevisions(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	first := h.put("a", "1").Revision
	if got := h.get("a").Revision; got != first {
		t.Fatalf("read revision %d, want %d", got, first)
	}
	second := h.put("a", "2").Revision
	if second <= first {
		t.Fatalf("revision did not increase on overwrite: %d then %d", first, second)
	}
	// Reads go through the log and consume indexes but do not change
	// revisions.
	h.get("a")
	h.get("b")
	if got := h.get("a").Revision; got != second {
		t.Fatalf("revision changed by reads: %d, want %d", got, second)
	}

	// Delete and recreate: the new incarnation gets a higher revision, so a
	// revision remembered from before can never match again.
	h.do("c1", Command{Op: OpDelete, Key: "a"})
	third := h.put("a", "2").Revision
	if third <= second {
		t.Fatalf("revision after recreate %d is not above %d", third, second)
	}
	cas := h.do("c1", Command{Op: OpCAS, Key: "a", Expect: ExpectRevision, ExpectRevision: second, Value: []byte("x")})
	if cas.Status != StatusCASFailed || cas.Revision != third {
		t.Fatalf("CAS with a revision from a deleted incarnation = %+v, want CAS_FAILED reporting revision %d", cas, third)
	}
}

func TestCompareAndSwap(t *testing.T) {
	type state struct {
		exists bool
		value  string
	}
	tests := []struct {
		name        string
		before      state
		cmd         func(rev uint64) Command
		wantStatus  Status
		wantAfter   state
		wantExisted bool
	}{
		{"absent: key missing", state{}, func(uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectAbsent, Value: []byte("new")}
		}, StatusOK, state{true, "new"}, false},
		{"absent: key present", state{true, "old"}, func(uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectAbsent, Value: []byte("new")}
		}, StatusCASFailed, state{true, "old"}, true},
		{"value: match", state{true, "old"}, func(uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectValue, ExpectValue: []byte("old"), Value: []byte("new")}
		}, StatusOK, state{true, "new"}, false},
		{"value: mismatch", state{true, "old"}, func(uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectValue, ExpectValue: []byte("other"), Value: []byte("new")}
		}, StatusCASFailed, state{true, "old"}, true},
		{"value: key missing never matches, even an empty expectation", state{}, func(uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectValue, ExpectValue: nil, Value: []byte("new")}
		}, StatusCASFailed, state{}, false},
		{"revision: match", state{true, "old"}, func(rev uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectRevision, ExpectRevision: rev, Value: []byte("new")}
		}, StatusOK, state{true, "new"}, false},
		{"revision: mismatch", state{true, "old"}, func(rev uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectRevision, ExpectRevision: rev + 100, Value: []byte("new")}
		}, StatusCASFailed, state{true, "old"}, true},
		{"revision: key missing", state{}, func(uint64) Command {
			return Command{Op: OpCAS, Expect: ExpectRevision, ExpectRevision: 1, Value: []byte("new")}
		}, StatusCASFailed, state{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, DefaultLimits)
			var rev uint64
			if tc.before.exists {
				rev = h.put("k", tc.before.value).Revision
			}
			cmd := tc.cmd(rev)
			cmd.Key = "k"
			res := h.do("c2", cmd)
			if res.Status != tc.wantStatus {
				t.Fatalf("status = %s, want %s", res.Status, tc.wantStatus)
			}
			if res.Status == StatusCASFailed {
				if res.Existed != tc.wantExisted || res.Revision != rev {
					t.Fatalf("failure reports existed=%v revision=%d, want %v and %d", res.Existed, res.Revision, tc.wantExisted, rev)
				}
			} else if res.Revision != h.index {
				t.Fatalf("success revision = %d, want %d", res.Revision, h.index)
			}
			if tc.wantAfter.exists {
				h.wantValue("k", tc.wantAfter.value)
			} else {
				h.wantMissing("k")
			}
		})
	}
}

// ---- retries ---------------------------------------------------------------

func TestRetryReturnsOriginalResultWithoutReapplying(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	put := Command{Op: OpPut, Key: "counter", Value: []byte("1"), ClientID: "alice", Seq: 1}
	first := h.apply(put)

	// Someone else overwrites the key before alice's retry arrives.
	h.do("bob", Command{Op: OpPut, Key: "counter", Value: []byte("2")})

	retry := h.apply(put)
	if !retry.Duplicate || retry.Status != StatusOK || retry.Revision != first.Revision {
		t.Fatalf("retry = %+v, want the original result (revision %d) marked as a duplicate", retry, first.Revision)
	}
	h.wantValue("counter", "2") // the retry did not write "1" again
}

func TestRetryOfFailedCASReportsTheSameFailure(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	h.put("k", "a")
	cas := Command{Op: OpCAS, Key: "k", Expect: ExpectValue, ExpectValue: []byte("zzz"), Value: []byte("b"), ClientID: "alice", Seq: 1}
	first := h.apply(cas)
	if first.Status != StatusCASFailed {
		t.Fatalf("first attempt = %+v", first)
	}
	// The key now happens to hold exactly what the CAS expects.
	h.put("k", "zzz")
	retry := h.apply(cas)
	if retry.Status != StatusCASFailed || !retry.Duplicate {
		t.Fatalf("retry = %+v: a retry must not get a second chance at the condition", retry)
	}
	h.wantValue("k", "zzz")
}

func TestRetryOfSuccessfulCASDoesNotFail(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	h.put("k", "a")
	cas := Command{Op: OpCAS, Key: "k", Expect: ExpectValue, ExpectValue: []byte("a"), Value: []byte("b"), ClientID: "alice", Seq: 1}
	first := h.apply(cas)
	// Without de-duplication the retry would now compare against "b" and
	// tell the client its successful swap had failed.
	retry := h.apply(cas)
	if first.Status != StatusOK || retry.Status != StatusOK || !retry.Duplicate || retry.Revision != first.Revision {
		t.Fatalf("first=%+v retry=%+v, want the retry to report the original success", first, retry)
	}
}

func TestSequenceRules(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	put := func(seq uint64, value string) Result {
		return h.apply(Command{Op: OpPut, Key: "k", Value: []byte(value), ClientID: "alice", Seq: seq})
	}
	put(5, "five")

	if res := put(5, "different"); res.Status != StatusIdentityReused {
		t.Fatalf("same identity, different payload = %s, want IDENTITY_REUSED", res.Status)
	}
	h.wantValue("k", "five")

	if res := put(4, "four"); res.Status != StatusStaleSequence {
		t.Fatalf("older sequence = %s, want STALE_SEQUENCE", res.Status)
	}
	h.wantValue("k", "five")

	// Gaps are fine: the client alone decides its numbers.
	if res := put(9, "nine"); res.Status != StatusOK {
		t.Fatalf("newer sequence = %s, want OK", res.Status)
	}
	// Once the client has moved on, the old result is gone.
	if res := put(5, "five"); res.Status != StatusStaleSequence {
		t.Fatalf("retry of a superseded request = %s, want STALE_SEQUENCE", res.Status)
	}
	h.wantValue("k", "nine")

	// The same sequence number from another client is unrelated.
	if res := h.apply(Command{Op: OpPut, Key: "k", Value: []byte("bob"), ClientID: "bob", Seq: 5}); res.Status != StatusOK {
		t.Fatalf("other client = %s, want OK", res.Status)
	}
}

func TestSessionTableIsBoundedAndEvictsLeastRecentlyUsed(t *testing.T) {
	l := DefaultLimits
	l.MaxSessions = 3
	h := newHarness(t, l)
	cmd := func(client string) Command {
		return Command{Op: OpPut, Key: "k-" + client, Value: []byte("v"), ClientID: client, Seq: 1}
	}
	h.apply(cmd("a"))
	h.apply(cmd("b"))
	h.apply(cmd("c"))
	// A retry counts as use: "a" becomes the most recent.
	if res := h.apply(cmd("a")); !res.Duplicate {
		t.Fatalf("retry by a = %+v, want duplicate", res)
	}
	h.apply(cmd("d")) // evicts "b", the least recently used
	if h.s.Sessions() != 3 {
		t.Fatalf("sessions = %d, want 3", h.s.Sessions())
	}
	if res := h.apply(cmd("a")); !res.Duplicate {
		t.Fatal("session a should have survived")
	}
	// "b" was forgotten, so its retry is executed again. This is the
	// documented boundary of the retry guarantee.
	if res := h.apply(cmd("b")); res.Duplicate || res.Status != StatusOK {
		t.Fatalf("retry by evicted client b = %+v, want a fresh execution", res)
	}
}

// ---- limits ----------------------------------------------------------------

func TestStateSizeLimit(t *testing.T) {
	l := DefaultLimits
	l.MaxStateBytes = 20
	h := newHarness(t, l)
	if res := h.put("aaaa", "123456"); res.Status != StatusOK { // 10 bytes
		t.Fatalf("put = %s", res.Status)
	}
	if res := h.put("bbbb", "123456"); res.Status != StatusOK { // 20 bytes
		t.Fatalf("put = %s", res.Status)
	}
	if res := h.put("cccc", "1"); res.Status != StatusCapacityExceeded {
		t.Fatalf("put beyond the limit = %s, want CAPACITY_EXCEEDED", res.Status)
	}
	h.wantMissing("cccc")
	if res := h.put("aaaa", "1234567"); res.Status != StatusCapacityExceeded {
		t.Fatalf("growing a value beyond the limit = %s, want CAPACITY_EXCEEDED", res.Status)
	}
	h.wantValue("aaaa", "123456")
	if res := h.do("c1", Command{Op: OpCAS, Key: "cccc", Expect: ExpectAbsent, Value: []byte("1")}); res.Status != StatusCapacityExceeded {
		t.Fatalf("CAS beyond the limit = %s, want CAPACITY_EXCEEDED", res.Status)
	}
	// Shrinking and deleting free space.
	if res := h.put("aaaa", "1"); res.Status != StatusOK {
		t.Fatalf("shrinking put = %s", res.Status)
	}
	h.do("c1", Command{Op: OpDelete, Key: "bbbb"})
	if h.s.Size() != 5 || h.s.Len() != 1 {
		t.Fatalf("size=%d len=%d, want 5 and 1", h.s.Size(), h.s.Len())
	}
	if res := h.put("cccc", "12345678901"); res.Status != StatusOK { // 15 more
		t.Fatalf("put after freeing space = %s", res.Status)
	}
}

func TestValidate(t *testing.T) {
	l := DefaultLimits
	l.MaxKeyBytes, l.MaxValueBytes = 8, 8
	ok := Command{Op: OpPut, Key: "k", Value: []byte("v"), ClientID: "c", Seq: 1}
	tests := []struct {
		name string
		mod  func(*Command)
		ok   bool
	}{
		{"valid put", func(*Command) {}, true},
		{"valid read", func(c *Command) { *c = Command{Op: OpRead, Key: "k"} }, true},
		{"valid delete", func(c *Command) { c.Op, c.Value = OpDelete, nil }, true},
		{"key at the limit", func(c *Command) { c.Key = "12345678" }, true},
		{"unicode key", func(c *Command) { c.Key = "ключ" }, true},
		{"empty value", func(c *Command) { c.Value = nil }, true},
		{"empty key", func(c *Command) { c.Key = "" }, false},
		{"key too long", func(c *Command) { c.Key = "123456789" }, false},
		{"key with newline", func(c *Command) { c.Key = "a\nb" }, false},
		{"key with NUL", func(c *Command) { c.Key = "a\x00b" }, false},
		{"key not UTF-8", func(c *Command) { c.Key = "a\xff" }, false},
		{"value too long", func(c *Command) { c.Value = []byte("123456789") }, false},
		{"unknown op", func(c *Command) { c.Op = 99 }, false},
		{"mutation without client", func(c *Command) { c.ClientID = "" }, false},
		{"mutation without seq", func(c *Command) { c.Seq = 0 }, false},
		{"client ID too long", func(c *Command) { c.ClientID = strings.Repeat("x", MaxClientIDBytes+1) }, false},
		{"read with identity", func(c *Command) { *c = Command{Op: OpRead, Key: "k", ClientID: "c", Seq: 1} }, false},
		{"delete with value", func(c *Command) { c.Op = OpDelete }, false},
		{"put with condition", func(c *Command) { c.Expect = ExpectAbsent }, false},
		{"cas without condition", func(c *Command) { c.Op = OpCAS }, false},
		{"cas with zero revision", func(c *Command) { c.Op, c.Expect = OpCAS, ExpectRevision }, false},
		{"cas expected value too long", func(c *Command) {
			c.Op, c.Expect, c.ExpectValue = OpCAS, ExpectValue, []byte("123456789")
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := ok
			tc.mod(&c)
			err := c.Validate(l)
			if tc.ok && err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestInvalidCommandInLogChangesNothing(t *testing.T) {
	h := newHarness(t, DefaultLimits)
	h.put("k", "v")
	before := h.s.Snapshot()
	for _, data := range [][]byte{nil, {1}, {9, 9, 9}, []byte("garbage"),
		(&Command{Op: OpPut, Key: "", Value: []byte("v"), ClientID: "c", Seq: 1}).Encode()} {
		h.index++
		if res := h.s.Apply(h.index, data); res.Status != StatusInvalid {
			t.Fatalf("Apply(%q) = %s, want INVALID", data, res.Status)
		}
	}
	// Only the applied index moved.
	h.s.applied = 1
	if !bytes.Equal(before, h.s.Snapshot()) {
		t.Fatal("an invalid command changed the store")
	}
}

// ---- codec -----------------------------------------------------------------

func sampleCommands() []Command {
	return []Command{
		{Op: OpPut, Key: "k", Value: []byte("v"), ClientID: "c", Seq: 1},
		{Op: OpPut, Key: "k", ClientID: "client-with-long-name", Seq: 1 << 40},
		{Op: OpDelete, Key: "some/key", ClientID: "c", Seq: 2},
		{Op: OpRead, Key: "k"},
		{Op: OpCAS, Key: "k", Expect: ExpectAbsent, Value: []byte("v"), ClientID: "c", Seq: 3},
		{Op: OpCAS, Key: "k", Expect: ExpectValue, ExpectValue: []byte("old"), Value: []byte("new"), ClientID: "c", Seq: 4},
		{Op: OpCAS, Key: "k", Expect: ExpectValue, Value: []byte("new"), ClientID: "c", Seq: 4},
		{Op: OpCAS, Key: "k", Expect: ExpectRevision, ExpectRevision: 77, ClientID: "c", Seq: 5},
	}
}

func TestCodecRoundTrip(t *testing.T) {
	for _, c := range sampleCommands() {
		got, err := Decode(c.Encode())
		if err != nil {
			t.Fatalf("Decode(%+v): %v", c, err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatalf("round trip changed the command:\n got %+v\nwant %+v", got, c)
		}
	}
}

func TestDecodeRejectsMalformedInput(t *testing.T) {
	valid := (&Command{Op: OpCAS, Key: "key", Expect: ExpectValue, ExpectValue: []byte("old"), Value: []byte("new"), ClientID: "c", Seq: 4}).Encode()
	// Every strict prefix is incomplete.
	for n := 0; n < len(valid); n++ {
		if _, err := Decode(valid[:n]); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Decode of a %d-byte prefix = %v, want ErrInvalid", n, err)
		}
	}
	if _, err := Decode(append(append([]byte(nil), valid...), 0)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("trailing byte accepted: %v", err)
	}
	wrongVersion := append([]byte(nil), valid...)
	wrongVersion[0] = 2
	if _, err := Decode(wrongVersion); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown version accepted: %v", err)
	}
	// A length prefix far larger than the data must not allocate.
	huge := []byte{codecVersion, byte(OpPut), 0xff, 0xff, 0xff, 0xff, 0x0f}
	if _, err := Decode(huge); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized length accepted: %v", err)
	}
}

// ---- snapshots -------------------------------------------------------------

func populated(t *testing.T) *harness {
	h := newHarness(t, DefaultLimits)
	for i := 0; i < 50; i++ {
		h.do(fmt.Sprintf("client-%d", i%7), Command{Op: OpPut, Key: fmt.Sprintf("key-%03d", i), Value: []byte(fmt.Sprintf("value-%d", i))})
	}
	h.do("client-1", Command{Op: OpDelete, Key: "key-010"})
	h.do("client-2", Command{Op: OpCAS, Key: "key-011", Expect: ExpectAbsent, Value: []byte("x")}) // fails
	h.put("empty", "")
	return h
}

func TestSnapshotRestoreReproducesStateAndRetryBehaviour(t *testing.T) {
	h := populated(t)
	snap := h.s.Snapshot()

	restored := New(DefaultLimits)
	if err := restored.Restore(bytes.NewReader(snap)); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !bytes.Equal(restored.Snapshot(), snap) {
		t.Fatal("snapshot of the restored store differs from the original snapshot")
	}
	if restored.AppliedIndex() != h.s.AppliedIndex() || restored.Len() != h.s.Len() || restored.Size() != h.s.Size() || restored.Sessions() != h.s.Sessions() {
		t.Fatal("restored store reports different counters")
	}

	// From here both stores must behave identically, including for retries
	// of requests made before the snapshot.
	retry := Command{Op: OpCAS, Key: "key-011", Expect: ExpectAbsent, Value: []byte("x"), ClientID: "client-2", Seq: h.seq["client-2"]}
	next := []Command{
		retry,
		{Op: OpRead, Key: "key-011"},
		{Op: OpRead, Key: "key-010"},
		{Op: OpPut, Key: "new", Value: []byte("v"), ClientID: "client-3", Seq: h.seq["client-3"] + 1},
		{Op: OpPut, Key: "new", Value: []byte("v"), ClientID: "client-3", Seq: h.seq["client-3"]},
	}
	index := h.index
	for _, c := range next {
		index++
		a, b := h.s.Apply(index, c.Encode()), restored.Apply(index, c.Encode())
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("command %+v: original returned %+v, restored returned %+v", c, a, b)
		}
	}
	if res := restored.Apply(index+1, retry.Encode()); !res.Duplicate || res.Status != StatusCASFailed {
		t.Fatalf("retry after restore = %+v, want the remembered CAS failure", res)
	}
}

func TestSnapshotIsDeterministic(t *testing.T) {
	// Two stores that applied the same log produce byte-identical snapshots,
	// whatever order Go happens to iterate their maps in.
	a, b := populated(t), populated(t)
	if !bytes.Equal(a.s.Snapshot(), b.s.Snapshot()) {
		t.Fatal("equal states produced different snapshots")
	}
}

func TestSnapshotPreservesSessionEvictionOrder(t *testing.T) {
	l := DefaultLimits
	l.MaxSessions = 3
	h := newHarness(t, l)
	for _, c := range []string{"a", "b", "c"} {
		h.apply(Command{Op: OpPut, Key: "k", Value: []byte(c), ClientID: c, Seq: 1})
	}
	h.apply(Command{Op: OpPut, Key: "k", Value: []byte("a"), ClientID: "a", Seq: 1}) // touch a

	restored := New(l)
	if err := restored.Restore(bytes.NewReader(h.s.Snapshot())); err != nil {
		t.Fatal(err)
	}
	// Both must now evict "b".
	d := Command{Op: OpPut, Key: "k", Value: []byte("d"), ClientID: "d", Seq: 1}
	h.s.Apply(10, d.Encode())
	restored.Apply(10, d.Encode())
	if !bytes.Equal(h.s.Snapshot(), restored.Snapshot()) {
		t.Fatal("original and restored stores evicted different sessions")
	}
	if _, ok := restored.sessions["b"]; ok {
		t.Fatal("session b should have been evicted")
	}
}

func TestRestoreRejectsDamageAndLeavesStoreUntouched(t *testing.T) {
	h := populated(t)
	snap := h.s.Snapshot()

	target := New(DefaultLimits)
	target.Apply(1, (&Command{Op: OpPut, Key: "keep", Value: []byte("me"), ClientID: "c", Seq: 1}).Encode())
	before := target.Snapshot()

	for n := 0; n < len(snap); n += 3 {
		if err := target.Restore(bytes.NewReader(snap[:n])); err == nil {
			t.Fatalf("a snapshot truncated to %d bytes was accepted", n)
		}
	}
	if err := target.Restore(bytes.NewReader(append(append([]byte(nil), snap...), 0))); err == nil {
		t.Fatal("a snapshot with trailing data was accepted")
	}
	if !bytes.Equal(before, target.Snapshot()) {
		t.Fatal("a failed Restore modified the store")
	}

	small := DefaultLimits
	small.MaxStateBytes = 100
	if err := New(small).Restore(bytes.NewReader(snap)); err == nil {
		t.Fatal("a snapshot larger than the state limit was accepted")
	}
}
