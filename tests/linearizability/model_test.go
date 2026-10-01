package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
)

// These tests check the checker. A model that accepted everything would make
// every other test in this package pass, so before trusting a green result we
// feed it hand-written histories whose verdict is known, in both directions.

// op builds one operation on key "x" for client c over the interval
// [call, ret].
func op(c int, call, ret int64, in Input, out Output) porcupine.Operation {
	in.Key = "x"
	return porcupine.Operation{ClientId: c, Call: call, Return: ret, Input: in, Output: out}
}

func put(v string) Input           { return Input{Op: kv.OpPut, Value: v} }
func get() Input                   { return Input{Op: kv.OpRead} }
func del() Input                   { return Input{Op: kv.OpDelete} }
func casValue(old, v string) Input { return Input{Op: kv.OpCAS, ExpectValue: old, Value: v} }
func casAbsent(v string) Input     { return Input{Op: kv.OpCAS, ExpectAbsent: true, Value: v} }

var (
	okOut      = Output{Status: kv.StatusOK}
	unknown    = Output{Unknown: true}
	notFound   = Output{Status: kv.StatusNotFound}
	deleted    = Output{Status: kv.StatusOK, Existed: true}
	deletedNop = Output{Status: kv.StatusOK, Existed: false}
)

func read(v string) Output { return Output{Status: kv.StatusOK, Value: v} }
func casFailed(exists bool) Output {
	return Output{Status: kv.StatusCASFailed, Existed: exists}
}

func TestModelOnKnownHistories(t *testing.T) {
	tests := []struct {
		name    string
		history []porcupine.Operation
		want    porcupine.CheckResult
	}{
		// ---- legal ----
		{"sequential put then get", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(0, 3, 4, get(), read("a")),
		}, porcupine.Ok},
		{"get of a key never written", []porcupine.Operation{
			op(0, 1, 2, get(), notFound),
		}, porcupine.Ok},
		{"concurrent puts, either order is fine", []porcupine.Operation{
			op(0, 1, 10, put("a"), okOut),
			op(1, 2, 9, put("b"), okOut),
			op(2, 11, 12, get(), read("a")),
		}, porcupine.Ok},
		{"read concurrent with a write may see either value", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(0, 3, 10, put("b"), okOut),
			op(1, 4, 5, get(), read("a")),
			op(2, 6, 7, get(), read("b")),
		}, porcupine.Ok},
		{"delete then recreate with cas-if-absent", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(0, 3, 4, del(), deleted),
			op(0, 5, 6, del(), deletedNop),
			op(0, 7, 8, casAbsent("b"), okOut),
			op(0, 9, 10, casAbsent("c"), casFailed(true)),
			op(0, 11, 12, get(), read("b")),
		}, porcupine.Ok},
		{"empty value is a value", []porcupine.Operation{
			op(0, 1, 2, put(""), okOut),
			op(0, 3, 4, get(), read("")),
			op(0, 5, 6, casAbsent("x"), casFailed(true)),
			op(0, 7, 8, casValue("", "y"), okOut),
		}, porcupine.Ok},
		{"two racing cas: exactly one wins", []porcupine.Operation{
			op(0, 1, 2, put("0"), okOut),
			op(1, 3, 8, casValue("0", "1a"), okOut),
			op(2, 4, 9, casValue("0", "1b"), casFailed(true)),
			op(0, 10, 11, get(), read("1a")),
		}, porcupine.Ok},
		{"timed-out write that did take effect", []porcupine.Operation{
			op(0, 1, Infinity, put("a"), unknown),
			op(1, 5, 6, get(), read("a")),
		}, porcupine.Ok},
		{"timed-out write that never took effect", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, Infinity, put("b"), unknown),
			op(0, 5, 6, get(), read("a")),
			op(0, 7, 8, get(), read("a")),
		}, porcupine.Ok},
		{"timed-out write that took effect late", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, Infinity, put("b"), unknown),
			op(0, 5, 6, get(), read("a")),
			op(0, 7, 8, get(), read("b")), // it surfaced after the partition healed
		}, porcupine.Ok},
		{"timed-out cas that took effect", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, Infinity, casValue("a", "b"), unknown),
			op(0, 5, 6, get(), read("b")),
		}, porcupine.Ok},

		// ---- illegal ----
		{"stale read: old value after a newer one was acknowledged", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(0, 3, 4, put("b"), okOut),
			op(1, 5, 6, get(), read("a")), // what a deposed leader would serve from memory
		}, porcupine.Illegal},
		{"lost acknowledged write", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, 4, get(), notFound),
		}, porcupine.Illegal},
		{"read of a value nobody wrote", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, 4, get(), read("zzz")),
		}, porcupine.Illegal},
		{"value flips back without a write", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(0, 3, 4, put("b"), okOut),
			op(1, 5, 6, get(), read("b")),
			op(1, 7, 8, get(), read("a")),
		}, porcupine.Illegal},
		{"both racing cas succeed: a lost update", []porcupine.Operation{
			op(0, 1, 2, put("0"), okOut),
			op(1, 3, 8, casValue("0", "1a"), okOut),
			op(2, 4, 9, casValue("0", "1b"), okOut),
		}, porcupine.Illegal},
		{"cas reported failure although its condition held", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, 4, casValue("a", "b"), casFailed(true)),
		}, porcupine.Illegal},
		{"retry applied twice: cas reports failure but the value changed", []porcupine.Operation{
			// The first attempt swapped a->b; a retry under a new identity
			// then compared against b and reported failure. The client was
			// told "not swapped", yet the key holds b.
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, 6, casValue("a", "b"), casFailed(true)),
			op(0, 7, 8, get(), read("b")),
		}, porcupine.Illegal},
		{"cas-if-absent succeeded over an existing key", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, 4, casAbsent("b"), okOut),
		}, porcupine.Illegal},
		{"delete reported the key missing although it existed", []porcupine.Operation{
			op(0, 1, 2, put("a"), okOut),
			op(1, 3, 4, del(), deletedNop),
		}, porcupine.Illegal},
		{"failed cas misreported whether the key exists", []porcupine.Operation{
			op(0, 1, 2, casValue("a", "b"), casFailed(true)), // the key is absent
		}, porcupine.Illegal},
		{"timed-out write cannot explain a value it did not carry", []porcupine.Operation{
			op(0, 1, Infinity, put("a"), unknown),
			op(1, 5, 6, get(), read("b")),
		}, porcupine.Illegal},
		{"timed-out write cannot take effect before it was sent", []porcupine.Operation{
			op(1, 1, 2, get(), read("a")),
			op(0, 5, Infinity, put("a"), unknown),
		}, porcupine.Illegal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := Check(tc.history, 10*time.Second)
			if res.Verdict != tc.want {
				t.Fatalf("verdict = %s, want %s\n%s", res.Verdict, tc.want, res.Explanation)
			}
			if tc.want == porcupine.Illegal && res.Explanation == "" {
				t.Fatal("an illegal history came with no explanation")
			}
		})
	}
}

func TestKeysAreCheckedIndependently(t *testing.T) {
	// Each key's history is fine on its own; interleaving across keys does
	// not matter.
	history := []porcupine.Operation{
		{ClientId: 0, Call: 1, Return: 4, Input: Input{Op: kv.OpPut, Key: "a", Value: "1"}, Output: okOut},
		{ClientId: 1, Call: 2, Return: 3, Input: Input{Op: kv.OpPut, Key: "b", Value: "2"}, Output: okOut},
		{ClientId: 0, Call: 5, Return: 6, Input: Input{Op: kv.OpRead, Key: "a"}, Output: read("1")},
		{ClientId: 1, Call: 5, Return: 6, Input: Input{Op: kv.OpRead, Key: "b"}, Output: read("2")},
	}
	if res := Check(history, 10*time.Second); res.Verdict != porcupine.Ok {
		t.Fatalf("verdict = %s\n%s", res.Verdict, res.Explanation)
	}
	// One bad key makes the whole history illegal, and the explanation
	// names that key.
	history = append(history, porcupine.Operation{ClientId: 2, Call: 7, Return: 8, Input: Input{Op: kv.OpRead, Key: "b"}, Output: read("1")})
	res := Check(history, 10*time.Second)
	if res.Verdict != porcupine.Illegal {
		t.Fatalf("verdict = %s, want Illegal", res.Verdict)
	}
	if want := `key "b" is not linearizable`; len(res.Explanation) < len(want) || res.Explanation[:len(want)] != want {
		t.Fatalf("explanation = %q", res.Explanation)
	}
}

func TestCheckerTimeoutIsReportedAsUnknownNotAsPass(t *testing.T) {
	// Many overlapping writes with unknown outcomes make a large search
	// space. With no time to search it, the verdict must be Unknown; callers
	// in this package treat that as a failure to verify, never as a pass.
	var history []porcupine.Operation
	for i := 0; i < 400; i++ {
		history = append(history, op(i, int64(i+1), Infinity, put(string(rune('a'+i%26))), unknown))
	}
	for i := 0; i < 400; i++ {
		history = append(history, op(1000+i, int64(1000+i), int64(2000+i), get(), read(string(rune('a'+i%26)))))
	}
	res := Check(history, time.Nanosecond)
	if res.Verdict == porcupine.Ok {
		t.Skip("the checker finished before the 1ns timeout was noticed; nothing to assert")
	}
	if res.Verdict != porcupine.Unknown {
		t.Fatalf("verdict = %s, want Unknown", res.Verdict)
	}
}
