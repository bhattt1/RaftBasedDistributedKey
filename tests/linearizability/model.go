// Package linearizability checks recorded client histories against a
// sequential specification of the key-value API, using Porcupine.
//
// A history is a list of operations, each with the time it was invoked, the
// time it returned, what was asked and what was answered. The history is
// linearizable if every operation can be assigned a single instant between
// its invocation and its return such that, executed one at a time in that
// order, a plain single-threaded map would have given the same answers.
//
// What a passing check means: this particular history has such an ordering.
// It is evidence, in proportion to how hard the schedule tried to break
// things. It is not a proof about executions that were not recorded.
package linearizability

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/kv"
)

// Input is what a client asked for.
type Input struct {
	Op    kv.Op
	Key   string
	Value string // PUT and CAS: the value to write
	// CAS condition. Histories use value-based conditions only: the model
	// would otherwise have to predict log indexes to model revisions.
	ExpectAbsent bool
	ExpectValue  string
}

// Output is what the client was told.
type Output struct {
	// Unknown means the client never got a definite answer: every attempt
	// timed out or was cut off. The operation may have taken effect, at any
	// time after it was invoked, or never.
	Unknown bool
	Status  kv.Status
	Value   string // READ
	Existed bool   // DELETE: there was a key; failed CAS: the key exists
}

// state is the specification's state for one key.
type state struct {
	exists bool
	value  string
}

// Infinity is the return time recorded for an operation whose outcome is
// unknown. It makes the operation concurrent with everything after its
// invocation, which is exactly what is known about it.
const Infinity = math.MaxInt64 / 2

// Model is the sequential specification.
//
// It is nondeterministic for exactly one reason: an operation with an
// unknown outcome may or may not have happened, so stepping over one yields
// both possible next states. Because such an operation also has an infinite
// return time, the checker is free to place it anywhere after its
// invocation, or (by choosing the "did not happen" branch) nowhere.
//
// A retried request appears in a history once, from its first attempt to its
// final answer. The retries are invisible to the specification, which is the
// point: if a retry were applied twice, the history would show an effect that
// no single operation can explain.
var Model = porcupine.NondeterministicModel{
	// Keys are independent, so each key's operations are checked on their
	// own. This keeps the search small.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(Input).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() []interface{} { return []interface{}{state{}} },
	Step: func(st, in, out interface{}) []interface{} {
		return step(st.(state), in.(Input), out.(Output))
	},
	DescribeOperation: func(in, out interface{}) string { return Describe(in.(Input), out.(Output)) },
	DescribeState: func(st interface{}) string {
		s := st.(state)
		if !s.exists {
			return "(absent)"
		}
		return fmt.Sprintf("%q", s.value)
	},
}

// step returns every state the key could be in after the operation, given
// the answer the client got. An empty result means the answer is impossible
// from this state.
func step(s state, in Input, out Output) []interface{} {
	one := func(n state) []interface{} { return []interface{}{n} }
	switch in.Op {
	case kv.OpRead:
		switch {
		case out.Unknown:
			return one(s) // an unanswered read tells us nothing
		case out.Status == kv.StatusOK && s.exists && out.Value == s.value:
			return one(s)
		case out.Status == kv.StatusNotFound && !s.exists:
			return one(s)
		}
	case kv.OpPut:
		written := state{exists: true, value: in.Value}
		switch {
		case out.Unknown:
			return []interface{}{s, written}
		case out.Status == kv.StatusOK:
			return one(written)
		}
	case kv.OpDelete:
		switch {
		case out.Unknown:
			return []interface{}{s, state{}}
		case out.Status == kv.StatusOK && out.Existed == s.exists:
			return one(state{})
		}
	case kv.OpCAS:
		holds := (in.ExpectAbsent && !s.exists) || (!in.ExpectAbsent && s.exists && s.value == in.ExpectValue)
		written := state{exists: true, value: in.Value}
		switch {
		case out.Unknown && holds:
			return []interface{}{s, written}
		case out.Unknown:
			return one(s)
		case out.Status == kv.StatusOK && holds:
			return one(written)
		case out.Status == kv.StatusCASFailed && !holds && out.Existed == s.exists:
			return one(s)
		}
	}
	return nil
}

// Describe renders one operation for failure messages.
func Describe(in Input, out Output) string {
	var req string
	switch in.Op {
	case kv.OpRead:
		req = fmt.Sprintf("get(%s)", in.Key)
	case kv.OpPut:
		req = fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
	case kv.OpDelete:
		req = fmt.Sprintf("delete(%s)", in.Key)
	case kv.OpCAS:
		if in.ExpectAbsent {
			req = fmt.Sprintf("cas(%s, absent -> %q)", in.Key, in.Value)
		} else {
			req = fmt.Sprintf("cas(%s, %q -> %q)", in.Key, in.ExpectValue, in.Value)
		}
	}
	switch {
	case out.Unknown:
		return req + " -> ?"
	case out.Status == kv.StatusOK && in.Op == kv.OpRead:
		return fmt.Sprintf("%s -> %q", req, out.Value)
	case out.Status == kv.StatusOK && in.Op == kv.OpDelete:
		return fmt.Sprintf("%s -> ok (existed=%v)", req, out.Existed)
	default:
		return fmt.Sprintf("%s -> %s", req, out.Status)
	}
}

// OutputOf converts a state machine result into a history output.
func OutputOf(res kv.Result) Output {
	return Output{Status: res.Status, Value: string(res.Value), Existed: res.Existed}
}

// Result is the verdict on a history.
type Result struct {
	// Verdict is Ok, Illegal, or Unknown when the checker ran out of time.
	// Unknown is not a pass.
	Verdict porcupine.CheckResult
	// Explanation lists, for an illegal history, the operations of the first
	// key that could not be linearized.
	Explanation string
}

// Check runs the checker with a time limit.
func Check(history []porcupine.Operation, timeout time.Duration) Result {
	model := Model.ToModel()
	res := Result{Verdict: porcupine.CheckOperationsTimeout(model, history, timeout)}
	if res.Verdict == porcupine.Illegal {
		res.Explanation = explain(model, history)
	}
	return res
}

// explain lists the operations of the first key whose history is not
// linearizable, in invocation order.
func explain(model porcupine.Model, history []porcupine.Operation) string {
	for _, part := range model.Partition(history) {
		if porcupine.CheckOperations(model, part) {
			continue
		}
		sort.Slice(part, func(a, b int) bool { return part[a].Call < part[b].Call })
		out := fmt.Sprintf("key %q is not linearizable. Its %d operations, by invocation time:\n", part[0].Input.(Input).Key, len(part))
		for _, op := range part {
			ret := fmt.Sprint(op.Return)
			if op.Return >= Infinity {
				ret = "never"
			}
			out += fmt.Sprintf("  client %-3d [%d, %s]  %s\n", op.ClientId, op.Call, ret, Describe(op.Input.(Input), op.Output.(Output)))
		}
		return out
	}
	return "no single key is illegal on its own (unexpected)"
}
