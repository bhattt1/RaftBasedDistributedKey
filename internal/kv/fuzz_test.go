package kv

import (
	"bytes"
	"testing"
)

// FuzzDecodeCommand checks the command decoder against arbitrary bytes. Input
// reaches it from the log, so it must never panic, and anything it accepts
// must re-encode to exactly the input (the encoding is canonical).
func FuzzDecodeCommand(f *testing.F) {
	for _, c := range sampleCommands() {
		f.Add(c.Encode())
	}
	f.Add([]byte{})
	f.Add([]byte{1, 1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1})

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := Decode(data)
		if err != nil {
			return
		}
		if got := cmd.Encode(); !bytes.Equal(got, data) {
			t.Fatalf("decoded %x to %+v, which re-encodes to %x", data, cmd, got)
		}
	})
}

// FuzzApply applies arbitrary bytes as log entries. Whatever they are, the
// store must stay within its limits and two stores fed the same entries must
// end in the same state.
func FuzzApply(f *testing.F) {
	for _, c := range sampleCommands() {
		f.Add(c.Encode(), c.Encode())
	}
	f.Fuzz(func(t *testing.T, first, second []byte) {
		limits := Limits{MaxKeyBytes: 16, MaxValueBytes: 64, MaxStateBytes: 256, MaxSessions: 2}
		a, b := New(limits), New(limits)
		for i, data := range [][]byte{first, second, first} {
			ra, rb := a.Apply(uint64(i+1), data), b.Apply(uint64(i+1), data)
			if ra.Status != rb.Status || ra.Revision != rb.Revision || ra.Duplicate != rb.Duplicate {
				t.Fatalf("two stores disagreed on entry %d: %+v vs %+v", i, ra, rb)
			}
		}
		if a.Size() > limits.MaxStateBytes || a.Sessions() > limits.MaxSessions {
			t.Fatalf("limits exceeded: size %d, sessions %d", a.Size(), a.Sessions())
		}
		if !bytes.Equal(a.Snapshot(), b.Snapshot()) {
			t.Fatal("two stores fed the same entries produced different snapshots")
		}
	})
}

// FuzzRestore feeds arbitrary bytes to the snapshot decoder. It may reject
// them; if it accepts them, the store it built must snapshot back to the same
// bytes and respect the limits.
func FuzzRestore(f *testing.F) {
	s := New(DefaultLimits)
	for i, c := range sampleCommands() {
		s.Apply(uint64(i+1), c.Encode())
	}
	valid := s.Snapshot()
	f.Add(valid)
	f.Add(valid[:len(valid)/2])
	f.Add([]byte("KVS1"))
	// A key count far beyond what the data could hold.
	f.Add([]byte{'K', 'V', 'S', '1', 5, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f})

	f.Fuzz(func(t *testing.T, data []byte) {
		limits := Limits{MaxKeyBytes: 64, MaxValueBytes: 256, MaxStateBytes: 4096, MaxSessions: 8}
		st := New(limits)
		if err := st.Restore(bytes.NewReader(data)); err != nil {
			return
		}
		if st.Size() > limits.MaxStateBytes || st.Sessions() > limits.MaxSessions {
			t.Fatalf("restored state exceeds limits: size %d, sessions %d", st.Size(), st.Sessions())
		}
		again := New(limits)
		if err := again.Restore(bytes.NewReader(st.Snapshot())); err != nil {
			t.Fatalf("a snapshot of an accepted state was rejected: %v", err)
		}
		if !bytes.Equal(st.Snapshot(), again.Snapshot()) {
			t.Fatal("restore is not stable across a snapshot round trip")
		}
	})
}
