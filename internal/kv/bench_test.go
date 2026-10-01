package kv

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkEncode(b *testing.B) {
	cmd := Command{Op: OpPut, Key: "bench/key-12345", Value: make([]byte, 128), ClientID: "client-123456", Seq: 99}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cmd.Encode()
	}
}

func BenchmarkDecode(b *testing.B) {
	data := Command{Op: OpPut, Key: "bench/key-12345", Value: make([]byte, 128), ClientID: "client-123456", Seq: 99}.Encode()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Decode(data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApplyPut is the per-command cost of the state machine itself:
// decode, validate, fingerprint for de-duplication, update the map and the
// session table. Everything else a write costs is consensus and disk.
func BenchmarkApplyPut(b *testing.B) {
	s := New(DefaultLimits)
	cmds := make([][]byte, 1000)
	for i := range cmds {
		cmds[i] = Command{Op: OpPut, Key: fmt.Sprintf("bench/key-%d", i), Value: make([]byte, 128), ClientID: fmt.Sprintf("client-%d", i%16), Seq: 1}.Encode()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// After the first pass over cmds every command is a retry of one
		// already seen, so this mixes fresh writes with de-duplicated ones.
		// Both paths decode, validate and fingerprint the command.
		s.Apply(uint64(i+1), cmds[i%len(cmds)])
	}
}

func BenchmarkApplyRead(b *testing.B) {
	s := New(DefaultLimits)
	for i := 0; i < 1000; i++ {
		s.Apply(uint64(i+1), Command{Op: OpPut, Key: fmt.Sprintf("bench/key-%d", i), Value: make([]byte, 128), ClientID: "c", Seq: uint64(i + 1)}.Encode())
	}
	reads := make([][]byte, 1000)
	for i := range reads {
		reads[i] = Command{Op: OpRead, Key: fmt.Sprintf("bench/key-%d", i)}.Encode()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Apply(uint64(2000+i), reads[i%len(reads)])
	}
}

// BenchmarkSnapshot serialises and restores a store of 10000 keys with
// 128-byte values, about 1.5 MB.
func BenchmarkSnapshot(b *testing.B) {
	s := New(DefaultLimits)
	for i := 0; i < 10000; i++ {
		s.Apply(uint64(i+1), Command{Op: OpPut, Key: fmt.Sprintf("bench/key-%05d", i), Value: make([]byte, 128), ClientID: fmt.Sprintf("client-%d", i%64), Seq: uint64(i + 1)}.Encode())
	}
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.SetBytes(int64(len(s.Snapshot())))
		}
	})
	snap := s.Snapshot()
	b.Run("restore", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(snap)))
		for i := 0; i < b.N; i++ {
			if err := New(DefaultLimits).Restore(bytes.NewReader(snap)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
