//go:build unix

package storage_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/faultfs"
)

func benchEntries(n int, size int) []raft.Entry {
	data := make([]byte, size)
	out := make([]raft.Entry, n)
	for i := range out {
		out[i] = raft.Entry{Term: 1, Type: raft.EntryCommand, Data: data}
	}
	return out
}

// BenchmarkWALAppendSynced appends batches of 128-byte entries to a real file
// and fsyncs each batch, which is what a node does for every Ready. It
// reports entries per second, so the effect of batching several client
// requests under one fsync is directly visible.
//
// The numbers depend entirely on the storage device behind the temporary
// directory.
func BenchmarkWALAppendSynced(b *testing.B) {
	for _, batch := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			s, err := storage.Open(storage.Options{Dir: filepath.Join(b.TempDir(), "node"), ClusterID: "bench", NodeID: "n1"})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			ents := benchEntries(batch, 128)
			var index uint64
			hs := raft.HardState{Term: 1}
			if err := s.Save(&hs, nil, 0, true); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := range ents {
					index++
					ents[j].Index = index
				}
				if err := s.Save(nil, ents, index, true); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(batch)*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
		})
	}
}

// BenchmarkWALAppendNoSync is the same work without fsync, on the in-memory
// filesystem: the cost of framing and checksumming alone.
func BenchmarkWALAppendNoSync(b *testing.B) {
	for _, batch := range []int{1, 64} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			s, err := storage.Open(storage.Options{Dir: "/bench", ClusterID: "bench", NodeID: "n1", FS: faultfs.New(1), SegmentSize: 1 << 30})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			ents := benchEntries(batch, 128)
			var index uint64
			b.SetBytes(int64(batch * 128))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := range ents {
					index++
					ents[j].Index = index
				}
				if err := s.Save(nil, ents, index, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRecovery measures reopening a store whose log holds 20000 entries.
func BenchmarkRecovery(b *testing.B) {
	fs := faultfs.New(1)
	opts := storage.Options{Dir: "/bench", ClusterID: "bench", NodeID: "n1", FS: fs, SegmentSize: 4 << 20}
	s, err := storage.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	ents := benchEntries(100, 128)
	var index uint64
	for i := 0; i < 200; i++ {
		for j := range ents {
			index++
			ents[j].Index = index
		}
		if err := s.Save(nil, ents, index, true); err != nil {
			b.Fatal(err)
		}
	}
	s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := storage.Open(opts)
		if err != nil {
			b.Fatal(err)
		}
		if got := len(s.InitialState().Entries); got != 20000 {
			b.Fatalf("recovered %d entries", got)
		}
		s.Close()
	}
}
