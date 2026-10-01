package storage_test

import (
	"path/filepath"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/faultfs"
)

const identityJSON = `{"format":1,"cluster_id":"test-cluster","node_id":"n1"}`

// seedLog returns the bytes of a real, valid segment for the fuzzer to mutate.
func seedLog(tb testing.TB) []byte {
	fs := faultfs.New(1)
	opts := options(fs)
	opts.SegmentSize = 1 << 20
	s, err := storage.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	hs := raft.HardState{Term: 2, Vote: "n1"}
	if err := s.Save(&hs, entries(2, 1, 4), 2, true); err != nil {
		tb.Fatal(err)
	}
	if err := s.Save(nil, entries(2, 3, 5), 2, true); err != nil {
		tb.Fatal(err)
	}
	s.Close()
	data, _ := fs.ReadFile(filepath.Join(testDir, "wal", "wal-0000000000000001.log"))
	return data
}

// FuzzWALRecovery feeds arbitrary bytes to recovery as a WAL segment. Recovery
// may refuse them, but it must not panic, must not allocate without bound,
// and if it accepts them the store must then behave: a write followed by a
// restart has to come back.
func FuzzWALRecovery(f *testing.F) {
	valid := seedLog(f)
	f.Add(valid)
	f.Add(valid[:len(valid)/2])
	f.Add([]byte("RKVWAL\x00\x01"))
	f.Add([]byte{})
	f.Add(append(append([]byte(nil), valid...), make([]byte, 40)...))
	// A header that claims a huge payload.
	f.Add(append([]byte("RKVWAL\x00\x01"), 2, 0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0, 0, 0, 0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		fs := faultfs.New(1)
		fs.WriteFile(filepath.Join(testDir, "meta.json"), []byte(identityJSON))
		fs.WriteFile(filepath.Join(testDir, "wal", "wal-0000000000000001.log"), data)
		opts := options(fs)
		opts.SegmentSize = 1 << 20

		s, err := storage.Open(opts)
		if err != nil {
			return
		}
		st := s.InitialState()
		// Whatever was accepted must be internally consistent, because it is
		// handed straight to Raft.
		for i, e := range st.Entries {
			if e.Index != st.Snapshot.Index+1+uint64(i) {
				t.Fatalf("recovered entries are not contiguous: position %d has index %d", i, e.Index)
			}
		}
		last := st.Snapshot.Index + uint64(len(st.Entries))
		if st.Commit > last {
			t.Fatalf("recovered commit %d beyond last index %d", st.Commit, last)
		}
		hs := raft.HardState{Term: st.HardState.Term + 1, Vote: "n1"}
		if err := s.Save(&hs, []raft.Entry{{Index: last + 1, Term: hs.Term, Type: raft.EntryCommand, Data: []byte("x")}}, st.Commit, true); err != nil {
			t.Fatalf("Save after accepting fuzzed log: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err = storage.Open(opts)
		if err != nil {
			t.Fatalf("store accepted the log, took a write, and then refused to reopen: %v", err)
		}
		if got := s.InitialState(); got.HardState != hs || uint64(len(got.Entries)) != uint64(len(st.Entries))+1 {
			t.Fatalf("write after recovery was not kept: hs=%+v entries=%d", got.HardState, len(got.Entries))
		}
		s.Close()
	})
}

// FuzzSnapshotDecoding feeds arbitrary bytes to both snapshot entry points:
// a file found on disk at startup and a stream received from a peer.
func FuzzSnapshotDecoding(f *testing.F) {
	fs := faultfs.New(1)
	s, err := storage.Open(options(fs))
	if err != nil {
		f.Fatal(err)
	}
	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 3, Term: 1}, []byte("some state")); err != nil {
		f.Fatal(err)
	}
	s.Close()
	valid, _ := fs.ReadFile(snapFiles(fs)[0])
	f.Add(valid)
	f.Add(valid[:20])
	f.Add([]byte("RKVSNAP\x01"))
	huge := append([]byte(nil), valid...)
	copy(huge[24:32], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x3f}) // payload length
	f.Add(huge)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			return
		}
		// As a received stream.
		fs := faultfs.New(1)
		s, err := storage.Open(options(fs))
		if err != nil {
			t.Fatal(err)
		}
		in, err := s.NewIncomingSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := in.Write(data); err == nil {
			if meta, err := in.Finish(); err == nil {
				// Accepted: it must then install and be readable.
				if err := s.InstallSnapshot(in, false); err != nil {
					t.Fatalf("verified snapshot failed to install: %v", err)
				}
				if got := s.Snapshot(); got != meta {
					t.Fatalf("installed %+v, store reports %+v", meta, got)
				}
			}
		}
		in.Discard()
		s.Close()

		// As a file already in the snapshot directory.
		fs2 := faultfs.New(1)
		fs2.WriteFile(filepath.Join(testDir, "meta.json"), []byte(identityJSON))
		fs2.WriteFile(filepath.Join(testDir, "snap", "snap-0000000000000003-0000000000000001.snap"), data)
		if s2, err := storage.Open(options(fs2)); err == nil {
			s2.Close()
		}
	})
}
