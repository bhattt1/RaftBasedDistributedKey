//go:build unix

package storage_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
)

// These tests use the real filesystem. They cannot inject crashes, but they
// confirm that the store works against actual files, fsync and flock, which
// the in-memory filesystem only imitates.

func osOptions(dir string) storage.Options {
	return storage.Options{Dir: dir, ClusterID: testCluster, NodeID: testNode, SegmentSize: 4096, MaxSnapshotBytes: 1 << 20}
}

func TestRealFilesystemRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	s, err := storage.Open(osOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	hs := raft.HardState{Term: 4, Vote: "n3"}
	if err := s.Save(&hs, nil, 0, true); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 300; i++ {
		if err := s.Save(nil, entries(4, i, i), i, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 250, Term: 4}, []byte("state at 250")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(250); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	segs, _ := filepath.Glob(filepath.Join(dir, "wal", "*.log"))
	if len(segs) == 0 || len(segs) > 4 {
		t.Fatalf("%d WAL segments on disk after compaction, expected a few", len(segs))
	}
	if info, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("meta.json: %v, mode %v, want 0600", err, info.Mode().Perm())
	}

	s, err = storage.Open(osOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	st := s.InitialState()
	if st.HardState != hs || st.Snapshot.Index != 250 || st.Commit != 300 {
		t.Fatalf("recovered hs=%+v snapshot=%+v commit=%d", st.HardState, st.Snapshot, st.Commit)
	}
	if !reflect.DeepEqual(st.Entries, entries(4, 251, 300)) {
		t.Fatalf("recovered %d entries, want 251..300", len(st.Entries))
	}
	if _, data := readPayload(t, s); string(data) != "state at 250" {
		t.Fatalf("snapshot payload = %q", data)
	}
}

func TestRealFilesystemLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	s, err := storage.Open(osOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Open(osOptions(dir)); !errors.Is(err, storage.ErrLocked) {
		t.Fatalf("second Open error = %v, want ErrLocked", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = storage.Open(osOptions(dir))
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	s.Close()
}

func TestRealFilesystemTornTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	opts := osOptions(dir)
	opts.SegmentSize = 1 << 20
	s, err := storage.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(&raft.HardState{Term: 1}, entries(1, 1, 3), 0, true); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Append half a record, as an interrupted write would.
	seg := filepath.Join(dir, "wal", "wal-0000000000000001.log")
	f, err := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{2, 40, 0, 0, 0, 1, 2, 3})
	f.Close()

	s, err = storage.Open(opts)
	if err != nil {
		t.Fatalf("Open with a torn tail: %v", err)
	}
	if got := s.InitialState().Entries; !reflect.DeepEqual(got, entries(1, 1, 3)) {
		t.Fatalf("recovered %+v, want the three complete entries", got)
	}
	s.Close()
}
