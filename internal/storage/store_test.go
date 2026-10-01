package storage_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/storage"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/faultfs"
)

const (
	testDir     = "/data"
	testCluster = "test-cluster"
	testNode    = "n1"
)

func options(fs storage.FS) storage.Options {
	return storage.Options{Dir: testDir, ClusterID: testCluster, NodeID: testNode, FS: fs, SegmentSize: 512, MaxSnapshotBytes: 1 << 20}
}

func mustOpen(t *testing.T, fs storage.FS) *storage.Store {
	t.Helper()
	s, err := storage.Open(options(fs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// reopen closes s and opens the same directory again, as a clean restart.
func reopen(t *testing.T, fs storage.FS, s *storage.Store) *storage.Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return mustOpen(t, fs)
}

func entry(index, term uint64) raft.Entry {
	return raft.Entry{Index: index, Term: term, Type: raft.EntryCommand, Data: []byte(fmt.Sprintf("data-%d-%d", index, term))}
}

func entries(term, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, entry(i, term))
	}
	return out
}

func mustSave(t *testing.T, s *storage.Store, hs *raft.HardState, ents []raft.Entry, commit uint64) {
	t.Helper()
	if err := s.Save(hs, ents, commit, true); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func walFiles(fs *faultfs.FS) []string {
	var out []string
	for _, n := range fs.Names() {
		if strings.Contains(n, "/wal/") {
			out = append(out, n)
		}
	}
	return out
}

func snapFiles(fs *faultfs.FS) []string {
	var out []string
	for _, n := range fs.Names() {
		if strings.Contains(n, "/snap/") {
			out = append(out, n)
		}
	}
	return out
}

// ---- log -------------------------------------------------------------------

func TestSaveAndRecover(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	if st := s.InitialState(); st.HardState != (raft.HardState{}) || len(st.Entries) != 0 || st.Commit != 0 {
		t.Fatalf("fresh store recovered %+v, want empty state", st)
	}
	mustSave(t, s, &raft.HardState{Term: 3, Vote: "n2"}, entries(3, 1, 5), 0)
	mustSave(t, s, nil, entries(3, 6, 7), 4)

	s = reopen(t, fs, s)
	st := s.InitialState()
	if st.HardState != (raft.HardState{Term: 3, Vote: "n2"}) {
		t.Fatalf("hard state = %+v", st.HardState)
	}
	if !reflect.DeepEqual(st.Entries, entries(3, 1, 7)) {
		t.Fatalf("entries = %+v", st.Entries)
	}
	if st.Commit != 4 {
		t.Fatalf("commit = %d, want 4", st.Commit)
	}
}

func TestEmptyValueAndNoopEntriesRoundTrip(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	in := []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNoop},
		{Index: 2, Term: 1, Type: raft.EntryCommand, Data: []byte{0}},
		{Index: 3, Term: 1, Type: raft.EntryCommand},
	}
	mustSave(t, s, &raft.HardState{Term: 1}, in, 0)
	s = reopen(t, fs, s)
	if got := s.InitialState().Entries; !reflect.DeepEqual(got, in) {
		t.Fatalf("entries = %+v, want %+v", got, in)
	}
}

func TestSuffixReplacementSurvivesRestart(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 6), 3)
	// A new leader in term 2 overwrites the uncommitted entries 4..6.
	mustSave(t, s, &raft.HardState{Term: 2}, entries(2, 4, 5), 3)

	s = reopen(t, fs, s)
	want := append(entries(1, 1, 3), entries(2, 4, 5)...)
	if got := s.InitialState().Entries; !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %+v, want %+v", got, want)
	}
}

func TestSegmentsRotateAndAreCompactedAfterSnapshot(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	hs := raft.HardState{Term: 2, Vote: "n3"}
	mustSave(t, s, &hs, nil, 0)
	for i := uint64(1); i <= 60; i++ {
		mustSave(t, s, nil, entries(2, i, i), i)
	}
	before := len(walFiles(fs))
	if before < 4 {
		t.Fatalf("only %d segments, expected rotation with a 512-byte segment size", before)
	}

	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 50, Term: 2}, []byte("state at 50")); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(50); err != nil {
		t.Fatal(err)
	}
	after := len(walFiles(fs))
	if after >= before {
		t.Fatalf("segments before=%d after=%d, expected old segments to be deleted", before, after)
	}

	// The hard state was written long ago, in a segment that is now gone.
	// It must survive because every segment restates it at its head.
	s = reopen(t, fs, s)
	st := s.InitialState()
	if st.HardState != hs {
		t.Fatalf("hard state = %+v, want %+v after deleting old segments", st.HardState, hs)
	}
	if st.Snapshot != (raft.SnapshotMeta{Index: 50, Term: 2}) {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	if !reflect.DeepEqual(st.Entries, entries(2, 51, 60)) {
		t.Fatalf("entries = %d..%d (%d), want 51..60", st.Entries[0].Index, st.Entries[len(st.Entries)-1].Index, len(st.Entries))
	}
	if st.Commit != 60 {
		t.Fatalf("commit = %d, want 60", st.Commit)
	}
}

// ---- torn writes versus corruption ----------------------------------------

// buildLog writes two batches and returns the single segment's contents and
// its length before the second batch.
func buildLog(t *testing.T) (fs *faultfs.FS, name string, full []byte, lenBefore int) {
	t.Helper()
	fs = faultfs.New(1)
	opts := options(fs)
	opts.SegmentSize = 1 << 20
	s, err := storage.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	mustSave(t, s, &raft.HardState{Term: 1, Vote: "n1"}, entries(1, 1, 3), 2)
	name = filepath.Join(testDir, "wal", "wal-0000000000000001.log")
	before, _ := fs.ReadFile(name)
	mustSave(t, s, nil, entries(1, 4, 4), 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	full, _ = fs.ReadFile(name)
	if len(full) <= len(before) {
		t.Fatal("second batch did not grow the segment")
	}
	return fs, name, full, len(before)
}

func openBig(fs storage.FS) (*storage.Store, error) {
	opts := options(fs)
	opts.SegmentSize = 1 << 20
	return storage.Open(opts)
}

func TestTornTailIsRecoveredAtEveryCutPoint(t *testing.T) {
	fs, name, full, lenBefore := buildLog(t)
	for cut := lenBefore; cut < len(full); cut++ {
		fs.WriteFile(name, full[:cut])
		s, err := openBig(fs)
		if err != nil {
			t.Fatalf("cut at %d of %d: Open failed: %v", cut, len(full), err)
		}
		st := s.InitialState()
		if !reflect.DeepEqual(st.Entries, entries(1, 1, 3)) || st.HardState.Term != 1 {
			t.Fatalf("cut at %d: recovered %d entries, want exactly the 3 from the complete batch", cut, len(st.Entries))
		}
		// The log must be usable again: the garbage tail is gone.
		mustSave(t, s, nil, entries(1, 4, 5), 3)
		s = reopen(t, fs, s)
		if got := s.InitialState().Entries; !reflect.DeepEqual(got, entries(1, 1, 5)) {
			t.Fatalf("cut at %d: after appending, recovered %d entries, want 5", cut, len(got))
		}
		s.Close()
	}
}

func TestZeroFilledTailIsRecovered(t *testing.T) {
	fs, name, full, _ := buildLog(t)
	for _, zeros := range []int{1, 12, 13, 14, 500} {
		fs.WriteFile(name, append(append([]byte(nil), full...), make([]byte, zeros)...))
		s, err := openBig(fs)
		if err != nil {
			t.Fatalf("%d zero bytes after the log: Open failed: %v", zeros, err)
		}
		if got := s.InitialState().Entries; !reflect.DeepEqual(got, entries(1, 1, 4)) {
			t.Fatalf("%d zero bytes: recovered %d entries, want 4", zeros, len(got))
		}
		s.Close()
	}
}

func TestEveryFlippedByteIsDetected(t *testing.T) {
	fs, name, full, _ := buildLog(t)
	for i := range full {
		damaged := append([]byte(nil), full...)
		damaged[i] ^= 0x40
		fs.WriteFile(name, damaged)
		s, err := openBig(fs)
		if err == nil {
			s.Close()
			t.Fatalf("byte %d of %d flipped and Open still succeeded", i, len(full))
		}
		if !errors.Is(err, storage.ErrCorrupt) {
			t.Fatalf("byte %d flipped: error %v is not ErrCorrupt", i, err)
		}
	}
}

func TestDamagedLengthIsNotMistakenForATornTail(t *testing.T) {
	// Blow up the length of a record in the middle of the log. If recovery
	// believed that length, the record would appear to run past the end of
	// the file and everything after it would be dropped as a torn write.
	fs, name, full, _ := buildLog(t)
	damaged := append([]byte(nil), full...)
	// The first record starts right after the 8-byte magic string and its
	// length field is bytes 1..4 of the record header.
	damaged[8+3] = 0x7f
	fs.WriteFile(name, damaged)
	if s, err := openBig(fs); !errors.Is(err, storage.ErrCorrupt) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func TestIncompleteRecordInOlderSegmentIsCorruption(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	for i := uint64(1); i <= 40; i++ {
		mustSave(t, s, &raft.HardState{Term: 1}, entries(1, i, i), 0)
	}
	s.Close()
	files := walFiles(fs)
	if len(files) < 3 {
		t.Fatalf("need at least 3 segments, have %d", len(files))
	}
	// Cut the tail off a segment that is not the last one. An interrupted
	// append can only ever affect the newest segment.
	first, _ := fs.ReadFile(files[0])
	fs.WriteFile(files[0], first[:len(first)-5])
	if s, err := storage.Open(options(fs)); !errors.Is(err, storage.ErrCorrupt) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

func TestMissingSegmentIsCorruption(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	for i := uint64(1); i <= 40; i++ {
		mustSave(t, s, &raft.HardState{Term: 1}, entries(1, i, i), 0)
	}
	s.Close()
	files := walFiles(fs)
	if err := fs.Remove(files[1]); err != nil {
		t.Fatal(err)
	}
	if s, err := storage.Open(options(fs)); !errors.Is(err, storage.ErrCorrupt) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("Open error = %v, want ErrCorrupt for a hole in the segment sequence", err)
	}
}

// ---- failures --------------------------------------------------------------

func TestWriteAndSyncFailuresStopTheStore(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opsOff int // which operation of the next Save fails: 1 = write, 2 = fsync
		err    error
	}{
		{"disk full on write", 1, syscall.ENOSPC},
		{"fsync error", 2, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := faultfs.New(1)
			opts := options(fs)
			opts.SegmentSize = 1 << 20
			s, err := storage.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 2), 0)

			fs.FailAt(fs.Ops()+tc.opsOff, tc.err)
			err = s.Save(nil, entries(1, 3, 3), 0, true)
			if !errors.Is(err, storage.ErrFailed) {
				t.Fatalf("Save error = %v, want ErrFailed", err)
			}
			// The disk "works" again, but the store must not: a later
			// success would hide the lost write.
			if err := s.Save(nil, entries(1, 3, 3), 0, true); !errors.Is(err, storage.ErrFailed) {
				t.Fatalf("Save after a failure = %v, want ErrFailed", err)
			}
			if err := s.Compact(1); !errors.Is(err, storage.ErrFailed) {
				t.Fatalf("Compact after a failure = %v, want ErrFailed", err)
			}

			// After a restart everything acknowledged is still there.
			fs.Restart()
			s2, err := storage.Open(opts)
			if err != nil {
				t.Fatalf("Open after failure: %v", err)
			}
			got := s2.InitialState().Entries
			if len(got) < 2 || !reflect.DeepEqual(got[:2], entries(1, 1, 2)) {
				t.Fatalf("recovered %+v, want at least the two acknowledged entries", got)
			}
		})
	}
}

func TestDirectoryIsLockedAgainstASecondProcess(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	if _, err := storage.Open(options(fs)); !errors.Is(err, storage.ErrLocked) {
		t.Fatalf("second Open error = %v, want ErrLocked", err)
	}
	s.Close()
	s2, err := storage.Open(options(fs))
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	s2.Close()
}

func TestIdentityIsCheckedOnRestart(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 1), 0)
	s.Close()

	wrongCluster := options(fs)
	wrongCluster.ClusterID = "another-cluster"
	if _, err := storage.Open(wrongCluster); err == nil || !strings.Contains(err.Error(), "cluster") {
		t.Fatalf("Open with the wrong cluster ID: %v", err)
	}
	wrongNode := options(fs)
	wrongNode.NodeID = "n2"
	if _, err := storage.Open(wrongNode); err == nil || !strings.Contains(err.Error(), "node") {
		t.Fatalf("Open with the wrong node ID: %v", err)
	}
	// A failed Open must release the lock.
	mustOpen(t, fs).Close()
}

func TestDataWithoutIdentityFileIsRejected(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 1), 0)
	s.Close()
	if err := fs.Remove(filepath.Join(testDir, "meta.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Open(options(fs)); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
}

// ---- snapshots -------------------------------------------------------------

func readPayload(t *testing.T, s *storage.Store) (storage.SnapshotHeader, []byte) {
	t.Helper()
	h, rc, err := s.OpenSnapshotPayload()
	if err != nil {
		t.Fatalf("OpenSnapshotPayload: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return h, data
}

func TestLocalSnapshotRoundTrip(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 2}, entries(2, 1, 20), 20)
	payload := bytes.Repeat([]byte("kv-state "), 1000)
	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 12, Term: 2}, payload); err != nil {
		t.Fatal(err)
	}
	// A second snapshot replaces the first.
	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 15, Term: 2}, payload[:100]); err != nil {
		t.Fatal(err)
	}
	if files := snapFiles(fs); len(files) != 1 {
		t.Fatalf("snapshot files = %v, want only the newest", files)
	}

	s = reopen(t, fs, s)
	st := s.InitialState()
	if st.Snapshot != (raft.SnapshotMeta{Index: 15, Term: 2}) {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	if !reflect.DeepEqual(st.Entries, entries(2, 16, 20)) {
		t.Fatalf("entries after snapshot = %+v, want 16..20", st.Entries)
	}
	h, got := readPayload(t, s)
	if h.Meta != st.Snapshot || h.ClusterID != testCluster || !bytes.Equal(got, payload[:100]) {
		t.Fatalf("payload round trip failed: header %+v, %d bytes", h, len(got))
	}
}

func TestOlderSnapshotDoesNotReplaceNewer(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 10), 10)
	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 8, Term: 1}, []byte("new")); err != nil {
		t.Fatal(err)
	}
	// A slower background snapshot of an older state finishes afterwards.
	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 5, Term: 1}, []byte("old")); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot(); got.Index != 8 {
		t.Fatalf("newest snapshot index = %d, want 8", got.Index)
	}
	if _, data := readPayload(t, s); string(data) != "new" {
		t.Fatalf("payload = %q, want the newer snapshot", data)
	}
	if files := snapFiles(fs); len(files) != 1 {
		t.Fatalf("snapshot directory = %v, want one file", files)
	}
}

func TestLeftoverTemporarySnapshotsAreRemoved(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 5), 5)
	s.Close()
	fs.WriteFile(filepath.Join(testDir, "snap", "local-7.tmp"), []byte("half written"))
	fs.WriteFile(filepath.Join(testDir, "snap", "incoming-9.tmp"), []byte("half received"))

	s = mustOpen(t, fs)
	defer s.Close()
	if files := snapFiles(fs); len(files) != 0 {
		t.Fatalf("temporary files survived a restart: %v", files)
	}
	if s.InitialState().Snapshot.Index != 0 || len(s.InitialState().Entries) != 5 {
		t.Fatalf("state changed by leftover temporary files: %+v", s.InitialState())
	}
}

func TestDamagedSnapshotRefusesToStart(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 5), 5)
	if err := s.CreateSnapshot(raft.SnapshotMeta{Index: 5, Term: 1}, []byte("important applied state")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	name := snapFiles(fs)[0]
	good, _ := fs.ReadFile(name)
	for i := range good {
		damaged := append([]byte(nil), good...)
		damaged[i] ^= 0x01
		fs.WriteFile(name, damaged)
		if s, err := storage.Open(options(fs)); err == nil {
			s.Close()
			t.Fatalf("snapshot byte %d flipped and Open still succeeded", i)
		}
	}
	fs.WriteFile(name, good[:len(good)-1])
	if s, err := storage.Open(options(fs)); err == nil {
		s.Close()
		t.Fatal("truncated snapshot and Open still succeeded")
	}
	fs.WriteFile(name, good)
	mustOpen(t, fs).Close()
}

func TestLogMarkerWithoutItsSnapshotIsCorruption(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 1}, entries(1, 1, 3), 0)
	in := incoming(t, s, snapshotBytes(t, testCluster, raft.SnapshotMeta{Index: 9, Term: 1}, []byte("x")))
	if err := s.InstallSnapshot(in, false); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := fs.Remove(snapFiles(fs)[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Open(options(fs)); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt: the log says state was replaced by a snapshot that no longer exists", err)
	}
}

// snapshotBytes produces the exact bytes a leader would send for a snapshot.
func snapshotBytes(t *testing.T, cluster string, meta raft.SnapshotMeta, payload []byte) []byte {
	t.Helper()
	fs := faultfs.New(99)
	s, err := storage.Open(storage.Options{Dir: "/leader", ClusterID: cluster, NodeID: "leader", FS: fs})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.CreateSnapshot(meta, payload); err != nil {
		t.Fatal(err)
	}
	h, size, rc, err := s.OpenSnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) != size || h.Meta != meta {
		t.Fatalf("OpenSnapshotFile reported size %d and meta %+v, read %d bytes", size, h.Meta, len(data))
	}
	return data
}

func incoming(t *testing.T, s *storage.Store, data []byte) *storage.IncomingSnapshot {
	t.Helper()
	in, err := s.NewIncomingSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Deliver in small chunks, as the transport does.
	for len(data) > 0 {
		n := min(len(data), 37)
		if _, err := in.Write(data[:n]); err != nil {
			t.Fatalf("Write: %v", err)
		}
		data = data[n:]
	}
	if _, err := in.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return in
}

func TestInstallSnapshot(t *testing.T) {
	tests := []struct {
		name        string
		meta        raft.SnapshotMeta
		keepLog     bool
		wantEntries []raft.Entry
	}{
		{"beyond the log: log is discarded", raft.SnapshotMeta{Index: 50, Term: 3}, false, nil},
		{"matches an existing entry: suffix is kept", raft.SnapshotMeta{Index: 6, Term: 1}, true, entries(1, 7, 10)},
		{"conflicts with an existing entry: log is discarded", raft.SnapshotMeta{Index: 6, Term: 2}, false, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := faultfs.New(1)
			s := mustOpen(t, fs)
			mustSave(t, s, &raft.HardState{Term: 3, Vote: "n2"}, entries(1, 1, 10), 2)
			in := incoming(t, s, snapshotBytes(t, testCluster, tc.meta, []byte("leader state")))
			if err := s.InstallSnapshot(in, tc.keepLog); err != nil {
				t.Fatal(err)
			}
			in.Discard() // harmless after a successful install
			if got := s.Snapshot(); got != tc.meta {
				t.Fatalf("Snapshot() = %+v, want %+v", got, tc.meta)
			}

			s = reopen(t, fs, s)
			st := s.InitialState()
			if st.Snapshot != tc.meta || st.Commit != tc.meta.Index {
				t.Fatalf("recovered snapshot %+v commit %d, want %+v and %d", st.Snapshot, st.Commit, tc.meta, tc.meta.Index)
			}
			if !reflect.DeepEqual(st.Entries, tc.wantEntries) {
				t.Fatalf("entries = %+v, want %+v", st.Entries, tc.wantEntries)
			}
			if st.HardState != (raft.HardState{Term: 3, Vote: "n2"}) {
				t.Fatalf("hard state = %+v: installing a snapshot must not touch term or vote", st.HardState)
			}
			if _, data := readPayload(t, s); string(data) != "leader state" {
				t.Fatalf("payload = %q", data)
			}
			// The log continues from the snapshot.
			next := tc.meta.Index + uint64(len(tc.wantEntries)) + 1
			mustSave(t, s, nil, entries(3, next, next), tc.meta.Index)
			s = reopen(t, fs, s)
			if got := s.InitialState().Entries; len(got) != len(tc.wantEntries)+1 {
				t.Fatalf("after appending, %d entries, want %d", len(got), len(tc.wantEntries)+1)
			}
			s.Close()
		})
	}
}

func TestIncomingSnapshotIsValidated(t *testing.T) {
	good := snapshotBytes(t, testCluster, raft.SnapshotMeta{Index: 9, Term: 2}, bytes.Repeat([]byte("s"), 500))

	feed := func(t *testing.T, s *storage.Store, data []byte) error {
		in, err := s.NewIncomingSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer in.Discard()
		if _, err := in.Write(data); err != nil {
			return err
		}
		_, err = in.Finish()
		return err
	}

	t.Run("accepted when intact", func(t *testing.T) {
		fs := faultfs.New(1)
		s := mustOpen(t, fs)
		defer s.Close()
		if err := feed(t, s, good); err != nil {
			t.Fatal(err)
		}
		if files := snapFiles(fs); len(files) != 0 {
			t.Fatalf("a discarded incoming snapshot left files behind: %v", files)
		}
	})
	t.Run("flipped byte", func(t *testing.T) {
		fs := faultfs.New(1)
		s := mustOpen(t, fs)
		defer s.Close()
		for i := 0; i < len(good); i += 7 {
			damaged := append([]byte(nil), good...)
			damaged[i] ^= 0x10
			if err := feed(t, s, damaged); err == nil {
				t.Fatalf("byte %d flipped and the snapshot was accepted", i)
			}
		}
	})
	t.Run("truncated transfer", func(t *testing.T) {
		fs := faultfs.New(1)
		s := mustOpen(t, fs)
		defer s.Close()
		for _, n := range []int{0, 10, len(good) / 2, len(good) - 1} {
			if err := feed(t, s, good[:n]); err == nil {
				t.Fatalf("snapshot truncated to %d bytes was accepted", n)
			}
		}
	})
	t.Run("wrong cluster", func(t *testing.T) {
		fs := faultfs.New(1)
		s := mustOpen(t, fs)
		defer s.Close()
		other := snapshotBytes(t, "some-other-cluster", raft.SnapshotMeta{Index: 9, Term: 2}, []byte("x"))
		if err := feed(t, s, other); err == nil || !strings.Contains(err.Error(), "cluster") {
			t.Fatalf("snapshot from another cluster: err = %v", err)
		}
	})
	t.Run("larger than the limit", func(t *testing.T) {
		fs := faultfs.New(1)
		opts := options(fs)
		opts.MaxSnapshotBytes = 100
		s, err := storage.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := feed(t, s, good); err == nil {
			t.Fatal("a 500-byte payload was accepted with a 100-byte limit")
		}
		// A sender that lies about nothing but simply keeps sending is
		// stopped by the byte count, before any validation.
		in, _ := s.NewIncomingSnapshot()
		defer in.Discard()
		var werr error
		for i := 0; i < 100 && werr == nil; i++ {
			_, werr = in.Write(make([]byte, 64))
		}
		if werr == nil {
			t.Fatal("unbounded incoming data was not stopped")
		}
	})
	t.Run("unfinished snapshot cannot be installed", func(t *testing.T) {
		fs := faultfs.New(1)
		s := mustOpen(t, fs)
		defer s.Close()
		in, _ := s.NewIncomingSnapshot()
		in.Write(good)
		if err := s.InstallSnapshot(in, false); err == nil {
			t.Fatal("InstallSnapshot accepted a snapshot that was never verified")
		}
		in.Discard()
	})
}
