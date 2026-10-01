package storage_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/testutil/faultfs"
)

// Each test here pins down a recovery bug that the crash-injection sweep
// (TestCrashAtEveryFilesystemOperation) found while the WAL was being written.

// The snapshot file was published but the node died before the log marker
// was written. Recovery adopted the snapshot in memory but left the log file
// ending before it, so the next append created a hole and the following
// restart refused to start.
func TestRegressionInterruptedInstallIsCompletedOnRecovery(t *testing.T) {
	fs := faultfs.New(1)
	s := mustOpen(t, fs)
	mustSave(t, s, &raft.HardState{Term: 2}, entries(1, 1, 5), 2)
	meta := raft.SnapshotMeta{Index: 9, Term: 2}
	in := incoming(t, s, snapshotBytes(t, testCluster, meta, []byte("leader state")))

	// InstallSnapshot does: rename, sync directory, then write the marker.
	// Die on the marker write.
	fs.CrashAt(fs.Ops() + 3)
	if err := s.InstallSnapshot(in, false); err == nil {
		t.Fatal("expected the install to be interrupted")
	}
	fs.Restart()

	s = mustOpen(t, fs)
	st := s.InitialState()
	if st.Snapshot != meta || len(st.Entries) != 0 || st.Commit != 9 {
		t.Fatalf("recovered snapshot=%+v entries=%d commit=%d, want the published snapshot and an empty log", st.Snapshot, len(st.Entries), st.Commit)
	}
	mustSave(t, s, nil, entries(2, 10, 11), 9)
	s = reopen(t, fs, s)
	if got := s.InitialState().Entries; !reflect.DeepEqual(got, entries(2, 10, 11)) {
		t.Fatalf("entries after the snapshot = %+v, want 10..11", got)
	}
	s.Close()
}

// A snapshot was installed at a point the log already contained, so the
// entries after it were kept. Later, older segments were deleted. Replay then
// met the marker without the entry that had justified keeping the suffix and
// wrongly dropped the log.
func TestRegressionKeptSuffixSurvivesSegmentDeletion(t *testing.T) {
	for last := uint64(30); last <= 48; last++ {
		t.Run(fmt.Sprintf("last=%d", last), func(t *testing.T) {
			fs := faultfs.New(1)
			s := mustOpen(t, fs)
			mustSave(t, s, &raft.HardState{Term: 1}, nil, 0)
			for i := uint64(1); i <= last; i++ {
				mustSave(t, s, nil, entries(1, i, i), min(i, 20))
			}
			snapAt := last - 5
			in := incoming(t, s, snapshotBytes(t, testCluster, raft.SnapshotMeta{Index: snapAt, Term: 1}, []byte("x")))
			if err := s.InstallSnapshot(in, true); err != nil {
				t.Fatal(err)
			}
			mustSave(t, s, nil, entries(1, last+1, last+3), last+2)
			if err := s.CreateSnapshot(raft.SnapshotMeta{Index: last + 2, Term: 1}, []byte("y")); err != nil {
				t.Fatal(err)
			}
			if err := s.Compact(last + 2); err != nil {
				t.Fatal(err)
			}
			s = reopen(t, fs, s)
			if got := s.InitialState().Entries; !reflect.DeepEqual(got, entries(1, last+3, last+3)) {
				t.Fatalf("entries = %+v, want only %d", got, last+3)
			}
			s.Close()
		})
	}
}

// A follower replaced an uncommitted suffix; the replacement started at an
// index that lived in an older segment. After that segment was deleted,
// replay saw the replacement reach below where the visible log began and
// reported corruption.
func TestRegressionSuffixReplacementBelowVisibleLogStart(t *testing.T) {
	for last := uint64(11); last <= 30; last++ {
		t.Run(fmt.Sprintf("last=%d", last), func(t *testing.T) {
			fs := faultfs.New(1)
			s := mustOpen(t, fs)
			mustSave(t, s, &raft.HardState{Term: 1}, nil, 0)
			for i := uint64(1); i <= last; i++ {
				mustSave(t, s, nil, entries(1, i, i), 0)
			}
			from := last - 3
			mustSave(t, s, &raft.HardState{Term: 2}, entries(2, from, last+4), last+2)
			if err := s.CreateSnapshot(raft.SnapshotMeta{Index: last + 2, Term: 2}, []byte("y")); err != nil {
				t.Fatal(err)
			}
			if err := s.Compact(last + 2); err != nil {
				t.Fatal(err)
			}
			s = reopen(t, fs, s)
			if got := s.InitialState().Entries; !reflect.DeepEqual(got, entries(2, last+3, last+4)) {
				t.Fatalf("entries = %+v, want %d..%d in term 2", got, last+3, last+4)
			}
			s.Close()
		})
	}
}

// A segment was created (its name made durable) but the node died before the
// hard state at its head was complete. Recovery kept the segment with only
// its magic string; once older segments were deleted the current term and
// vote existed nowhere on disk.
func TestRegressionHardStateSurvivesInterruptedRotation(t *testing.T) {
	hs := raft.HardState{Term: 7, Vote: "n2"}
	found := false
	for crashAt := 1; crashAt <= 400 && !found; crashAt++ {
		fs := faultfs.New(int64(crashAt))
		s := mustOpen(t, fs)
		mustSave(t, s, &hs, nil, 0)
		fs.CrashAt(fs.Ops() + crashAt)
		last := uint64(0)
		for i := uint64(1); i <= 60; i++ {
			if err := s.Save(nil, entries(7, i, i), i, true); err != nil {
				break
			}
			last = i
		}
		if !fs.Crashed() {
			found = true // every crash point of this workload has been tried
			break
		}
		fs.Restart()

		// Recover, take a snapshot of everything and delete old segments.
		s = mustOpen(t, fs)
		st := s.InitialState()
		got := uint64(len(st.Entries))
		if got < last {
			t.Fatalf("crash at %d: %d entries recovered, %d were acknowledged", crashAt, got, last)
		}
		if got > 0 {
			if err := s.Save(nil, nil, got, true); err != nil {
				t.Fatal(err)
			}
			if err := s.CreateSnapshot(raft.SnapshotMeta{Index: got, Term: 7}, []byte("z")); err != nil {
				t.Fatal(err)
			}
			if err := s.Compact(got); err != nil {
				t.Fatal(err)
			}
		}
		s = reopen(t, fs, s)
		if s.InitialState().HardState != hs {
			t.Fatalf("crash at %d: hard state after compaction = %+v, want %+v", crashAt, s.InitialState().HardState, hs)
		}
		s.Close()
	}
	if !found {
		t.Fatal("workload has more crash points than the loop covers")
	}
}
