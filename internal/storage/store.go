package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// formatVersion identifies the on-disk layout. A node refuses a directory
// written with a different version.
const formatVersion = 1

// Options configures a Store.
type Options struct {
	// Dir is the node's data directory. One process may use it at a time.
	Dir string
	// ClusterID and NodeID are written on first start and checked on every
	// later start, so a directory cannot be attached to the wrong node.
	ClusterID string
	NodeID    string
	// FS defaults to the real filesystem.
	FS FS
	// SegmentSize is the size at which a WAL segment is closed.
	SegmentSize int64
	// MaxSnapshotBytes bounds the payload of a snapshot received from a peer.
	MaxSnapshotBytes int64
	// NoSync skips fsync. Acknowledged writes can then be lost on a crash or
	// power failure. It exists only so benchmarks can show what fsync costs.
	NoSync bool
	// OnSync observes how long each WAL fsync took.
	OnSync func(time.Duration)
}

type identity struct {
	Format    int    `json:"format"`
	ClusterID string `json:"cluster_id"`
	NodeID    string `json:"node_id"`
}

// Store is a node's durable state. Save, InstallSnapshot and Compact are
// called by the single goroutine that owns the Raft node. CreateSnapshot and
// the snapshot readers may be called from other goroutines.
type Store struct {
	fs      FS
	opts    Options
	walDir  string
	snapDir string
	lock    io.Closer
	wal     *wal
	initial raft.InitialState

	mu       sync.Mutex // guards the snapshot directory and the fields below
	snap     raft.SnapshotMeta
	tmpCount int
	closed   bool
}

// Open locks the directory, validates what is in it and recovers the state a
// node needs to restart.
func Open(opts Options) (*Store, error) {
	if opts.FS == nil {
		opts.FS = OSFS{}
	}
	if opts.Dir == "" || opts.ClusterID == "" || opts.NodeID == "" {
		return nil, errors.New("storage: Dir, ClusterID and NodeID are required")
	}
	fs := opts.FS
	if err := fs.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := fs.Lock(filepath.Join(opts.Dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	s := &Store{
		fs:      fs,
		opts:    opts,
		walDir:  filepath.Join(opts.Dir, "wal"),
		snapDir: filepath.Join(opts.Dir, "snap"),
		lock:    lock,
	}
	if err := s.recover(); err != nil {
		lock.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) recover() error {
	if err := s.checkIdentity(); err != nil {
		return err
	}
	if err := s.fs.MkdirAll(s.snapDir, 0o700); err != nil {
		return err
	}
	snap, err := s.recoverSnapshots()
	if err != nil {
		return err
	}
	w, st, err := openWAL(s.fs, s.walDir, walOptions{segmentSize: s.opts.SegmentSize, noSync: s.opts.NoSync, onSync: s.opts.OnSync})
	if err != nil {
		return err
	}
	initial, snapshotAhead, keepLog, err := reconcile(st, snap)
	if err != nil {
		w.close()
		return err
	}
	if snapshotAhead {
		// The newest snapshot is ahead of where the log starts. That is the
		// normal state after a local snapshot, and also what an interrupted
		// InstallSnapshot leaves behind: file published, marker not written.
		// In the second case the log on disk still ends before the snapshot,
		// and appending after it would leave a hole that the next replay
		// rejects. Writing the marker now completes the interrupted install;
		// for a local snapshot it merely restates what replay would conclude.
		if err := w.saveSnapshotMarker(snap, keepLog); err != nil {
			w.close()
			return err
		}
	}
	s.wal, s.snap, s.initial = w, snap, initial
	return nil
}

// checkIdentity writes the identity file on first start and compares it on
// later starts.
func (s *Store) checkIdentity() error {
	path := filepath.Join(s.opts.Dir, "meta.json")
	want := identity{Format: formatVersion, ClusterID: s.opts.ClusterID, NodeID: s.opts.NodeID}
	data, err := readFile(s.fs, path, 1<<20)
	if errors.Is(err, os.ErrNotExist) {
		// A directory with data but no identity is not a fresh directory.
		for _, sub := range []string{s.walDir, s.snapDir} {
			if names, err := s.fs.ReadDir(sub); err == nil && len(names) > 0 {
				return corruptf("%s has data but meta.json is missing", s.opts.Dir)
			}
		}
		data, err := json.Marshal(want)
		if err != nil {
			return err
		}
		return atomicWrite(s.fs, s.opts.Dir, "meta.json", data)
	}
	if err != nil {
		return err
	}
	var got identity
	if err := json.Unmarshal(data, &got); err != nil {
		return corruptf("meta.json: %v", err)
	}
	switch {
	case got.Format != formatVersion:
		return fmt.Errorf("storage: data directory has format %d, this binary supports %d", got.Format, formatVersion)
	case got.ClusterID != want.ClusterID:
		return fmt.Errorf("storage: data directory belongs to cluster %q, not %q", got.ClusterID, want.ClusterID)
	case got.NodeID != want.NodeID:
		return fmt.Errorf("storage: data directory belongs to node %q, not %q", got.NodeID, want.NodeID)
	}
	return nil
}

// recoverSnapshots removes leftovers of interrupted work and returns the
// newest snapshot after verifying it in full.
func (s *Store) recoverSnapshots() (raft.SnapshotMeta, error) {
	var newest raft.SnapshotMeta
	names, err := s.fs.ReadDir(s.snapDir)
	if err != nil {
		return newest, err
	}
	var published []raft.SnapshotMeta
	for _, name := range names {
		if strings.HasSuffix(name, ".tmp") {
			// Never renamed into place, so never part of the durable state.
			if err := s.fs.Remove(filepath.Join(s.snapDir, name)); err != nil {
				return newest, err
			}
			continue
		}
		meta, ok := parseSnapshotName(name)
		if !ok {
			return newest, corruptf("unexpected file %q in the snapshot directory", name)
		}
		published = append(published, meta)
		if meta.Index >= newest.Index {
			newest = meta
		}
	}
	if len(published) == 0 {
		return newest, nil
	}
	// A published snapshot was fully synced before it was renamed, so a
	// checksum failure here is damage, not an interrupted write. Falling
	// back to an older snapshot could silently lose applied state.
	h, err := verifySnapshotFile(s.fs, filepath.Join(s.snapDir, snapshotName(newest)), s.opts.ClusterID, 0)
	if err != nil {
		return newest, fmt.Errorf("snapshot %s: %w", snapshotName(newest), err)
	}
	if h.Meta != newest {
		return newest, corruptf("snapshot %s contains position (%d, %d)", snapshotName(newest), h.Meta.Index, h.Meta.Term)
	}
	for _, meta := range published {
		if meta != newest {
			if err := s.fs.Remove(filepath.Join(s.snapDir, snapshotName(meta))); err != nil {
				return newest, err
			}
		}
	}
	return newest, nil
}

// reconcile combines the replayed log with the newest snapshot into the
// state handed to Raft, and rejects combinations that cannot be explained by
// a crash at some point of normal operation.
func reconcile(st *walState, snap raft.SnapshotMeta) (out raft.InitialState, snapshotAhead, keepLog bool, err error) {
	if st.marker.Index > snap.Index {
		return out, false, false, corruptf("the log records a snapshot installed at index %d, but the newest snapshot file covers index %d", st.marker.Index, snap.Index)
	}
	switch {
	case !st.started:
		st.base, st.baseTerm, st.baseKnown, st.started = snap.Index, snap.Term, true, true
	case snap.Index < st.base:
		return out, false, false, corruptf("the log starts at index %d but the newest snapshot covers only index %d", st.base+1, snap.Index)
	case snap.Index == st.base:
		if st.baseKnown && st.baseTerm != snap.Term {
			return out, false, false, corruptf("snapshot term %d at index %d disagrees with the log (term %d)", snap.Term, snap.Index, st.baseTerm)
		}
		st.baseTerm, st.baseKnown = snap.Term, true
	default:
		// The snapshot is ahead of the log start: either a local snapshot
		// whose covered entries were not yet deleted, or a snapshot from the
		// leader that was published just before the node stopped.
		// Apply the rule Raft applies: keep the log after the snapshot point
		// only if the log contains the snapshot's last entry.
		t, ok := st.termAt(snap.Index)
		keepLog = ok && t == snap.Term
		if err := st.installSnapshot(snap, keepLog); err != nil {
			return out, false, false, err
		}
		snapshotAhead = true
	}
	commit := max(st.commit, snap.Index)
	if commit > st.lastIndex() {
		return out, false, false, corruptf("stored commit index %d is beyond the last log index %d", commit, st.lastIndex())
	}
	out.HardState = st.hs
	out.Commit = commit
	out.Snapshot = snap
	out.Entries = st.entries
	return out, snapshotAhead, keepLog, nil
}

// InitialState returns what was recovered when the store was opened.
func (s *Store) InitialState() raft.InitialState { return s.initial }

// Save persists the parts of a Ready that belong in the log.
func (s *Store) Save(hs *raft.HardState, entries []raft.Entry, commit uint64, mustSync bool) error {
	return s.wal.save(hs, entries, commit, mustSync)
}

// Compact deletes log segments made obsolete by a snapshot covering index.
func (s *Store) Compact(index uint64) error { return s.wal.compact(index) }

// Snapshot returns the position of the newest durable snapshot.
func (s *Store) Snapshot() raft.SnapshotMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap
}

// CreateSnapshot durably publishes a snapshot of the local state machine.
//
// The order is: write a temporary file, fsync it, rename it into place, fsync
// the directory. Only after that may log entries the snapshot covers be
// deleted. A crash before the rename leaves a temporary file that the next
// start removes; a crash after it leaves a complete, verified snapshot.
//
// It is safe to call from a goroutine other than the one calling Save.
func (s *Store) CreateSnapshot(meta raft.SnapshotMeta, payload []byte) error {
	tmp := s.tmpName("local")
	h := SnapshotHeader{Meta: meta, ClusterID: s.opts.ClusterID, PayloadLen: int64(len(payload))}
	if err := writeSnapshotFile(s.fs, tmp, h, payload); err != nil {
		s.fs.Remove(tmp)
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if meta.Index <= s.snap.Index {
		// A newer snapshot arrived from the leader in the meantime.
		return s.fs.Remove(tmp)
	}
	return s.publish(tmp, meta)
}

// publish renames a complete snapshot file into place. The caller holds mu.
func (s *Store) publish(tmp string, meta raft.SnapshotMeta) error {
	if s.closed {
		s.fs.Remove(tmp)
		return errors.New("storage: store is closed")
	}
	if err := s.fs.Rename(tmp, filepath.Join(s.snapDir, snapshotName(meta))); err != nil {
		return err
	}
	if err := s.fs.SyncDir(s.snapDir); err != nil {
		return err
	}
	old := s.snap
	s.snap = meta
	if old.Index > 0 {
		// Best effort: a leftover old snapshot is removed on the next start.
		// A sender still streaming it keeps its open file.
		s.fs.Remove(filepath.Join(s.snapDir, snapshotName(old)))
	}
	return nil
}

func (s *Store) tmpName(kind string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tmpCount++
	return filepath.Join(s.snapDir, fmt.Sprintf("%s-%d.tmp", kind, s.tmpCount))
}

// OpenSnapshotPayload opens the newest snapshot for restoring the state
// machine. The reader yields exactly the payload.
func (s *Store) OpenSnapshotPayload() (SnapshotHeader, io.ReadCloser, error) {
	f, h, err := s.openNewest()
	if err != nil {
		return h, nil, err
	}
	return h, &payloadReader{Reader: io.LimitReader(f.r, h.PayloadLen), f: f.f}, nil
}

// OpenSnapshotFile opens the newest snapshot for sending to a peer. The
// reader yields the whole file, header and checksums included, so the
// receiver can verify it independently. size is the number of bytes.
func (s *Store) OpenSnapshotFile() (h SnapshotHeader, size int64, rc io.ReadCloser, err error) {
	// Read the header to learn the size, then rewind to stream from the start.
	hf, h, err := s.openNewest()
	if err != nil {
		return h, 0, nil, err
	}
	if _, err := hf.f.Seek(0, io.SeekStart); err != nil {
		hf.f.Close()
		return h, 0, nil, err
	}
	return h, h.fileSize(), hf.f, nil
}

type openSnap struct {
	f File
	r *bufio.Reader
}

func (s *Store) openNewest() (openSnap, SnapshotHeader, error) {
	s.mu.Lock()
	meta := s.snap
	s.mu.Unlock()
	if meta.Index == 0 {
		return openSnap{}, SnapshotHeader{}, os.ErrNotExist
	}
	f, err := s.fs.OpenFile(filepath.Join(s.snapDir, snapshotName(meta)), os.O_RDONLY, 0)
	if err != nil {
		return openSnap{}, SnapshotHeader{}, err
	}
	r := bufio.NewReaderSize(f, 256<<10)
	h, err := readSnapshotHeader(r)
	if err != nil {
		f.Close()
		return openSnap{}, h, err
	}
	return openSnap{f: f, r: r}, h, nil
}

// IncomingSnapshot is a snapshot being received from the leader. It is
// written to a temporary file and becomes part of the node's state only
// through InstallSnapshot.
type IncomingSnapshot struct {
	s       *Store
	path    string
	f       File
	written int64
	limit   int64
	header  SnapshotHeader
	done    bool
}

// NewIncomingSnapshot starts receiving a snapshot.
func (s *Store) NewIncomingSnapshot() (*IncomingSnapshot, error) {
	path := s.tmpName("incoming")
	f, err := s.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	limit := int64(1 << 62)
	if s.opts.MaxSnapshotBytes > 0 {
		// Room for the header and trailer on top of the payload limit.
		limit = s.opts.MaxSnapshotBytes + snapFixedHeader + maxClusterIDLength + 8
	}
	return &IncomingSnapshot{s: s, path: path, f: f, limit: limit}, nil
}

// Write appends received bytes. It fails once the size limit is exceeded, so
// a misbehaving sender cannot fill the disk.
func (in *IncomingSnapshot) Write(p []byte) (int, error) {
	if in.written+int64(len(p)) > in.limit {
		return 0, fmt.Errorf("storage: incoming snapshot exceeds the limit of %d bytes", in.s.opts.MaxSnapshotBytes)
	}
	n, err := in.f.Write(p)
	in.written += int64(n)
	return n, err
}

// Finish syncs the received file and verifies it end to end.
func (in *IncomingSnapshot) Finish() (raft.SnapshotMeta, error) {
	if err := in.f.Sync(); err != nil {
		in.f.Close()
		return raft.SnapshotMeta{}, err
	}
	if err := in.f.Close(); err != nil {
		return raft.SnapshotMeta{}, err
	}
	in.f = nil
	h, err := verifySnapshotFile(in.s.fs, in.path, in.s.opts.ClusterID, in.s.opts.MaxSnapshotBytes)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	in.header, in.done = h, true
	return h.Meta, nil
}

// Discard removes the temporary file. It is safe to call after
// InstallSnapshot, when it does nothing.
func (in *IncomingSnapshot) Discard() {
	if in.f != nil {
		in.f.Close()
		in.f = nil
	}
	if in.path != "" {
		in.s.fs.Remove(in.path)
		in.path = ""
	}
}

// InstallSnapshot makes a received snapshot the node's newest snapshot and
// records in the log that the log now continues from it.
//
// The snapshot file is published first and the log marker written second. If
// the node stops in between, recovery finds a snapshot ahead of the log and
// applies the same rule the marker would have.
//
// keepLog is Ready.SnapshotKeepsLog: whether the stored log contains the
// snapshot's last entry and therefore keeps the entries after it.
func (s *Store) InstallSnapshot(in *IncomingSnapshot, keepLog bool) error {
	if !in.done || in.path == "" {
		return errors.New("storage: snapshot was not received completely")
	}
	s.mu.Lock()
	err := s.publish(in.path, in.header.Meta)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	in.path = ""
	return s.wal.saveSnapshotMarker(in.header.Meta, keepLog)
}

// Close syncs and releases the store.
func (s *Store) Close() error {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.mu.Unlock()
	if already {
		return nil
	}
	err := s.wal.close()
	if cerr := s.lock.Close(); err == nil {
		err = cerr
	}
	return err
}

// atomicWrite replaces dir/name with data: write a temporary file, fsync it,
// rename it over the target, fsync the directory.
func atomicWrite(fs FS, dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := fs.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return fs.SyncDir(dir)
}
