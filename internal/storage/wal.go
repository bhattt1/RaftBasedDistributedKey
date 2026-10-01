package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// The write-ahead log is a sequence of segment files, wal-<seq>.log. Each
// starts with an 8-byte magic string and continues with records:
//
//	offset  size  field
//	0       1     record type
//	1       4     payload length (little endian)
//	5       4     CRC-32C of the payload
//	9       4     CRC-32C of bytes 0..8 of this header
//	13      n     payload
//
// The header has its own checksum so that a damaged length field is
// recognised as damage. Without it, a flipped bit in a length could make the
// rest of the file look like one record that runs past the end of the file,
// which is exactly what an interrupted write looks like, and recovery would
// then quietly drop everything after it.
//
// The log is append-only. A follower replacing a conflicting suffix does not
// truncate the file; it appends the new entries, and an entry record whose
// index is not past the end of the log replaces everything from that index
// on when the log is replayed. That makes suffix replacement a single
// appended write rather than a truncate followed by a write, so there is no
// intermediate state to crash in.
const (
	walMagic         = "RKVWAL\x00\x01"
	recordHeaderSize = 13
	// maxRecordPayload bounds a single record. Commands are limited to far
	// less than this, so a larger length can only come from corruption.
	maxRecordPayload = 16 << 20
	// maxSegmentFile bounds how much of one segment recovery reads into
	// memory.
	maxSegmentFile = 1 << 30
)

const (
	recHardState byte = 1
	recEntry     byte = 2
	recCommit    byte = 3
	recSnapshot  byte = 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors reported by the storage layer.
var (
	// ErrCorrupt means stored data failed validation in a way that cannot be
	// explained by an interrupted write. The node refuses to start rather
	// than guess.
	ErrCorrupt = errors.New("storage: corrupt data")
	// ErrLocked means another process holds the data directory.
	ErrLocked = errors.New("storage: data directory is locked by another process")
	// ErrFailed means an earlier write or sync failed. The store accepts no
	// further writes: after a failed fsync the kernel may have dropped the
	// dirty pages, so retrying could report success for data that is gone.
	ErrFailed = errors.New("storage: store has failed and accepts no more writes")
)

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

type segment struct {
	seq uint64
	// maxIndex is the highest entry index recorded in the segment. A
	// segment can be deleted once a snapshot covers that index.
	maxIndex uint64
}

func segmentName(seq uint64) string { return fmt.Sprintf("wal-%016x.log", seq) }

// walState is the result of replaying the log.
type walState struct {
	hs     raft.HardState
	commit uint64
	// started is false when the log holds no entry and no snapshot marker.
	started bool
	// base is the index just before entries[0]. baseKnown says whether its
	// term is known; it is not when older segments were deleted.
	base      uint64
	baseTerm  uint64
	baseKnown bool
	entries   []raft.Entry
	// marker is the last installed-snapshot marker in the log, if any.
	marker raft.SnapshotMeta
}

func (st *walState) lastIndex() uint64 { return st.base + uint64(len(st.entries)) }

func (st *walState) termAt(index uint64) (uint64, bool) {
	if index <= st.base || index > st.lastIndex() {
		return 0, false
	}
	return st.entries[index-st.base-1].Term, true
}

// installSnapshot replays a snapshot marker. keepLog records the decision
// Raft made when the snapshot was installed: either the stored log contained
// the snapshot's last entry and the entries after it were kept, or the whole
// stored log was dropped.
//
// The decision is stored rather than recomputed because, once older segments
// have been deleted, replay may no longer see the entry that justified it.
func (st *walState) installSnapshot(meta raft.SnapshotMeta, keepLog bool) error {
	switch {
	case !keepLog:
		st.entries = nil
		st.base, st.baseTerm, st.baseKnown, st.started = meta.Index, meta.Term, true, true
	case !st.started || meta.Index < st.base:
		// The entries that were kept lived in segments that have since been
		// deleted, or the log visible to this replay already starts after
		// the snapshot point. Either way there is nothing to adjust: the
		// first entry record that follows says where the log continues.
	case meta.Index == st.base:
		if st.baseKnown && st.baseTerm != meta.Term {
			return corruptf("snapshot marker (%d, term %d) disagrees with the log start term %d", meta.Index, meta.Term, st.baseTerm)
		}
		st.baseTerm, st.baseKnown = meta.Term, true
	default:
		if t, ok := st.termAt(meta.Index); !ok || t != meta.Term {
			return corruptf("snapshot marker (%d, term %d) claims the log contains that entry, but it does not", meta.Index, meta.Term)
		}
		st.entries = append([]raft.Entry(nil), st.entries[meta.Index-st.base:]...)
		st.base, st.baseTerm, st.baseKnown = meta.Index, meta.Term, true
	}
	return nil
}

func (st *walState) applyRecord(typ byte, p []byte) error {
	switch typ {
	case recHardState:
		if len(p) < 8 {
			return corruptf("hard state record of %d bytes", len(p))
		}
		st.hs = raft.HardState{Term: binary.LittleEndian.Uint64(p), Vote: string(p[8:])}
	case recCommit:
		if len(p) != 8 {
			return corruptf("commit record of %d bytes", len(p))
		}
		st.commit = binary.LittleEndian.Uint64(p)
	case recSnapshot:
		if len(p) != 17 || p[16] > 1 {
			return corruptf("malformed snapshot marker of %d bytes", len(p))
		}
		st.marker = raft.SnapshotMeta{Index: binary.LittleEndian.Uint64(p), Term: binary.LittleEndian.Uint64(p[8:])}
		if err := st.installSnapshot(st.marker, p[16] == 1); err != nil {
			return err
		}
	case recEntry:
		if len(p) < 17 {
			return corruptf("entry record of %d bytes", len(p))
		}
		e := raft.Entry{
			Index: binary.LittleEndian.Uint64(p),
			Term:  binary.LittleEndian.Uint64(p[8:]),
			Type:  raft.EntryType(p[16]),
		}
		if len(p) > 17 {
			e.Data = append([]byte(nil), p[17:]...)
		}
		switch {
		case e.Index == 0:
			return corruptf("entry with index 0")
		case !st.started:
			// The first record of a log whose older segments were deleted.
			// Its predecessor is covered by a snapshot; Open checks that.
			st.base, st.baseKnown, st.started = e.Index-1, e.Index == 1, true
		case e.Index <= st.base && st.baseKnown:
			return corruptf("entry %d at or below the log start %d", e.Index, st.base)
		case e.Index <= st.base:
			// The visible log began in the middle (older segments are gone)
			// and this record replaces a suffix that starts before that
			// point. Everything seen so far is superseded.
			st.entries = nil
			st.base, st.baseKnown = e.Index-1, e.Index == 1
		case e.Index > st.lastIndex()+1:
			return corruptf("entry %d leaves a gap after %d", e.Index, st.lastIndex())
		}
		// An index inside the log replaces the suffix from there on.
		st.entries = append(st.entries[:e.Index-st.base-1], e)
	default:
		return corruptf("unknown record type %d", typ)
	}
	return nil
}

// wal is the append-only log. It is used by a single goroutine.
type wal struct {
	fs          FS
	dir         string
	segmentSize int64
	syncWrites  bool
	onSync      func(time.Duration)

	segments []segment // oldest first; the last one is being written
	f        File
	size     int64
	buf      []byte

	// The values most recently written, repeated at the head of each new
	// segment so that older segments can be deleted.
	hs     raft.HardState
	commit uint64

	err error // sticky: the first write or sync failure
}

type walOptions struct {
	segmentSize int64
	noSync      bool
	onSync      func(time.Duration)
}

// openWAL replays every segment in dir and opens the log for appending.
func openWAL(fs FS, dir string, opts walOptions) (*wal, *walState, error) {
	if opts.segmentSize <= 0 {
		opts.segmentSize = 16 << 20
	}
	if err := fs.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	names, err := fs.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	w := &wal{fs: fs, dir: dir, segmentSize: opts.segmentSize, syncWrites: !opts.noSync, onSync: opts.onSync}
	for _, name := range names {
		var seq uint64
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		if _, err := fmt.Sscanf(name, "wal-%016x.log", &seq); err != nil || segmentName(seq) != name {
			return nil, nil, corruptf("unexpected file %q in the WAL directory", name)
		}
		if n := len(w.segments); n > 0 && w.segments[n-1].seq+1 != seq {
			return nil, nil, corruptf("WAL segment %d is missing (found %d after %d)", w.segments[n-1].seq+1, seq, w.segments[n-1].seq)
		}
		w.segments = append(w.segments, segment{seq: seq})
	}

	st := &walState{}
	for i := range w.segments {
		last := i == len(w.segments)-1
		validLen, err := w.replaySegment(&w.segments[i], st, last)
		if err != nil {
			return nil, nil, fmt.Errorf("replaying %s: %w", segmentName(w.segments[i].seq), err)
		}
		if last {
			if err := w.openActive(validLen, st); err != nil {
				return nil, nil, err
			}
		}
	}
	w.hs, w.commit = st.hs, st.commit
	if len(w.segments) == 0 {
		if err := w.createSegment(1); err != nil {
			return nil, nil, err
		}
	}
	return w, st, nil
}

// replaySegment applies every valid record of one segment to st and returns
// the length of the valid prefix.
//
// Only the final segment may end in an incomplete record, and only in one of
// two provable shapes: the record is cut short by the end of the file, or
// everything from the record's start to the end of the file is zero bytes.
// Both are what an interrupted append leaves behind, and such a record was
// never acknowledged because acknowledgement waits for fsync. Anything else
// that fails a checksum is treated as corruption.
func (w *wal) replaySegment(seg *segment, st *walState, last bool) (int64, error) {
	data, err := readFile(w.fs, filepath.Join(w.dir, segmentName(seg.seq)), maxSegmentFile)
	if err != nil {
		return 0, err
	}
	if last && (allZero(data) || isPrefix(data, walMagic)) {
		return 0, nil // segment creation was interrupted; it is finished below
	}
	if len(data) < len(walMagic) {
		return 0, corruptf("segment header is %d bytes", len(data))
	}
	if string(data[:len(walMagic)]) != walMagic {
		return 0, corruptf("bad segment magic")
	}
	off := len(walMagic)
	for off < len(data) {
		rest := data[off:]
		torn := func(why string) (int64, error) {
			if last {
				return int64(off), nil
			}
			return 0, corruptf("%s at offset %d of a segment that is not the last", why, off)
		}
		if len(rest) < recordHeaderSize {
			return torn("incomplete record header")
		}
		h := rest[:recordHeaderSize]
		if crc32.Checksum(h[:9], castagnoli) != binary.LittleEndian.Uint32(h[9:13]) {
			if allZero(rest) {
				return torn("zero-filled tail")
			}
			return 0, corruptf("record header checksum mismatch at offset %d", off)
		}
		n := int(binary.LittleEndian.Uint32(h[1:5]))
		if n > maxRecordPayload {
			return 0, corruptf("record of %d bytes at offset %d exceeds the limit", n, off)
		}
		if len(rest) < recordHeaderSize+n {
			return torn("incomplete record payload")
		}
		payload := rest[recordHeaderSize : recordHeaderSize+n]
		if crc32.Checksum(payload, castagnoli) != binary.LittleEndian.Uint32(h[5:9]) {
			return 0, corruptf("record payload checksum mismatch at offset %d", off)
		}
		if err := st.applyRecord(h[0], payload); err != nil {
			return 0, fmt.Errorf("offset %d: %w", off, err)
		}
		if h[0] == recEntry {
			if idx := binary.LittleEndian.Uint64(payload); idx > seg.maxIndex {
				seg.maxIndex = idx
			}
		}
		off += recordHeaderSize + n
	}
	return int64(off), nil
}

// openActive opens the final segment for appending, first cutting off an
// incomplete tail so that new records are not written after garbage.
func (w *wal) openActive(validLen int64, st *walState) error {
	seg := w.segments[len(w.segments)-1]
	name := filepath.Join(w.dir, segmentName(seg.seq))
	f, err := w.fs.OpenFile(name, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	if size != validLen {
		if err := f.Truncate(validLen); err != nil {
			f.Close()
			return err
		}
		if _, err := f.Seek(validLen, io.SeekStart); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	w.f, w.size = f, validLen
	w.hs, w.commit = st.hs, st.commit
	if validLen == 0 {
		// The segment was being created when the node stopped. Finish it.
		return w.writeSegmentHead()
	}
	// Restate the hard state and commit index in the active segment. Older
	// segments may be deleted once a snapshot covers their entries, and that
	// is only safe if the newest segment always holds the current term and
	// vote. A segment whose creation was interrupted after the magic string
	// but before these records would otherwise break that rule.
	w.buf = appendHardState(w.buf[:0], w.hs)
	w.buf = appendCommit(w.buf, w.commit)
	if err := w.write(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *wal) createSegment(seq uint64) error {
	name := filepath.Join(w.dir, segmentName(seq))
	f, err := w.fs.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	w.f, w.size = f, 0
	w.segments = append(w.segments, segment{seq: seq})
	if err := w.writeSegmentHead(); err != nil {
		return err
	}
	// The new file's directory entry must be durable before anything that
	// lands in the file is acknowledged.
	return w.fs.SyncDir(w.dir)
}

// writeSegmentHead writes the magic string followed by the current hard
// state and commit index, and syncs them.
func (w *wal) writeSegmentHead() error {
	w.buf = append(w.buf[:0], walMagic...)
	w.buf = appendHardState(w.buf, w.hs)
	w.buf = appendCommit(w.buf, w.commit)
	if err := w.write(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *wal) write() error {
	n, err := w.f.Write(w.buf)
	w.size += int64(n)
	if err == nil && n != len(w.buf) {
		err = io.ErrShortWrite
	}
	return err
}

// fail records the first error and makes every later call return ErrFailed.
func (w *wal) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return fmt.Errorf("%w: %v", ErrFailed, w.err)
}

// save appends the given state as one write. When mustSync is set the data
// is on stable storage before save returns.
//
// Record order inside the write matters for recovery from a torn write: the
// hard state first, then entries, then the commit index. A commit record is
// therefore always preceded in the file by every entry it covers.
func (w *wal) save(hs *raft.HardState, entries []raft.Entry, commit uint64, mustSync bool) error {
	if w.err != nil {
		return w.fail(w.err)
	}
	w.buf = w.buf[:0]
	if hs != nil {
		w.buf = appendHardState(w.buf, *hs)
		w.hs = *hs
	}
	seg := &w.segments[len(w.segments)-1]
	for _, e := range entries {
		w.buf = appendEntry(w.buf, e)
		if e.Index > seg.maxIndex {
			seg.maxIndex = e.Index
		}
	}
	if commit != w.commit {
		w.buf = appendCommit(w.buf, commit)
		w.commit = commit
	}
	if len(w.buf) == 0 {
		return nil
	}
	if err := w.write(); err != nil {
		return w.fail(err)
	}
	if mustSync {
		if err := w.sync(); err != nil {
			return w.fail(err)
		}
	}
	if w.size >= w.segmentSize {
		if err := w.rotate(); err != nil {
			return w.fail(err)
		}
	}
	return nil
}

// saveSnapshotMarker durably records that the state machine was replaced by
// a snapshot at meta, which resets the log to continue from that point.
func (w *wal) saveSnapshotMarker(meta raft.SnapshotMeta, keepLog bool) error {
	if w.err != nil {
		return w.fail(w.err)
	}
	var p [17]byte
	binary.LittleEndian.PutUint64(p[:], meta.Index)
	binary.LittleEndian.PutUint64(p[8:], meta.Term)
	if keepLog {
		p[16] = 1
	}
	w.buf = appendRecord(w.buf[:0], recSnapshot, p[:])
	if err := w.write(); err != nil {
		return w.fail(err)
	}
	if err := w.sync(); err != nil {
		return w.fail(err)
	}
	return nil
}

func (w *wal) sync() error {
	if !w.syncWrites {
		return nil
	}
	start := time.Now()
	err := w.f.Sync()
	if w.onSync != nil {
		w.onSync(time.Since(start))
	}
	return err
}

func (w *wal) rotate() error {
	if err := w.sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	return w.createSegment(w.segments[len(w.segments)-1].seq + 1)
}

// compact deletes whole segments that hold only entries at or below index.
// The caller guarantees a durable snapshot covers index. Segments are removed
// oldest first, so an interruption leaves a contiguous run of files.
func (w *wal) compact(index uint64) error {
	if w.err != nil {
		return w.fail(w.err)
	}
	removed := false
	for len(w.segments) > 1 && w.segments[0].maxIndex <= index {
		if err := w.fs.Remove(filepath.Join(w.dir, segmentName(w.segments[0].seq))); err != nil {
			return w.fail(err)
		}
		w.segments = w.segments[1:]
		removed = true
	}
	if removed {
		if err := w.fs.SyncDir(w.dir); err != nil {
			return w.fail(err)
		}
	}
	return nil
}

func (w *wal) close() error {
	if w.f == nil {
		return nil
	}
	var err error
	if w.err == nil && w.syncWrites {
		err = w.f.Sync()
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

// ---- record encoding -------------------------------------------------------

func appendRecord(buf []byte, typ byte, payload []byte) []byte {
	var h [recordHeaderSize]byte
	h[0] = typ
	binary.LittleEndian.PutUint32(h[1:5], uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[5:9], crc32.Checksum(payload, castagnoli))
	binary.LittleEndian.PutUint32(h[9:13], crc32.Checksum(h[:9], castagnoli))
	buf = append(buf, h[:]...)
	return append(buf, payload...)
}

func appendHardState(buf []byte, hs raft.HardState) []byte {
	p := make([]byte, 8, 8+len(hs.Vote))
	binary.LittleEndian.PutUint64(p, hs.Term)
	p = append(p, hs.Vote...)
	return appendRecord(buf, recHardState, p)
}

func appendCommit(buf []byte, commit uint64) []byte {
	var p [8]byte
	binary.LittleEndian.PutUint64(p[:], commit)
	return appendRecord(buf, recCommit, p[:])
}

func appendEntry(buf []byte, e raft.Entry) []byte {
	p := make([]byte, 17, 17+len(e.Data))
	binary.LittleEndian.PutUint64(p, e.Index)
	binary.LittleEndian.PutUint64(p[8:], e.Term)
	p[16] = byte(e.Type)
	p = append(p, e.Data...)
	return appendRecord(buf, recEntry, p)
}

// ---- small helpers ---------------------------------------------------------

func readFile(fs FS, name string, limit int64) ([]byte, error) {
	f, err := fs.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, corruptf("%s is larger than %d bytes", filepath.Base(name), limit)
	}
	return data, nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func isPrefix(data []byte, of string) bool {
	return len(data) <= len(of) && string(data) == of[:len(data)]
}
