package raft

import "fmt"

// raftLog is the in-memory log. Entries up to and including offset have been
// compacted away; entries[i] holds index offset+1+i.
//
// Index 0 is a sentinel with term 0 that every log contains, so "the entry
// before the first entry" always exists.
type raftLog struct {
	entries    []Entry
	offset     uint64
	offsetTerm uint64
	// stable is the highest index known to be on stable storage.
	stable uint64
}

func newLog(snap SnapshotMeta, entries []Entry) (*raftLog, error) {
	l := &raftLog{offset: snap.Index, offsetTerm: snap.Term}
	for i, e := range entries {
		if want := snap.Index + 1 + uint64(i); e.Index != want {
			return nil, fmt.Errorf("raft: recovered log is not contiguous: entry %d has index %d, want %d", i, e.Index, want)
		}
	}
	l.entries = append(l.entries, entries...)
	l.stable = l.lastIndex()
	return l, nil
}

func (l *raftLog) firstIndex() uint64 { return l.offset + 1 }

func (l *raftLog) lastIndex() uint64 { return l.offset + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.offsetTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index i. ok is false when i has been
// compacted away or is past the end of the log.
func (l *raftLog) term(i uint64) (term uint64, ok bool) {
	switch {
	case i == l.offset:
		return l.offsetTerm, true
	case i < l.offset || i > l.lastIndex():
		return 0, false
	}
	return l.entries[i-l.offset-1].Term, true
}

// matches reports whether the log holds an entry with the given index and term.
func (l *raftLog) matches(index, term uint64) bool {
	t, ok := l.term(index)
	return ok && t == term
}

// isUpToDate implements the election restriction from section 5.4.1 of the
// Raft paper: compare the term of the last entries first, and only when they
// are equal compare log length.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != l.lastTerm() {
		return lastTerm > l.lastTerm()
	}
	return lastIndex >= l.lastIndex()
}

// slice returns entries in [lo, hi], bounded by maxEntries and maxBytes. At
// least one entry is returned when the range is non-empty, so a single
// oversized entry can still be replicated.
func (l *raftLog) slice(lo, hi uint64, maxEntries, maxBytes int) []Entry {
	if hi > l.lastIndex() {
		hi = l.lastIndex()
	}
	if lo > hi || lo <= l.offset {
		return nil
	}
	start := lo - l.offset - 1
	end := start
	bytes := 0
	for end < hi-l.offset && int(end-start) < maxEntries {
		bytes += l.entries[end].size()
		if bytes > maxBytes && end > start {
			break
		}
		end++
	}
	// The three-index slice caps capacity so that an append by the caller can
	// never write into the log's backing array.
	return l.entries[start:end:end]
}

func (l *raftLog) append(es ...Entry) {
	l.entries = append(l.entries, es...)
}

// truncateFrom discards every entry with index >= i.
//
// The surviving entries are copied into a fresh slice. Slices previously
// handed out through Ready may still be read by a sender goroutine, and
// reusing the backing array would let a later append overwrite what that
// goroutine is reading.
func (l *raftLog) truncateFrom(i uint64) {
	if i > l.lastIndex() {
		return
	}
	if i <= l.offset {
		panic(fmt.Sprintf("raft: truncating at %d, at or below compaction point %d", i, l.offset))
	}
	keep := i - l.offset - 1
	l.entries = append([]Entry(nil), l.entries[:keep]...)
	if l.stable >= i {
		l.stable = i - 1
	}
}

// compactTo drops entries up to and including i. The caller guarantees i has
// been applied and is covered by a durable snapshot.
func (l *raftLog) compactTo(i uint64) {
	if i <= l.offset {
		return
	}
	t, ok := l.term(i)
	if !ok {
		panic(fmt.Sprintf("raft: compacting to %d, beyond last index %d", i, l.lastIndex()))
	}
	l.entries = append([]Entry(nil), l.entries[i-l.offset:]...)
	l.offset, l.offsetTerm = i, t
	// Entries at or below a snapshot point never need to be written to the
	// log again; the snapshot stands in for them. Without this, a snapshot
	// landing in the middle of not-yet-persisted entries would leave stable
	// pointing below the log start and the remaining suffix would be skipped.
	if l.stable < i {
		l.stable = i
	}
}

// restore replaces the whole log with the position of a snapshot.
func (l *raftLog) restore(snap SnapshotMeta) {
	l.entries = nil
	l.offset, l.offsetTerm = snap.Index, snap.Term
	l.stable = snap.Index
}
