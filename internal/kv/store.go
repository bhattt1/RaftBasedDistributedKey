package kv

import (
	"bufio"
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// Status is the outcome of applying a command.
type Status uint8

const (
	StatusOK Status = iota
	// StatusNotFound: a read of a key that does not exist.
	StatusNotFound
	// StatusCASFailed: the condition did not hold; nothing changed.
	StatusCASFailed
	// StatusIdentityReused: this client ID and sequence number were already
	// used for a different operation.
	StatusIdentityReused
	// StatusStaleSequence: the client has already moved past this sequence
	// number, and the result of the old request is no longer kept.
	StatusStaleSequence
	// StatusCapacityExceeded: storing the value would exceed MaxStateBytes;
	// nothing changed.
	StatusCapacityExceeded
	// StatusInvalid: the command in the log failed validation; nothing
	// changed. Servers validate before proposing, so this indicates a
	// client talking to the log through some other path.
	StatusInvalid
)

func (s Status) String() string {
	names := [...]string{"OK", "NOT_FOUND", "CAS_FAILED", "IDENTITY_REUSED", "STALE_SEQUENCE", "CAPACITY_EXCEEDED", "INVALID"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("status(%d)", uint8(s))
}

// Result is what applying a command produced.
type Result struct {
	Status Status
	// Revision is the key's revision after a successful PUT or CAS, the
	// revision read by a READ, or the key's current revision when a CAS
	// fails. Zero when the key does not exist.
	//
	// A key's revision is the log index of the command that last wrote it.
	// It strictly increases over the life of the cluster, also across
	// deleting and recreating a key, so a revision observed once can never
	// match a later incarnation of the same key.
	Revision uint64
	// Value is set by READ. It must be treated as read-only.
	Value []byte
	// Existed: for DELETE, whether there was a key to delete; for a failed
	// CAS, whether the key currently exists.
	Existed bool
	// Duplicate is true when the command was recognised as a retry and the
	// stored result of its first execution was returned.
	Duplicate bool
}

type item struct {
	value    []byte
	revision uint64
}

// session is what the store remembers about one client: its latest mutation
// and what that mutation returned.
type session struct {
	id          string
	seq         uint64
	fingerprint [16]byte
	result      Result // never holds a Value; mutations do not return one
	elem        *list.Element
}

// Store is the state machine. It is not safe for concurrent use; the node's
// apply loop is its only caller.
type Store struct {
	limits   Limits
	data     map[string]item
	size     int64 // total bytes of keys and values
	sessions map[string]*session
	lru      *list.List // front: most recently used session
	applied  uint64
}

// New returns an empty store.
func New(limits Limits) *Store {
	return &Store{
		limits:   limits,
		data:     make(map[string]item),
		sessions: make(map[string]*session),
		lru:      list.New(),
	}
}

// AppliedIndex returns the log index of the last command applied.
func (s *Store) AppliedIndex() uint64 { return s.applied }

// Len returns the number of keys.
func (s *Store) Len() int { return len(s.data) }

// Size returns the total bytes of keys and values held.
func (s *Store) Size() int64 { return s.size }

// Sessions returns the number of client sessions remembered.
func (s *Store) Sessions() int { return len(s.sessions) }

// Skip records that a log entry with no command (a leader's no-op) was
// applied at index.
func (s *Store) Skip(index uint64) { s.applied = index }

// Apply executes the command stored in the log at index. This is the only
// way state changes.
func (s *Store) Apply(index uint64, data []byte) Result {
	s.applied = index
	cmd, err := Decode(data)
	if err == nil {
		err = cmd.Validate(s.limits)
	}
	if err != nil {
		return Result{Status: StatusInvalid}
	}
	if cmd.Op == OpRead {
		it, ok := s.data[cmd.Key]
		if !ok {
			return Result{Status: StatusNotFound}
		}
		return Result{Value: it.value, Revision: it.revision}
	}

	// De-duplication. The decision uses only replicated state, so every
	// node makes the same one, and a retry sent to a different leader after
	// a failover is recognised there too.
	fp := fingerprint(data)
	sess := s.sessions[cmd.ClientID]
	if sess != nil {
		switch {
		case cmd.Seq == sess.seq && fp == sess.fingerprint:
			s.lru.MoveToFront(sess.elem)
			res := sess.result
			res.Duplicate = true
			return res
		case cmd.Seq == sess.seq:
			return Result{Status: StatusIdentityReused}
		case cmd.Seq < sess.seq:
			return Result{Status: StatusStaleSequence}
		}
	}

	res := s.mutate(index, &cmd)

	// Remember the outcome, whatever it was. A failed CAS that is retried
	// must report the same failure even if the key has changed since.
	if sess == nil {
		sess = &session{id: cmd.ClientID}
		sess.elem = s.lru.PushFront(sess)
		s.sessions[cmd.ClientID] = sess
		if len(s.sessions) > s.limits.MaxSessions {
			oldest := s.lru.Remove(s.lru.Back()).(*session)
			delete(s.sessions, oldest.id)
		}
	} else {
		s.lru.MoveToFront(sess.elem)
	}
	sess.seq, sess.fingerprint, sess.result = cmd.Seq, fp, res
	return res
}

func (s *Store) mutate(index uint64, cmd *Command) Result {
	cur, exists := s.data[cmd.Key]
	switch cmd.Op {
	case OpPut:
		return s.set(index, cmd.Key, cmd.Value, cur, exists)
	case OpDelete:
		if !exists {
			return Result{}
		}
		delete(s.data, cmd.Key)
		s.size -= int64(len(cmd.Key) + len(cur.value))
		return Result{Existed: true}
	case OpCAS:
		// The comparison and the write happen inside one applied command,
		// with nothing able to run in between. That is the whole of what
		// makes the operation atomic.
		var ok bool
		switch cmd.Expect {
		case ExpectAbsent:
			ok = !exists
		case ExpectValue:
			ok = exists && bytes.Equal(cur.value, cmd.ExpectValue)
		case ExpectRevision:
			ok = exists && cur.revision == cmd.ExpectRevision
		}
		if !ok {
			return Result{Status: StatusCASFailed, Existed: exists, Revision: cur.revision}
		}
		return s.set(index, cmd.Key, cmd.Value, cur, exists)
	}
	return Result{Status: StatusInvalid}
}

func (s *Store) set(index uint64, key string, value []byte, cur item, exists bool) Result {
	size := s.size + int64(len(value))
	if exists {
		size -= int64(len(cur.value))
	} else {
		size += int64(len(key))
	}
	if size > s.limits.MaxStateBytes {
		return Result{Status: StatusCapacityExceeded}
	}
	s.data[key] = item{value: value, revision: index}
	s.size = size
	return Result{Revision: index}
}

// fingerprint identifies the exact command bytes. The encoding is canonical,
// so equal fingerprints mean the same operation with the same arguments.
func fingerprint(data []byte) [16]byte {
	sum := sha256.Sum256(data)
	var fp [16]byte
	copy(fp[:], sum[:16])
	return fp
}

// ---- snapshots -------------------------------------------------------------

const snapshotMagic = "KVS1"

// Snapshot serialises the whole state: the data and the session table. Keys
// are written in sorted order and sessions from least to most recently used,
// so two stores with equal state produce identical bytes.
func (s *Store) Snapshot() []byte {
	buf := make([]byte, 0, int(s.size)+len(s.data)*12+len(s.sessions)*48+32)
	buf = append(buf, snapshotMagic...)
	buf = binary.AppendUvarint(buf, s.applied)

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		it := s.data[k]
		buf = appendBytes(buf, []byte(k))
		buf = appendBytes(buf, it.value)
		buf = binary.AppendUvarint(buf, it.revision)
	}

	buf = binary.AppendUvarint(buf, uint64(len(s.sessions)))
	for e := s.lru.Back(); e != nil; e = e.Prev() {
		sess := e.Value.(*session)
		buf = appendBytes(buf, []byte(sess.id))
		buf = binary.AppendUvarint(buf, sess.seq)
		buf = append(buf, sess.fingerprint[:]...)
		buf = append(buf, byte(sess.result.Status), boolByte(sess.result.Existed))
		buf = binary.AppendUvarint(buf, sess.result.Revision)
	}
	return buf
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// Restore replaces the store's contents with a snapshot read from r. It reads
// incrementally and checks every length against the limits before
// allocating, so a damaged or hostile snapshot cannot exhaust memory. On
// error the store is left unchanged.
func (s *Store) Restore(r io.Reader) error {
	br := bufio.NewReaderSize(r, 64<<10)
	next := New(s.limits)

	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != snapshotMagic {
		return invalidf("not a kv snapshot")
	}
	applied, err := binary.ReadUvarint(br)
	if err != nil {
		return invalidf("snapshot applied index: %v", err)
	}
	next.applied = applied

	nKeys, err := binary.ReadUvarint(br)
	if err != nil {
		return invalidf("snapshot key count: %v", err)
	}
	prev := ""
	for i := uint64(0); i < nKeys; i++ {
		key, err := readBytes(br, s.limits.MaxKeyBytes)
		if err != nil {
			return invalidf("snapshot key %d: %v", i, err)
		}
		value, err := readBytes(br, s.limits.MaxValueBytes)
		if err != nil {
			return invalidf("snapshot value %d: %v", i, err)
		}
		rev, err := binary.ReadUvarint(br)
		if err != nil {
			return invalidf("snapshot revision %d: %v", i, err)
		}
		k := string(key)
		if k == "" || (i > 0 && k <= prev) {
			return invalidf("snapshot keys are not in strictly increasing order")
		}
		if rev == 0 || rev > applied {
			return invalidf("snapshot revision %d is outside (0, %d]", rev, applied)
		}
		prev = k
		next.size += int64(len(key) + len(value))
		if next.size > s.limits.MaxStateBytes {
			return invalidf("snapshot exceeds the state size limit of %d bytes", s.limits.MaxStateBytes)
		}
		next.data[k] = item{value: value, revision: rev}
	}

	nSessions, err := binary.ReadUvarint(br)
	if err != nil {
		return invalidf("snapshot session count: %v", err)
	}
	if nSessions > uint64(s.limits.MaxSessions) {
		return invalidf("snapshot has %d sessions, the limit is %d", nSessions, s.limits.MaxSessions)
	}
	for i := uint64(0); i < nSessions; i++ {
		id, err := readBytes(br, MaxClientIDBytes)
		if err != nil {
			return invalidf("snapshot session %d: %v", i, err)
		}
		sess := &session{id: string(id)}
		if sess.id == "" || next.sessions[sess.id] != nil {
			return invalidf("snapshot session %d has an empty or repeated client ID", i)
		}
		if sess.seq, err = binary.ReadUvarint(br); err != nil {
			return invalidf("snapshot session %d: %v", i, err)
		}
		var tail [18]byte
		if _, err := io.ReadFull(br, tail[:]); err != nil {
			return invalidf("snapshot session %d: %v", i, err)
		}
		copy(sess.fingerprint[:], tail[:16])
		if tail[16] > byte(StatusInvalid) || tail[17] > 1 {
			return invalidf("snapshot session %d has a malformed result", i)
		}
		sess.result.Status, sess.result.Existed = Status(tail[16]), tail[17] == 1
		if sess.result.Revision, err = binary.ReadUvarint(br); err != nil {
			return invalidf("snapshot session %d: %v", i, err)
		}
		// Written oldest first, so pushing to the front rebuilds the order.
		sess.elem = next.lru.PushFront(sess)
		next.sessions[sess.id] = sess
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return invalidf("snapshot has trailing data")
	}
	*s = *next
	return nil
}

func readBytes(br *bufio.Reader, max int) ([]byte, error) {
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	if n > uint64(max) {
		return nil, fmt.Errorf("length %d exceeds the limit of %d", n, max)
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(br, out); err != nil {
		return nil, err
	}
	return out, nil
}
