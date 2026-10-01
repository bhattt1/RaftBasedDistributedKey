// Package raft implements the Raft consensus algorithm as a deterministic state
// machine. It performs no I/O, starts no goroutines and never reads a clock:
// time arrives through Tick, network input through Step, client input through
// Propose, and every side effect (things to persist, messages to send, entries
// to apply) is handed back to the caller through Ready.
//
// That shape is what makes the simulation tests possible: the same code runs
// under a seeded fake network in tests and under gRPC and a real WAL in
// production.
package raft

import (
	"errors"
	"fmt"
)

// Role is the node's current role in the protocol.
type Role uint8

const (
	Follower Role = iota
	// PreCandidate is asking peers whether an election could succeed. It has
	// not incremented its term and has not voted for itself.
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("role(%d)", uint8(r))
}

// EntryType distinguishes entries the consensus layer creates for itself from
// entries carrying an application command.
type EntryType uint8

const (
	// EntryNoop is appended by a new leader. It carries no data.
	EntryNoop EntryType = iota
	// EntryCommand carries an opaque application command.
	EntryCommand
)

// Entry is one slot of the replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// size is the approximate number of bytes the entry occupies on the wire.
func (e Entry) size() int { return 24 + len(e.Data) }

// MessageType enumerates protocol messages. Requests and responses are both
// messages; the transport maps each request/response pair onto one RPC.
type MessageType uint8

const (
	MsgPreVote MessageType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgSnap
	MsgSnapResp
)

func (t MessageType) String() string {
	names := [...]string{"PreVote", "PreVoteResp", "Vote", "VoteResp", "App", "AppResp", "Snap", "SnapResp"}
	if int(t) < len(names) {
		return names[t]
	}
	return fmt.Sprintf("msg(%d)", uint8(t))
}

// IsResponse reports whether t answers a request.
func (t MessageType) IsResponse() bool {
	return t == MsgPreVoteResp || t == MsgVoteResp || t == MsgAppResp || t == MsgSnapResp
}

// Message is a protocol message. Fields are shared between message types to
// keep one flat struct; the comment on each field says who uses it.
type Message struct {
	Type MessageType
	From string
	To   string
	// Term is the sender's current term, with one exception: a PreVote
	// request and a granted PreVote response carry the term the sender would
	// campaign in (its current term + 1).
	Term uint64
	// Seq is an opaque correlation number. A response carries the Seq of the
	// request that caused it, which lets the transport pair them up.
	Seq uint64

	// LogIndex/LogTerm: for PreVote and Vote, the candidate's last log
	// position. For App, the position immediately before Entries. An AppResp
	// echoes the App's LogIndex so the leader can recognise stale rejections.
	LogIndex uint64
	LogTerm  uint64

	// App only.
	Entries []Entry
	Commit  uint64

	// Responses.
	Reject bool
	// MatchIndex (AppResp, SnapResp): highest index the follower knows to
	// match the leader's log.
	MatchIndex uint64
	// ConflictIndex/ConflictTerm (rejected AppResp): hint for where the
	// leader should retry from.
	ConflictIndex uint64
	ConflictTerm  uint64

	// Snap only: the position the snapshot covers.
	SnapIndex uint64
	SnapTerm  uint64
}

// HardState is the state that must be durable before the node acts on it.
type HardState struct {
	Term uint64
	// Vote is the node this node voted for in Term, or "" for none.
	Vote string
}

// SnapshotMeta identifies the log position a snapshot covers.
type SnapshotMeta struct {
	Index uint64
	Term  uint64
}

// Ready is everything the caller must do after feeding input to the node.
// The required order is:
//
//  1. If Snapshot is set, durably install that snapshot.
//  2. Durably store HardState (if HardStateChanged) and Entries.
//  3. Send Messages.
//  4. Apply CommittedEntries to the state machine.
//  5. Call Advance.
//
// Steps 1 and 2 must complete before step 3. A vote, an append
// acknowledgement or a leader's own copy of an entry only counts once it is on
// disk, and the messages in step 3 are what tell other nodes that it counts.
type Ready struct {
	HardState        HardState
	HardStateChanged bool
	// Entries are new or replaced log entries. If Entries[0].Index is not
	// past the end of the stored log, every stored entry from that index on
	// must be discarded before appending.
	Entries []Entry
	// Snapshot, when set, replaces the state machine and the log prefix.
	Snapshot *SnapshotMeta
	// SnapshotKeepsLog says what installing Snapshot means for the stored
	// log. True: the stored log already contains the snapshot's last entry,
	// so the stored entries after it stay. False: the stored log is
	// discarded entirely and continues from the snapshot.
	SnapshotKeepsLog bool
	// Commit is the current commit index. It may be persisted lazily.
	Commit           uint64
	CommittedEntries []Entry
	Messages         []Message
}

// MustSync reports whether the Ready contains anything that has to reach
// stable storage before messages are sent.
func (rd Ready) MustSync() bool {
	return rd.HardStateChanged || len(rd.Entries) > 0 || rd.Snapshot != nil
}

// InitialState is what a node recovered from stable storage.
type InitialState struct {
	HardState HardState
	// Commit is a previously persisted commit index. Zero is always safe: the
	// node then relearns the commit index from the leader.
	Commit uint64
	// Snapshot is the position covered by the most recent snapshot. The
	// state machine must already reflect it.
	Snapshot SnapshotMeta
	// Entries are the stored log entries after the snapshot, contiguous,
	// starting at Snapshot.Index+1.
	Entries []Entry
}

// Config configures a node.
type Config struct {
	// ID is this node's identity. It must be listed in Peers.
	ID string
	// Peers is the fixed voter set, including this node.
	Peers []string
	// ElectionTicks is the base election timeout in ticks. The actual
	// timeout is randomised per election in [ElectionTicks, 2*ElectionTicks).
	ElectionTicks int
	// HeartbeatTicks is how often a leader sends heartbeats.
	HeartbeatTicks int
	// MaxEntriesPerAppend and MaxBytesPerAppend bound one AppendEntries batch.
	MaxEntriesPerAppend int
	MaxBytesPerAppend   int
	// MaxUncommittedEntries bounds how far a leader's log may run ahead of
	// its commit index before Propose returns ErrOverloaded.
	MaxUncommittedEntries int
	// MaxApplyBatch bounds CommittedEntries in one Ready.
	MaxApplyBatch int
	// SnapshotRetryTicks is how long a leader waits for the outcome of a
	// snapshot transfer before starting another one.
	SnapshotRetryTicks int
	// PreVote makes a node ask before disrupting the cluster with a new term.
	PreVote bool
	// CheckQuorum makes a leader step down when it has not heard from a
	// quorum within an election timeout.
	CheckQuorum bool
	// Rand returns a non-negative pseudo-random number in [0, n). It is the
	// only source of randomness, so a seeded generator makes runs repeatable.
	Rand func(n int) int
	// Trace, if set, receives a line for every role change. Tests use it to
	// print the history of a failing simulation.
	Trace func(format string, args ...any)
}

func (c *Config) validate() error {
	if c.ID == "" {
		return errors.New("raft: empty node ID")
	}
	seen := make(map[string]bool, len(c.Peers))
	for _, p := range c.Peers {
		if p == "" {
			return errors.New("raft: empty peer ID")
		}
		if seen[p] {
			return fmt.Errorf("raft: duplicate peer %q", p)
		}
		seen[p] = true
	}
	if !seen[c.ID] {
		return fmt.Errorf("raft: node %q is not in the peer list", c.ID)
	}
	if c.HeartbeatTicks <= 0 {
		return errors.New("raft: HeartbeatTicks must be positive")
	}
	if c.ElectionTicks <= c.HeartbeatTicks {
		return errors.New("raft: ElectionTicks must be greater than HeartbeatTicks")
	}
	if c.Rand == nil {
		return errors.New("raft: Rand is required")
	}
	if c.MaxEntriesPerAppend <= 0 {
		c.MaxEntriesPerAppend = 256
	}
	if c.MaxBytesPerAppend <= 0 {
		c.MaxBytesPerAppend = 1 << 20
	}
	if c.MaxUncommittedEntries <= 0 {
		c.MaxUncommittedEntries = 4096
	}
	if c.MaxApplyBatch <= 0 {
		c.MaxApplyBatch = 1024
	}
	if c.SnapshotRetryTicks <= 0 {
		c.SnapshotRetryTicks = 60 * c.ElectionTicks
	}
	return nil
}

// Errors returned by Propose.
var (
	// ErrNotLeader means the entry was not appended anywhere.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrOverloaded means the leader has too many uncommitted entries. The
	// entry was not appended.
	ErrOverloaded = errors.New("raft: too many uncommitted entries")
)

// Status is a point-in-time copy of the node's state for diagnostics.
type Status struct {
	ID         string
	Role       Role
	Term       uint64
	Vote       string
	Leader     string
	Commit     uint64
	Applied    uint64
	LastIndex  uint64
	FirstIndex uint64
	// Peers is the leader's view of its followers; empty on other roles.
	Peers []PeerStatus
}

// PeerStatus is the leader's view of one follower.
type PeerStatus struct {
	ID           string
	Match        uint64
	Next         uint64
	Snapshotting bool
	RecentActive bool
}
