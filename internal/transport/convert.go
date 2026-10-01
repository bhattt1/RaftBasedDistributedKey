package transport

import (
	"fmt"

	raftv1 "github.com/bhattt1/RaftBasedDistributedKey/internal/gen/raft/v1"
	"github.com/bhattt1/RaftBasedDistributedKey/internal/raft"
)

// The Raft core works with one flat Message type. On the wire each request
// and response has its own protobuf message. The functions here translate in
// both directions; fields that an RPC response does not carry (who it is from,
// which request it answers) are filled in from the request that caused it.

func (t *Transport) header(m raft.Message) *raftv1.Header {
	return &raftv1.Header{ClusterId: t.cfg.ClusterID, From: m.From, To: m.To, Term: m.Term}
}

func voteRequest(t *Transport, m raft.Message) *raftv1.VoteRequest {
	return &raftv1.VoteRequest{Header: t.header(m), LastLogIndex: m.LogIndex, LastLogTerm: m.LogTerm}
}

func appendRequest(t *Transport, m raft.Message) *raftv1.AppendEntriesRequest {
	req := &raftv1.AppendEntriesRequest{
		Header:       t.header(m),
		PrevLogIndex: m.LogIndex,
		PrevLogTerm:  m.LogTerm,
		LeaderCommit: m.Commit,
		Entries:      make([]*raftv1.Entry, len(m.Entries)),
	}
	for i, e := range m.Entries {
		req.Entries[i] = &raftv1.Entry{Index: e.Index, Term: e.Term, Type: entryTypeToProto(e.Type), Data: e.Data}
	}
	return req
}

func entryTypeToProto(t raft.EntryType) raftv1.EntryType {
	if t == raft.EntryCommand {
		return raftv1.EntryType_ENTRY_TYPE_COMMAND
	}
	return raftv1.EntryType_ENTRY_TYPE_NOOP
}

func entryTypeFromProto(t raftv1.EntryType) (raft.EntryType, error) {
	switch t {
	case raftv1.EntryType_ENTRY_TYPE_NOOP:
		return raft.EntryNoop, nil
	case raftv1.EntryType_ENTRY_TYPE_COMMAND:
		return raft.EntryCommand, nil
	}
	return 0, fmt.Errorf("unknown entry type %d", t)
}

// voteMessage turns a received vote or pre-vote request into a core message.
func voteMessage(typ raft.MessageType, req *raftv1.VoteRequest) raft.Message {
	h := req.GetHeader()
	return raft.Message{Type: typ, From: h.GetFrom(), To: h.GetTo(), Term: h.GetTerm(),
		LogIndex: req.GetLastLogIndex(), LogTerm: req.GetLastLogTerm()}
}

func appendMessage(req *raftv1.AppendEntriesRequest) (raft.Message, error) {
	h := req.GetHeader()
	m := raft.Message{Type: raft.MsgApp, From: h.GetFrom(), To: h.GetTo(), Term: h.GetTerm(),
		LogIndex: req.GetPrevLogIndex(), LogTerm: req.GetPrevLogTerm(), Commit: req.GetLeaderCommit()}
	if n := len(req.GetEntries()); n > 0 {
		m.Entries = make([]raft.Entry, n)
		for i, e := range req.GetEntries() {
			typ, err := entryTypeFromProto(e.GetType())
			if err != nil {
				return raft.Message{}, err
			}
			m.Entries[i] = raft.Entry{Index: e.GetIndex(), Term: e.GetTerm(), Type: typ, Data: e.GetData()}
		}
	}
	return m, nil
}

// respHeader builds the header of a response from the core's response
// message.
func (t *Transport) respHeader(m raft.Message) *raftv1.Header {
	return &raftv1.Header{ClusterId: t.cfg.ClusterID, From: m.From, To: m.To, Term: m.Term}
}

// voteResponseMessage turns an RPC reply into the message the core expects,
// using the request to fill in what the reply does not repeat.
func voteResponseMessage(req raft.Message, resp *raftv1.VoteResponse) raft.Message {
	typ := raft.MsgVoteResp
	if req.Type == raft.MsgPreVote {
		typ = raft.MsgPreVoteResp
	}
	return raft.Message{Type: typ, From: req.To, To: req.From, Term: resp.GetHeader().GetTerm(), Reject: !resp.GetGranted()}
}

func appendResponseMessage(req raft.Message, resp *raftv1.AppendEntriesResponse) raft.Message {
	return raft.Message{
		Type: raft.MsgAppResp, From: req.To, To: req.From, Term: resp.GetHeader().GetTerm(),
		Reject:        !resp.GetSuccess(),
		LogIndex:      req.LogIndex, // lets the leader recognise a stale rejection
		MatchIndex:    resp.GetMatchIndex(),
		ConflictIndex: resp.GetConflictIndex(),
		ConflictTerm:  resp.GetConflictTerm(),
	}
}

func snapshotResponseMessage(req raft.Message, resp *raftv1.InstallSnapshotResponse) raft.Message {
	return raft.Message{Type: raft.MsgSnapResp, From: req.To, To: req.From, Term: resp.GetHeader().GetTerm(),
		Reject: !resp.GetSuccess(), MatchIndex: resp.GetMatchIndex()}
}
