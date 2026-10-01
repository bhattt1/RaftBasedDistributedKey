# Protocol

This is the Raft implementation in `internal/raft`, described rule by rule,
with the test that checks each rule. It follows the Raft paper (Ongaro and
Ousterhout, "In Search of an Understandable Consensus Algorithm", extended
version) and, for PreVote and check-quorum, Ongaro's dissertation, sections
9.6 and 6.2. The etcd Raft library was read for its `Ready` interface design
and its handling of edge cases; no code was copied from it.

Section numbers below refer to the Raft paper unless stated otherwise.

## Failure model

The cluster is designed for:

- nodes that **crash** and later **restart** with their disk intact;
- messages that are **delayed, lost, duplicated or reordered**;
- **network partitions**, including one-way and partial ones;
- clocks that run at different speeds (only local timers are used, never
  timestamps from another machine).

It assumes:

- every member runs this code and follows the protocol. Members are
  **trusted**. A node that lies about its log or votes twice can break
  safety. This is not Byzantine fault tolerance; BFT protocols need 3f+1
  nodes to tolerate f liars and a different algorithm.
- data that `fsync` reported as written is still there after a crash. A disk
  that loses or corrupts synced data is detected by checksums where possible
  and stops the node; it is not tolerated silently.

Two kinds of promise, which fail differently:

- **Safety** (nothing bad happens): at most one leader per term; a committed
  entry is never lost or changed; every node applies the same commands in
  the same order; a read reflects every write acknowledged before it. These
  hold under every failure in the model, whatever the timing.
- **Progress** (something good eventually happens): a leader gets elected
  and requests get answered. This needs a majority of nodes up and able to
  talk to each other, and messages arriving within the election timeout most
  of the time. During a partition the minority side makes no progress, by
  design.

With `n` nodes a quorum is `n/2 + 1`: 2 of 3, 3 of 5. Three nodes tolerate
one failure, five tolerate two. Losing a majority makes the cluster
unavailable, for reads as well as writes, until a majority is back. Losing
the *disks* of a majority can lose committed data and is outside the
guarantee.

## State

Durable, written before any message that depends on it is sent:

| State | Meaning |
|---|---|
| `term` | latest term this node has seen |
| `vote` | who this node voted for in `term`, if anyone |
| log entries | `(index, term, type, data)`, type being `NOOP` or `COMMAND` |

Durable but only as an optimisation: the commit index is written lazily, in
the same file. Recovering with an older value is always safe; the node
relearns the current one from the leader.

Volatile: role, known leader, commit index, last applied index, election
timer, and on a leader each follower's `next`, `match`, `inflight` and
`snapshotting`.

## Elections

**Timers.** Time is counted in ticks (100 ms by default). A follower that
hears nothing from a leader for a randomised number of ticks in
`[election, 2 × election)` starts an election (10 to 20 ticks, 1 to 2 s, by
default). The randomisation makes it unlikely that two nodes time out
together, and re-randomising on every attempt resolves ties when they do
(§5.2). `TestSimSplitVotesResolve`.

**What resets the timer.** Only two things: granting a vote, and receiving
`AppendEntries` or `InstallSnapshot` from the current term's leader (even one
that is then rejected for a log mismatch: the leader is alive). A vote
request that is refused does not reset it, nor does a PreVote, nor does a
message from a stale term. Otherwise a node with an out-of-date log could
keep postponing the election of a node that can actually win.
`TestElectionTimerResetOnlyByLegitimateEvents`.

**Voting.** A node grants a vote in term T if it has not voted for someone
else in T, knows no leader in T, and the candidate's log is at least as up
to date as its own. "Up to date" compares the **term of the last entry
first**, and only if those are equal the length (§5.4.1). A longer log with
an older last term loses. `TestVoteComparesLastTermBeforeLength`,
`TestVotesOnlyOncePerTerm`, `TestVoteSurvivesRestartAndBlocksSecondVote`.

**Persistence.** The vote and the term it was cast in are in the same
`Ready` as the vote response, so they are synced before the response leaves.
A candidate's new term and self-vote are in the same `Ready` as its vote
requests. `TestVoteIsInSameReadyAsItsHardState`,
`TestCandidatePersistsTermAndSelfVoteWithItsRequests`.

**Counting.** The first answer from each peer counts; a duplicated or
retransmitted response cannot be counted twice.
`TestDuplicateVoteResponsesCountOnce`.

**At most one leader per term** follows from one vote per node per term and
majority quorums: two candidates cannot both collect a majority of the same
voters. The simulator checks it after every step of every run.

**The no-op.** A new leader immediately appends an empty entry in its own
term. A leader may only commit by counting replicas for entries of its *own*
term (see below), and it does not otherwise know which of the older entries
in its log are committed. Committing the no-op commits everything before it,
after which the leader knows its true commit index and can serve requests.
`TestNewLeaderInitialisesProgressAndAppendsNoop`.

## Terms and stale messages

Every message carries the sender's term.

- **Higher term than ours**: adopt it, clear the vote, become a follower.
  Then process the message. This is unconditional for real terms.
  `TestHigherTermMessageMakesLeaderFollow`,
  `TestRealHigherTermVoteIsHonouredDespiteHealthyLeader`.
- **Lower term, a request**: refuse, and reply with our term so the sender
  learns it is behind. Nothing else changes: not the log, not the leader,
  not the timer. `TestStaleTermRequestsGetCurrentTermBack`.
- **Lower term, a response**: ignore. `TestStaleTermResponsesAreIgnored`.

A response from the current term can still be out of date, because the
network may deliver it late or twice. The leader handles each kind so that
order does not matter:

- A *success* reports the highest index the follower holds. `match` only
  ever moves forward, so replaying or reordering successes is harmless.
  `TestSuccessResponsesApplyInAnyOrder`.
- A *success that reports nothing new* triggers no send. If it did, every
  duplicated or late response would start another request-response chain
  next to the existing ones. `TestResponsesWithoutProgressDoNotTriggerSends`.
- A *rejection* echoes the position it rejected. The leader acts on it only
  if that is the position it would send next; otherwise it answers an old
  probe. `TestStaleRejectionDoesNotMoveNextBackwards`.

## Replication

**Leader bookkeeping.** For each follower: `next`, the index of the next
entry to send, starting at the leader's last index + 1; and `match`, the
highest index known to be on the follower, starting at 0.

**Consistency check.** `AppendEntries` carries the index and term of the
entry just before the new ones. The follower accepts only if its log has
that entry (§5.3). `TestFollowerChecksPreviousEntry`.

**On a mismatch** the follower returns a hint: either "my log ends at X", or
"I have term T there, and T starts at index X". The leader uses it to skip
back a whole term per round trip instead of one index.
`TestRejectionUsesConflictHint`.

**On a match** the follower walks the new entries. Entries it already has
are skipped. At the first one that differs it discards its log from there
and appends the rest. It **never truncates merely because the request was
shorter than its log**: a delayed request carrying entries 5 to 7 must not
remove entry 8, which a later request already delivered.
`TestFollowerReplacesConflictingUncommittedSuffix`,
`TestShortOrDelayedAppendDoesNotTruncate`.

**Committed entries are never rewritten.** A request whose previous index is
below the follower's commit index is answered with "I match up to my commit
index" and nothing is touched. `TestFollowerNeverRewritesCommittedPrefix`.

**Follower commit index.** A follower advances its commit index to
`min(leaderCommit, last index this request verified)`. Not to
`leaderCommit` outright: entries beyond what the request verified may be
leftovers from a deposed leader that happen to sit at committed indexes.
`TestFollowerCommitIsBoundedByVerifiedPrefix`.

**Leader commit index.** The leader commits index N when a quorum stores N
*and N's entry is from the leader's current term* (§5.4.2, figure 8).
Counting replicas for an entry from an earlier term is unsafe; such entries
become committed only as part of the prefix of a current-term entry.
`TestLeaderCommitsOldTermEntriesOnlyThroughCurrentTerm`.

**The leader's own copy** counts toward the quorum only once it is on the
leader's disk, which the core learns through `Advance`.
`TestLeaderCountsItsOwnCopyOnlyAfterPersisting`.

**Applying.** Committed entries are handed out in index order, in bounded
batches; `lastApplied` never passes the commit index.
`TestAppliesInOrderInBoundedBatches`.

**Flow control.** One batch (at most 256 entries or 1 MiB) is outstanding
per follower. A response that reports progress releases the next one. Every
heartbeat interval the leader sends each follower whatever it still lacks,
which is an empty request when it is caught up; that is both the heartbeat
and the retransmission timer. `TestProposalsLeaveInOneBatchWithOneOutstandingPerFollower`,
`TestHeartbeatResendsAnUnacknowledgedBatch`.

## PreVote

**The problem.** A node cut off from the cluster keeps timing out. In plain
Raft each timeout increments its term. When it reconnects, its higher term
forces the working leader to step down, although nothing was wrong.

**The mechanism.** Before a real election a node becomes a *pre-candidate*
and asks its peers "would you vote for me in term T+1?". It does **not**
increment its term and does **not** vote for itself; nothing durable
changes. Only if a quorum says yes does it become a real candidate.

A node says yes when all of these hold:

1. the proposed term is higher than its own;
2. the pre-candidate's log is at least as up to date as its own;
3. it has no reason to think a leader is alive: it knows no leader, or it
   has not heard from one for a full base election timeout. A leader always
   says no.

Answering changes nothing on the responder either: not its term, vote or
timer.

**A PreVote request is not a higher-term message.** It carries a term the
sender has not entered. It is handled before, and separately from, the rule
"a higher term makes me a follower". Treating it as a real term would
recreate the very disruption PreVote prevents.
`TestPreVoteRequestIsNotARealHigherTerm`,
`TestPreCandidateDoesNotTouchDurableState`.

**There is no rule that ignores real higher terms.** Some implementations
make a follower disregard `RequestVote` while it believes its leader is
healthy. This one does not. If a real vote request with a higher term
arrives, the term is adopted. PreVote makes that rare; it does not need a
second mechanism that would also suppress legitimate elections.

A rejected PreVote from a node in a higher real term makes the pre-candidate
catch up to that term. A grant from an older round is ignored.
`TestPreVoteRejectionWithHigherTermUpdatesTerm`,
`TestStalePreVoteGrantIsIgnored`.

In the simulator, a follower isolated for 500 ticks keeps its term and
causes no election when it returns (`TestSimPartitionedFollowerDoesNotDisrupt`);
with PreVote switched off the same schedule forces the cluster into a higher
term (`TestSimWithoutPreVoteRejoiningNodeForcesElection`).
`scripts/demo-link-failure.sh` shows it with real processes.

## Check-quorum

**The problem.** A leader cut off from its followers still believes it is
the leader. It will never commit anything, but it will accept requests and
keep clients waiting, and it will tell them it is the leader.

**The mechanism.** A leader records which followers have answered anything.
Once per base election timeout it checks: did a quorum, counting itself,
answer since the last check? If not it steps down to follower, with no
leader known. It then answers clients with `NO_LEADER` instead of holding
them, and `/readyz` reports it as not ready. `TestCheckQuorum`.

## What PreVote and check-quorum are not

They make elections less disruptive and make a cut-off leader notice sooner.
**They are not what makes reads and writes safe.**

For up to an election timeout after a partition begins, the old leader and a
newly elected one can both hold the leader role, in different terms. That is
expected; the tests check election safety per term and commit behaviour, not
that every node's opinion of itself agrees at every instant. What keeps the
old leader from doing damage is that it cannot commit anything, because it
cannot reach a quorum, and that **nothing is answered without committing**:
writes and reads alike go through the log
([ADR 0003](adr/0003-reads-through-the-log.md)).

## Snapshots and log compaction

After 10,000 applied entries, or 64 MiB of them (both configurable), a node
serialises its state machine as of one log index, writes it durably, and
then discards log entries up to that index, less a trailing 1,000 kept in
memory so a slightly lagging follower can still catch up from the log.

Log positions keep working after compaction: the index and term of the last
discarded entry are retained, so the consistency check for the first
remaining entry still has something to compare.
`TestLeaderSendsSnapshotWhenEntriesAreCompacted`.

When a follower needs an entry the leader no longer has, the leader sends
its latest snapshot instead. While a transfer is outstanding the follower
still gets heartbeats but no entries. A transfer that fails, or whose outcome
is never reported, is retried. `TestFailedSnapshotIsRetried`,
`TestSnapshotWithUnknownOutcomeIsRetried`.

A follower receiving a snapshot:

- ignores it if it covers no more than the follower has already committed.
  Installing it would roll applied state back. `TestStaleSnapshotIsIgnored`.
- never lets it lower `currentTerm`: a snapshot from a stale term is refused
  like any other stale request.
- keeps the log entries after the snapshot's last index **if** its log
  contains that last entry with the same term; otherwise discards its whole
  log, which then continues from the snapshot.
  `TestSnapshotKeepsMatchingSuffix`, `TestSnapshotDiscardsConflictingLog`.

## The RPCs

Defined in `api/raft/v1/raft.proto`.

| RPC | Kind | Notes |
|---|---|---|
| `PreVote` | unary | |
| `RequestVote` | unary | |
| `AppendEntries` | unary | Carries a batch of entries in one request. That is batching; it is not a streaming RPC. Also the heartbeat. |
| `InstallSnapshot` | client-streaming | The leader streams the snapshot file in 256 KiB chunks; the follower replies once at the end. |

Every request carries a header with the cluster ID, the sender, the intended
recipient and a term. The receiving transport refuses, before Raft sees it,
any message from another cluster, addressed to another node, or from a
sender not in the configured voter set. With TLS on, the sender must also be
the identity in the client certificate.
`TestRequestsFromOutsideTheClusterAreRefused`,
`TestCertificateIdentityMustMatchTheClaimedSender`.

Connections are long-lived, one per peer. Vote and append RPCs have a
deadline of one election timeout; a snapshot transfer has two minutes.
Messages are capped at 8 MiB. gRPC's own reconnection backs off from 100 ms
to 3 s, and while a connection is down RPCs fail immediately rather than
queueing, so a recovering peer is not greeted by a backlog.
