# Interview guide

Material for talking about this project: a short overview, a longer
walkthrough, one request traced through the system, what happens in a
partition, questions you are likely to be asked with answers that point at
the code, the general concepts behind them, and exercises to prove to
yourself that you understand it.

Everything here should be checked against the code before you say it. The
file and test names are given so that you can.

## 60 seconds

> raft-kv is a replicated key-value store I built in Go to learn consensus
> properly. Three or five nodes keep the same log using Raft, which I
> implemented myself rather than using a library. A write is acknowledged
> only after a majority has it on disk, so it survives a node crashing, and
> reads go through the same log, so a node that has been cut off can never
> serve stale data.
>
> The part I spent the most time on is testing. The Raft core is a
> deterministic state machine with no I/O, so I can run a whole cluster in a
> simulator with a seeded fake network and replay any failure. The storage
> layer is tested by crashing it at every single filesystem operation. And I
> record what concurrent clients see and check the history for
> linearizability. That work turned up ten real bugs, which I can walk you
> through.
>
> It is a learning project: fixed membership, single keys, everything in
> memory with a 64 MiB limit. I know what it would need to be more than that.

## Five minutes

**The problem.** One machine holding your data is one failure away from
losing it or being unavailable. Several machines holding copies is better
only if they agree on what the data is, including while some are crashing
and the network is dropping messages. That agreement is consensus.

**The approach.** Raft elects one leader. Clients send commands to it; it
appends them to a log and sends them to the followers. Once a majority has
stored an entry it is *committed*: it will never be lost or changed. Each
node applies committed entries, in order, to a state machine, here a map.
Same log, same order, same map everywhere.

**How the code is organised.** Four layers, each testable alone:

- `internal/raft` is the algorithm with no side effects. You feed it ticks,
  messages and proposals. It hands back a `Ready`: things to write to disk,
  messages to send, entries to apply. It never does any of that itself.
- `internal/storage` is a write-ahead log and snapshot files.
- `internal/kv` is the state machine: put, get, delete, compare-and-swap,
  and a table that makes retries safe.
- `internal/node` is one goroutine, the event loop, that owns all three and
  runs them in the right order: write to disk, *then* send, *then* apply.

Around that: gRPC between nodes, HTTP for clients, a CLI.

**Three decisions I would point at.**

1. *One goroutine owns the consensus state.* No locks around it, because
   nothing shares it. Everything else sends it messages on bounded channels.
   The cost is that while it waits for the disk it does nothing else.
2. *Reads go through the log.* The obvious shortcut, "the leader reads its
   own map", is wrong: a leader that has been partitioned away still thinks
   it is the leader for a second or two. Putting reads in the log means a
   read is answered only if a majority took part.
3. *Retries are de-duplicated inside the replicated state.* A client that
   times out cannot know whether its write happened. It retries with the
   same client ID and sequence number, and the state machine, on every node,
   remembers the last result per client. That table is in the snapshots too.

**What I can show.** `make demo` starts three containers, kills the leader,
then partitions a leader with packet filtering while clients can still reach
it, and checks at each step that nothing acknowledged is lost and the
isolated node acknowledges nothing.

**What it is not.** Membership is fixed. I measured throughput on one laptop
where all three nodes share a disk, so the numbers describe that setup. It
has not run in production.

## One request, start to finish

`kvctl put greeting hello` against a three-node cluster. Follow it in the
code.

1. **`cmd/kvctl`** builds a client with a random ID. `client.Put` assigns
   sequence number 1 and tries the first endpoint.
2. That node is a follower. **`httpapi.run`** validates, proposes, and
   **`node.handleProposal`** gets `raft.ErrNotLeader`. The response is
   `421 NOT_LEADER` with the leader's URL.
3. The client follows the hint, same ID and sequence number
   (`client.do`).
4. On the leader, **`httpapi.put`** decodes the body; **`setIdentity`**
   reads the two headers; **`Command.Validate`** checks sizes;
   **`Command.Encode`** produces the bytes.
5. **`node.Propose`** does a non-blocking send on `proposeC` and waits on
   its own one-slot channel, the request context, and node shutdown.
6. **`node.run`** picks it up, with anything else waiting.
   **`raft.Propose`** appends entry `(index 7, term 3)`. The loop stores the
   waiting channel in `waiters[7]` along with term 3.
7. **`node.processReady`**: `store.Save` writes the entry to the WAL and
   fsyncs. Then `dispatch` puts one `AppendEntries` per follower on that
   follower's queue.
8. **`transport.sendOne`** makes the RPC. On each follower,
   **`transport.AppendEntries`** checks the header, and `node.Call` puts the
   message on the inbox and waits.
9. Follower: **`raft.handleAppend`** checks the previous entry matches,
   appends. `processReady` fsyncs, *then* releases the response.
10. Leader: **`raft.handleAppendResp`** raises that follower's `match`.
    **`maybeCommit`** sorts the match values, takes the quorum-th highest,
    sees its entry is from the current term, and advances the commit index.
11. Next `Ready` has entry 7 in `CommittedEntries`. **`node.apply`** calls
    **`kv.Store.Apply`**: no session for this client yet, so it executes the
    put, sets the key with revision 7, and records
    `(client, seq 1, fingerprint, result)`.
12. `waiters[7]` has term 3, the applied entry has term 3: the result is
    sent to the handler, which writes `200` with `"revision": 7`.

If the leader had lost leadership between 6 and 11, a different entry would
be applied at index 7, the terms would differ, and the handler would get
`ErrProposalDropped` (`503 LEADERSHIP_LOST`). If the deadline passed first,
`504 TIMEOUT` with outcome unknown, and the retry in step 3's style is what
makes that safe.

## A partition, start to finish

Three nodes, n1 is leader in term 5. The network cuts n1 off from n2 and
n3, but clients can still reach all three.

- **n1** keeps sending heartbeats that go nowhere. It still believes it is
  leader. A client that reaches it has its write appended to n1's log, where
  it cannot commit: n1 has one copy and needs two. The client's request
  times out and it is told the outcome is unknown.
- After one election timeout without hearing from a majority, n1's
  **check-quorum** makes it step down (`tickLeader`). Now it answers
  `NO_LEADER` and `/readyz` is 503. It keeps pre-voting; nobody answers.
  Because of **PreVote** its term stays 5.
- **n2 and n3** stop hearing heartbeats. One times out, pre-votes, the other
  agrees (no leader heard for a full timeout, log is up to date), it becomes
  a candidate in term 6, wins, appends a no-op, and commits it with the
  other's acknowledgement. Clients that reach n2 or n3 are served.
- For a moment, n1 (term 5) and the new leader (term 6) both had the leader
  role. That is fine: there is one leader *per term*, and n1 could not
  commit. Nothing was ever answered from n1's memory.
- **The network heals.** n1 receives an `AppendEntries` with term 6, adopts
  the term, becomes a follower. The consistency check fails at the index of
  its uncommitted entry, the leader backs up, and n1's entry is replaced by
  the leader's. The client that timed out on n1 retries with the same
  identity against the new leader, and its write happens once.

`scripts/demo-partition.sh` does exactly this and checks each claim.
`TestIsolatedLeaderCannotAcknowledgeReadsOrWrites` does it in-process, and
`TestSimIsolatedLeaderCannotCommit` in the simulator with five nodes.

## Questions and answers

### Raft

**1. Why can there be only one leader per term?**
A node votes at most once per term, and the vote is on disk before it is
sent (`handleVote`; the vote and its response are in the same `Ready`). A
candidate needs a majority. Two majorities share a node, and that node voted
once. The simulator asserts it after every step (`sim.check`).

**2. A follower's log is longer than the candidate's. Can the candidate
still win its vote?**
Yes, if the candidate's last entry has a higher term. `isUpToDate` compares
last terms first and lengths only on a tie. A long log full of entries from
an old term, which were never committed, loses to a shorter log with a newer
entry. `TestVoteComparesLastTermBeforeLength`.

**3. Why does a new leader append a no-op?**
It may only commit entries of its own term by counting replicas
(`maybeCommit` checks `term(n) == r.term`). The no-op gives it one.
Committing it commits everything before it, and only then does the leader
know what is really committed. Without it, a leader with no client traffic
would never commit its predecessors' entries.

**4. Why not commit an old-term entry once a majority has it?**
Figure 8 of the paper. A leader replicates an entry from term 2 to a
majority, crashes, and a node that never had the entry wins term 3 with a
newer-term entry of its own. It overwrites the term-2 entry everywhere. If
the first leader had counted that entry as committed, a committed entry
would have been lost. `TestLeaderCommitsOldTermEntriesOnlyThroughCurrentTerm`.

**5. What does a follower do with an `AppendEntries` that is shorter than
its log?**
Nothing destructive. It skips entries it already has and truncates only at
an actual conflict (`handleAppend`). The request may simply be old: a later
one already delivered more. `TestShortOrDelayedAppendDoesNotTruncate`.

**6. A follower gets `leaderCommit = 10` in a heartbeat that verifies up to
index 3. What is its commit index?**
3. `min(leaderCommit, last index this request verified)`. Entries 4 to 10 in
its log might be leftovers from a deposed leader.
`TestFollowerCommitIsBoundedByVerifiedPrefix`.

**7. What is PreVote for, and what is it not for?**
It stops a node that was cut off from forcing an election when it returns:
without it the node's term climbs while isolated and deposes the leader on
reconnect. It asks first, without changing its term. It is not a safety
mechanism and has nothing to do with read consistency.
`TestSimPartitionedFollowerDoesNotDisrupt` and its counterpart without
PreVote.

**8. And check-quorum?**
A leader that has not heard from a majority for an election timeout steps
down, so it stops holding clients and reports itself not ready. Also not a
safety mechanism: the old leader could not commit anyway. `TestCheckQuorum`.

**9. How do you handle a response that arrives late or twice?**
Successes carry the follower's highest matching index and `match` only moves
forward, so order does not matter. A success that reports nothing new sends
nothing. A rejection echoes the position it refers to and is ignored unless
that is still the position the leader would send. Stale-term responses are
dropped. See `handleAppendResp`.

### Storage

**10. What exactly is on disk before a follower says "I have it"?**
The entries, in the WAL, fsynced. `processReady` calls `store.Save` and only
then `dispatch`. The same for a vote: term and vote are fsynced before the
vote response leaves.

**11. How do you tell a crash in the middle of a write from corruption?**
Only the last segment may end badly, and only as a record cut short by the
end of the file or a run of zeros to the end. Those are what an interrupted
append leaves, and such a record was never acknowledged. Anything else that
fails a checksum stops the node from starting (`replaySegment`). The record
header has its own checksum so that a damaged length cannot make the rest of
the file look like a torn record.

**12. Why not replay the whole log into the map at startup?**
Because not everything in the log is committed. A deposed leader's
uncommitted entries can be sitting there. The state machine is rebuilt from
the snapshot, and Raft applies entries only up to the commit index it knows.

**13. An fsync fails. What do you do?**
Stop. The store refuses all further writes (`wal.fail`), the event loop
exits, the process exits non-zero. After a failed fsync the kernel may have
dropped the dirty pages; trying again can report success for data that never
reached the disk.

**14. How is a snapshot made safely?**
Serialise on the event loop, between two commands, so it is the state at one
index. Write to a temporary file, fsync, rename, fsync the directory. Only
after that delete the log entries it covers. A crash at any point leaves
either the old snapshot with the full log or the new one.

### The state machine and API

**15. A client's write times out. What should it do?**
Retry with the same client ID and sequence number. If the first attempt
committed, the state machine recognises the pair and returns the stored
result without applying it again (`kv.Store.Apply`). That works on any node
because the table is replicated.

**16. Is that exactly-once?**
No, and I do not call it that. It is at most once per identity, for as long
as the client's session is among the 4096 most recent and the client has not
moved on to a higher sequence number. Outside that window a retry is a new
request.

**17. Why is CAS atomic?**
The comparison and the write are one command applied by one goroutine from
one log position (`Store.mutate`). There is no gap for another operation.
`TestConcurrentCASLosesNoIncrements` has eight clients incrementing a counter
with read-then-CAS; the final value equals the number of successful swaps.

**18. Why are the size limits not configurable?**
They decide whether a command succeeds. If one node allowed 64 KiB values
and another 32 KiB, the same log would produce different states.

### Go

**19. Why channels here and not a mutex?**
See below. Short version: consensus state has invariants across many
fields; one owner is easier to get right than many visitors.

**20. How do you stop a slow client or peer from stalling the node?**
The event loop never blocks on them. Proposals arrive on a bounded channel
with a non-blocking send; when it is full the client gets `OVERLOADED`.
Results go out on a buffered channel of one. Peer messages go on a bounded
per-peer queue and are dropped when it is full.

**21. How does cancellation work?**
The HTTP request's `context.Context` gets a deadline and is passed to
`node.Propose`, which selects on it. If the client disconnects or the
deadline passes, the handler returns. The proposal itself cannot be
withdrawn, so the answer is "unknown", not "failed".

**22. How do you know there are no goroutine leaks?**
`goleak.VerifyTestMain` in the node and transport test packages fails the
run if any goroutine is left after the tests, which start and stop nodes
dozens of times.

**23. What did the race detector find?**
Nothing in the final code; CI runs every test under `-race`. One hazard was
designed out: a slice of log entries handed to a sender goroutine could have
been overwritten by a later truncate-and-append reusing the same backing
array, so truncation copies (`raftLog.truncateFrom`).

### Testing

**24. How do you test something as timing-dependent as consensus?**
By removing time. The core takes ticks as input. The simulator runs several
cores in one goroutine and decides, from a seed, when each message arrives.
Any failure replays exactly.

**25. Tell me about a bug the tests found.**
Pick from [testing-and-failures.md](testing-and-failures.md#bugs-found-during-development).
A good one: every acknowledgement from a follower triggered another send,
including duplicates. Under steady load with message duplication each
duplicate started another request-response chain and traffic grew
exponentially. No unit test saw it. The linearizability simulation, the
first test with sustained load, ground to a halt.

**26. What does your linearizability test prove?**
That the histories it recorded are linearizable. It is evidence, not proof.
What makes the evidence worth something is that I broke the system on
purpose twice (local reads; retries with a new identity) and the checker
caught both.

## Concepts

### Mutexes and channels

A mutex lets many goroutines take turns touching shared data. A channel
passes data to the one goroutine that owns it. Neither is "better"; they fit
different shapes.

Use a mutex when the data is simple and the critical section is short: the
status snapshot here is guarded by an `RWMutex` because readers just copy a
struct. Use a single owner and channels when there are invariants spanning
many fields and operations that take several steps: the Raft state here.

Coming from Java or C#: a `synchronized` block or `lock` statement is the
mutex. The channel-and-owner pattern is closer to an actor, or a
single-threaded executor with a queue.

### Goroutines and threads

A goroutine is a function running concurrently, scheduled by the Go runtime
onto a small number of OS threads. It starts with a few kilobytes of stack
that grows as needed, so thousands are routine: this server has one per HTTP
request and per RPC. When a goroutine blocks on a channel or network I/O the
runtime parks it and runs another on the same thread. A blocking system call
such as `fsync` does occupy a thread, and the runtime starts another to keep
the rest going.

Compared with `Task` in .NET or virtual threads in Java 21: similar idea,
but there is no `async`/`await` colouring. Blocking code is the normal way
to write Go.

### Cancellation

`context.Context` carries a deadline and a cancellation signal down a call
chain. Each layer passes it on and selects on `ctx.Done()` wherever it
waits. It is the equivalent of `CancellationToken`. It is cooperative:
cancelling tells a waiter to stop waiting. It does not undo work already
handed to someone else. That distinction is the whole reason for the
"outcome unknown" response.

### Backpressure

When work arrives faster than it can be done, something has to give. The
choices are: queue without limit (memory grows until the process dies), block
the producer, or refuse. This server uses bounded queues and refuses at the
edge: `429 OVERLOADED` to clients, dropped messages between peers (Raft
retransmits). The client's part is to back off, with jitter so that clients
do not retry in step.

### Durability and replication

Durability: after a crash, the data is still there. That takes `fsync`.
Replication: the data is on more than one machine. They cover different
failures. Replication without fsync survives one machine dying but not a
power cut to the whole rack. Fsync without replication survives the power
cut but not the disk dying. This project does both: each node fsyncs before
it acknowledges, and a write is acknowledged to the client only when a
majority has done so.

### Linearizability and eventual consistency

Linearizable: every operation appears to happen at one instant between its
start and its end, and everyone sees the same order. Once a write is
acknowledged, every later read sees it. It behaves like one machine.

Eventually consistent: replicas converge if writes stop, but in the meantime
a read may return old data and two clients may see different values.

Linearizability costs availability under partition. The minority side must
refuse to answer, which is what this system does. Eventual consistency keeps
answering on both sides and reconciles later. That is the CAP trade-off, and
this is a CP system.

### Safety and availability in Raft

Raft never gives up safety: no two leaders in a term, no lost committed
entries, under any timing. What it gives up, when it must, is availability:
no majority, no progress. And progress when there *is* a majority still
depends on timing: elections need messages to arrive within the timeout most
of the time. Asynchronous consensus cannot guarantee both in all cases (the
FLP result); Raft, like Paxos, chooses to be always safe and usually live.

## Exercises

Small changes to make yourself. Each has a way to check your work.

1. **Add a `raftkv_proposals_pending_max` gauge** that reports the highest
   number of waiting proposals since start. You will touch
   `observability/metrics.go` and decide where the value is tracked. Check:
   it appears in `/metrics` and rises under `kvbench`.

2. **Break a safety rule and watch the tests catch it.** In
   `handleAppend`, change the commit rule to `r.commit = m.Commit`. Run
   `go test ./internal/raft/`. Read which tests fail and why, then put it
   back. Do the same by removing the `term(n) == r.term` check in
   `maybeCommit`.

3. **Add `GET /v1/kv/{key}?revision_only=true`** that returns the revision
   without the value. It still has to go through the log. Add a table-driven
   case to `httpapi_test.go`.

4. **Add a `kvctl watch-leader` command** that polls status once a second and
   prints a line when the leader or term changes. Run it during
   `scripts/demo-kill-leader.sh`.

5. **Write a new simulator scenario**: five nodes, isolate two, write, heal,
   isolate a different two that include the leader, write, heal. Assert
   convergence. Run it over 500 seeds with `RAFT_SIM_SEEDS=500`.

Harder, if you want one: implement ReadIndex behind a flag and make the
linearizability simulation run both read modes.
