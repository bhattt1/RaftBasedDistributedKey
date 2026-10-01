# ADR 0003: Reads go through the log

## Context

A linearizable read must reflect every write that was acknowledged before
the read began. The tempting implementation is "if I am the leader, read my
map". It is wrong, because a node can believe it is the leader after it has
been replaced. Cut a leader off from its peers and, for up to an election
timeout, there are two nodes that think they lead: the old one in the old
term and the new one in the new term. A client that reaches the old one and
is answered from its memory gets a value that newer acknowledged writes have
already replaced.

## Decision

A `GET` is replicated like a write. The handler proposes a `READ` command;
the leader appends it to the log; once a quorum has stored it, it is applied,
and applying a `READ` means looking the key up at that point in the log's
order. The handler returns that result.

No code path in the server reads the key-value map except the apply path.

## Consequences

Good:

- The argument for correctness is short. A read is answered only if a quorum
  accepted its log entry, and a deposed leader cannot get a quorum. The read
  observes exactly the writes that precede it in the log.
- Reads and writes share one mechanism, one set of error codes and one set
  of tests. The linearizability checker treats them the same way.
- No dependence on clocks.

Bad:

- Every read costs what a write costs: a log entry, an fsync on the leader,
  an fsync on a quorum of followers, and the network round trip. Measured
  read throughput and latency are the same as for writes
  ([benchmarks.md](../benchmarks.md)).
- Reads grow the log and are replayed after a restart, where they do
  nothing.
- A read-heavy workload gets no benefit from being read-heavy.

## Alternatives considered

- **ReadIndex.** The leader records its commit index, confirms with a round
  of heartbeats that it is still leader, waits until it has applied up to
  that index, then reads locally. No log entry and no fsync. It needs the
  leader to have committed an entry in its current term first, the heartbeat
  round has to be tied to the specific read, and a term change in the middle
  has to cancel it. It is the natural next step and is listed as future
  work; the logged read would stay as the reference to test it against.
- **Leader leases.** The leader assumes nobody else can be elected for a
  bounded time after its last heartbeat round and reads locally with no
  round trip. Correctness then depends on bounded clock drift between
  machines. Not implemented, and not something to substitute quietly.
- **Follower reads.** Stale by design unless combined with ReadIndex.

## How it is tested

`TestCheckerCatchesReadsServedFromLocalState` in `tests/linearizability`
swaps in the tempting implementation and requires the history checker to
reject the result, which it does. `TestIsolatedLeaderCannotAcknowledgeReadsOrWrites`
and `scripts/demo-partition.sh` send reads to an isolated leader and require
that none is answered.
