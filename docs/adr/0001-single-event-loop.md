# ADR 0001: One event loop owns consensus state

## Context

A Raft node's state is a handful of fields with invariants between them: the
term and the vote, the log and the commit index, a leader's view of each
follower. Messages, timers, client requests and disk completions all want to
change it, and they arrive concurrently.

There are two ways to make that safe. One is a mutex around the state, taken
by whichever goroutine has something to do. The other is to give the state to
one goroutine and have everyone else send it requests.

## Decision

One goroutine, the event loop in `internal/node`, owns the Raft core, the WAL
writer, the key-value store and the table of waiting clients. Other
goroutines reach it through bounded channels. The Raft core itself is a plain
struct with no synchronisation at all, driven through `Tick`, `Step`,
`Propose`, `Ready` and `Advance`.

The loop performs its own disk writes synchronously: it writes, calls fsync,
and only then goes on to send and apply.

## Consequences

Good:

- No lock ordering to get wrong, and no "which fields does this mutex cover"
  to document. The race detector has nothing to find in consensus state
  because only one goroutine touches it.
- The core is deterministic, so the same code runs under a simulator with a
  seeded clock, network and disk. That simulator found bugs that ordinary
  tests had not (see [testing-and-failures.md](../testing-and-failures.md)).
- Ordering rules such as "persist before acknowledging" are a few adjacent
  lines in one function, `processReady`.
- Requests that arrive while the loop is in fsync are handled together in
  the next round and share one fsync. Group commit falls out of the structure
  rather than being built.

Bad:

- While the loop is in fsync it does nothing else. Acknowledgements from
  followers wait in the inbox until the write returns. A write on a
  three-node cluster therefore costs about two fsyncs of latency in series
  (leader, then follower) rather than one.
- A slow disk slows the whole node, not just writes.
- State machine work also runs on the loop. Serialising a snapshot of the
  full 64 MiB state would pause it for a little over 100 ms (encoding runs at
  about 500 MB/s). Writing the snapshot file, the slower part, is done on
  another goroutine.

## Alternatives considered

- **A mutex around the Raft state.** Familiar, and it allows a blocked disk
  write not to block message handling. It also means every method has to
  decide what it may do while holding the lock, callbacks into the lock are
  a hazard, and deterministic simulation is no longer possible.
- **A separate WAL writer goroutine**, with the loop continuing to process
  messages while a write is in flight. This is what larger implementations
  do. It removes the "nothing else during fsync" cost and adds a second
  owner of ordering: the loop must track which Ready has been persisted
  before releasing its messages. The measured gain from a related change was
  not distinguishable from noise on the hardware available
  ([benchmarks/experiments/overlapped-replication.md](../../benchmarks/experiments/overlapped-replication.md)),
  so the simple form was kept.
