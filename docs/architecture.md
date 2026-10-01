# Architecture

This document explains how a node is put together: which package does what,
which goroutine owns which data, and in what order things happen to a
request. For the consensus rules themselves see [protocol.md](protocol.md);
for the on-disk formats see [storage-and-recovery.md](storage-and-recovery.md).

## Packages

| Package | Responsibility |
|---|---|
| `internal/raft` | The Raft algorithm as a pure state machine. No I/O, no goroutines, no clock. |
| `internal/storage` | Write-ahead log, snapshot files, recovery, the data directory lock. |
| `internal/kv` | The replicated key-value state machine, command encoding, retry de-duplication. |
| `internal/node` | The event loop that connects the three packages above to the network and to clients. |
| `internal/transport` | gRPC between peers. |
| `internal/httpapi` | The client HTTP API. |
| `internal/client` | Go client: leader discovery and safe retries. Used by `kvctl`, `kvbench` and tests. |
| `internal/config`, `internal/security`, `internal/observability` | Settings, TLS and token handling, metrics and logging. |
| `internal/testutil` | Simulated cluster, crash-modelling filesystem, in-memory network, test certificates. |
| `cmd/kvserver`, `cmd/kvctl`, `cmd/kvbench` | The server, the CLI, the load generator. |

Dependencies point one way. `raft` imports nothing from this repository.
`storage` and `kv` know `raft`'s types and nothing else. `node` knows all
three. `transport` and `httpapi` each define a small interface describing what
they need from a node, and `*node.Node` happens to satisfy both; neither
imports the other. In Go the consumer declares the interface, which is the
reverse of the Java or .NET habit of the implementer declaring `IFoo`.

## The shape of the Raft core

`internal/raft` exposes five things:

```go
func (r *Raft) Tick()                 // one unit of logical time has passed
func (r *Raft) Step(m Message)        // a message arrived
func (r *Raft) Propose(data []byte)   // a client wants this replicated
func (r *Raft) Ready() Ready          // what must now be done
func (r *Raft) Advance()              // it has been done
```

`Ready` is a list of side effects: state to persist, messages to send,
entries to apply. The core never performs them. That one decision buys two
things.

First, tests can run a whole cluster inside a single goroutine with a fake
clock, a fake network and a fake disk, all driven by one seeded random number
generator (`internal/testutil/sim`). A run is determined by its seed, so a
failure found on seed 5 is found again on seed 5.

Second, the rules about ordering live in one place. `Ready` documents the
order the caller must follow, and both callers (the real node and the
simulator) follow it:

1. install a snapshot, if the Ready carries one;
2. persist the term, the vote and new log entries;
3. send messages;
4. apply committed entries;
5. call `Advance`.

Step 2 before step 3 is what makes a vote or an acknowledgement mean
something: by the time another node hears "I voted for you" or "I have your
entries", it is already on disk. The leader's own copy of an entry is counted
toward the quorum only in `Advance`, after step 2.

## Goroutines and who owns what

A running node has these goroutines. "Owns" means: is the only code that
reads or writes it.

| Goroutine | Started by | Owns | Ends when |
|---|---|---|---|
| Event loop (`node.run`) | `node.Start` | the Raft core, the WAL writer, the `kv.Store`, the table of waiting clients, the table of pending peer calls | `Stop` is called or storage fails |
| Snapshot writer | the event loop, at most one at a time | one temporary snapshot file | the file is written and published |
| Peer sender, one per peer | `transport.New` | that peer's outbound queue | `transport.Close` |
| Snapshot sender, at most one per peer | a peer sender | one open snapshot file and one gRPC stream | the transfer ends |
| gRPC server handlers | gRPC, one per inbound RPC | nothing shared | the RPC returns |
| HTTP handlers | `net/http`, one per request | nothing shared | the request returns |
| `main` | the runtime | listeners, the shutdown sequence | exit |

Everything that would be shared state in a lock-based design belongs to the
event loop. Other goroutines talk to it through channels:

| Channel | Capacity | Sender | When full |
|---|---|---|---|
| `proposeC` | `max-pending-proposals` (1024) | HTTP handlers | the handler does not wait: the client gets `OVERLOADED` |
| `inboxC` | 1024 | gRPC handlers, peer senders | the sender waits until its RPC deadline, then drops the message |
| `reportC` | one per peer | snapshot senders | the report is dropped; Raft's own retry timer covers it |
| `snapDoneC` | 1 | the snapshot writer | cannot fill: one writer at a time |
| per-request `done` | 1 | the event loop | cannot block: each receives exactly one value |
| per-peer `queue` | 64 | the event loop | the message is dropped; Raft resends on the next heartbeat |

Two rules follow from the table and are worth stating because they are what
keep the node from seizing up:

- **The event loop never blocks on a peer or a client.** It sends to peers
  with a non-blocking channel send and to waiting clients through a buffered
  channel of size one. The only thing it waits for is its own disk.
- **Nothing holds a lock across a network call.** There are three mutexes in
  the server: one guards the published status snapshot, one guards the
  snapshot directory, one is inside the metrics library. None is held while
  sending or receiving.

The status that `/v1/status`, `/readyz` and the metrics read is a copy the
event loop publishes after each round. Readers never touch live Raft state.

### Mutexes versus channels here

A mutex protects data that several goroutines must touch. A channel hands
data from one goroutine to another so that only one touches it at a time.
Consensus state has invariants spanning many fields (term, vote, log, commit
index, per-peer progress), and it is far easier to keep them when exactly one
goroutine can see the fields. So the design gives that state a single owner
and uses channels to reach it. The mutexes that remain guard small things
with no invariants across them, where a channel would be ceremony.

## A write, end to end

`PUT /v1/kv/greeting` with body `{"value":"hello"}`, on a three-node cluster,
arriving at the leader:

1. **HTTP handler** (`httpapi.put`). Checks the bearer token if one is
   configured. Reads the body through a 1 MiB limit. Takes the request
   identity from `X-Client-Id` and `X-Request-Seq`, or generates one. Builds
   a `kv.Command`, validates it against the limits, encodes it.
2. **Deadline.** The handler derives a context from the request with the
   client's `?timeout=` (capped by the server) or the default. The context
   also ends if the client disconnects.
3. **`node.Propose`.** Places the command on `proposeC` without blocking, or
   returns `ErrOverloaded`. Then waits on its own `done` channel, the
   context, and the node's shutdown.
4. **Event loop, round N.** Takes this proposal and whatever else is waiting.
   `raft.Propose` appends the command to the in-memory log as entry
   `(index, term)`; the loop files the waiting handler under that index and
   remembers the term.
5. **`processReady`.** The Ready contains the new entries and one
   `AppendEntries` message per follower. The loop writes the entries to the
   WAL and calls `fsync` once for the batch. Only then does it hand the
   messages to the transport.
6. **Followers.** Each receives the RPC, checks that the entry before the new
   ones matches its log, appends, fsyncs, and only then returns success.
7. **Event loop, a later round.** The first acknowledgement makes two durable
   copies of three. The entry is from the leader's current term, so the
   commit index advances.
8. **Apply.** The loop passes the entry to `kv.Store.Apply`, which runs the
   de-duplication check and then the write. The result goes to the waiting
   handler, after checking that the applied entry's term is the one the
   handler was promised.
9. **Response.** The handler writes `{"key":"greeting","revision":7,"duplicate":false}`.

If the node is not the leader, step 4 fails immediately and the handler
returns `421 NOT_LEADER` with the leader's client URL. If the deadline passes
between steps 4 and 8, the handler returns `504 TIMEOUT` with
`"outcome":"unknown"`: the entry is in the log and may still commit.

A read (`GET`) follows exactly the same path. It is appended to the log as a
`READ` command and answered with what the state machine returned when that
entry was applied. Nothing in the server reads the map any other way.

## Backpressure and bounds

| What | Bound | What happens at the bound |
|---|---|---|
| Proposals queued for the loop | 1024 | `429 OVERLOADED` |
| Proposals waiting to commit | 1024 | `429 OVERLOADED` |
| Leader's uncommitted log entries | 1024 | `429 OVERLOADED` |
| Entries per `AppendEntries` | 256 entries or 1 MiB | the rest goes in the next batch |
| Batches in flight per follower | 1 between heartbeats | new entries wait for the acknowledgement |
| Messages queued per peer | 64 | dropped; resent on the next heartbeat |
| gRPC message size | 8 MiB | the RPC is refused |
| HTTP request body / headers | 1 MiB / 16 KiB | `413` / connection closed |
| Key / value / client ID size | 256 B / 64 KiB / 64 B | `400 INVALID_ARGUMENT` |
| Total keys and values | 64 MiB | `507 CAPACITY_EXCEEDED` |
| Client sessions for de-duplication | 4096 | least recently used is forgotten |
| Log entries kept in memory | 10000 entries or 64 MiB since the last snapshot, plus 1000 trailing | a snapshot is taken and the log compacted |
| Incoming snapshot | 128 MiB | the transfer is refused |

**A slow follower** costs the leader one queued batch and dropped
heartbeats. It never delays the leader or the other follower, because the
leader does not wait on it. If it falls behind the leader's compacted log it
receives a snapshot.

**A slow disk** on the leader slows everything: the event loop does one
thing at a time and waits for fsync. Requests that arrive during an fsync are
taken together in the next round and share its fsync, so throughput rises
with concurrency while latency stays near one or two fsyncs (see
[benchmarks.md](benchmarks.md)). When requests arrive faster than the loop
drains them, the queue reaches its bound and clients are told to back off. A
slow disk on a follower only makes that follower lag.

## Shutdown

`kvserver` handles `SIGINT` and `SIGTERM`:

1. Stop the node. The loop answers every proposal it had accepted but not
   yet applied with "outcome unknown" (it may still commit on the other
   nodes), answers queued proposals it never proposed with "shutting down",
   and exits. A background snapshot is allowed to finish.
2. Shut down the HTTP server. Handlers return at once because step 1
   answered them.
3. Close the transport: stop the senders, stop the gRPC server (gracefully,
   then forcibly after two seconds), close connections.
4. Close the store: a final fsync, then release the directory lock.

The whole sequence is bounded by `--shutdown-timeout` (10 s by default). If
storage fails while running, the loop stops on its own and the process exits
with status 1.

## Design decisions

The reasoning behind the main choices is recorded as short decision records
in [adr/](adr/):

- [0001](adr/0001-single-event-loop.md): one event loop owns consensus state
- [0002](adr/0002-append-only-wal.md): an append-only, segmented WAL
- [0003](adr/0003-reads-through-the-log.md): reads go through the log
- [0004](adr/0004-client-sessions-for-retries.md): client sessions with sequence numbers
- [0005](adr/0005-fixed-membership.md): fixed membership
