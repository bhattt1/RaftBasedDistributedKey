# Storage and recovery

What a node keeps on disk, in what order it writes it, and what it does when
it starts up and finds the disk in some state or other. The code is in
`internal/storage`; the reasons for the overall design are in
[ADR 0002](adr/0002-append-only-wal.md).

## What "durable" means here

A write is acknowledged to a client only after its log entry has been written
and `fsync`ed on a majority of nodes and applied on the leader. The claim
that follows from that:

> An acknowledged write survives any combination of process crashes and
> power failures, as long as a majority of the nodes that acknowledged it
> keep their disks, and those disks return what `fsync` said they stored.

What it rests on:

- `fsync` on a file makes its contents durable, and `fsync` on a directory
  makes creations, renames and removals in it durable. The code syncs both.
  This is the POSIX contract as Linux filesystems such as ext4 and XFS
  implement it.
- The storage device honours flush requests. A disk or hypervisor that
  acknowledges a flush and keeps the data in a volatile cache breaks the
  guarantee, and no software above it can tell.
- A failed `fsync` is treated as fatal (see below), because after one the
  kernel may have discarded the dirty pages.

What it does not cover:

- losing the disks of a majority of nodes;
- a node restarted with an empty data directory under its old ID
  ([ADR 0005](adr/0005-fixed-membership.md) explains why that is unsafe);
- corruption of synced data. Checksums detect it and the node refuses to
  start; the data is not repaired.
- running with `--unsafe-no-fsync`, which exists for benchmarks.

The server runs only on Unix-like systems. It needs `flock` and directory
`fsync`, and refuses to start elsewhere rather than run with weaker
guarantees. On Windows, use WSL2.

## Layout of a data directory

```
data/
  LOCK                               held with flock while a node runs
  meta.json                          format version, cluster ID, node ID
  wal/
    wal-0000000000000007.log         log segments, numbered consecutively
    wal-0000000000000008.log
  snap/
    snap-<index>-<term>.snap         the newest snapshot
    local-3.tmp, incoming-4.tmp      snapshots being written; removed on start
```

`LOCK` stops two processes from using one directory. The kernel releases the
lock when the process dies, however it dies, so a crash never leaves a stale
lock. `meta.json` ties the directory to one node of one cluster; a node
started with a different `--node-id` or `--cluster-id` refuses to open it.

## The write-ahead log

A segment starts with the 8 bytes `RKVWAL\0\x01` and continues with records:

```
offset  size  field
0       1     record type
1       4     payload length
5       4     CRC-32C of the payload
9       4     CRC-32C of bytes 0..8 (the header's own checksum)
13      n     payload
```

| Type | Payload | Meaning |
|---|---|---|
| `HardState` | term, vote | current term and vote |
| `Entry` | index, term, type, data | one log entry. If the index is not past the end of the log, it replaces everything from that index on. |
| `Commit` | index | commit index, written lazily |
| `Snapshot` | index, term, kept flag | the log now continues from this snapshot; the flag says whether the entries after it were kept |

### Write order

One `Ready` from Raft becomes one `write` and one `fsync`:

```
[HardState]  [Entry] [Entry] ...  [Commit]
```

always in that order. The node sends nothing that depends on the batch until
`fsync` has returned. The commit index is the exception: it rides along and
is synced whenever the next batch is, because recovering with an older value
is safe.

When a segment reaches 16 MiB it is synced and closed, and a new one is
created whose first records restate the current hard state and commit index.
The new segment's directory entry is synced before it is used.

### Why these choices are crash-safe

*A torn write leaves a valid state.* If power fails during the write, what
is on disk is a prefix of the batch. Every prefix is harmless: a new term
without the entries that came with it; the first few entries of a batch; all
the entries without the commit index. Nothing in the batch had been
acknowledged, because acknowledgement waits for `fsync`.

*Replacing a suffix has no intermediate state.* The replacement is appended.
Until its records are durable, replay yields the old log; once they are, the
new one. There is never a moment when the old suffix is gone and the new one
is absent.

*A commit record never points past the log.* It is written after the entries
it covers, so if it survived, so did they.

*Deleting segments never loses the term and vote.* Every segment begins with
them, and recovery restates them in the last segment each time it opens it.

## Snapshots

A snapshot file:

```
"RKVSNAP\x01"            magic and version
last included index      8 bytes
last included term       8 bytes
payload length           8 bytes
cluster ID               2-byte length, then the bytes
header CRC-32C
payload                  the state machine's encoding
payload CRC-32C
```

The payload (`internal/kv`) holds the keys in sorted order with their values
and revisions, then the retry-session table from least to most recently
used. Two nodes with the same state produce byte-identical payloads.

### Taking a snapshot

1. The event loop serialises the state machine between two applied commands,
   so the bytes are the state at exactly one log index. Nothing can be
   applied while this happens.
2. A background goroutine writes the bytes to a temporary file and fsyncs it.
3. It renames the file to its final name and fsyncs the directory. The
   snapshot is now published.
4. Back on the event loop: only now are older log entries dropped from
   memory and old WAL segments deleted, oldest first, followed by an fsync
   of the WAL directory.
5. The previous snapshot file is removed.

### Installing a snapshot from the leader

1. Chunks are written to a temporary file as they arrive. The total size is
   capped while receiving.
2. At the end the whole file is verified: both checksums, the declared
   length, and the cluster ID.
3. Raft decides whether to accept it. A snapshot no newer than what the node
   has committed is discarded.
4. The file is renamed into place and the directory synced.
5. A `Snapshot` marker is appended to the WAL and synced.
6. The state machine is replaced from the file.
7. Only then is the leader told.

An interrupted transfer leaves a temporary file, which is removed
immediately if the stream breaks and on the next start otherwise.

## Recovery

On start, in order:

1. Take the directory lock. Fail if another process holds it.
2. Check `meta.json` against the configured node and cluster. If the
   directory has data but no `meta.json`, refuse.
3. Remove `*.tmp` files in `snap/`. They were never published.
4. Find the newest snapshot and verify it in full. Remove older ones.
5. Replay the WAL segments in order.
6. Reconcile the two (below).
7. Rebuild the state machine from the snapshot. Raft then re-applies log
   entries as it learns they are committed.

### An interrupted write versus damage

Only the **last** segment may end badly, and only in one of two shapes:

- the final record is cut short by the end of the file; or
- everything from some record's start to the end of the file is zero bytes
  (the file was extended but the data never arrived).

Either is what an interrupted append looks like. The tail is cut off, the
file is synced, and the node continues. This discards nothing that was
acknowledged.

Everything else is treated as corruption and the node **refuses to start**:

- a checksum mismatch on a complete record, anywhere, including the last one;
- an incomplete record in any segment but the last;
- a missing segment in the middle of the sequence;
- an entry that would leave a gap in the log;
- a commit index beyond the end of the log;
- a `Snapshot` marker with no snapshot file to match;
- a snapshot file that fails verification.

The reasoning for the first item: a complete record with a bad checksum at
the very end *could* be a torn write, but it could equally be damage to the
last acknowledged record, and those cannot be told apart. Guessing "torn"
would silently drop an acknowledged write. Refusing costs an operator's
attention, which is the cheaper mistake.

A node that refuses to start does not endanger the cluster: the others carry
on if they are a majority. What to do about the node is in
[operations.md](operations.md#a-node-refuses-to-start).

### Not everything in the log is committed

Entries can be on disk without ever having been committed: a leader wrote
them and lost leadership before a quorum had them. Replaying the whole log
into the key-value map would apply writes that never happened.

So recovery restores the state machine from the snapshot only, and gives
Raft the log together with the last commit index it managed to write down.
Raft applies entries up to that index, and beyond it only as the current
leader confirms them. If the stored commit index is missing or old, the node
simply applies a little later.
`TestRecoveredNodeDoesNotApplyBeyondKnownCommit`.

### Reconciling snapshot and log

| What recovery finds | What it means | What it does |
|---|---|---|
| snapshot index = start of the log | normal after a snapshot and compaction | continue |
| snapshot ahead of the log's start, log has the snapshot's last entry | a local snapshot was published but old segments were not yet deleted | drop entries up to the snapshot |
| snapshot ahead of the log, log lacks that entry or disagrees | a snapshot from the leader was published but its marker was not written | discard the log; write the missing marker now |
| log starts after the newest snapshot | entries are missing | refuse to start |
| marker names a snapshot newer than any file | the state machine's contents are gone | refuse to start |

The third row is the crash window between steps 4 and 5 of installing a
snapshot. Finishing the install during recovery, by writing the marker,
matters: without it the log file still ends before the snapshot, and the
next append would leave a hole that the *following* restart rejects. That
was a real bug, found by the crash test below.

### Crash windows, one by one

| Crash point | State after restart |
|---|---|
| during a WAL append | a prefix of the batch; nothing was acknowledged |
| after the write, before fsync returns | the batch may or may not be there; nothing was acknowledged |
| during segment rotation | the new segment may be absent, empty or partial; it is completed on open |
| snapshot: before the rename | a temporary file, removed on start; the old snapshot and the full log remain |
| snapshot: after the rename, before the directory fsync | either outcome of the rename; both are complete, valid states |
| snapshot: after publishing, before WAL cleanup | new snapshot plus the full log; entries below the snapshot are skipped |
| during WAL cleanup | some or all of the old segments reappear, always a contiguous run; replay handles it |
| install: after publishing the file, before the marker | recovery completes the install |
| install: after the marker | installed |

## When the disk fails

- **A write or fsync returns an error** (disk full, I/O error, short write):
  the store marks itself failed and refuses every later write, even if the
  disk recovers. The node's event loop stops, `/healthz` turns to 503, and
  `kvserver` exits with status 1. Retrying a failed fsync is not safe: the
  kernel may have dropped the pages and a second fsync can succeed without
  the data being on disk. `TestWriteAndSyncFailuresStopTheStore`,
  `TestStorageFailureStopsTheNodeButNotTheCluster`.
- **A snapshot cannot be written**: logged, not fatal. The log is intact and
  simply is not compacted; the next threshold tries again.
- On restart after any of these, recovery runs as usual. Whatever had been
  acknowledged is there.

## How this is tested

All of it in `internal/storage`, most against `internal/testutil/faultfs`,
an in-memory filesystem that models exactly what survives a power failure:
contents up to the last fsync, names up to the last directory fsync, and for
bytes written after the last fsync, any of lost, partly kept, or zeroed.

- **`TestCrashAtEveryFilesystemOperation`** runs a random workload (saves,
  suffix replacements, local snapshots with compaction, installed snapshots)
  and kills the process at filesystem operation 1, then 2, then 3, and so on
  through the whole workload. After each crash it reopens the store and
  compares what came back with an independent model of what the node could
  legitimately have had. Then it writes again and reopens again.
- **`TestRepeatedCrashes`** keeps one directory alive through 30 crashes.
- **`TestTornTailIsRecoveredAtEveryCutPoint`** truncates the log at every
  byte offset inside the last batch.
- **`TestEveryFlippedByteIsDetected`** flips each byte of a log in turn and
  requires `ErrCorrupt` every time. `TestDamagedSnapshotRefusesToStart` does
  the same for a snapshot.
- **`TestDamagedLengthIsNotMistakenForATornTail`** is the case the separate
  header checksum exists for.
- **`FuzzWALRecovery`** and **`FuzzSnapshotDecoding`** feed arbitrary bytes
  to recovery. It may refuse them; it must not panic, and if it accepts them
  a write followed by a restart must come back.
- `osfs_test.go` repeats the basics against the real filesystem, with real
  `fsync` and `flock`.
- `tests/integration` kills real processes with `SIGKILL`, all three at
  once, and corrupts a real log file.

The four bugs this found are described, each with its regression test, in
`internal/storage/regression_test.go`.
