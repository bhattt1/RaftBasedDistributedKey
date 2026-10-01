# ADR 0002: An append-only, segmented write-ahead log

## Context

Raft needs three things on disk before it may act on them: the current term,
the vote cast in that term, and the log. A follower sometimes has to replace
the end of its log when a new leader disagrees with it. And the log must not
grow for ever.

The obvious layout is one file per concern: a small file for term and vote,
rewritten on change, and a log file that is truncated and appended. Both
operations are awkward to make crash-safe. Rewriting a file in place can
leave half of each version. Truncating and then appending has a window where
the old suffix is gone and the new one has not arrived.

## Decision

Everything goes into one sequence of append-only segment files as typed,
checksummed records: `HardState` (term and vote), `Entry`, `Commit`, and
`Snapshot` (a marker that the log now continues from a snapshot).

- **Replacing a suffix is an append.** An entry record whose index is not
  past the end of the log replaces everything from that index on when the
  log is replayed. No truncate is ever issued against data that might be
  needed.
- **A batch is one write and one fsync**, with records in a fixed order:
  hard state, then entries, then commit index. If the write is torn, what
  survives is a prefix in that order, and every prefix is a state the node
  could legitimately have been in.
- **Each record header has its own checksum**, separate from the payload's.
  A damaged length field is then recognised as damage, instead of making
  the rest of the file look like one long record that runs off the end,
  which is what an interrupted write looks like.
- **Every segment restates the current term and vote at its head**, and
  recovery restates them again when it opens the last segment. Deleting old
  segments after a snapshot can therefore never delete the only copy.
- **Segments are deleted oldest first**, only when a durable snapshot covers
  every entry in them.

Snapshots are separate files, published by write, fsync, rename, fsync of
the directory.

## Consequences

Good:

- One code path for writing, one for replay. No in-place update anywhere.
- Crash-safety of suffix replacement and of term changes is a property of
  the format, not of careful sequencing.
- Recovery can tell an interrupted write (a record cut short by the end of
  the last segment, or a zero-filled tail) from damage (anything else that
  fails a checksum) and refuses to start on the latter.

Bad:

- Replayed space is wasted until the segment is deleted: a replaced suffix
  stays in the file, and every restart adds a small restatement record.
- Replay must handle a log whose beginning has been deleted, where the first
  record seen is in the middle of the history. Three of the recovery bugs
  found by crash testing were in exactly that logic.
- A record that is complete in length but fails its checksum at the very end
  of the log is treated as corruption, not as a torn write, and the node
  refuses to start. That is deliberate (it cannot be told apart from damage
  to an acknowledged record) but it means an operator has to look.

## Alternatives considered

- **An embedded store such as bbolt or Pebble.** Less code and well tested.
  It would also hide the part of the project that is most worth
  understanding, and its durability settings would have to be learned and
  trusted rather than written.
- **Separate metadata file plus a truncatable log.** Rejected for the two
  crash windows described above.
- **One fsync per entry.** Simplest to reason about and far slower: on the
  test machine one fsync takes about 1.5 ms, so under 700 entries per second,
  against 80,000 to 150,000 with 256 entries per fsync
  ([benchmarks.md](../benchmarks.md)).
