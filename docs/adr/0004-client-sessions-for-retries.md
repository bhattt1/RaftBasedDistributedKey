# ADR 0004: Client sessions with sequence numbers for safe retries

## Context

A client sends a write and hears nothing back. The request may have been
lost before it arrived, or it may have committed and the reply was lost. The
client cannot tell which. If it gives up, a write it believes failed may be
in effect. If it retries, a write may be applied twice, and for
compare-and-swap that is worse than it sounds: the second attempt compares
against the value the first attempt wrote, fails, and the client is told its
successful swap did not happen.

Consensus alone does not solve this. It guarantees that the log is the same
everywhere; it says nothing about the same request appearing in the log
twice.

## Decision

Every mutation carries a **client ID** and a **sequence number**, sent in
the `X-Client-Id` and `X-Request-Seq` headers. The state machine keeps, per
client, the highest sequence number it has applied, a fingerprint of that
command, and the result it produced. When a mutation is applied:

| Incoming sequence number | Action |
|---|---|
| equal to the stored one, same fingerprint | return the stored result; change nothing (`duplicate: true`) |
| equal to the stored one, different fingerprint | refuse with `IDENTITY_REUSED` |
| lower than the stored one | refuse with `STALE_SEQUENCE` |
| higher, or no session yet | execute, then store the new number, fingerprint and result |

The session table is part of the replicated state. It is updated only by
applying log entries, so every node has the same table; it is included in
snapshots, so it survives restarts and reaches followers that catch up from
a snapshot.

The contract a client must keep: **one outstanding mutation per client ID**,
numbered in increasing order. A retry reuses the number. Concurrent workers
use different client IDs. `kvctl` and the Go client do this for you.

Results of every kind are remembered, including a failed compare-and-swap:
a retry of a CAS that failed reports the same failure even if the key now
holds the expected value.

## Bounds, and what happens outside them

The table holds at most 4096 sessions. When a new client arrives and the
table is full, the session that has gone longest without a mutation is
dropped. "Longest" is measured in log order, not in time, so every node
drops the same one.

That gives the guarantee its edges:

- A retry is recognised as long as the client's session is still among the
  4096 most recently active. After that it is executed as a new request.
- A session remembers only the latest request. Once a client sends sequence
  number N+1, a late retry of N is refused with `STALE_SEQUENCE` rather than
  answered.
- A request sent without the headers gets a random identity from the server
  and therefore cannot be retried safely. The response says which identity
  was used.

This is "at most once per identity, within the session window". It is not
unconditional exactly-once, and the documentation does not call it that.

## Alternatives considered

- **A table of idempotency keys**, one arbitrary string per request, kept
  for the last N requests. Easier for clients (no sequence discipline) but
  the table has one row per request instead of one per client, so the
  retention window is short for the same memory.
- **Making operations idempotent by design** (only blind PUTs, CAS only on
  revisions). It does not help CAS on values, and it pushes the problem onto
  every caller.
- **De-duplicating in the HTTP layer**, with a cache in the leader's memory.
  It breaks at exactly the moment it matters: after a failover the retry
  goes to a different node with an empty cache.

## How it is tested

- `internal/kv`: the table above, eviction order, survival across
  snapshot and restore, determinism of two stores fed the same log.
- `internal/node`: a mutation committed on one leader, the leader killed,
  the retry sent to the next leader (`TestRetryAfterLeaderChangeIsNotAppliedTwice`).
- `tests/integration`: the same across a restart of every process.
- `tests/linearizability`: a client that retries under a fresh identity
  produces histories the checker rejects
  (`TestCheckerCatchesRetriesUnderANewIdentity`), which shows both that the
  mechanism matters and that the checker would notice if it broke.
