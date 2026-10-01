# Scope and acceptance checklist

What the first release contains, what it deliberately leaves out, where the
implementation departs from the original brief and why, and the checklist
used to decide whether it is done. The evidence for each item is in
[claims-and-evidence.md](claims-and-evidence.md).

## In scope

- A Raft core written from scratch: elections, log replication, PreVote,
  check-quorum, snapshots, log compaction. No Raft library is used.
- Fixed membership. Three nodes by default; five supported and tested.
- `PUT`, `GET`, `DELETE` and atomic compare-and-swap on single keys.
- Durable term, vote and log; crash recovery; snapshots; follower catch-up
  from a snapshot.
- Linearizable reads; retries that are not applied twice; explicit meaning
  for timeouts.
- gRPC between peers, HTTP for clients, a CLI.
- Deterministic simulation, real-process tests, partition tests, history
  checking.
- Metrics, structured logs, bounded shutdown, bounded resource use, a secure
  configuration.
- Reproducible benchmarks, CI, documentation.

## Out of scope (future work)

In rough order of value:

1. **Membership changes** (single-server changes from the Raft
   dissertation). Without them a node whose disk is lost cannot be safely
   replaced. This is the largest operational gap.
2. **ReadIndex reads**, to stop paying a log entry and an fsync per read.
   The logged read stays as the reference.
3. **Snapshot export and a key listing**, for backup and migration.
4. **A WAL writer that does not block the event loop**, if measurement on
   hardware with one disk per node shows it pays.
5. Multi-key transactions, range scans, watches, TTLs.
6. Sharding (multiple Raft groups).
7. Kubernetes manifests, a dashboard.
8. Lease-based reads. These trade a clock assumption for speed and should
   never replace the default quietly.

## Departures from the brief

Places where the implementation does something other than the most literal
reading of the request, and why.

| Brief | What was done | Why |
|---|---|---|
| Repository named `raft-kv` | Module path `github.com/bhattt1/RaftBasedDistributedKey` | The module path has to match the real repository for `go install` and imports to work. The binaries and metrics use the `raftkv` name. |
| "Do not publish the repository" | Pushed to the owner's GitHub repository | Explicit instruction from the owner, given after the brief. Nothing else is published and no paid infrastructure is used. |
| Optional ReadIndex | Not implemented | Optional in the brief, and the logged read had to be right first. Listed above. |
| "Support Linux and document Windows through WSL2 with Docker Desktop" | Built and tested on WSL2 with Docker Engine installed inside WSL2 | Docker Desktop was not installed on the development machine. The Compose files and scripts do not depend on which one provides the daemon. |
| Configurable limits | Key, value, state and session limits are compile-time constants | They decide command outcomes, so they must be identical on every node; a per-node setting could make states diverge. |
| Persist first, then send | Kept, after trying the alternative | Sending before the leader's fsync was implemented, measured and removed when it showed no gain ([experiment](../benchmarks/experiments/overlapped-replication.md)). |
| Docker network faults for all chaos | Random fault schedules run in the simulator and in-process; Docker runs scripted scenarios | Deterministic replay matters most where the schedules are random. The Docker scripts cover the scenarios the brief names. |
| Benchmarks in containers with limits | Benchmarks run as host processes without limits | Fewer layers between the measurement and the disk. Recorded as such in every report. |

## Corrections to common shortcuts

The brief warned against several designs. For the record, what was built
instead:

- Reads are not served from the leader's memory. They go through the log.
- A timed-out write is reported as "outcome unknown", not as failed.
- De-duplication state is replicated and snapshotted, not held in an HTTP
  cache.
- The quorum is computed from the configured membership, never from the
  nodes currently reachable.
- Recovery does not replay the whole log into the state machine; only
  entries known to be committed are applied.
- A follower does not truncate its log because a request was short.
- A PreVote request is not treated as a higher term.
- Scripts check outcomes and exit non-zero; none prints "should recover".

## Acceptance checklist

| # | Gate | Status |
|---|---|---|
| 1 | A fresh checkout can follow the README and run the project | Verified from a clean clone; see PROGRESS.md |
| 2 | Three-node replication and the five-node configuration exercised | Pass |
| 3 | A minority cannot acknowledge consensus-dependent operations | Pass |
| 4 | A healthy majority recovers progress after a leader failure | Pass |
| 5 | Every acknowledged mutation survives the tested failures | Pass |
| 6 | Reads, CAS and retries match their documented semantics | Pass |
| 7 | WAL recovery, snapshots, compaction and snapshot installation verified | Pass |
| 8 | Race, integration, failure and history-checking gates pass | Pass locally |
| 9 | Benchmarks reproducible, raw results present | Pass |
| 10 | Secure and local modes, limits and operational gaps documented | Pass |
| 11 | README, interview answers and resume claims match the implementation | Pass |
| 12 | CI green on GitHub | **Not verified**: the workflows are written and every job's commands pass locally, but no run on GitHub had been observed when this was written |
