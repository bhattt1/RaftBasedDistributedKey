# Testing and failure handling

How the project is tested, what each layer can and cannot show, the failure
demonstrations and what they produced when run, and the bugs the tests found.

## Layers

| Layer | Where | What is real | What is simulated | Repeatable? |
|---|---|---|---|---|
| Unit tests | next to each package | the code under test | everything around it | yes |
| Raft simulation | `internal/raft/sim_test.go` | the Raft core | clock, network, disk, crashes | yes, by seed |
| Storage crash tests | `internal/storage` | WAL and snapshot code | the filesystem, including power loss | yes, by seed |
| History checking (simulated) | `tests/linearizability/sim_test.go` | Raft core and key-value state machine | clock, network, disk, clients | yes, by seed |
| In-process cluster | `internal/node`, `tests/linearizability/live_test.go` | event loops, goroutines, WAL, state machine | network (in memory), filesystem | no |
| gRPC transport | `internal/transport` | all of the above plus gRPC and TLS on loopback | filesystem | no |
| Process tests | `tests/integration` | `kvserver` and `kvctl` binaries, real files, real sockets | nothing; no network faults | no |
| Docker fault demos | `scripts/demo-*.sh` | containers, volumes, packet filtering | nothing | no |
| Fuzzing | `internal/kv`, `internal/storage` | decoders and recovery | inputs | corpus |

The lower layers are deterministic and explore many schedules quickly. The
upper ones run the real thing and explore few. A property is trusted when
the layers that can reach it agree.

No test relies on a fixed sleep to decide that something has happened.
Simulations advance a logical clock one tick at a time. Tests with real
goroutines or processes poll for a condition with a deadline and fail with a
description of the cluster if it is not met.

## Commands

| Command | Runs | Time on the development machine |
|---|---|---|
| `make test` | every unit test and the simulations at default size | 15 s |
| `make test-race` | the same under the race detector | 25 s |
| `make sim` | Raft simulation, 300 seeds per scenario | 15 s |
| `make crash` | storage crash injection, 60 seeds | 10 s |
| `make linearizability` | model self-tests, 300 simulated seeds, one live run | 15 s |
| `make integration` | real processes | 35 s |
| `make chaos` | the six Docker demos | a few minutes |
| `make fuzz` | each of the five fuzz targets for 20 s | 2 min |
| `make check` | lint plus everything above except `chaos` and `fuzz` | 2 to 3 min |

Times are from the development machine with a warm build cache. The widest
sweeps run so far: 1,500 seeds per Raft scenario (52 s), 400 storage
workloads with 84,524 crash points (79 s), 3,000 linearizability seeds with
2.06 million operations (50 s).

Seed counts can be raised: `make sim RAFT_SIM_SEEDS=5000`. A failing seed is
replayed with `RAFT_SIM_SEED=<n>`, `STORAGE_CRASH_SEED=<n>` or
`LIN_SIM_SEED=<n>`; the failure message prints the seed and, for Raft, a
trace of role changes and injected faults.

## The Raft simulator

`internal/testutil/sim` runs N Raft cores in one goroutine. One seeded
generator decides message delays, drops and duplicates, election timeouts,
and (in the chaos test) which fault to inject next. "Disk" is a struct that
is updated when a `Ready` says to persist, before its messages are released;
a crash throws away everything else.

After every tick it checks:

- **Election safety**: no term ever has two leaders.
- **Leader completeness**: a node that becomes leader holds every entry
  committed before its election.
- **Commit safety**: no two nodes consider different entries committed at
  the same index.
- **State machine safety**: every node applies the same entry at each index.
- per node: the term never decreases, a vote within a term never changes,
  the commit index never decreases, `applied ≤ commit ≤ last`, nothing is
  written over a committed index, and the stored log has no gaps.

Scenarios (each run over many seeds): initial election with 1, 3 and 5
nodes; split votes on a lossy network without PreVote; replication under
duplication, reordering and loss; a follower down and back; a leader down
and back; an isolated leader in a five-node cluster; loss and return of the
quorum; an isolated follower with and without PreVote; a one-way link
failure; catch-up from a snapshot; full-cluster restart, with and without
the stored commit index; and `TestSimChaos`, which mixes crashes, restarts,
random partitions, one-way blocks, healing and network degradation for 3,000
ticks while clients propose to anyone who claims to be leader, including
stale leaders, then heals everything and requires convergence with no
acknowledged command lost.

A note on what "leader" means in these tests. During a partition two nodes
can each believe they lead, in different terms. That is allowed. The
invariant is one leader *per term*, plus the fact that the stale one cannot
commit. Tests assert those, not that role flags agree at every instant.

## History checking

`tests/linearizability` records, for every client operation, when it was
invoked, when it returned, what was asked and what was answered, and gives
the history to [Porcupine](https://github.com/anishathalye/porcupine) with a
sequential specification of the API (`model.go`).

Decisions that matter:

- **A retried request is one operation**, from its first attempt to its
  final answer. If a retry were applied twice, the history would contain an
  effect no single operation accounts for.
- **A request with no definite answer is recorded as unknown**, with an
  infinite return time. The specification is nondeterministic at that one
  point: stepping over an unknown write yields both "it happened" and "it
  did not". The checker may place it anywhere after its invocation, or
  nowhere. Timed-out writes are *not* discarded as failures.
- **A request that no node ever accepted** (every attempt refused as "not
  leader") did not happen and is left out.
- **Timestamps** come from one clock: the simulator's event counter, or, in
  the live test, one monotonic clock in the test process read just before
  sending and just after receiving.
- **A checker timeout is a failure of the test**, reported as inconclusive.
  It is never counted as a pass.
- Compare-and-swap in histories uses value and absence conditions. Revisions
  are log indexes, which the specification cannot predict; they are covered
  by unit tests instead.

The checker is itself tested:

- `TestModelOnKnownHistories` runs 23 hand-written histories whose verdict
  is known: legal ones (including timed-out writes that did, did not, and
  only later took effect) and illegal ones (a stale read, a lost
  acknowledged write, two CAS winners, a CAS reported as failed that
  changed the value).
- `TestCheckerCatchesReadsServedFromLocalState` replaces reads with "any
  node that believes it is leader answers from memory" and requires the
  checker to reject at least one of 40 runs. It rejects 3.
- `TestCheckerCatchesRetriesUnderANewIdentity` makes clients retry under a
  fresh identity, defeating de-duplication. The checker rejects 32 of 40.

The last two are what give a passing result its meaning: the harness
demonstrably notices the two bugs it exists to catch.

What a pass does and does not show: the recorded histories are
linearizable. That is evidence about the executions that were run. It is
not a proof about the ones that were not.

## Failure demonstrations

Each script checks its claims and exits non-zero if one fails. They need the
cluster from `scripts/cluster.sh up` (three nodes plus the test-only override
that allows packet filtering). `scripts/demo.sh` runs them all.

The four kinds of fault are different and the scripts keep them apart:

| Fault | What happens | Script |
|---|---|---|
| Crash | the process is gone; its ports refuse connections; unsynced data is lost | `demo-kill-leader.sh` |
| Symmetric partition | the node runs and answers clients, but no peer traffic passes in either direction | `demo-partition.sh` |
| Single link failure | one pair of nodes cannot talk; every other pair can | `demo-link-failure.sh` |
| Quorum loss | too few nodes remain to commit anything | `demo-quorum-loss.sh`, `demo-five-node.sh` |

Added latency is a fifth kind (messages arrive, late). It is exercised in
the simulator, where delays of up to 14 ticks reorder messages; there is no
Docker demo for it.

Partitions are made with `iptables` rules inside the container, in a chain
of the script's own (`RAFTKV_CHAOS`); healing deletes that chain and nothing
else. Before relying on a partition, `demo-partition.sh` verifies it: from
each side, a TCP connection to the other's peer port must fail, and the
isolated node's client port must still answer.

### What the runs showed

From `make chaos` on the development machine (WSL2, Docker Engine 29.1,
default timing: 100 ms ticks, election after 1 to 2 s).

**Kill the leader.** New leader observed 0.8 s and 1.1 s after `SIGKILL` in
two runs. Twenty acknowledged writes, ten before and ten after, all present
once the old leader rejoined as a follower.

**Isolate the leader.** A write and a read sent straight to the isolated
node, each with a 4 s deadline, both returned `504` (`TIMEOUT`, outcome
unknown): the node had taken them into its log and could not commit them.
The other two nodes elected a leader in the next term and acknowledged five
writes. The isolated node gave up leadership, reported `/healthz` 200 and
`/readyz` 503, and refused further reads. After healing, all three nodes
reported the same applied index and key count, and all ten acknowledged
writes were present. The write that had timed out on the isolated node was
absent in these runs: its log entry was overwritten by the new leader. The
script reports that outcome and does not assert it, because a timed-out
write is *allowed* to take effect.

**Break one link.** With the leader unable to reach one follower for 12 s,
24 writes were acknowledged and the leader and term did not change. The
cut-off follower sat in the pre-candidate role with its term unchanged, fell
24 entries behind, and caught up after healing.

**Lose the quorum.** With two of three nodes stopped, the survivor answered
a write with `504` and a read with `503`, then reported not ready; `kvctl`
exited 1. When the nodes returned, service resumed.

**Catch up from a snapshot.** With a follower stopped, about 3,000 writes
made the leader snapshot and drop the log the follower needed. On restart
the follower's `raftkv_snapshots_installed_total` went to 1 and its log
began after the snapshot. Its key count and state size then matched the
leader's.

**Five nodes.** Writes and reads continued with two nodes stopped, stopped
being acknowledged with three stopped, and resumed 1 to 2 s after one came
back. Across runs the key ended up holding either the last acknowledged
value or the value from the write attempted without a quorum. Both are
legal: that attempt was reported as unknown, and whether its entry survives
depends on which node wins the next election.

## Bugs found during development

These are real, from building this repository. Each has a regression test.

**In the Raft core, found by the simulator:**

1. *Entries lost when a snapshot arrived among unpersisted entries.* A
   follower received entries and, before they were written, a snapshot
   covering some of them. The bookkeeping for "what still needs writing"
   ended up pointing below the new start of the log and the remaining
   entries were never written. Found by `TestSimChaos` seeds 17 and 31 with
   five nodes. Fixed in `raftLog.compactTo`;
   `TestSnapshotAmongUnpersistedEntriesKeepsTheRest`.
2. *A snapshot with a lost acknowledgement was never resent.* The leader
   waited for an outcome that was not coming, and the follower stayed
   behind indefinitely. Found by `TestSimChaos` seed 5 with three nodes.
   Fixed with a retry timer; `TestSnapshotWithUnknownOutcomeIsRetried`.
3. *Traffic amplification.* Every success response triggered another send,
   including duplicates and responses reporting nothing new. With duplicated
   messages and steady load each one started an extra request-response
   chain and the message count grew exponentially. Found when the
   linearizability simulation, the first test with sustained client load,
   stopped making progress. Fixed by sending only on real progress or on
   the heartbeat timer; `TestResponsesWithoutProgressDoNotTriggerSends`.

**In storage, found by crashing at every filesystem operation:**

4. *Hole in the log after an interrupted snapshot install*
   (`TestRegressionInterruptedInstallIsCompletedOnRecovery`).
5. *Log dropped on replay* when a snapshot marker's justification had been
   deleted with older segments
   (`TestRegressionKeptSuffixSurvivesSegmentDeletion`).
6. *False corruption report* for a suffix replacement reaching below the
   first surviving segment
   (`TestRegressionSuffixReplacementBelowVisibleLogStart`).
7. *Term and vote could be lost* after an interrupted segment rotation
   followed by compaction. Found by review while writing the crash test, not
   by the test (`TestRegressionHardStateSurvivesInterruptedRotation`).

**In the transport and client, found by the Docker demos:**

8. *A restarted node was unreachable for up to 30 s.* gRPC's DNS resolver
   caches addresses, and a restarted container usually gets a new one. With
   three of five nodes up, the cluster had no leader for 26 s. Fixed by
   resolving on every connection attempt. Guarded by `demo-five-node.sh`,
   which requires writes to resume within 15 s.
9. *The client gave up during elections.* A follower that has not noticed
   its leader is dead keeps hinting at it; the client bounced between the
   two until it ran out of attempts.
   `TestHintToAnUnreachableLeaderIsNotFollowedTwice`.

**In the benchmark setup:**

10. *The first benchmark numbers were measuring nothing.* The data directory
    was under `/tmp`, which on the development machine is `tmpfs`, where
    `fsync` returns immediately. `scripts/bench.sh` now refuses to run on
    `tmpfs`, and `scripts/local-cluster.sh` defaults to the home directory.

## Limits of the testing

- No test injects faults below the filesystem interface: no real power
  cuts, no disks that lie about `fsync`.
- Network faults against real processes are limited to the scripted
  scenarios. Random fault schedules run only in the simulator and the
  in-process cluster.
- The simulator runs the Raft core and the state machine but not the node's
  event loop, the WAL or the transport. Those are covered by the other
  layers, with less schedule diversity.
- The live history test is not reproducible from a seed.
- Linearizability checking covers three keys and value-based CAS.
- CI has been written but, at the time of writing, has not run on GitHub.
