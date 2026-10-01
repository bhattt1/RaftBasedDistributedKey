# Claims and evidence

Every claim made in the README, and every claim you might put on a resume,
with the code that implements it, the command that checks it, what that
command produced, and what the claim does not cover.

"Result" is what was observed on the development machine (Intel i5-1135G7,
Ubuntu on WSL2, Go 1.27.1) on 2026-10-01 and 2026-10-02. A result is a
statement about those runs.

## Consensus

| Claim | Implementation | Check | Result | Limits |
|---|---|---|---|---|
| Raft is implemented here, not wrapped | `internal/raft/` (about 1,300 lines); `go.mod` has no Raft library | `grep -i raft go.mod` shows only this module's own path | no etcd/raft or hashicorp/raft dependency | PreVote, check-quorum, snapshots included; no membership changes, no ReadIndex |
| At most one leader per term | `handleVote`, `wonVotes` in `raft.go`; vote persisted before the response | `make sim RAFT_SIM_SEEDS=1500` (checked after every simulated tick) | pass, 1,500 seeds for each of the 14 simulation tests | simulated network and disk |
| A committed entry is never lost or changed | `maybeCommit`, `handleAppend` | same; "leader completeness" and "state machine safety" checks in `sim.go` | pass | same |
| Old-term entries are not committed by counting replicas | `maybeCommit` | `go test ./internal/raft -run OldTermEntries` | pass | |
| A cut-off node does not disrupt the cluster on return | PreVote: `becomePreCandidate`, `handlePreVote` | `go test ./internal/raft -run 'PartitionedFollower\|WithoutPreVote'`; `scripts/demo-link-failure.sh` | term unchanged after 500 ticks of isolation; with PreVote off the term rises. Demo: 12 s link failure, no election | |
| A leader without a quorum steps down | `tickLeader`, `quorumActive` | `go test ./internal/raft -run CheckQuorum`; `scripts/demo-partition.sh` step 5 | isolated leader reported not ready | within one to two base election timeouts |

## Reads, writes and retries

| Claim | Implementation | Check | Result | Limits |
|---|---|---|---|---|
| Reads are linearizable | reads are log entries: `httpapi.get` to `node.Propose` to `kv.Store.Apply`; no other read path exists | `make linearizability LIN_SIM_SEEDS=3000` | pass: 3,000 seeds, 2,046,005 completed operations, all histories linearizable | evidence about recorded histories, not a proof; 3 keys, value-based CAS |
| The checker would notice a violation | `tests/linearizability/model.go` | `go test ./tests/linearizability -run 'TestModel\|TestChecker' -v` | 23 known histories classified correctly; local reads rejected in 3 of 40 runs; fresh-identity retries in 32 of 40 | |
| An isolated leader acknowledges neither reads nor writes | same path; nothing is answered without commit | `go test ./internal/node -run IsolatedLeader`; `scripts/demo-partition.sh` | write and read to the isolated node both returned 504 | |
| A minority cannot acknowledge anything | quorum from configured membership (`raft.New`) | `scripts/demo-quorum-loss.sh`; `scripts/demo-five-node.sh`; `go test -tags integration ./tests/integration -run FiveNode` | 1 of 3 and 2 of 5 nodes acknowledged nothing | |
| CAS is atomic | `kv.Store.mutate`: compare and write in one applied command | `go test ./internal/node -run ConcurrentCAS` | 8 clients, 120 increments, final value 120 | single key |
| A retried write is not applied twice | `kv.Store.Apply` session check; sessions in snapshots | `go test ./internal/node -run RetryAfterLeaderChange`; `go test -tags integration ./tests/integration -run GracefulRestart` | retry after failover and after full restart returned the original result with `duplicate: true` | latest request per client; 4,096 most recent clients |
| A timed-out write is reported as unknown, not failed | `node.propose`; `httpapi.writeProposeError` | `go test ./internal/httpapi -run Deadline` | 504, `"outcome":"unknown"` | |
| Overload is refused, not queued | bounded `proposeC`; `handleProposal` | `go test ./internal/node -run Overload` | requests beyond the bound got `ErrOverloaded` immediately; pending never exceeded it | |

## Durability and recovery

| Claim | Implementation | Check | Result | Limits |
|---|---|---|---|---|
| Nothing is acknowledged before it is fsynced | `node.processReady`: `Save` before `dispatch` | `go test ./internal/raft -run 'SameReady\|PersistsTerm\|OnlyAfterPersisting'` and the simulator's ordering | pass | relies on the device honouring fsync |
| Recovery is correct after a crash at any point | `internal/storage/wal.go`, `store.go` | `make crash STORAGE_CRASH_SEEDS=400` | pass: 400 workloads, 84,524 crash points in total (161 to 246 per workload); the recovered state matched the model every time | simulated filesystem |
| Acknowledged writes survive all nodes being killed | same | `go test -tags integration ./tests/integration -run AllNodesKilled -v` | every acknowledged write (over 500 per run) present after `SIGKILL` of all three; all three recovered from a snapshot plus log | `SIGKILL` loses no page cache; power loss is covered only by the simulated filesystem |
| Corruption is detected, not ignored | per-record and per-header CRC-32C; `replaySegment` | `go test ./internal/storage -run 'FlippedByte\|DamagedLength\|DamagedSnapshot'`; integration `CorruptLog` | every single flipped byte in a log and in a snapshot rejected; a real process with a damaged log exited 1 | detection only; no repair |
| A storage error stops the node | `wal.fail`; `node.run` | `go test ./internal/node -run StorageFailure` | node stopped, reported failed; the other two continued | |
| Snapshots and compaction work, including for a lagging follower | `node.maybeSnapshot`, `storage.CreateSnapshot`, `transport.sendSnapshot` | `scripts/demo-snapshot.sh`; `go test ./internal/transport -run Snapshot`; integration `CatchesUpFromSnapshot` | follower about 3,000 entries behind installed a snapshot; key count and size matched the leader | 64 MiB state limit |
| One process per data directory | `flock` in `lock_unix.go` | integration `SecondProcess` | second process exited 1 with "locked by another process" | Unix only |

## Operations and security

| Claim | Implementation | Check | Result | Limits |
|---|---|---|---|---|
| Failover with real processes | | `scripts/demo-kill-leader.sh`; integration `LeaderKilled` | new leader 0.8 to 1.1 s after `SIGKILL` (default timing); every acknowledged write present in the integration run | |
| Five-node configuration works | `docker-compose.5node.yml` | `scripts/demo-five-node.sh`; integration `FiveNode` | tolerated two failures; refused with three down; resumed 1 to 2 s after one returned | |
| Peers authenticate each other | `internal/security`; `transport.check` | `go test ./internal/security ./internal/transport` | wrong CA, wrong identity, impersonation and plaintext all refused | members are trusted once authenticated |
| Clients need a token in secure mode | `httpapi.auth` | integration `SecureCluster` | 401 without or with a wrong token; metrics too | one shared token |
| No goroutine leaks | | `goleak` in `internal/node` and `internal/transport` tests | pass | |
| No data races found | | `make test-race` | pass | the race detector finds races that occur, not all that could |
| The container does not run as root | `Dockerfile` | `docker compose exec n1 id` | `uid=10001(raftkv)` | the test-only override adds capabilities |

## Performance

All from [`benchmarks/results/20261001T223620Z/`](../benchmarks/results/20261001T223620Z/);
method and caveats in [benchmarks.md](benchmarks.md). **One laptop, all
nodes on one disk.**

| Claim | Check | Result | Limits |
|---|---|---|---|
| Three-node write throughput and latency | `scripts/bench.sh`; `n3-write.json` | 1,258 ops/s (1,132 to 1,376 over three runs), p50 11.6 ms, p99 31.0 ms, 16 clients, fsync on | this machine only; runs vary 10 to 40% |
| Batching scales throughput with concurrency | `n3-write-clients{1,64}.json` | 171 ops/s at 1 client, 3,795 at 64; median latency 5.6 to 15.7 ms | |
| fsync dominates | `n3-write-UNSAFE-nofsync.json`; `micro-benchmarks.txt` | 9,964 ops/s with fsync off against 1,258 with it on; one fsync about 1.5 ms | the fsync-off mode can lose acknowledged writes |
| Failover interruption | `n3-failover-{1,2,3}.json` | 1.4 to 1.9 s without a successful request; no failed requests | default timing |
| Overlapping leader fsync with replication does not help here | [`benchmarks/experiments/overlapped-replication.md`](../benchmarks/experiments/overlapped-replication.md) | differences within noise; change removed | says nothing about separate disks |

## Size and coverage

| Claim | Check | Result |
|---|---|---|
| Size | `find cmd internal tests -name '*.go' ! -path '*/gen/*' \| xargs wc -l` | about 10,000 lines of non-test Go and 8,600 of tests |
| Tests | `grep -rhE '^func (Test\|Fuzz\|Benchmark)' --include='*_test.go' . \| wc -l` | 205 test functions, 5 fuzz targets, 8 benchmarks |
| Coverage | `make cover` | 75.8% of statements in `internal/` and `cmd/` from unit and in-process tests. The three `main` packages are exercised by the integration tests, which are not instrumented, and generated code is in the denominator. |

## Not verified

- **CI on GitHub.** The workflows exist and each job's commands pass
  locally. No run on GitHub had been observed when this was written.
- **Docker Desktop.** Everything was run with Docker Engine inside WSL2.
- **Nodes on separate machines**, and real power loss.

## Resume bullets

Each is limited to what the table above supports.

- Built a Raft consensus core in Go from scratch (elections, log
  replication, PreVote, check-quorum, snapshots); verified it with a
  deterministic simulator across 1,500 seeded fault schedules.
- Designed a checksummed write-ahead log with crash recovery and snapshot
  compaction; a fault-injecting filesystem crashed it at every filesystem
  operation and exposed three recovery bugs, all fixed.
- Verified linearizable reads and duplicate-safe retries with Porcupine over
  2 million simulated operations under partitions and crashes; measured 1.4
  to 1.9 s failover with no failed client requests.

If you prefer a throughput figure in the third bullet, the supported one is:
"1,258 writes/s at 11.6 ms median on three nodes with fsync, on a single
laptop". Say "on a single laptop" if you use it.

To refresh any number before quoting it:

```sh
make sim RAFT_SIM_SEEDS=1500
make crash STORAGE_CRASH_SEEDS=400
make linearizability LIN_SIM_SEEDS=3000
scripts/bench.sh
```
