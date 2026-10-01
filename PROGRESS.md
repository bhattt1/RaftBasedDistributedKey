# Progress

A record of what has been built, what was run to check it, what those runs
produced, and what is left. Three words are used precisely here:

- **Implemented**: the code exists.
- **Verified**: a command was run and its result is stated.
- **Not verified**: the code exists but the check could not be run here.

Last updated 2026-10-02.

## Status

The first release described in [docs/scope.md](docs/scope.md) is complete.
Eleven of the twelve acceptance gates are verified. The twelfth, a green CI
run on GitHub, is not: the workflows are written and every command they run
passes locally, but no run on GitHub has been observed.

## Environment

| | |
|---|---|
| Machine | Intel Core i5-1135G7 (4 cores, 8 threads), Windows 11 |
| Where things ran | Ubuntu 26.04 on WSL2, kernel 6.18.33.2-microsoft-standard-WSL2 |
| Go | 1.27.1 |
| Docker | Engine 29.1.3 and Compose 2.40.3, installed inside WSL2 (not Docker Desktop) |
| protoc and plugins | 36.2, protoc-gen-go 1.36.12, protoc-gen-go-grpc 1.6.2 |
| Filesystems | ext4 for data and benchmarks; `/tmp` is tmpfs |

The machine had none of the toolchain at the start. Go was installed under
the home directory; `build-essential`, `unzip`, `jq`, `docker.io` and
`docker-compose-v2` were installed in the WSL2 distribution with `apt`.

## Milestones

| # | Milestone | State |
|---|---|---|
| 0 | Scope, module, pinned tools, licence | done |
| 1 | Raft core as a deterministic state machine, with unit tests | done |
| 2 | Deterministic cluster simulator and chaos tests | done |
| 3 | Write-ahead log, snapshots, recovery, crash-injection tests | done |
| 4 | Key-value state machine, CAS, retry de-duplication | done |
| 5 | Node event loop; in-process cluster tests | done |
| 6 | Protocol definition, gRPC transport, mutual TLS | done |
| 7 | HTTP API, Go client | done |
| 8 | Configuration, metrics, `kvserver`, `kvctl`, `kvbench` | done |
| 9 | Linearizability checking | done |
| 10 | Real-process integration tests | done |
| 11 | Container image, Compose clusters, fault demos | done |
| 12 | Makefile, CI workflows, vulnerability check | done; CI not yet observed running |
| 13 | Benchmarks, profiling | done |
| 14 | Documentation | done |
| 15 | Audit, clean-checkout verification | done |

The commit history follows the same order, roughly one commit per milestone.

## Verification

Run on the final code unless a commit is named. "Clean clone" means a fresh
`git clone` into an empty directory on ext4, with no earlier containers,
volumes or images.

| Check | Command | Result |
|---|---|---|
| Formatting, vet, staticcheck | `make lint` | pass |
| Generated code matches | `make generate-check` | pass, no diff |
| Unit tests and default simulations | `make test` | pass, 10 packages, 15 s |
| Same under the race detector | `make test-race` | pass, 25 s |
| Raft simulation, wide | `make sim RAFT_SIM_SEEDS=1500` | pass, 52 s |
| Storage crash injection, wide | `make crash STORAGE_CRASH_SEEDS=400` | pass: 84,524 crash points, 79 s |
| Linearizability, wide | `make linearizability LIN_SIM_SEEDS=3000` | pass: 2,062,829 operations of which 16,824 had unknown outcomes; 50 s |
| Checker self-tests | part of `make linearizability` | 23 known histories classified correctly; both deliberately broken variants rejected (3 of 40 and 32 of 40 runs) |
| Live in-process history check | part of `make linearizability` | pass (about 117,000 operations under 26 injected faults in the last run) |
| Real processes | `TMPDIR=<dir on ext4> make integration` | pass, 11 tests, 32 s |
| Fuzzing | each of 5 targets for 12 to 20 s | no failures; not a long campaign |
| Cross-compile | `make build-windows` | pass |
| Dependency scan | `make vuln` | 1 finding, GO-2026-6443, an accepted exception documented in `.vuln-exceptions` |
| Coverage | `make cover` | 75.8% of statements in `internal/` and `cmd/` |
| **Clean clone**: build | `make build` | pass |
| **Clean clone**: start | `docker compose up -d --build` | three nodes up, one leader |
| **Clean clone**: README examples | `kvctl put/get/cas/delete`, the `curl` lines | outputs and exit codes as documented |
| **Clean clone**: restart keeps data | `docker compose down`, `up -d`, `kvctl get` | value still present |
| **Clean clone**: demos | `make demo` | the five three-node demos pass, 68 s. The five-node demo was added to `make demo` afterwards; all six pass on the main tree, 117 s including an image build |
| **Clean clone**: five nodes | `scripts/demo-five-node.sh` | pass |
| **Clean clone**: secure mode | `scripts/gen-dev-certs.sh`, `scripts/cluster.sh secure-up`, `kvctl` with and without token | works with CA and token; `UNAUTHENTICATED` without |
| Benchmarks | `scripts/bench.sh`, `make bench-micro` | completed; reports in `benchmarks/results/20261001T223620Z/` |
| CI on GitHub | push and watch | **not verified** |

### Acceptance gates

| Gate | Evidence |
|---|---|
| A fresh environment can follow the README | the clean-clone rows above |
| Three-node replication; five-node configuration | integration `LeaderKilled`, `FiveNode`; `demo-five-node.sh` |
| A minority cannot acknowledge | `demo-partition.sh`, `demo-quorum-loss.sh`, `TestIsolatedLeaderCannotAcknowledgeReadsOrWrites`, `TestSimIsolatedLeaderCannotCommit` |
| A majority recovers after leader failure | `demo-kill-leader.sh` (0.8 to 1.1 s); failover benchmark (1.4 to 1.9 s) |
| Acknowledged mutations survive the tested failures | integration `AllNodesKilled`, `LeaderKilled`; crash sweep; every demo's final check |
| Reads, CAS, retries match their documentation | linearizability suite; `TestConcurrentCAS...`; `TestRetryAfterLeaderChange...` |
| WAL recovery, snapshots, compaction, installation | crash sweep; `demo-snapshot.sh`; integration `CatchesUpFromSnapshot` |
| Race, integration, failure and history gates | rows above |
| Benchmarks reproducible, raw results present | `scripts/bench.sh`; `benchmarks/results/` |
| Modes, limits, gaps documented | `docs/operations.md` |
| Documentation matches the implementation | `docs/claims-and-evidence.md` |

## Bugs found and fixed along the way

Ten, each with a regression test or a script that guards it. Details in
[docs/testing-and-failures.md](docs/testing-and-failures.md#bugs-found-during-development).

1. Raft: entries lost when a snapshot arrived among unpersisted entries.
2. Raft: a snapshot whose acknowledgement was lost was never resent.
3. Raft: every acknowledgement triggered another send; traffic grew without
   bound under load with duplicated messages.
4. Storage: hole in the log after an interrupted snapshot install.
5. Storage: log dropped on replay after older segments were deleted.
6. Storage: valid suffix replacement reported as corruption.
7. Storage: term and vote could be lost after an interrupted rotation.
8. Transport: a restarted node unreachable for up to 30 s (DNS caching).
9. Client: gave up during elections by following a stale leader hint.
10. Benchmarks: first numbers taken on tmpfs, where fsync does nothing.

Also corrected: a failover benchmark that killed the leader at the wrong
moment (rerun), and two test assertions that read a node's status once
instead of waiting for it.

## Decisions that changed during the work

- **Overlapping the leader's fsync with replication** was implemented,
  passed all tests, was measured, showed no gain outside noise on a
  single-disk machine, and was removed. Recorded in
  `benchmarks/experiments/overlapped-replication.md`.
- **Snapshot markers in the WAL** gained a flag recording whether the log
  suffix was kept, after crash testing showed replay could not always
  recompute it.
- **A byte-based snapshot trigger** was added during the audit: a count of
  entries alone did not bound the memory the log could use.
- **gRPC's passthrough resolver** replaced the default DNS resolver after
  the five-node demo exposed the caching problem.

## Unresolved and unverified

- CI has not been observed running on GitHub.
- All nodes shared one disk in every benchmark. Nothing was measured with
  nodes on separate machines or disks.
- Not tested with Docker Desktop; Docker Engine inside WSL2 was used.
- No real power-loss test; durability across power loss rests on the
  simulated filesystem and on fsync behaving as documented.
- Fuzzing ran for seconds per target, not hours.
- One open advisory in the pinned gRPC release, with no released fix.
- `kvserver` runs on Linux only.

## Next

In order: membership changes; ReadIndex reads; snapshot export and key
listing; benchmarks on separate machines. See
[docs/scope.md](docs/scope.md#out-of-scope-future-work).

## Commands worth remembering

```sh
make build && make test            # build and fast tests
make check                         # everything CI runs
make demo                          # the full demonstration
scripts/cluster.sh up|down|status  # the Docker cluster (down keeps data)
scripts/local-cluster.sh start     # three plain processes, no Docker
scripts/bench.sh                   # benchmarks
RAFT_SIM_SEED=17 go test ./internal/raft -run TestSimChaos   # replay one seed
```
