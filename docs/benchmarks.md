# Benchmarks

Measured throughput and latency, how they were measured, and what they do
and do not tell you. Every number here comes from a report file in
[`benchmarks/results/20261001T223620Z/`](../benchmarks/results/20261001T223620Z/).

Read the limits first. These numbers describe **one laptop running every
node, with all nodes sharing one virtual disk**. They show how the design
behaves and where its time goes. They are not capacity figures for a
deployment, and they are not comparable with numbers published for other
systems on other hardware.

## Setup

| | |
|---|---|
| Date, commit | 2026-10-01; `f25bbd4` (failover runs: `824a26a`, which changed only the benchmark tooling) |
| Machine | Intel Core i5-1135G7 at 2.40 GHz, 4 cores / 8 threads, 7.6 GiB RAM visible to WSL2 |
| OS | Ubuntu on WSL2, kernel 6.18.33.2-microsoft-standard-WSL2, on Windows 11 |
| Go | 1.27.1 |
| Storage | ext4 on the WSL2 virtual disk (`stat` reports the filesystem family as "ext2/ext3") |
| Topology | 1, 3 or 5 `kvserver` processes on that machine, loopback networking |
| Limits | none: host processes, no containers, no CPU or memory limits |
| Durability | fsync on for every acknowledged write, except the one run marked UNSAFE |
| Reads | every read is replicated through the log; no ReadIndex, no leases |
| Timing | defaults: 100 ms tick, election after 1 to 2 s, heartbeat every 200 ms |
| Load | `kvbench`, closed loop, on the same machine |
| Run length | 3 runs of 15 s per configuration, each after a 3 s warm-up that is not counted |
| Keys and values | 1,000 keys chosen uniformly; 128-byte values unless stated |

Two properties of this setup shape everything below:

- **One disk for all nodes.** In a real cluster each node has its own disk
  and their fsyncs run in parallel. Here they queue behind each other, so a
  five-node cluster does five fsyncs per batch on one device. Adding nodes
  costs more here than it would on separate machines.
- **Load generator and servers share the CPU.** With 64 clients and 3
  servers on 8 threads, they compete.

An fsync on this disk takes about 1.5 ms (below). Since WSL2 puts a
hypervisor between the filesystem and the physical device, that figure says
how long the flush call takes, not what the physical device guarantees.

### About the load generator

`kvbench` is a **closed loop**: each client sends a request, waits for the
answer, then sends the next. Throughput is therefore whatever N patient
clients achieve, and when the server slows down the clients slow down with
it. That hides queueing: it understates the latency that requests arriving
at a fixed rate would see during a stall (coordinated omission). The results
answer "what do N concurrent clients get", not "what happens at X requests
per second". Overload behaviour was tested separately
(`TestOverloadIsRefusedNotQueuedForever`), not benchmarked.

Each client may retry a request up to 4 times with the same identity. A
request counts as completed when it gets a definite successful answer.
Latency is measured from the first attempt to that answer.

## Results

Throughput is the mean of three runs, with the lowest and highest run.
Latencies are means of the three runs' percentiles. In every run below,
every request completed: no errors and no unknown outcomes.

### Cluster size, 16 clients

| Workload | 1 node | 3 nodes | 5 nodes |
|---|---|---|---|
| **Write** (PUT) | 3,106 ops/s (2,965 to 3,246) | 1,258 ops/s (1,132 to 1,376) | 1,038 ops/s (939 to 1,219) |
| p50 / p95 / p99 | 4.7 / 7.8 / 11.0 ms | 11.6 / 20.0 / 31.0 ms | 14.0 / 26.8 / 50.3 ms |
| **Read** (GET) | 3,187 ops/s (2,786 to 3,427) | 1,406 ops/s (1,069 to 1,582) | 1,104 ops/s (879 to 1,241) |
| p50 / p95 / p99 | 4.3 / 9.0 / 13.6 ms | 11.0 / 18.4 / 29.4 ms | 13.5 / 24.7 / 40.4 ms |
| **Mixed** (90% read) | 3,262 ops/s (2,467 to 3,669) | 1,535 ops/s (1,363 to 1,634) | 1,135 ops/s (940 to 1,386) |
| p50 / p95 / p99 | 4.4 / 8.4 / 12.6 ms | 9.1 / 16.9 / 25.3 ms | 13.2 / 24.0 / 33.2 ms |
| **Contended CAS** (4 keys) | 1,633 ops/s (1,365 to 1,803) | 688 ops/s (511 to 780) | 473 ops/s (378 to 555) |
| p50 / p95 / p99 | 8.8 / 15.7 / 22.9 ms | 22.3 / 37.4 / 53.8 ms | 28.5 / 63.1 / 116.6 ms |

What to take from it:

- **Reads cost what writes cost.** That is the price of putting reads in the
  log ([ADR 0003](adr/0003-reads-through-the-log.md)): a read is a log
  entry, an fsync on the leader and on a quorum of followers. A read-heavy
  workload is no faster than a write-heavy one.
- **One node to three cuts throughput by about 60% and raises median
  latency about 2.5 times.** A single node needs one fsync to commit. Three
  nodes need the leader's and then a follower's, in series, on a disk they
  share.
- **Three to five costs little more**, about 17% on writes. A quorum of five
  is three, so the leader waits for the two fastest of four followers, but
  on this machine all five fsync to the same disk.
- **The CAS workload** counts one operation as a read followed by a
  conditional write, so it is two trips through the log; it runs at about
  half the write rate. Sixteen clients on four counters collide constantly:
  two thirds of attempts lost the race (20,629 of 30,948 on three nodes) and
  were correctly refused. A lost race is counted as a completed operation,
  because the client got a correct, definite answer.

### Concurrency: what batching buys (3 nodes, writes)

| Clients | Throughput | p50 | p95 | p99 |
|---:|---|---:|---:|---:|
| 1 | 171 ops/s (169 to 175) | 5.6 ms | 8.0 ms | 9.5 ms |
| 4 | 380 ops/s (314 to 427) | 10.2 ms | 16.9 ms | 23.6 ms |
| 16 | 1,130 ops/s (813 to 1,361) | 13.9 ms | 24.0 ms | 39.4 ms |
| 64 | 3,795 ops/s (3,223 to 4,271) | 15.7 ms | 28.2 ms | 46.9 ms |

From 1 client to 64, throughput rises 22-fold while median latency rises
less than 3-fold. One client waits for its own fsyncs: 5.6 ms is the
leader's fsync, the round trip, the follower's fsync and the way back, one
after another. With many clients, everything that arrives while the event
loop is in fsync is written and synced together in the next round, so the
number of fsyncs grows far more slowly than the number of requests.

The 16-client row is the same configuration as "3 nodes, write" in the
first table, measured a few minutes later: 1,130 against 1,258 ops/s. That
10% gap between two measurements of the same thing is the noise floor of
this setup. Individual runs differ by up to 40%.

### Value size (3 nodes, 16 clients, writes)

| Value size | Throughput | p50 | p99 |
|---|---|---:|---:|
| 128 bytes | 1,258 ops/s | 11.6 ms | 31.0 ms |
| 4 KiB | 1,080 ops/s | 13.1 ms | 36.7 ms |

Values 32 times larger cost 14%. The cost of a write is dominated by the
fsync, not by the bytes.

### What fsync costs

One run has fsync switched off (`--unsafe-no-fsync`). **It does not offer
the same guarantee and is not an alternative configuration.** Without fsync,
writes that were acknowledged can be lost if the machines lose power. It is
here only to show where the time goes.

| 3 nodes, 16 clients, writes | Throughput | p50 | p99 | Guarantee |
|---|---|---:|---:|---|
| fsync on | 1,258 ops/s | 11.6 ms | 31.0 ms | acknowledged writes survive power loss |
| **UNSAFE: fsync off** | 9,964 ops/s | 1.4 ms | 4.9 ms | acknowledged writes can be lost on power loss |

With the disk taken out, the same code handles eight times the load. On this
machine the system is bound by fsync, not by CPU, the network or the
protocol.

### Failover

Three separate runs. In each, 16 clients ran a 50/50 read-write mix against
three nodes for 20 s, and the leader was killed with `SIGKILL` about 8 s in
and not restarted. These are reported separately on purpose: their latency
percentiles include the interruption and are not healthy-cluster numbers.

| Run | Longest gap with no successful request | Slowest request | Requests | Failed | Unknown outcome |
|---|---:|---:|---:|---:|---:|
| 1 | 1.85 s | 4.17 s | 32,940 | 0 | 0 |
| 2 | 1.43 s | 3.23 s | 32,373 | 0 | 0 |
| 3 | 1.39 s | 3.59 s | 28,465 | 0 | 0 |

For 1.4 to 1.9 s no request succeeded. That is the election timeout (1 to
2 s with the default settings) plus the election itself. Then the surviving
two nodes had a leader and work resumed. No request failed: the clients
retried, with the same request identity, until the new leader answered, and
the slowest of them took 3 to 4 s. Shorter ticks would shorten the election
part of the gap, at the price of spurious elections whenever the network or
the disk stalls for longer than the timeout. That trade-off was not
measured.

## Micro-benchmarks

`make bench-micro`, three runs each, on the same machine and disk
(`micro-benchmarks.txt` in the results directory).

**WAL append with fsync**, 128-byte entries, to a real file:

| Entries per fsync | Time per fsync | Entries per second |
|---:|---:|---:|
| 1 | 1.46 to 1.52 ms | 658 to 685 |
| 8 | 1.36 to 2.15 ms | 3,726 to 5,871 |
| 64 | 1.74 to 1.98 ms | 32,296 to 36,729 |
| 256 | 1.74 to 3.12 ms | 82,169 to 147,322 |

An fsync costs about the same whether it covers one entry or 256. This is
the whole case for batching, in one table.

**Without the disk** (in-memory filesystem): framing and checksumming a
128-byte entry takes about 0.9 µs.

**State machine and codec**, per operation:

| Operation | Time | Allocations |
|---|---:|---:|
| Encode a PUT | 84 to 109 ns | 1 |
| Decode a PUT | 159 to 196 ns | 5 |
| Apply a PUT (decode, validate, de-duplicate, write) | 377 to 476 ns | 5 |
| Apply a READ | 125 to 207 ns | 2 |

**Snapshots**, 10,000 keys with 128-byte values (1.7 MB): encoding runs at
450 to 550 MB/s and restoring at 400 to 440 MB/s. A full 64 MiB store would
take a little over 100 ms to serialise, during which the event loop is busy.

**Recovery**: reopening a store whose log holds 20,000 entries takes 9 to
11 ms on the in-memory filesystem.

The state machine applies a write in under half a microsecond. A committed
write takes about ten milliseconds end to end. The difference is the disk
and the round trips.

## Profiling, and an optimisation that did not pay

A CPU profile of the leader under 64 clients showed no hot spot: about 28%
of samples in system calls, 11% in the scheduler's futex, nothing in this
repository's code above 2%, and the process using under one of eight
threads. The node was waiting for the disk.

That pointed at the structure: the leader fsyncs, then sends, then the
follower fsyncs. Sending before the leader's fsync, so the two overlap, is
safe by the Raft rules, so it was implemented and measured. An interleaved
A/B comparison showed differences of +5%, −16% and +4% at 1, 16 and 64
clients, inside the run-to-run noise. The likely reason is that all nodes
share one disk here, so their fsyncs cannot overlap however they are issued.
The change was removed.

The full record, with raw numbers, is in
[`benchmarks/experiments/overlapped-replication.md`](../benchmarks/experiments/overlapped-replication.md).

## Reproducing

```sh
make build
scripts/bench.sh                 # the whole matrix, about 25 minutes
BENCH_QUICK=1 scripts/bench.sh   # short runs, to check the setup
make bench-micro
```

`scripts/bench.sh` starts clusters of plain processes with their data under
`$RAFTKV_BENCH_DIR` (default `~/.cache/raftkv-bench`), refuses to run if that
is on `tmpfs`, checks that the cluster answering is the one it started, and
writes one JSON report per configuration with every run's numbers, a
100 ms timeline, the environment and the commit. Stop any other cluster on
ports 8001 and up first.

Expect different absolute numbers on different hardware, and run-to-run
variation of 10 to 40% on a laptop. The relationships are what should hold:
reads cost what writes cost, throughput grows with concurrency much faster
than latency, and fsync dominates.

## What was not measured

- Nodes on separate machines or separate disks. This is the most important
  gap: it is the configuration the system is meant for.
- Real network latency between nodes.
- An open-loop (fixed arrival rate) workload, and therefore latency under
  overload.
- State close to the 64 MiB limit, or behaviour while a large snapshot is
  being taken.
- Runs longer than 15 seconds.
- Any other system. No comparison with etcd, Redis or anything else is made
  or implied.
