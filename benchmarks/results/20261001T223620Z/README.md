# Benchmark results, 2026-10-01

Raw output of `scripts/bench.sh` and `make bench-micro`. How to read them,
and what they do and do not show, is in
[docs/benchmarks.md](../../../docs/benchmarks.md).

| File | Contents |
|---|---|
| `environment.txt` | machine, kernel, Go version, data directory, run length |
| `n{1,3,5}-{write,read,mixed,cas-contended}.json` | the cluster-size matrix, 16 clients |
| `n3-write-clients{1,4,16,64}.json` | concurrency sweep on three nodes |
| `n3-write-value4k.json` | 4 KiB values |
| `n3-write-UNSAFE-nofsync.json` | fsync disabled; a weaker guarantee, not a like-for-like result |
| `n3-failover-{1,2,3}.json` | leader killed about 8 s into a 20 s run |
| `micro-benchmarks.txt` | `go test -bench` output for the WAL, the state machine and the codec |
| `console.log` | what `scripts/bench.sh` printed |

Each JSON report holds the workload settings, the environment, the commit
the binaries were built from, and for every run: requests attempted and
completed, throughput, latency percentiles overall and per operation, a
count of each result, the longest gap without a successful request, a
timeline in 100 ms buckets, and the leader before and after.

Commit `f25bbd4` for everything except the failover runs.

The failover case was run twice. The first attempt killed the leader about
2 s into the measured phase instead of 8 s, because the load generator was
still writing its keys when the timer started; `console.log` shows that
attempt (longest gap 1.3 s). The script was corrected in commit `824a26a`
and the three `n3-failover-*.json` files are from the corrected run. The
mistimed report was discarded.
