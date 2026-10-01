# Experiment: overlapping the leader's disk write with replication

**Result: inconclusive on the test machine. The change was not kept.**

Date: 2026-10-01. Machine: Intel Core i5-1135G7 (4 cores / 8 threads), WSL2
on Windows 11, ext4 on a WSL2 virtual disk. Three `kvserver`
processes on that one machine, sharing that one disk, loopback networking,
fsync on.

## What was profiled

A 10-second CPU profile of the leader while 64 clients wrote 128-byte values
to a three-node cluster (`benchmarks/profiles/leader-cpu-before-optimization.txt`):

- total CPU was 8.2 s over 10 s of wall time on an 8-thread machine: the
  leader was not CPU-bound;
- 28% of samples were in the system call entry (`write`, `fsync`, socket I/O)
  and 11% in `futex`; nothing in this repository's own code reached 2%;
- the node's own metrics agree. In a later 10-second run with 16 clients
  (on the overlapped build described below) the leader performed 4,331
  fsyncs totalling 8.46 s (`raftkv_wal_fsync_duration_seconds_sum`): its
  event loop spent about 85% of the run waiting for the disk.

So there was no hot function to optimise. The cost of a write was structural:
the leader wrote and fsynced an entry, *then* sent it, and each follower then
wrote and fsynced it. A write therefore paid for two fsyncs in series, plus
waiting for the event loop to come back from whatever fsync it was in.

## What was tried

Two changes to the node's event loop, both safe by the Raft rules:

1. **Send before persist.** The leader's `AppendEntries` requests left before
   the leader's own write instead of after it, so the followers' fsyncs ran at
   the same time as the leader's. This is safe because the leader counts its
   own copy toward the quorum only once it is durable (it already did), and an
   entry on a quorum of followers' disks is committed regardless of the
   leader's disk (Ongaro's dissertation, section 10.2.1).
2. **Apply before the next write.** Committed entries that were already
   durable locally were applied, and their clients answered, before starting
   the fsync for the next batch rather than after it.

The deterministic simulator was extended to crash a node between "sent" and
"persisted". The Raft simulation (1,500 seeds) and the linearizability
simulation (2,000 seeds, 1.25 million operations) passed with the changes in.

## Measurements

Closed-loop writes, 128-byte values, 1,000 keys.

First comparison, three 10-second runs per cell after a 2-second warm-up
(raw reports in `benchmarks/experiments/overlapped-replication/`):

| clients | serial (baseline)       | change 1 only           | changes 1 and 2         |
|--------:|-------------------------|-------------------------|-------------------------|
| 1       | 216 ops/s, p50 4.45 ms  | 221 ops/s, p50 4.21 ms  | 270 ops/s, p50 3.57 ms  |
| 16      | 1,418 ops/s, p50 10.4 ms| 982 ops/s, p50 16.1 ms  | 1,374 ops/s, p50 11.7 ms|
| 64      | 4,136 ops/s, p50 15.2 ms| 3,686 ops/s, p50 16.7 ms| 3,661 ops/s, p50 17.8 ms|

Those were run one variant after another, minutes apart, and the spread
between runs of the *same* variant was 10 to 15 percent. To separate the
change from drift, both variants were then built as separate binaries and run
alternately, three rounds, one 8-second run per cell after a 2-second warm-up:

| clients | serial: three rounds (ops/s) | mean  | overlapped (1 and 2): three rounds | mean  | difference |
|--------:|------------------------------|------:|------------------------------------|------:|-----------:|
| 1       | 241, 170, 200                | 204   | 170, 224, 252                      | 215   | +5%        |
| 16      | 1,742, 1,245, 1,139          | 1,375 | 1,362, 1,126, 970                  | 1,153 | −16%       |
| 64      | 4,770, 3,971, 4,801          | 4,514 | 5,072, 4,029, 5,018                | 4,706 | +4%        |

Within one variant the rounds differ by 20 to 40 percent. The differences
between variants are smaller than that and point in both directions.

## Conclusion

On this machine the change cannot be shown to help, and cannot be shown to
hurt. The likely reason is the test bed, not the idea: all three nodes fsync
to the same virtual disk, so their flushes queue behind each other in the
same journal whether the nodes issue them one after another or at the same
time. Overlapping fsyncs can only pay off when each node has its own device.
That configuration was not available, so nothing is claimed about it.

Because the benefit was not demonstrated, the event loop was returned to the
simpler order: persist, then send, then apply. One line of reasoning about
ordering is easier to hold in your head, and to defend, than two.

What stayed:

- the Raft core still counts the leader's own copy only after `Advance`
  (`maybeCommit` in `internal/raft/raft.go`), so the experiment can be
  repeated by changing only `processReady` in `internal/node/node.go`;
- `TestEntriesHeldByAQuorumOfFollowersCommitBeforeTheLeadersWriteFinishes`
  pins the property that makes the change safe.

## Reproducing

```
git stash   # or work on a branch
# in internal/node/node.go processReady: dispatch MsgApp/MsgSnap before
# n.store.Save, and apply committed entries with index < rd.Entries[0].Index
# before it
make build
scripts/local-cluster.sh start 3
bin/kvbench --workload write --clients 16 --duration 10s --warmup 2s --runs 3
```

Run the two builds alternately, several rounds each, and compare means
against the spread between rounds before believing a difference.
