#!/usr/bin/env bash
# Runs the benchmark matrix and writes one JSON report per configuration to
# benchmarks/results/<UTC timestamp>/.
#
#   scripts/bench.sh            full matrix (about 20 minutes)
#   BENCH_QUICK=1 scripts/bench.sh   shorter runs, for checking the script
#
# The clusters are plain kvserver processes on this machine (no containers,
# no CPU or memory limits), talking over loopback, with their data under
# $RAFTKV_BENCH_DIR. That directory must be on a real disk: on tmpfs fsync
# does nothing and the "durable" numbers would be meaningless. The script
# refuses to run on tmpfs.
#
# Everything measured here uses fsync for every acknowledged write, except
# the one configuration explicitly labelled UNSAFE, which exists only to show
# what fsync costs and offers a weaker guarantee.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -x bin/kvserver && -x bin/kvbench && -x bin/kvctl ]] || { echo "build first: make build" >&2; exit 2; }

export RAFTKV_LOCAL_DIR="${RAFTKV_BENCH_DIR:-$HOME/.cache/raftkv-bench}"
mkdir -p "$RAFTKV_LOCAL_DIR"
FSTYPE="$(stat -f -c %T "$RAFTKV_LOCAL_DIR")"
if [[ "$FSTYPE" == "tmpfs" || "$FSTYPE" == "ramfs" ]]; then
  echo "$RAFTKV_LOCAL_DIR is on $FSTYPE, where fsync is a no-op. Set RAFTKV_BENCH_DIR to a directory on a real disk." >&2
  exit 2
fi

if [[ -n "${BENCH_QUICK:-}" ]]; then
  DURATION=4s WARMUP=1s RUNS=1
else
  DURATION=15s WARMUP=3s RUNS=3
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${BENCH_OUT:-benchmarks/results/$STAMP}"
mkdir -p "$OUT"

# The commit the binaries were built from. "-dirty" means code differed from
# that commit; documentation and result files are not counted.
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo uncommitted)"
if [[ -n "$(git status --porcelain -- cmd internal api scripts go.mod go.sum Makefile 2>/dev/null)" ]]; then COMMIT="$COMMIT-dirty"; fi

# BENCH_SECTIONS selects parts of the matrix; BENCH_OUT appends to an
# existing results directory instead of creating a new one.
SECTIONS="${BENCH_SECTIONS:-topology clients value nofsync failover}"
want() { [[ " $SECTIONS " == *" $1 "* ]]; }
DEVICE="$(df --output=source "$RAFTKV_LOCAL_DIR" | tail -n 1)"

cleanup() { scripts/local-cluster.sh reset >/dev/null 2>&1 || true; }
trap cleanup EXIT

endpoints() { scripts/local-cluster.sh endpoints; }

# wait_ready <nodes>: wait until the cluster accepts a write, then confirm
# that what answers is the cluster this script started: the right number of
# nodes, all reporting the local-dev cluster ID.
wait_ready() {
  local nodes="$1" deadline=$((SECONDS + 30)) seen
  until bin/kvctl --endpoints "$(endpoints)" --timeout 2s put bench/ready ok >/dev/null 2>&1; do
    ((SECONDS < deadline)) || { echo "cluster did not become ready" >&2; exit 1; }
    sleep 0.2
  done
  seen="$(bin/kvctl --endpoints "$(endpoints)" --json status | jq '[.[] | select(.status.cluster_id == "local-dev")] | length')"
  if [[ "$seen" != "$nodes" ]]; then
    echo "expected $nodes nodes of cluster local-dev, found $seen. Is another cluster using ports 8001+?" >&2
    exit 1
  fi
}

# start_cluster <nodes> [extra kvserver flags]
start_cluster() {
  local n="$1"
  shift
  scripts/local-cluster.sh reset >/dev/null 2>&1 || true
  RAFTKV_EXTRA_FLAGS="$*" scripts/local-cluster.sh start "$n" >/dev/null
  wait_ready "$n"
}

# bench <label> <nodes> <fsync on|off> <kvbench args...>
bench() {
  local label="$1" nodes="$2" fsync="$3"
  shift 3
  echo
  echo "### $label"
  bin/kvbench --endpoints "$(endpoints)" --label "$label" --output "$OUT/$label.json" \
    --duration "$DURATION" --warmup "$WARMUP" --runs "$RUNS" \
    --meta "commit=$COMMIT" \
    --meta "topology=$nodes kvserver process(es) on one machine, loopback networking" \
    --meta "container_limits=none (host processes, no cgroup limits)" \
    --meta "fsync=$fsync" \
    --meta "read_mode=every read is replicated through the log (no ReadIndex, no leases)" \
    --meta "storage=$FSTYPE on $DEVICE" \
    --meta "tick_interval=100ms election_ticks=10 heartbeat_ticks=2 (server defaults)" \
    "$@"
}

echo "results: $OUT"
echo "data dir: $RAFTKV_LOCAL_DIR ($FSTYPE on $DEVICE), commit $COMMIT"
[[ -f "$OUT/environment.txt" ]] || {
  echo "started: $STAMP"
  echo "commit: $COMMIT"
  echo "go: $(go version)"
  echo "kernel: $(uname -srm)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | xargs) ($(nproc) logical CPUs)"
  echo "memory: $(grep MemTotal /proc/meminfo | awk '{printf "%.1f GiB", $2/1048576}')"
  echo "data dir: $RAFTKV_LOCAL_DIR ($FSTYPE on $DEVICE)"
  echo "runs per configuration: $RUNS x $DURATION after $WARMUP warm-up"
} >"$OUT/environment.txt"

# ---- topology: 1, 3 and 5 nodes, four workloads each -------------------------
if want topology; then
  for nodes in 1 3 5; do
    start_cluster "$nodes"
    bench "n${nodes}-write" "$nodes" on --workload write --clients 16 --keys 1000 --value-bytes 128
    bench "n${nodes}-read" "$nodes" on --workload read --clients 16 --keys 1000 --value-bytes 128
    bench "n${nodes}-mixed" "$nodes" on --workload mixed --read-fraction 0.9 --clients 16 --keys 1000 --value-bytes 128
    bench "n${nodes}-cas-contended" "$nodes" on --workload cas --clients 16 --keys 4
  done
fi

# ---- concurrency sweep on three nodes: what group commit buys ----------------
if want clients; then
  start_cluster 3
  for clients in 1 4 16 64; do
    bench "n3-write-clients${clients}" 3 on --workload write --clients "$clients" --keys 1000 --value-bytes 128
  done
fi

# ---- value size on three nodes ----------------------------------------------
if want value; then
  start_cluster 3
  bench "n3-write-value4k" 3 on --workload write --clients 16 --keys 1000 --value-bytes 4096
fi

# ---- fsync disabled: a different, weaker guarantee ---------------------------
if want nofsync; then
  start_cluster 3 --unsafe-no-fsync
  bench "n3-write-UNSAFE-nofsync" 3 "OFF (acknowledged writes can be lost on power failure; not comparable with the durable runs as a like-for-like result)" \
    --workload write --clients 16 --keys 1000 --value-bytes 128
fi

# ---- failover: kill the leader in the middle of a run ------------------------
# Three separate runs, each on a fresh cluster. The keys are written first so
# that the measured run starts at once and the kill lands where intended:
# 1 s of warm-up, then 8 s into a 20 s measured phase.
if want failover; then
  for i in 1 2 3; do
    start_cluster 3
    bin/kvbench --endpoints "$(endpoints)" --workload read --clients 4 --keys 1000 --value-bytes 128 \
      --duration 1s --warmup 0s --runs 1 >/dev/null
    leader="$(bin/kvctl --endpoints "$(endpoints)" --json status | jq -r '.[] | select(.status.role == "leader") | .status.node_id')"
    echo
    echo "### n3-failover-$i (killing leader $leader 8s into a 20s run)"
    (
      sleep 9
      scripts/local-cluster.sh kill "$leader" >/dev/null
    ) &
    killer=$!
    bin/kvbench --endpoints "$(endpoints)" --label "n3-failover-$i" --output "$OUT/n3-failover-$i.json" \
      --workload mixed --read-fraction 0.5 --clients 16 --keys 1000 --value-bytes 128 --preload=false \
      --duration 20s --warmup 1s --runs 1 --attempts 30 --attempt-timeout 2s \
      --meta "commit=$COMMIT" --meta "fsync=on" --meta "storage=$FSTYPE on $DEVICE" \
      --meta "topology=3 kvserver processes on one machine, loopback networking" \
      --meta "event=leader $leader killed with SIGKILL about 8s into the measured phase and not restarted" \
      --meta "note=this run measures the interruption clients see during failover; its latency percentiles are not healthy-cluster numbers"
    wait "$killer"
  done
fi

echo
echo "done. Reports are in $OUT"
