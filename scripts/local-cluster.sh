#!/usr/bin/env bash
# Runs a cluster of kvserver processes on this machine, without Docker.
# Insecure development mode only: no TLS, no authentication, loopback ports.
#
#   scripts/local-cluster.sh start [nodes]   start nodes (default 3) in the background
#   scripts/local-cluster.sh stop            stop them (SIGTERM, graceful)
#   scripts/local-cluster.sh kill <id>       kill one node with SIGKILL, e.g. n2
#   scripts/local-cluster.sh restart <id>    start one node again
#   scripts/local-cluster.sh status          show each node's view
#   scripts/local-cluster.sh reset           stop and DELETE all data
#
# Node nX listens on: peers 127.0.0.1:700X, clients http://127.0.0.1:800X,
# admin/metrics 127.0.0.1:900X. Data and logs live under $RAFTKV_LOCAL_DIR
# (default ~/.cache/raftkv-local).
#
# The default is under the home directory on purpose. /tmp is a RAM-backed
# tmpfs on many systems, where fsync does nothing: fine for trying things
# out, misleading for anything that is supposed to demonstrate durability.
set -euo pipefail

cd "$(dirname "$0")/.."
DIR="${RAFTKV_LOCAL_DIR:-$HOME/.cache/raftkv-local}"
BIN="${RAFTKV_BIN:-bin}"

peers() {
  local n="$1" out="" i
  for ((i = 1; i <= n; i++)); do
    out+="n${i}=127.0.0.1:$((7000 + i))@http://127.0.0.1:$((8000 + i)),"
  done
  echo "${out%,}"
}

endpoints() {
  local n="$1" out="" i
  for ((i = 1; i <= n; i++)); do out+="http://127.0.0.1:$((8000 + i)),"; done
  echo "${out%,}"
}

node_count() { cat "$DIR/nodes" 2>/dev/null || echo 3; }

start_node() {
  local id="$1" n="$2" i="${1#n}"
  if [[ -f "$DIR/$id.pid" ]] && kill -0 "$(cat "$DIR/$id.pid")" 2>/dev/null; then
    echo "$id is already running (pid $(cat "$DIR/$id.pid"))"
    return
  fi
  # shellcheck disable=SC2086
  nohup "$BIN/kvserver" \
    --insecure-dev \
    --node-id "$id" \
    --cluster-id local-dev \
    --data-dir "$DIR/data/$id" \
    --raft-listen "127.0.0.1:$((7000 + i))" \
    --http-listen "127.0.0.1:$((8000 + i))" \
    --admin-listen "127.0.0.1:$((9000 + i))" \
    --peers "$(peers "$n")" \
    ${RAFTKV_EXTRA_FLAGS:-} \
    >>"$DIR/$id.log" 2>&1 &
  echo $! >"$DIR/$id.pid"
  echo "started $id (pid $!), log: $DIR/$id.log"
}

stop_node() {
  local id="$1" sig="${2:-TERM}" pid
  [[ -f "$DIR/$id.pid" ]] || return 0
  pid="$(cat "$DIR/$id.pid")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "-$sig" "$pid"
    for _ in $(seq 1 100); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "$id (pid $pid) did not exit within 10s after SIG$sig" >&2
      return 1
    fi
    echo "stopped $id (SIG$sig)"
  fi
  rm -f "$DIR/$id.pid"
}

case "${1:-}" in
  start)
    n="${2:-3}"
    [[ -x "$BIN/kvserver" ]] || { echo "build first: make build" >&2; exit 1; }
    mkdir -p "$DIR"
    echo "$n" >"$DIR/nodes"
    fstype="$(stat -f -c %T "$DIR" 2>/dev/null || echo unknown)"
    if [[ "$fstype" == "tmpfs" || "$fstype" == "ramfs" ]]; then
      echo "note: $DIR is on $fstype; fsync is a no-op there, so nothing survives a reboot" >&2
    fi
    for ((i = 1; i <= n; i++)); do start_node "n$i" "$n"; done
    # A node that cannot bind its ports (another cluster is using them)
    # exits at once. Catch that here instead of letting the caller talk to
    # whatever else is listening.
    sleep 0.5
    for ((i = 1; i <= n; i++)); do
      if ! kill -0 "$(cat "$DIR/n$i.pid")" 2>/dev/null; then
        echo "n$i exited during startup:" >&2
        tail -n 3 "$DIR/n$i.log" >&2
        exit 1
      fi
    done
    echo "endpoints: $(endpoints "$n")"
    ;;
  stop)
    for ((i = 1; i <= $(node_count); i++)); do stop_node "n$i"; done
    ;;
  kill)
    stop_node "${2:?usage: kill <id>}" KILL
    ;;
  restart)
    start_node "${2:?usage: restart <id>}" "$(node_count)"
    ;;
  status)
    "$BIN/kvctl" --endpoints "$(endpoints "$(node_count)")" status
    ;;
  endpoints)
    endpoints "$(node_count)"
    ;;
  reset)
    for ((i = 1; i <= $(node_count); i++)); do stop_node "n$i" KILL || true; done
    rm -rf "$DIR"
    echo "removed $DIR"
    ;;
  *)
    sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
