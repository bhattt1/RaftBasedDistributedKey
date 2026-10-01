#!/usr/bin/env bash
# Five-node demo: starts the five-node Compose cluster, takes nodes away one
# at a time, and checks where the cluster stops acknowledging writes.
#
# Five nodes need three for a quorum, so two failures are tolerated and a
# third is not. This script starts and stops its own cluster (project
# "raftkv5", ports 8101-8105) and leaves the three-node one alone.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -x bin/kvctl ]] || { echo "build first: make build" >&2; exit 2; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

FIVE=(docker compose -f docker-compose.5node.yml)
NODES=(n1 n2 n3 n4 n5)
url_of() { echo "http://127.0.0.1:$((8100 + ${1#n}))"; }
ENDPOINTS="$(for n in "${NODES[@]}"; do url_of "$n"; done | paste -sd, -)"
kvctl() { bin/kvctl --endpoints "$ENDPOINTS" "$@"; }
role_of() { curl -fsS --max-time 2 "$(url_of "$1")/v1/status" 2>/dev/null | jq -r .role 2>/dev/null || true; }

ok() { echo "    ok   $*"; }
fail() { echo "    FAIL $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

leader() {
  local n
  for n in "${NODES[@]}"; do
    if [[ "$(role_of "$n")" == "leader" ]]; then echo "$n"; return; fi
  done
}
wait_leader() {
  local deadline=$((SECONDS + 60))
  until [[ -n "$(leader)" ]]; do
    ((SECONDS < deadline)) || fail "no leader within 60s"
    sleep 0.2
  done
  leader
}

cleanup() { "${FIVE[@]}" start "${NODES[@]}" >/dev/null 2>&1 </dev/null || true; }
trap cleanup EXIT

RUN="f$(date +%s)"

step "Start five nodes"
"${FIVE[@]}" up -d --build >/dev/null 2>&1 </dev/null
L="$(wait_leader)"
ok "leader is $L"
kvctl status | sed 's/^/    /'
kvctl put "$RUN/k" "five-up" >/dev/null
ok "write acknowledged with five nodes"

step "Stop two nodes, including the leader (three of five remain)"
VICTIMS=("$L")
for n in "${NODES[@]}"; do
  [[ "$n" != "$L" && ${#VICTIMS[@]} -lt 3 ]] && VICTIMS+=("$n")
done
"${FIVE[@]}" stop "${VICTIMS[0]}" "${VICTIMS[1]}" >/dev/null 2>&1 </dev/null
kvctl put "$RUN/k" "three-up" >/dev/null || fail "a write failed with three of five nodes running"
[[ "$(kvctl get "$RUN/k")" == "three-up" ]] || fail "read back the wrong value"
ok "write and read acknowledged with three of five nodes (new leader: $(wait_leader))"

step "Stop a third node (two of five remain: no quorum)"
"${FIVE[@]}" stop "${VICTIMS[2]}" >/dev/null 2>&1 </dev/null
set +e
kvctl --timeout 4s --attempts 4 put "$RUN/k" "two-up" >/dev/null 2>&1
CODE=$?
set -e
[[ "$CODE" -ne 0 ]] || fail "a write was acknowledged with two of five nodes running"
ok "write refused or left unacknowledged with two of five nodes (kvctl exit code $CODE)"

step "Bring one node back (three of five again)"
"${FIVE[@]}" start "${VICTIMS[2]}" >/dev/null 2>&1 </dev/null
STARTED=$SECONDS
# The returning container usually gets a different IP address than it had.
# The other nodes must find it again promptly.
kvctl --timeout 15s put "$RUN/k2" "back" >/dev/null || fail "writes did not resume within 15s of the quorum returning"
ok "writes resumed $((SECONDS - STARTED))s after the third node came back"
VALUE="$(kvctl get "$RUN/k")"
case "$VALUE" in
  three-up | two-up) ok "service resumed; the key holds '$VALUE' (the acknowledged write, or the later one whose outcome was unknown)" ;;
  *) fail "the acknowledged write was lost: the key holds '$VALUE'" ;;
esac

step "Restart the remaining nodes"
"${FIVE[@]}" start "${NODES[@]}" >/dev/null 2>&1 </dev/null
deadline=$((SECONDS + 60))
until [[ "$(for n in "${NODES[@]}"; do curl -fsS --max-time 2 "$(url_of "$n")/v1/status" 2>/dev/null | jq -r .applied_index; done | sort -u | wc -l)" == "1" ]]; do
  ((SECONDS < deadline)) || fail "the five nodes did not converge within 60s"
  sleep 0.3
done
kvctl status | sed 's/^/    /'
ok "all five nodes applied the same index"

echo
echo "PASS five-node demo: tolerated two failures, refused to acknowledge with three down, recovered"
echo "The five-node cluster is still running. Stop it with: scripts/cluster.sh down5"
