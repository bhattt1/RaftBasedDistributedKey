#!/usr/bin/env bash
# Shared helpers for the cluster and demo scripts. Source it; do not run it.
#
# Conventions used by every script here:
#   - `set -euo pipefail`: a failed command stops the script.
#   - Every wait has a deadline. Nothing sleeps for a fixed time and assumes.
#   - Expectations are checked with `check`/`fail`; a script that prints
#     "PASS" has verified what it claims, and exits non-zero otherwise.
#   - `docker compose exec` is always called with -T (no TTY), so the scripts
#     work from CI and from pipes.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# The three-node development cluster with the fault-injection override.
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.chaos.yml)
NODES=(n1 n2 n3)
CHAIN="RAFTKV_CHAOS"

url_of() { echo "http://127.0.0.1:$((8000 + ${1#n}))"; }
admin_of() { echo "http://127.0.0.1:$((9000 + ${1#n}))"; }
ENDPOINTS="$(url_of n1),$(url_of n2),$(url_of n3)"

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; DIM=$'\033[2m'; RESET=$'\033[0m'
else
  BOLD=""; RED=""; GREEN=""; DIM=""; RESET=""
fi

step() { echo; echo "${BOLD}==> $*${RESET}"; }
info() { echo "    $*"; }
ok() { echo "    ${GREEN}ok${RESET}   $*"; }
fail() {
  echo "    ${RED}FAIL${RESET} $*" >&2
  exit 1
}

# check "description" command...  -- run a command, fail the script if it fails.
check() {
  local what="$1"
  shift
  if "$@"; then ok "$what"; else fail "$what"; fi
}

require_tools() {
  local missing=()
  for tool in docker curl jq; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  [[ -x bin/kvctl ]] || missing+=("bin/kvctl (run: make build)")
  if ((${#missing[@]})); then
    echo "missing prerequisites: ${missing[*]}" >&2
    exit 2
  fi
}

kvctl() { bin/kvctl --endpoints "$ENDPOINTS" "$@"; }

# status_field <node> <jq filter>: one field of a node's /v1/status, or empty
# if the node cannot be reached.
status_field() {
  curl -fsS --max-time 2 "$(url_of "$1")/v1/status" 2>/dev/null | jq -r "$2" 2>/dev/null || true
}

# wait_until <seconds> "description" command...: poll until the command
# succeeds, or fail after the deadline.
wait_until() {
  local timeout="$1" what="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    if ((SECONDS >= deadline)); then
      fail "timed out after ${timeout}s waiting for: $what"
    fi
    sleep 0.2
  done
}

# find_leader [excluded node]: print the node that reports itself leader in
# the highest term. Prints nothing if there is none.
find_leader() {
  local exclude="${1:-}" best="" best_term=-1 n role term
  for n in "${NODES[@]}"; do
    [[ "$n" == "$exclude" ]] && continue
    role="$(status_field "$n" .role)"
    term="$(status_field "$n" .term)"
    if [[ "$role" == "leader" && "${term:-0}" -gt "$best_term" ]]; then
      best="$n"
      best_term="$term"
    fi
  done
  echo "$best"
}

has_leader() { [[ -n "$(find_leader "${1:-}")" ]]; }

# wait_leader [excluded node]: wait for a leader and print it.
wait_leader() {
  wait_until 30 "a leader to be elected" has_leader "${1:-}"
  find_leader "${1:-}"
}

others() {
  local n
  for n in "${NODES[@]}"; do [[ "$n" != "$1" ]] && echo "$n"; done
  return 0
}

# converged: every node reports the same applied index, equal to its commit
# index, and knows a leader.
converged() {
  local n applied commit leader first=""
  for n in "${NODES[@]}"; do
    applied="$(status_field "$n" .applied_index)"
    commit="$(status_field "$n" .commit_index)"
    leader="$(status_field "$n" '.leader.id // empty')"
    [[ -n "$applied" && "$applied" == "$commit" && -n "$leader" ]] || return 1
    if [[ -z "$first" ]]; then first="$applied"; elif [[ "$applied" != "$first" ]]; then return 1; fi
  done
  return 0
}

# http_code <method> <url> [json body]: perform a request, print the status
# code ("000" if the connection failed).
http_code() {
  local method="$1" url="$2" body="${3:-}"
  local args=(-s -o /dev/null -w '%{http_code}' --max-time 10 -X "$method")
  [[ -n "$body" ]] && args+=(-H 'Content-Type: application/json' -d "$body")
  curl "${args[@]}" "$url" || true
}

# http_body <method> <url> [json body]: perform a request, print the body.
http_body() {
  local method="$1" url="$2" body="${3:-}"
  local args=(-s --max-time 10 -X "$method")
  [[ -n "$body" ]] && args+=(-H 'Content-Type: application/json' -d "$body")
  curl "${args[@]}" "$url" || true
}

# ---- network faults ----------------------------------------------------------
# Rules live in a chain of their own. Healing removes that chain and nothing
# else, so rules Docker or anyone else installed are left alone.

# stdin is closed explicitly: `exec -T` still forwards stdin, and would
# otherwise swallow input meant for the calling script when that script is
# itself being read from a pipe.
in_node() { "${COMPOSE[@]}" exec -T -u 0 "$@" </dev/null; }

# isolate <node>: drop all traffic between <node> and the other cluster
# members, in both directions. Traffic from clients on the host is untouched.
isolate() {
  local node="$1" peer ip
  in_node "$node" sh -c "iptables -N $CHAIN 2>/dev/null || iptables -F $CHAIN"
  in_node "$node" sh -c "iptables -C INPUT -j $CHAIN 2>/dev/null || iptables -I INPUT -j $CHAIN"
  in_node "$node" sh -c "iptables -C OUTPUT -j $CHAIN 2>/dev/null || iptables -I OUTPUT -j $CHAIN"
  for peer in $(others "$node"); do
    ip="$(in_node "$node" sh -c "getent hosts $peer | cut -d' ' -f1")"
    [[ -n "$ip" ]] || fail "could not resolve $peer from inside $node"
    in_node "$node" iptables -A "$CHAIN" -s "$ip" -j DROP
    in_node "$node" iptables -A "$CHAIN" -d "$ip" -j DROP
  done
}

# heal_node <node>: remove the rules isolate added. Safe to call when there
# are none.
heal_node() {
  local node="$1"
  in_node "$node" sh -c "
    iptables -D INPUT -j $CHAIN 2>/dev/null
    iptables -D OUTPUT -j $CHAIN 2>/dev/null
    iptables -F $CHAIN 2>/dev/null
    iptables -X $CHAIN 2>/dev/null
    true" >/dev/null 2>&1 || true
}

heal_all() {
  local n
  for n in "${NODES[@]}"; do
    # A stopped container has no rules to remove.
    if [[ -n "$("${COMPOSE[@]}" ps -q --status running "$n" 2>/dev/null)" ]]; then heal_node "$n"; fi
  done
}

# can_reach_raft <from> <to>: can <from> open a TCP connection to <to>'s
# peer port?
can_reach_raft() { in_node "$1" nc -z -w 1 "$2" 7000 >/dev/null 2>&1; }
cannot_reach_raft() { ! can_reach_raft "$@"; }

# now_ms: milliseconds since the epoch.
now_ms() { echo $(($(date +%s%N) / 1000000)); }

# cluster_must_be_up: the scripts assume a running, healthy chaos cluster.
cluster_must_be_up() {
  local n
  for n in "${NODES[@]}"; do
    if [[ -z "$("${COMPOSE[@]}" ps -q --status running "$n" 2>/dev/null)" ]]; then
      echo "node $n is not running. Start the cluster first: scripts/cluster.sh up" >&2
      exit 2
    fi
    if ! in_node "$n" sh -c 'command -v iptables' >/dev/null 2>&1; then
      echo "node $n was not started with the chaos override. Run: scripts/cluster.sh up" >&2
      exit 2
    fi
  done
}
