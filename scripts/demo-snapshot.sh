#!/usr/bin/env bash
# Snapshot demo: keep one follower down while the others write enough to
# snapshot and discard the log entries that follower still needs. When it
# returns, replaying the log is impossible; the leader must send it a
# snapshot.
#
# The Compose cluster snapshots every 1000 applied entries and keeps 100
# entries behind each snapshot (see docker-compose.yml), so this needs a few
# thousand writes, not millions.
source "$(dirname "$0")/lib.sh"
require_tools
cluster_must_be_up

STOPPED=""
cleanup() {
  [[ -n "$STOPPED" ]] && "${COMPOSE[@]}" start "$STOPPED" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

RUN="s$(date +%s)"
# metric <node> <name>: the value of an unlabelled metric, 0 if absent.
metric() {
  local v
  v="$(curl -fsS --max-time 2 "$(admin_of "$1")/metrics" 2>/dev/null | awk -v m="$2" '$1 == m {print $2}')"
  echo "${v:-0}"
}

step "1. Stop a follower"
heal_all
wait_until 30 "the cluster to be healthy before starting" converged
LEADER="$(wait_leader)"
BEHIND="$(others "$LEADER" | head -n 1)"
BEHIND_INDEX="$(status_field "$BEHIND" .last_index)"
"${COMPOSE[@]}" stop "$BEHIND" >/dev/null 2>&1
STOPPED="$BEHIND"
info "stopped $BEHIND at log index $BEHIND_INDEX; leader is $LEADER"

step "2. Write until the leader has compacted its log past $BEHIND"
WRITES=2500
bin/kvbench --endpoints "$ENDPOINTS" --workload write --clients 16 --keys 200 --value-bytes 64 \
  --duration 60s --warmup 0s --runs 1 >/dev/null 2>&1 &
BENCH=$!
compacted() {
  local first
  first="$(status_field "$(find_leader)" .first_index)"
  [[ -n "$first" && "$first" -gt $((BEHIND_INDEX + WRITES)) ]]
}
wait_until 120 "the leader's log to start beyond index $((BEHIND_INDEX + WRITES))" compacted
kill "$BENCH" 2>/dev/null || true
wait "$BENCH" 2>/dev/null || true
LEADER="$(wait_leader)"
kvctl put "$RUN/marker" "written-while-$BEHIND-was-down" >/dev/null
wait_until 30 "the two running nodes to agree" test "$(status_field "$LEADER" .applied_index)" = "$(status_field "$LEADER" .commit_index)"
FIRST="$(status_field "$LEADER" .first_index)"
SNAP="$(status_field "$LEADER" .snapshot_index)"
info "leader $LEADER: snapshot at index $SNAP, log now starts at index $FIRST, $(status_field "$LEADER" .keys) keys"
check "the entries $BEHIND needs (from $((BEHIND_INDEX + 1))) are gone from the leader's log (starts at $FIRST)" test "$FIRST" -gt $((BEHIND_INDEX + 1))

step "3. Restart $BEHIND"
"${COMPOSE[@]}" start "$BEHIND" >/dev/null 2>&1
STOPPED=""
caught_up() {
  [[ "$(status_field "$BEHIND" .applied_index)" == "$(status_field "$(find_leader)" .applied_index)" && "$(status_field "$BEHIND" .snapshot_index)" -gt "$BEHIND_INDEX" ]]
}
wait_until 120 "$BEHIND to install a snapshot and catch up" caught_up
wait_until 30 "all three nodes to converge" converged

step "4. Check how it caught up"
INSTALLED_AFTER="$(metric "$BEHIND" raftkv_snapshots_installed_total)"
info "$BEHIND: snapshot index $(status_field "$BEHIND" .snapshot_index), log starts at $(status_field "$BEHIND" .first_index), $(status_field "$BEHIND" .keys) keys"
# A restarted process starts its counters at zero, so any value above zero
# means this run installed one.
check "$BEHIND installed a snapshot from the leader (raftkv_snapshots_installed_total is $INSTALLED_AFTER since its restart)" test "${INSTALLED_AFTER%.*}" -ge 1
check "$BEHIND did not replay from its old position (its log starts at $(status_field "$BEHIND" .first_index), beyond $((BEHIND_INDEX + 1)))" \
  test "$(status_field "$BEHIND" .first_index)" -gt $((BEHIND_INDEX + 1))
check "$BEHIND holds as many keys as the leader" test "$(status_field "$BEHIND" .keys)" = "$(status_field "$LEADER" .keys)"
check "$BEHIND's state is the same size as the leader's" test "$(status_field "$BEHIND" .state_bytes)" = "$(status_field "$LEADER" .state_bytes)"

step "5. Stop the old leader: the cluster now depends on $BEHIND for its quorum"
# With $LEADER down, nothing commits and nothing is read unless $BEHIND
# takes part. It can only do that correctly with the state it installed.
"${COMPOSE[@]}" stop "$LEADER" >/dev/null 2>&1
STOPPED="$LEADER"
wait_leader "$LEADER" >/dev/null
check "a value written while $BEHIND was down is readable" test "$(kvctl get "$RUN/marker")" = "written-while-$BEHIND-was-down"
"${COMPOSE[@]}" start "$LEADER" >/dev/null 2>&1
STOPPED=""
wait_until 60 "all three nodes to converge" converged

echo
echo "${GREEN}${BOLD}PASS${RESET} snapshot demo: $BEHIND fell $((FIRST - BEHIND_INDEX - 1))+ entries behind the start of the leader's log and caught up from a snapshot"
