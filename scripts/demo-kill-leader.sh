#!/usr/bin/env bash
# Crash demo: kill the leader's process with SIGKILL while a client is
# writing, watch the other two nodes take over, restart the old leader, and
# check that every acknowledged write is still there.
#
# A crash differs from a partition: the process is gone, so nothing answers
# on its ports, and whatever it had not synced to disk is lost.
source "$(dirname "$0")/lib.sh"
require_tools
cluster_must_be_up

KILLED=""
cleanup() {
  [[ -n "$KILLED" ]] && "${COMPOSE[@]}" start "$KILLED" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

RUN="k$(date +%s)"

step "1. Find the leader and write through it"
heal_all
wait_until 30 "the cluster to be healthy before starting" converged
OLD="$(wait_leader)"
OLD_TERM="$(status_field "$OLD" .term)"
info "leader is $OLD in term $OLD_TERM"
declare -A WANT
for i in $(seq 1 10); do
  kvctl put "$RUN/before-$i" "value-$i" >/dev/null
  WANT["$RUN/before-$i"]="value-$i"
done
ok "10 writes acknowledged"

step "2. Kill $OLD with SIGKILL"
"${COMPOSE[@]}" kill -s KILL "$OLD" >/dev/null 2>&1
KILLED="$OLD"
KILLED_AT="$(now_ms)"
check "$OLD no longer answers" test "$(http_code GET "$(url_of "$OLD")/healthz")" = "000"

step "3. The remaining two nodes elect a leader"
NEW="$(wait_leader "$OLD")"
NEW_TERM="$(status_field "$NEW" .term)"
info "new leader is $NEW in term $NEW_TERM, observed $(($(now_ms) - KILLED_AT)) ms after the kill"
check "the new term ($NEW_TERM) is higher than the old one ($OLD_TERM)" test "$NEW_TERM" -gt "$OLD_TERM"

step "4. Writes continue with two of three nodes"
for i in $(seq 1 10); do
  kvctl put "$RUN/after-$i" "value-$i" >/dev/null
  WANT["$RUN/after-$i"]="value-$i"
done
ok "10 more writes acknowledged"

step "5. Restart $OLD; it rejoins as a follower and catches up"
"${COMPOSE[@]}" start "$OLD" >/dev/null 2>&1
KILLED=""
wait_until 60 "all three nodes to converge" converged
check "$OLD is a follower" test "$(status_field "$OLD" .role)" = "follower"
check "$OLD caught up to index $(status_field "$NEW" .applied_index)" test "$(status_field "$OLD" .applied_index)" = "$(status_field "$NEW" .applied_index)"

step "6. Every acknowledged write is present"
for key in "${!WANT[@]}"; do
  got="$(kvctl get "$key")" || fail "acknowledged key $key is missing"
  [[ "$got" == "${WANT[$key]}" ]] || fail "acknowledged key $key has value '$got', want '${WANT[$key]}'"
done
ok "all ${#WANT[@]} acknowledged writes are present"

echo
echo "${GREEN}${BOLD}PASS${RESET} kill-leader demo: $OLD killed, $NEW took over in term $NEW_TERM, ${#WANT[@]} acknowledged writes survived"
