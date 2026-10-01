#!/usr/bin/env bash
# Partition demo: cut the leader off from the other two nodes while clients
# can still reach it, and check what each side does.
#
# What it verifies, in order:
#   1. Writes through the elected leader are acknowledged.
#   2. After the leader is isolated, Raft traffic is really blocked in both
#      directions, and the client port is really still reachable.
#   3. The isolated node acknowledges neither writes nor reads.
#   4. The other two nodes elect a leader in a higher term and accept writes.
#   5. The isolated node gives up leadership and reports itself not ready.
#   6. After healing, all three nodes agree, and every acknowledged write
#      from both before and during the partition is present.
#
# What it does NOT claim: that a request which timed out on the isolated node
# can never take effect. A timeout means "unknown". This script reports what
# happened to those requests in this run and does not assert either way.
#
# A partition like this is symmetric (neither side can reach the other) and
# total. It is a different fault from a crash (the node keeps running and
# keeps answering clients), from added latency (messages arrive, late), and
# from a failure of a single link (which scripts/demo-link-failure.sh covers).
source "$(dirname "$0")/lib.sh"
require_tools
cluster_must_be_up

ISOLATED=""
TMP="$(mktemp -d)"
cleanup() {
  # Always remove our packet-filter rules, however the script ends.
  [[ -n "$ISOLATED" ]] && heal_node "$ISOLATED"
  rm -rf "$TMP"
  return 0
}
trap cleanup EXIT

RUN="p$(date +%s)"

step "1. Find the leader and write through it"
heal_all
wait_until 30 "the cluster to be healthy before starting" converged
OLD="$(wait_leader)"
OLD_TERM="$(status_field "$OLD" .term)"
info "leader is $OLD in term $OLD_TERM"
declare -A WANT
for i in 1 2 3 4 5; do
  kvctl put "$RUN/before-$i" "value-$i" >/dev/null
  WANT["$RUN/before-$i"]="value-$i"
done
ok "5 writes acknowledged"

step "2. Isolate $OLD from its peers, leaving its client port open"
isolate "$OLD"
ISOLATED="$OLD"
# Send a write and a read straight to $OLD right away, while it still
# believes it is the leader, each with a 4-second server-side deadline. They
# run in the background while the partition itself is verified below.
(http_code PUT "$(url_of "$OLD")/v1/kv/$RUN%2Fstranded?timeout=4s" '{"value":"written-to-isolated-leader"}' >"$TMP/write") &
(http_code GET "$(url_of "$OLD")/v1/kv/$RUN%2Fbefore-1?timeout=4s" >"$TMP/read") &
for peer in $(others "$OLD"); do
  check "$OLD cannot open a Raft connection to $peer" cannot_reach_raft "$OLD" "$peer"
  check "$peer cannot open a Raft connection to $OLD" cannot_reach_raft "$peer" "$OLD"
done
check "clients can still reach $OLD" test "$(http_code GET "$(url_of "$OLD")/healthz")" = "200"

step "3. What the isolated node told the clients that reached it"
wait
WRITE_CODE="$(cat "$TMP/write")"
READ_CODE="$(cat "$TMP/read")"
info "write returned HTTP $WRITE_CODE, read returned HTTP $READ_CODE"
info "(504: the node took the request into its log but could not commit it; outcome unknown."
info " 503 or 421: the node had already stopped claiming leadership and refused it.)"
check "the isolated node did not acknowledge the write" test "$WRITE_CODE" != "200"
check "the isolated node did not answer the read" test "$READ_CODE" != "200"
case "$WRITE_CODE" in 504 | 421 | 503) ;; *) fail "unexpected status $WRITE_CODE for the write" ;; esac
case "$READ_CODE" in 504 | 421 | 503) ;; *) fail "unexpected status $READ_CODE for the read" ;; esac

step "4. The majority elects a new leader and keeps working"
majority_leader() { [[ -n "$(find_leader "$OLD")" ]]; }
wait_until 30 "the other two nodes to elect a leader" majority_leader
NEW="$(find_leader "$OLD")"
NEW_TERM="$(status_field "$NEW" .term)"
info "new leader is $NEW in term $NEW_TERM"
check "the new leader is a different node" test "$NEW" != "$OLD"
check "the new term ($NEW_TERM) is higher than the old one ($OLD_TERM)" test "$NEW_TERM" -gt "$OLD_TERM"
MAJORITY="$(for n in $(others "$OLD"); do url_of "$n"; done | paste -sd, -)"
for i in 1 2 3 4 5; do
  bin/kvctl --endpoints "$MAJORITY" put "$RUN/during-$i" "value-$i" >/dev/null
  WANT["$RUN/during-$i"]="value-$i"
done
ok "5 writes acknowledged by the majority side during the partition"

step "5. The isolated node stops claiming leadership"
stepped_down() { [[ "$(status_field "$OLD" .role)" != "leader" ]]; }
wait_until 15 "$OLD to step down (check-quorum)" stepped_down
info "$OLD now reports role '$(status_field "$OLD" .role)' with $(status_field "$OLD" .pending_proposals) proposal(s) still pending"
check "$OLD is alive (/healthz is 200)" test "$(http_code GET "$(url_of "$OLD")/healthz")" = "200"
check "$OLD is not ready (/readyz is 503)" test "$(http_code GET "$(url_of "$OLD")/readyz")" = "503"
check "$OLD still refuses reads" test "$(http_code GET "$(url_of "$OLD")/v1/kv/$RUN%2Fbefore-1?timeout=1s")" != "200"

step "6. Heal the partition and check that the cluster agrees"
heal_node "$OLD"
ISOLATED=""
wait_until 60 "all three nodes to converge on one log" converged
info "$(kvctl status | sed 's/^/    /' | sed '1s/^    //')"
check "$OLD rejoined as a follower" test "$(status_field "$OLD" .role)" = "follower"
APPLIED="$(status_field n1 .applied_index)"
KEYS="$(status_field n1 .keys)"
for n in "${NODES[@]}"; do
  check "$n applied index $APPLIED and holds $KEYS keys" test "$(status_field "$n" .applied_index):$(status_field "$n" .keys)" = "$APPLIED:$KEYS"
done
for key in "${!WANT[@]}"; do
  got="$(kvctl get "$key")" || fail "acknowledged key $key is missing"
  [[ "$got" == "${WANT[$key]}" ]] || fail "acknowledged key $key has value '$got', want '${WANT[$key]}'"
done
ok "all ${#WANT[@]} acknowledged writes are present with the right values"

# The write sent to the isolated leader. If it was refused (503/421) it must
# be absent. If it timed out (504) its outcome was reported as unknown, and
# either result is legal: report which one happened, do not assert.
if stranded="$(kvctl get "$RUN/stranded" 2>/dev/null)"; then
  [[ "$WRITE_CODE" == "504" ]] || fail "a write that was refused with HTTP $WRITE_CODE took effect"
  info "the write that timed out on the isolated node DID take effect after healing (value '$stranded')."
  info "That is permitted: its outcome was reported as unknown, not as failed."
elif [[ "$WRITE_CODE" == "504" ]]; then
  info "the write that timed out on the isolated node did not take effect: its log entry was"
  info "overwritten by the new leader. Also permitted; the client had been told 'unknown'."
else
  ok "the write the isolated node refused (HTTP $WRITE_CODE) did not take effect"
fi

echo
echo "${GREEN}${BOLD}PASS${RESET} partition demo: isolated leader $OLD (term $OLD_TERM) acknowledged nothing, majority elected $NEW (term $NEW_TERM), ${#WANT[@]} acknowledged writes survived"
