#!/usr/bin/env bash
# Partial-failure demo: break the link between the leader and ONE follower.
# That follower can still talk to the other follower, and the leader still
# has a majority.
#
# This is the situation PreVote exists for. The cut-off follower stops
# hearing heartbeats and wants an election. Without PreVote it would raise
# its term over and over, and the moment anything it sent reached the others
# the healthy leader would be forced to step down. With PreVote it first asks
# whether it could win; the other follower still hears the leader and says
# no; nothing changes.
#
# What it verifies: for the whole duration of the fault the leader and the
# term stay the same and writes keep being acknowledged, and after healing
# the cut-off follower catches up.
#
# Note on "one-way" failures: the rule below drops packets in one direction
# only (leader to follower). Because the peers talk over TCP, losing one
# direction stalls connections in both, so at the level Raft sees it this
# link is simply dead while every other link works.
source "$(dirname "$0")/lib.sh"
require_tools
cluster_must_be_up

FAULTY=""
cleanup() {
  [[ -n "$FAULTY" ]] && heal_node "$FAULTY"
  return 0
}
trap cleanup EXIT

RUN="l$(date +%s)"

step "1. Find the leader"
heal_all
wait_until 30 "the cluster to be healthy before starting" converged
LEADER="$(wait_leader)"
TERM_BEFORE="$(status_field "$LEADER" .term)"
CUT="$(others "$LEADER" | head -n 1)"
OTHER="$(others "$LEADER" | tail -n 1)"
info "leader $LEADER (term $TERM_BEFORE); cutting its link to $CUT; $OTHER is untouched"

step "2. Drop packets from $LEADER to $CUT"
ip="$(in_node "$LEADER" sh -c "getent hosts $CUT | cut -d' ' -f1")"
in_node "$LEADER" sh -c "iptables -N $CHAIN 2>/dev/null || iptables -F $CHAIN"
in_node "$LEADER" sh -c "iptables -C OUTPUT -j $CHAIN 2>/dev/null || iptables -I OUTPUT -j $CHAIN"
in_node "$LEADER" iptables -A "$CHAIN" -d "$ip" -j DROP
FAULTY="$LEADER"
check "$LEADER cannot reach $CUT" cannot_reach_raft "$LEADER" "$CUT"
check "$CUT can still reach $OTHER" can_reach_raft "$CUT" "$OTHER"
check "$LEADER can still reach $OTHER" can_reach_raft "$LEADER" "$OTHER"

step "3. For 12 seconds, keep writing and watch the leader and term"
BEHIND_BEFORE="$(status_field "$CUT" .applied_index)"
for i in $(seq 1 24); do
  kvctl put "$RUN/key-$i" "value-$i" >/dev/null
  [[ "$(find_leader)" == "$LEADER" ]] || fail "leadership moved away from $LEADER during the fault"
  [[ "$(status_field "$LEADER" .term)" == "$TERM_BEFORE" ]] || fail "the term changed from $TERM_BEFORE to $(status_field "$LEADER" .term) during the fault"
  sleep 0.5
done
ok "24 writes acknowledged; leader stayed $LEADER and term stayed $TERM_BEFORE throughout"
check "$CUT did not raise its term (still $TERM_BEFORE)" test "$(status_field "$CUT" .term)" = "$TERM_BEFORE"
check "$CUT knows it has no leader contact (role: $(status_field "$CUT" .role))" test "$(status_field "$CUT" .role)" != "leader"
check "$CUT fell behind (applied $(status_field "$CUT" .applied_index), leader at $(status_field "$LEADER" .applied_index))" \
  test "$(status_field "$CUT" .applied_index)" -lt "$(status_field "$LEADER" .applied_index)"
info "$CUT was at index $BEHIND_BEFORE when the link broke"

step "4. Heal the link"
heal_node "$LEADER"
FAULTY=""
wait_until 60 "all three nodes to converge" converged
check "the leader is still $LEADER" test "$(find_leader)" = "$LEADER"
check "the term is still $TERM_BEFORE" test "$(status_field "$LEADER" .term)" = "$TERM_BEFORE"
check "$CUT caught up" test "$(status_field "$CUT" .applied_index)" = "$(status_field "$LEADER" .applied_index)"
check "the last write is readable" test "$(kvctl get "$RUN/key-24")" = "value-24"

echo
echo "${GREEN}${BOLD}PASS${RESET} link-failure demo: a follower cut off from the leader for 12s caused no election (term stayed $TERM_BEFORE)"
