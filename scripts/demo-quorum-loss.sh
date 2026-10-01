#!/usr/bin/env bash
# Quorum-loss demo: stop two of the three nodes. The survivor must refuse to
# acknowledge anything, because one node of three cannot know whether the
# other two have moved on without it. Then bring the nodes back and check
# that the cluster recovers with its data.
#
# This is the availability price of consistency: with a majority gone the
# cluster is unavailable for both reads and writes, by design.
source "$(dirname "$0")/lib.sh"
require_tools
cluster_must_be_up

STOPPED=()
cleanup() {
  ((${#STOPPED[@]})) && "${COMPOSE[@]}" start "${STOPPED[@]}" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

RUN="q$(date +%s)"

step "1. Write while all three nodes are up"
heal_all
wait_until 30 "the cluster to be healthy before starting" converged
LEADER="$(wait_leader)"
kvctl put "$RUN/key" "written-with-quorum" >/dev/null
ok "write acknowledged (leader $LEADER)"

step "2. Stop two nodes, leaving only $LEADER"
mapfile -t STOPPED < <(others "$LEADER")
"${COMPOSE[@]}" stop "${STOPPED[@]}" >/dev/null 2>&1
info "stopped ${STOPPED[*]}"

step "3. The survivor acknowledges neither writes nor reads"
WRITE_CODE="$(http_code PUT "$(url_of "$LEADER")/v1/kv/$RUN%2Fkey?timeout=3s" '{"value":"written-without-quorum"}')"
READ_CODE="$(http_code GET "$(url_of "$LEADER")/v1/kv/$RUN%2Fkey?timeout=3s")"
info "write returned HTTP $WRITE_CODE, read returned HTTP $READ_CODE"
check "the write was not acknowledged" test "$WRITE_CODE" != "200"
check "the read was not answered" test "$READ_CODE" != "200"
no_leader() { [[ "$(status_field "$LEADER" .role)" != "leader" ]]; }
wait_until 15 "$LEADER to give up leadership" no_leader
check "$LEADER is alive but not ready" test "$(http_code GET "$(url_of "$LEADER")/healthz"):$(http_code GET "$(url_of "$LEADER")/readyz")" = "200:503"
# kvctl with a short deadline reports the failure and a non-zero exit code.
set +e
bin/kvctl --endpoints "$(url_of "$LEADER")" --timeout 3s --attempts 3 put "$RUN/other" "x" >/dev/null 2>&1
CODE=$?
set -e
check "kvctl exits non-zero ($CODE) when there is no quorum" test "$CODE" -ne 0

step "4. Restart the stopped nodes"
"${COMPOSE[@]}" start "${STOPPED[@]}" >/dev/null 2>&1
STOPPED=()
NEW="$(wait_leader)"
wait_until 60 "all three nodes to converge" converged
info "leader is $NEW in term $(status_field "$NEW" .term)"

step "5. The cluster works again and the acknowledged write is intact"
VALUE="$(kvctl get "$RUN/key")"
info "the key now holds '$VALUE'"
# The acknowledged value must be there unless the unacknowledged write from
# step 3 replaced it. That write was reported as unknown, so either is legal.
case "$VALUE" in
  written-with-quorum) ok "the acknowledged write is present; the write attempted without a quorum did not take effect" ;;
  written-without-quorum) ok "the write attempted without a quorum took effect after quorum returned (it had been reported as unknown, not failed)" ;;
  *) fail "unexpected value '$VALUE'" ;;
esac
kvctl put "$RUN/after" "ok" >/dev/null
check "new writes are acknowledged again" test "$(kvctl get "$RUN/after")" = "ok"

echo
echo "${GREEN}${BOLD}PASS${RESET} quorum-loss demo: one node of three acknowledged nothing; service resumed when the majority returned"
