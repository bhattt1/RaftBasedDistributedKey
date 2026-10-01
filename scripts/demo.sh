#!/usr/bin/env bash
# The whole demonstration in one command:
#
#   scripts/demo.sh
#
# It builds and starts a three-node cluster in Docker, shows ordinary
# operations, then runs each failure demo in turn, the last one on a five-node
# cluster. Every demo checks its own claims and stops with a non-zero exit
# code if one does not hold.
#
# Takes a minute or two once the image is built. The three-node cluster is
# left running afterwards;
# `scripts/cluster.sh down` stops it (keeping data) and
# `scripts/cluster.sh reset` deletes its data.
source "$(dirname "$0")/lib.sh"

[[ -x bin/kvctl && -x bin/kvbench ]] || { echo "build first: make build" >&2; exit 2; }

step "Start the cluster"
scripts/cluster.sh up | tail -n 2 | sed 's/^/    /'
require_tools

step "Ordinary operations"
KEY="demo/greeting-$(date +%s)"
info "\$ kvctl put $KEY hello";              info "  $(kvctl put "$KEY" hello)"
info "\$ kvctl get $KEY";                    info "  $(kvctl get "$KEY")"
info "\$ kvctl cas $KEY hi --expect-value hello"
info "  $(kvctl cas "$KEY" hi --expect-value hello)"
info "\$ kvctl cas $KEY again --expect-value hello   (stale expectation)"
set +e
OUT="$(kvctl cas "$KEY" again --expect-value hello)"; CODE=$?
set -e
info "  $OUT   [exit code $CODE]"
check "the stale compare-and-swap was refused with exit code 4" test "$CODE" -eq 4
check "the value is the one the successful swap wrote" test "$(kvctl get "$KEY")" = "hi"
info "\$ kvctl delete $KEY";                 info "  $(kvctl delete "$KEY")"
info ""
kvctl status | sed 's/^/    /'

scripts/demo-kill-leader.sh
scripts/demo-partition.sh
scripts/demo-link-failure.sh
scripts/demo-quorum-loss.sh
scripts/demo-snapshot.sh
# The five-node demo runs its own cluster on other ports. Stop it afterwards
# (its data is kept) so only the three-node cluster is left running.
scripts/demo-five-node.sh | grep -v "still running"
docker compose -f docker-compose.5node.yml stop >/dev/null 2>&1 </dev/null

step "Final state"
kvctl status | sed 's/^/    /'
echo
echo "${GREEN}${BOLD}ALL DEMOS PASSED${RESET}"
echo "The cluster is still running. Stop it with: scripts/cluster.sh down"
