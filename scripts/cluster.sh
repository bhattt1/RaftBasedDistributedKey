#!/usr/bin/env bash
# Manages the Docker Compose clusters.
#
#   scripts/cluster.sh up           build and start the 3-node cluster (with the
#                                   fault-injection override the demos need)
#   scripts/cluster.sh down         stop it; data volumes are KEPT
#   scripts/cluster.sh status       every node's view of the cluster
#   scripts/cluster.sh leader       print the current leader
#   scripts/cluster.sh logs [node]  follow logs
#   scripts/cluster.sh heal         remove any network faults the demos left
#   scripts/cluster.sh reset        stop it and DELETE the data volumes
#
#   scripts/cluster.sh up5 | down5 | reset5          the 5-node cluster
#   scripts/cluster.sh secure-up | secure-down | secure-reset
#                                   the TLS cluster (needs scripts/gen-dev-certs.sh)
#
# Stopping never deletes data. Only the reset commands do, and they say so.
source "$(dirname "$0")/lib.sh"

FIVE=(docker compose -f docker-compose.5node.yml)
SECURE=(docker compose -f docker-compose.secure.yml)

confirm_reset() {
  echo "This DELETES the data volumes of the $1 cluster."
  if [[ "${RAFTKV_YES:-}" != "1" ]]; then
    read -r -p "Type 'delete' to continue: " answer
    [[ "$answer" == "delete" ]] || { echo "aborted"; exit 1; }
  fi
}

case "${1:-}" in
  up)
    "${COMPOSE[@]}" up -d --build
    require_tools
    leader="$(wait_leader)"
    wait_until 30 "all nodes to agree" converged
    echo "cluster is up; leader is $leader"
    echo "endpoints: $ENDPOINTS"
    ;;
  down)
    "${COMPOSE[@]}" down
    echo "stopped. Data volumes were kept; 'scripts/cluster.sh reset' deletes them."
    ;;
  reset)
    confirm_reset "3-node"
    "${COMPOSE[@]}" down -v
    ;;
  status)
    require_tools
    kvctl status
    ;;
  leader)
    require_tools
    leader="$(find_leader)"
    [[ -n "$leader" ]] || { echo "no leader" >&2; exit 1; }
    echo "$leader"
    ;;
  logs)
    shift
    "${COMPOSE[@]}" logs -f --tail=50 "$@"
    ;;
  heal)
    heal_all
    echo "network faults removed"
    ;;
  up5)
    "${FIVE[@]}" up -d --build
    echo "endpoints: http://127.0.0.1:8101,http://127.0.0.1:8102,http://127.0.0.1:8103,http://127.0.0.1:8104,http://127.0.0.1:8105"
    ;;
  down5)
    "${FIVE[@]}" down
    ;;
  reset5)
    confirm_reset "5-node"
    "${FIVE[@]}" down -v
    ;;
  secure-up)
    [[ -f certs/ca.pem && -f certs/client.token ]] || { echo "no certificates found. Run scripts/gen-dev-certs.sh first." >&2; exit 2; }
    RAFTKV_UID="$(id -u)" RAFTKV_GID="$(id -g)" "${SECURE[@]}" up -d --build
    echo "endpoints: https://127.0.0.1:8201,https://127.0.0.1:8202,https://127.0.0.1:8203"
    echo "use: bin/kvctl --endpoints <those> --ca-cert certs/ca.pem --token-file certs/client.token ..."
    ;;
  secure-down)
    RAFTKV_UID="$(id -u)" RAFTKV_GID="$(id -g)" "${SECURE[@]}" down
    ;;
  secure-reset)
    confirm_reset "secure"
    RAFTKV_UID="$(id -u)" RAFTKV_GID="$(id -g)" "${SECURE[@]}" down -v
    ;;
  *)
    sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
