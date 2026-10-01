# Operations

Running, configuring, securing and troubleshooting a cluster, and the limits
you should know about before relying on one.

## Platform

`kvserver` runs on Linux. It uses `flock` and directory `fsync` and refuses
to start on platforms without them. On Windows, develop inside WSL2, with
Docker Engine in WSL2 or Docker Desktop's WSL2 backend; this repository was
built and tested that way (Ubuntu on WSL2). `kvctl` and `kvbench` are plain
HTTP clients and build for Windows and macOS as well (`make build-windows`
checks that they compile).

Keep data directories on a real filesystem. `/tmp` is often `tmpfs`, where
`fsync` does nothing: everything appears to work and nothing survives a
reboot. Inside WSL2, also keep them off `/mnt/c`; the Windows filesystem
mount does not give Linux locking and sync semantics.

## Configuration

Settings are read from, in increasing order of precedence: built-in
defaults, a JSON file (`--config` or `RAFTKV_CONFIG`), environment variables,
and flags. Every setting has the same name in each form: the flag
`--raft-listen` is the variable `RAFTKV_RAFT_LISTEN` and the JSON key
`raft_listen`. A complete example file is in `configs/node.example.json`.
`kvserver --help` lists everything.

| Setting | Default | Meaning |
|---|---|---|
| `node-id` | (required) | this node's name; must appear in `peers` |
| `cluster-id` | `raft-kv` | shared by all members; stored in the data directory and sent on every peer message |
| `data-dir` | `data` | WAL and snapshots |
| `peers` | (required) | every member, as `id=raft-host:port@client-url`, comma separated |
| `raft-listen` | `:7000` | peer protocol |
| `http-listen` | `:8000` | client API |
| `admin-listen` | `127.0.0.1:9000` | metrics and pprof; empty disables it |
| `tick-interval` | `100ms` | one Raft tick |
| `election-ticks` | `10` | election timeout is 1 to 2 times this many ticks |
| `heartbeat-ticks` | `2` | leader heartbeat interval |
| `request-timeout` | `5s` | deadline for requests that do not set `?timeout=` |
| `max-request-timeout` | `30s` | upper limit for `?timeout=` |
| `shutdown-timeout` | `10s` | bound on graceful shutdown |
| `max-pending-proposals` | `1024` | requests queued or awaiting commit before `OVERLOADED` |
| `snapshot-threshold` | `10000` | applied entries between snapshots |
| `snapshot-trailing` | `1000` | entries kept in memory behind a snapshot |
| `wal-segment-bytes` | `16777216` | WAL segment size |
| `log-level`, `log-format` | `info`, `text` | `debug`/`info`/`warn`/`error`; `text`/`json` |
| `enable-pprof` | `false` | serve `/debug/pprof` on the admin listener |
| `tls-*`, `client-token-file` | | secure mode, below |
| `insecure-dev` | `false` | run without TLS and authentication |
| `unsafe-no-fsync` | `false` | skip fsync; benchmarks only |

The **client URL** in `peers` is what a node hands to clients as the leader
hint. It must be an address clients can reach. In the Compose files the
nodes dial each other as `n1:7000`, a name that only exists on the Compose
network, but advertise `http://127.0.0.1:8001`, which is what a client on
the host uses.

The limits on keys, values, total state and retry sessions are **not**
configurable. They decide whether a command succeeds, so every node must use
the same values or their states would diverge. They are constants in
`internal/kv` (`DefaultLimits`); changing one means upgrading every node
together.

Timing: the election timeout must comfortably exceed a round trip plus an
fsync. The defaults suit a LAN and ordinary disks. With much slower disks or
links, raise `election-ticks` rather than shortening ticks.

## Security

### Trust model

Members trust each other completely. A node holding a valid peer certificate
can vote, replicate and send snapshots; a malicious or buggy one can break
safety. Clients holding the token can read and write every key; there are no
per-key permissions and only one token.

What secure mode protects against is everyone else: someone on the network
who is neither a member nor a client.

### Development mode

`--insecure-dev` (or `insecure_dev` in the file) runs everything in
plaintext with no authentication. It has to be asked for: a node with no TLS
settings and without this flag refuses to start. It logs a warning at
startup. The Compose development files use it and publish ports on
`127.0.0.1` only.

### Secure mode

Set all six, or the node refuses to start:

| Setting | Purpose |
|---|---|
| `tls-peer-ca` | CA that signs every node's peer certificate |
| `tls-peer-cert`, `tls-peer-key` | this node's certificate; its common name must equal `node-id` |
| `tls-http-cert`, `tls-http-key` | certificate for the HTTPS client API |
| `client-token-file` | file with the bearer token (at least 24 characters) |

In this mode:

- **Peers use mutual TLS 1.3.** Both sides present certificates signed by
  the cluster CA. The dialling side also requires the server's certificate
  to carry the ID of the peer it meant to reach, on top of normal host name
  verification. The receiving side requires the sender named in each Raft
  message to be the identity in the client certificate, so one member cannot
  speak as another. Certificate verification is never disabled.
- **Clients use HTTPS** and send `Authorization: Bearer <token>`. The token
  is compared in constant time. `/healthz` and `/readyz` stay open so an
  orchestrator can probe without credentials; they return only a status.
- **The admin listener** requires the same token for `/metrics` and pprof.
  It is plain HTTP, which is why it defaults to loopback: put it behind your
  own TLS if it must be reachable from elsewhere.
- **Client URLs in `peers` must be `https://`**, since they are handed to
  clients.
- The token and private keys are read from files, not from flags, so they do
  not appear in process listings. They are never logged and never appear in
  metrics or error responses.

`scripts/gen-dev-certs.sh` creates a throwaway CA, node certificates and a
random token under `./certs` (git-ignored) for trying this out:

```sh
scripts/gen-dev-certs.sh
scripts/cluster.sh secure-up
bin/kvctl --endpoints https://127.0.0.1:8201,https://127.0.0.1:8202,https://127.0.0.1:8203 \
          --ca-cert certs/ca.pem --token-file certs/client.token put greeting hello
```

Those certificates are for experiments. For a real deployment issue them
from your own CA, keep the CA key away from the nodes, and deliver keys and
the token through a secret store.

### Containers

The image runs `kvserver` as UID 10001 with a read-only root filesystem, all
capabilities dropped and `no-new-privileges`. `docker-compose.chaos.yml` is a
**test-only** override that builds an image containing `iptables` and grants
`NET_ADMIN` and `NET_RAW` so scripts can cut nodes off from each other. The
server process still runs unprivileged; the rules are applied with
`docker compose exec -u 0`. Do not deploy that override.

### What is not covered

No encryption of data on disk. No audit log. No rate limiting per client
beyond the global overload bound. No token rotation without a restart. No
certificate revocation checking. One shared client token. The pprof
endpoints, if enabled, expose internals and should stay on loopback.

### Known dependency advisory

`make vuln` reports one advisory, listed with its justification in
`.vuln-exceptions`: GO-2026-6443, a server panic in grpc-go triggered by a
request without an authority header. The latest grpc-go release (v1.84.0) is
affected; the fix is only on an unreleased branch. Here it would stop one
node, which the cluster tolerates, and in secure mode only a holder of a peer
certificate can send the request. Upgrade when v1.85.0 is released. The scan
fails on any finding not listed in that file.

## Health and metrics

- `GET /healthz`: **liveness**. 200 while the process is up and its event
  loop has not been stopped by a storage failure.
- `GET /readyz`: **readiness**. 200 when this node currently knows a leader,
  itself or another. A node that is up but cut off, or in a cluster without
  a quorum, is alive and not ready.
- `GET /v1/status`: role, term, leader, commit and applied indexes, snapshot
  index, key count, and on the leader each follower's progress.
  `kvctl status` prints it for every endpoint.
- `GET /metrics` on the admin listener, in Prometheus format.

Metrics worth watching:

| Metric | Why |
|---|---|
| `raftkv_raft_is_leader`, `raftkv_raft_has_leader`, `raftkv_raft_term` | exactly one node should lead; a climbing term means repeated elections |
| `raftkv_raft_leader_changes_total` | elections over time |
| `raftkv_raft_commit_index`, `raftkv_raft_applied_index` | progress; should move together |
| `raftkv_replication_lag_entries{peer}` | on the leader, how far each follower is behind |
| `raftkv_wal_fsync_duration_seconds` | disk health; this bounds write latency |
| `raftkv_proposal_duration_seconds{outcome}` | request latency inside the node, and how requests end |
| `raftkv_rejected_total{reason}` | overload |
| `raftkv_peer_rpc_failures_total{rpc}` | peers unreachable |
| `raftkv_snapshots_created_total`, `..._installed_total`, `..._sent_total` | snapshot activity |
| `raftkv_kv_state_bytes`, `raftkv_kv_sessions` | closeness to the state limits |
| `raftkv_healthy` | 0 after a storage failure |

Labels are operation names, outcomes and peer IDs only. Keys, values and
client identities never become labels.

## Limits

| | |
|---|---|
| Cluster size | fixed at start; 3 and 5 tested. Odd sizes are sensible: 4 tolerates no more failures than 3. |
| Key | 1 to 256 bytes of UTF-8, no control characters |
| Value | 0 to 64 KiB. JSON strings, so UTF-8; base64-encode binary data. |
| Total data | 64 MiB of keys plus values, all held in memory |
| Operations | single-key only |
| Retry de-duplication | 4096 most recently active client IDs; latest request per client |
| Request deadline | 5 s default, 30 s maximum |

## Troubleshooting

### A node refuses to start

- **`configuration error`** (exit 2): the message names the setting.
- **`data directory is locked by another process`**: another `kvserver` has
  it. If none is running the lock is already gone; check the path.
- **`data directory belongs to cluster/node ...`**: the directory was created
  by a different node or cluster. Point the node at its own directory.
- **`corrupt data`**: the WAL or a snapshot failed verification in a way an
  interrupted write cannot explain. The node stops rather than guess. Do
  **not** delete the directory and restart the node under the same ID; a
  node that has forgotten its votes and acknowledgements can cause committed
  data to be lost ([ADR 0005](adr/0005-fixed-membership.md)). If the
  remaining nodes are a majority, the cluster keeps working. Keep the
  damaged directory, and rebuild the cluster from the healthy nodes' data
  when convenient (below).

### No leader

`kvctl status` shows every node as follower or pre-candidate and requests
return `NO_LEADER`.

- Fewer than a majority of nodes are running, or they cannot reach each
  other. Check `raftkv_peer_rpc_failures_total` and each node's log.
- The nodes disagree about `peers` or `cluster-id`. Every node needs the
  same list; mismatched cluster IDs show up as refused RPCs.
- In secure mode, a certificate problem: a wrong common name, a missing
  host name in the certificate, or an expired certificate. The dialling
  node logs the TLS error.

### Requests time out (`TIMEOUT`, outcome unknown)

The leader accepted the request and could not commit it in time. Either it
has lost contact with a majority (it will step down within two election
timeouts and start answering `NO_LEADER`), or the disk is slow: look at
`raftkv_wal_fsync_duration_seconds`. The request may still take effect.
Repeat it with the same `X-Client-Id` and `X-Request-Seq` to learn its result
without applying it twice; `kvctl` prints the exact flags.

### `OVERLOADED`

More requests are waiting than `max-pending-proposals`. The node is alive
and refusing work it cannot do promptly. Clients should back off, as
`kvctl` and the Go client do. If it persists the disk is usually the limit.

### A follower stays behind

`raftkv_replication_lag_entries` for it stays high on the leader. If it is
reachable it will catch up, from the log or, if it fell too far behind, from
a snapshot (`raftkv_snapshots_sent_total` on the leader). A follower that
keeps failing to install a snapshot logs the reason.

## Recovery boundaries

| Situation | Outcome |
|---|---|
| A minority of nodes crash or are cut off | cluster stays available; they catch up when they return |
| A majority is down or unreachable | cluster unavailable for reads and writes; no data lost; resumes when a majority is back |
| Every node is killed at once (power loss) | all acknowledged writes are there after restart |
| One node's disk is lost or corrupt | cluster stays available on the others; that node cannot be safely replaced in place |
| A majority of disks are lost | acknowledged writes may be lost; outside the guarantee |
| A disk returns wrong data after fsync | detected by checksum at startup when it affects the WAL or snapshot; not repaired |

### Rebuilding a cluster

There is no membership change and no backup command. To replace a node whose
disk was lost, or to change the cluster's size, build a new cluster and copy
the data in through the API:

1. Start a new cluster with a new `cluster-id` and empty data directories.
2. Read each key from the old cluster and write it to the new one.
3. Point clients at the new cluster.

There is no "list keys" endpoint, so step 2 needs the key set from your
application. That gap, a snapshot export, and membership changes are the
first things on the future-work list.

## Shutting down and upgrading

`SIGTERM` or `SIGINT` shuts a node down cleanly within `shutdown-timeout`.
Requests it had accepted but not answered get "outcome unknown" (they may
commit on the other nodes). Stopping containers with `docker compose down`
or `stop` never removes data; only `scripts/cluster.sh reset` does, and it
asks first.

Restart nodes one at a time and wait for `kvctl status` to show the
restarted node caught up before the next one, so a majority is always
available. All nodes must run a build with the same on-disk format, state
machine limits and peer protocol; there is no rolling upgrade across changes
to those.
