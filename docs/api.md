# HTTP API

All bodies are JSON. In secure mode every `/v1/` request needs
`Authorization: Bearer <token>`.

## Operations

### `PUT /v1/kv/{key}`

```
PUT /v1/kv/greeting
X-Client-Id: alice
X-Request-Seq: 1

{"value": "hello"}
```

```
200 OK
{"key": "greeting", "revision": 12, "duplicate": false}
```

### `GET /v1/kv/{key}`

```
200 OK
{"key": "greeting", "value": "hello", "revision": 12}
```

`404 KEY_NOT_FOUND` if the key does not exist. A read is linearizable: it
reflects every write acknowledged before the request was sent.

### `DELETE /v1/kv/{key}`

```
200 OK
{"key": "greeting", "existed": true, "duplicate": false}
```

Deleting a key that does not exist succeeds, with `"existed": false`.

### `POST /v1/kv/{key}/cas`

Writes `value` if exactly one stated condition holds:

| Body field | Holds when |
|---|---|
| `"expect_absent": true` | the key does not exist |
| `"expect_value": "..."` | the key exists and has exactly this value (an empty string is a value) |
| `"expect_revision": N` | the key exists at revision N |

```
POST /v1/kv/greeting/cas
{"value": "hi", "expect_value": "hello"}
```

```
200 OK
{"key": "greeting", "swapped": true, "revision": 15, "duplicate": false}
```

If the condition does not hold nothing changes:

```
409 Conflict
{"error": {"code": "CAS_FAILED", "message": "...", "outcome": "not_applied",
           "retryable": false, "exists": true, "revision": 15}}
```

The comparison and the write are one step in the replicated log. No other
operation can come between them.

A value condition is exposed to the ABA problem: it cannot tell "still
hello" from "changed and changed back". A revision condition can.

## Keys, values, revisions

- **Key**: 1 to 256 bytes of UTF-8, no control characters. Percent-encode it
  in the path; `/` must be sent as `%2F`.
- **Value**: 0 to 64 KiB, a JSON string. An empty value is not the same as a
  missing key. For binary data, base64-encode it yourself.
- **Revision**: the position in the replicated log of the write that last
  set the key. It grows across the whole store, not per key, and a deleted
  and recreated key gets a new, higher revision. A revision seen once can
  therefore never match a later incarnation of the key.

## Request identity and retries

Send `X-Client-Id` (up to 64 bytes) and `X-Request-Seq` (a positive integer)
on every `PUT`, `DELETE` and CAS. The rules:

- One client ID, one request at a time. Number requests in increasing order.
- To retry, send the same request again with the same two headers. If the
  earlier attempt took effect you get its original result back, with
  `"duplicate": true`, and nothing is applied a second time. That holds on
  any node, across a leader change and across restarts.
- The same identity with a different body is refused: `409 IDENTITY_REUSED`.
- A sequence number lower than one already used by that client is refused:
  `409 STALE_SEQUENCE`. Only the latest result per client is kept.
- The server remembers the 4096 most recently active client IDs. A retry
  from a client that has dropped out of that set is treated as new.

Without the headers the server assigns a one-off identity and returns it in
the response headers. The request works, but there is nothing to retry with.

`GET` needs no identity. Repeating a read is always safe.

## Deadlines

`?timeout=2s` sets how long the server may spend on a request, up to the
configured maximum (30 s); the default is 5 s. A longer value is capped, not
refused. When it passes the server answers `504 TIMEOUT`.

**A timeout does not mean the write failed.** The request was placed in the
log; it may still commit. The response says `"outcome": "unknown"`. Retry
with the same identity to get a definite answer.

## Errors

```
{"error": {"code": "NOT_LEADER", "message": "...", "outcome": "not_applied",
           "retryable": true, "leader": {"id": "n2", "url": "http://127.0.0.1:8002"}}}
```

`outcome` is `not_applied` when the request definitely changed nothing and
`unknown` when it may have, or may yet.

| HTTP | Code | Outcome | Retry? | Meaning |
|---|---|---|---|---|
| 400 | `INVALID_ARGUMENT` | not applied | no | malformed request, or a key or value outside the limits |
| 401 | `UNAUTHENTICATED` | not applied | no | missing or wrong token |
| 404 | `KEY_NOT_FOUND` | not applied | no | `GET` of a missing key |
| 409 | `CAS_FAILED` | not applied | no | the condition did not hold; includes the key's `exists` and `revision` |
| 409 | `IDENTITY_REUSED` | not applied | no | this identity was used for a different request |
| 409 | `STALE_SEQUENCE` | not applied | no | the client has already sent a higher sequence number |
| 413 | `PAYLOAD_TOO_LARGE` | not applied | no | body over 1 MiB |
| 421 | `NOT_LEADER` | not applied | yes | send it to the node in `leader.url` (also in the `X-Raft-Leader` header). The hint can be out of date. |
| 429 | `OVERLOADED` | not applied | yes, after a pause | too many requests waiting |
| 503 | `NO_LEADER` | not applied | yes | this node knows no leader: an election is under way, the node is cut off, or the cluster has no quorum |
| 503 | `LEADERSHIP_LOST` | not applied | yes | leadership changed before the request committed; its log entry was overwritten |
| 503 | `SHUTTING_DOWN` | not applied | yes, elsewhere | the node is stopping |
| 504 | `TIMEOUT` | **unknown** | yes, with the same identity | submitted, not completed in time; may still take effect |
| 507 | `CAPACITY_EXCEEDED` | not applied | no | the store's 64 MiB limit would be exceeded |
| 500 | `INTERNAL` | unknown | no | unexpected |

A node that is not the leader never forwards a request. It answers
`NOT_LEADER` with a hint and the client goes there. `kvctl` and the Go
client in `internal/client` do that, back off between attempts, and keep the
identity the same across them.

## Health and status

| Endpoint | Auth | Answer |
|---|---|---|
| `GET /healthz` | none | 200 while the process is up and storage has not failed |
| `GET /readyz` | none | 200 when this node knows a leader; 503 otherwise |
| `GET /v1/status` | token | role, term, leader, indexes, snapshot index, key count, follower progress |
| `GET /metrics` (admin listener) | token, if configured | Prometheus metrics |

## With curl

```sh
curl -s -X PUT localhost:8001/v1/kv/greeting \
     -H 'X-Client-Id: demo' -H 'X-Request-Seq: 1' -d '{"value":"hello"}'
curl -s localhost:8001/v1/kv/greeting
curl -s -X POST localhost:8001/v1/kv/greeting/cas \
     -H 'X-Client-Id: demo' -H 'X-Request-Seq: 2' -d '{"value":"hi","expect_value":"hello"}'
curl -s -X DELETE localhost:8001/v1/kv/greeting \
     -H 'X-Client-Id: demo' -H 'X-Request-Seq: 3'
```

If `localhost:8001` is not the leader these return `NOT_LEADER` with the
address to use. `kvctl` follows that for you.
