# HookRelay architecture

This document is cumulative through Stage 3 (`0.3.0`). It describes current
behavior and labels roadmap work explicitly. A successful local demonstration
is evidence of the implemented path, not evidence of exactly-once delivery,
high availability, production security, or benchmark scale.

## Current system: Stage 3 durable delivery pipeline

```text
Deployment operator                         Producer
        |                                      |
        | bootstrap token                      | tenant API key
        v                                      v
+-------------------------- FastAPI / Uvicorn ---------------------------+
| tenant bootstrap | endpoint creation | idempotent event API | health  |
+--------------------------------+---------------------------------------+
                                 |
                                 | event + N delivery snapshots
                                 | + N outbox rows, one transaction
                                 v
                         +-----------------+
                         |   PostgreSQL    |
                         | source of truth |
                         +--^-----------^--+
                            |           |
              claim / mark  |           | load / attempt / finish
                            |           |
                    +-------+--+        |
                    | outbox   |        |
                    | publisher|        |
                    +-----+----+        |
                          |             |
                          | ID-only     |
                          v             |
                 +----------------+     |
                 | NATS JetStream |-----+
                 | durable stream | durable pull
                 +----------------+     |
                                        v
                              +------------------+
                              | bounded delivery |
                              | worker           |
                              +--------+---------+
                                       | HMAC-signed HTTP
                                       v
                              +------------------+
                              | local receiver   |
                              | exact-byte ledger|
                              +------------------+
```

The API, publisher, worker, and receiver are independent processes. A broker or
receiver outage does not become a synchronous `POST /v1/events` dependency.
The API's durable-acceptance transaction remains PostgreSQL-only.

## Runtime components and ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| FastAPI application | Authentication, tenant scope, validation, idempotent event acceptance, current-state reads, PostgreSQL readiness | Broker publication, delivery execution, migrations, NATS health |
| PostgreSQL | Authoritative tenant, secret, event, delivery, attempt, and outbox state | Remote receiver side effects or NATS acknowledgment state |
| Outbox publisher | Short expiring claims, ID-only JetStream publication, PubAck-before-finalize ordering | Event acceptance, HTTP delivery, retry scheduling |
| NATS JetStream | Durable dispatch message and shared consumer acknowledgment cursor | Event bodies, endpoint URLs, secrets, final domain state |
| Delivery worker | Durable pulls, authoritative reconciliation, attempt lifecycle, signing, timeout-bounded HTTP, ACK decision | Full retry/crash-recovery policy or production SSRF defense |
| Test receiver | Configurable local response and bounded exact-byte/header capture | Signature trust enforcement, durable audit storage, customer behavior |
| SQLAlchemy async engine | One connection pool per database-using process | A global shared session |
| Alembic | Ordered reviewed schema transitions | Automatic application-start migration |

Every database operation uses a short `AsyncSession`. The publisher closes its
claim transaction before broker I/O. The worker closes its attempt transaction
before HTTP and opens a second short transaction for finalization.

## Public and local HTTP surfaces

Mutation routes require `Content-Type: application/json`. Domain failures use
stable `application/problem+json` responses. Tenant identity always comes from
a verified API key; a caller never supplies the tenant ID.

| Route | Scope | Meaning of success |
| --- | --- | --- |
| `GET /health/live` | public API | Process/event loop can respond; no dependency call |
| `GET /health/ready` | public API | A bounded real PostgreSQL `SELECT 1` succeeds |
| `POST /v1/bootstrap/tenants` | deployment bootstrap bearer | Tenant and one-time initial API key committed |
| `GET /v1/tenant` | tenant API key | Authenticated tenant metadata |
| `POST /v1/endpoints` | tenant API key | Endpoint and one-time signing secret committed |
| `GET /v1/endpoints/{id}` | tenant API key | Secret-free tenant-owned endpoint metadata |
| `POST /v1/events` | tenant key + idempotency key | Event, delivery snapshots, and outbox rows committed; delivery not implied |
| `GET /v1/events/{id}` | tenant API key | Current event and delivery states |

The local test receiver exposes `/health/live`, `POST /webhooks`,
`GET /requests`, `GET /requests/{delivery_id}`, and `DELETE /requests`. It is a
Compose/test instrument and must not be deployed as a product endpoint.

## Durable ingestion and idempotency boundary

Stage 2 behavior remains unchanged:

1. API-key authentication derives tenant context.
2. A versioned canonical fingerprint covers event type, payload, and endpoint
   set.
3. Uniqueness on `(tenant_id, idempotency_key)` plus PostgreSQL
   `ON CONFLICT DO NOTHING RETURNING` closes concurrent races.
4. A matching retry returns the original `201` representation and
   `Idempotency-Replayed: true`; changed input returns `409`.
5. One transaction creates one event, one pending delivery per endpoint, and
   one unpublished outbox row per delivery.
6. Each delivery snapshots its target URL and exact signing-secret row/version.

`201` means the acceptance transaction committed. The create response reports
the original pending representation; `GET /v1/events/{id}` is the current-state
view that can later report `delivering` or `succeeded`.

## Transactional outbox publication

### Message contract

The version-1 `delivery.requested` message contains only:

```text
type, schema_version, message_id, tenant_id,
event_id, endpoint_id, delivery_id
```

`message_id` equals the outbox UUID and becomes `Nats-Msg-Id`. Event payload,
URL, and signing secret remain in PostgreSQL.

### Claim transaction

Migration `20260803_0002` adds nullable `claim_token` and `claim_expires_at`
columns. Constraints require both-or-neither, require expiry after row creation,
and prevent a published row from remaining claimed. A partial index supports
unpublished eligibility scans.

The publisher:

1. selects unpublished, unclaimed/expired rows ordered by creation time and ID;
2. applies a bounded limit and `FOR UPDATE SKIP LOCKED`;
3. validates topic/schema/payload identity;
4. assigns one random token and database-time expiry;
5. commits before NATS I/O;
6. publishes sequentially and waits for the expected stream PubAck;
7. conditionally sets `published_at` and clears the claim using row ID + token;
8. releases still-owned current/remaining claims after a handled failure.

The settings model requires claim TTL to be strictly greater than
`batch_size * per_publish_timeout`, covering the maximum aggregate configured
NATS publish-wait budget. Per-item PostgreSQL finalization and loop overhead are
additional, so operators should retain margin rather than treating the formula
as a total batch-runtime bound. During cooperative shutdown, the publisher
checks its stop event between items and releases the unprocessed remainder.

A hard crash leaves the lease to expire. A broker success followed by lost
PubAck, or a crash before PostgreSQL finalization, can republish. That is an
intentional at-least-once boundary.

## JetStream topology

Local Compose runs `nats:2.14.3-alpine3.22` with JetStream enabled, storage at
`/data`, and a named `nats_data` volume. Client and monitoring ports bind to
host loopback.

### Stream

| Setting | Current value |
| --- | --- |
| Name | `HOOKRELAY_DELIVERIES_V1` |
| Subject | `hookrelay.delivery.requested.v1` |
| Retention | work queue |
| Storage | file |
| Discard policy | reject new messages when limits are reached |
| Maximum consumers | one overlapping consumer |
| Maximum message bytes | 16,384 |
| Default stream byte limit | 1 GiB |
| Duplicate window | 600 seconds |
| Replicas | one |

### Consumer

| Setting | Current value |
| --- | --- |
| Durable name | `HOOKRELAY_DELIVERY_WORKERS_V1` |
| Delivery | pull, deliver all, instant replay |
| Acknowledgment | explicit |
| Default `AckWait` | 30 seconds |
| `MaxDeliver` | unlimited (`-1`) |
| Default `MaxAckPending` | 32 |
| Filter | exact delivery-requested subject |

Every publisher/worker connection idempotently creates absent assets and
validates important existing settings. Incompatible drift fails startup rather
than silently mutating a durable contract.

One file-backed local replica survives ordinary process/container recreation
when the named volume remains. It is not a quorum, failover, backup, or HA
design.

## Worker execution and concurrency

The worker binds to the existing shared durable pull consumer. It fetches no
more than its local concurrency, waits for the batch to finish, and then fetches
again. An `asyncio.Semaphore` repeats the hard limit inside execution. The
long-lived async HTTP client's connection and keep-alive limits match worker
concurrency; consumer `MaxAckPending` must be at least that limit.

For each strict message:

1. Lock the tenant-owned delivery.
2. Reconcile the entire broker message with the persisted outbox and delivery.
3. If the delivery already succeeded, skip HTTP and ACK.
4. If it is delivering with an unfinished attempt, send broker progress and do
   not start a concurrent HTTP call.
5. Require pending state, load the event and snapshotted secret, and enforce the
   local/test outbound policy.
6. Decrypt the exact secret using tenant/endpoint/row/version AES-GCM AAD.
7. Insert the next attempt and set delivery `delivering`; commit.
8. Send timeout-bounded HTTP without a database lock/session.
9. Lock again and finish the owned attempt. A 2xx sets attempt and delivery
   `succeeded`; failure records `transient_failure` and returns delivery to
   `pending`.
10. Call `ack_sync` only after a success commit.

Malformed messages and messages whose identities contradict authoritative
PostgreSQL state are poison and are terminated. A destination blocked by the
temporary outbound policy is different: it remains unacknowledged and
recoverable, because a later reviewed policy/configuration may allow it.

## Versioned webhook wire contract

The exact compact, sorted-key UTF-8 body contains:

```json
{
  "created_at": "<original event UTC timestamp with microseconds>",
  "delivery_id": "<delivery UUID>",
  "id": "<stable event UUID>",
  "payload": {},
  "schema_version": 1,
  "type": "<event type>"
}
```

The worker sends the compact representation without formatting whitespace. It
computes:

```text
v1=hex(HMAC-SHA256(secret, ASCII(unix_seconds) + b"." + exact_body_bytes))
```

Headers are `Content-Type`, `User-Agent`, `HookRelay-Delivery-Id`,
`HookRelay-Event-Id`, `HookRelay-Signature`, `HookRelay-Timestamp`, and
`HookRelay-Webhook-Version`.

HMAC authenticates integrity and shared-secret possession; it does not encrypt
the payload. A real receiver must verify the captured raw bytes, use a bounded
timestamp-freshness window, compare signatures in constant time, and atomically
deduplicate the stable event ID with its side effect.

## HTTP and outbound safety boundary

One async client lives for the worker process. It uses explicit
connect/read/write/pool deadlines plus an outer wall-clock timeout, follows no
redirects, ignores environment proxy variables, limits connections, and streams
the response.

Stage 3 delivery execution is restricted to `local` and `test`. The normalized
URL hostname must be in `HOOKRELAY_DELIVERY_ALLOWED_HOSTS`; defaults are
`receiver`, `127.0.0.1`, and `localhost`.

This gate is deliberately incomplete. It does not resolve/classify IPs, detect
DNS rebinding, protect all IPv6/special-use ranges, or enforce network egress.
Stage 5 owns complete SSRF and traffic-control design. A wildcard allowlist is
rejected and is not an acceptable workaround.

## State and acknowledgment invariants

1. Event `201` occurs after event/delivery/outbox commit.
2. Outbox claims are short and expire; no database lock spans NATS I/O.
3. `published_at` occurs only after expected-stream PubAck.
4. Broker payloads never carry event bodies, URLs, or secrets.
5. Worker execution never trusts broker identities without PostgreSQL checks.
6. Only local/test and explicitly allowlisted hostnames can execute in Stage 3.
7. An attempt and `delivering` state commit before HTTP.
8. No database transaction spans HTTP.
9. Exact signed bytes equal exact sent bytes.
10. A 2xx attempt and delivery success commit together.
11. Broker ACK occurs only after that commit.
12. A succeeded delivery suppresses duplicate HTTP on broker redelivery.
13. Failed or policy-blocked work remains unacknowledged/recoverable; malformed
    or state-contradictory poison messages terminate.
14. No implementation or documentation claims exactly once.

## Failure boundaries

| Window | Result |
| --- | --- |
| API commit before publisher sees row | Durable outbox remains discoverable |
| Publisher claim before crash | Lease eventually expires |
| NATS stores message before PubAck/finalization is known | Possible duplicate publication |
| Attempt commit before worker crash | Unfinished `delivering` row can remain stuck in Stage 3 |
| Receiver side effect before HookRelay success commit | Possible duplicate HTTP request |
| Success commit before broker ACK is known | Redelivery skips HTTP using succeeded database state |
| Timeout/transport/non-2xx | Transient attempt recorded, delivery pending, message unacknowledged |

Stage 4 must add persistent scheduling, backoff/jitter, maximum attempts,
classification, explicit redelivery policy, stale-attempt recovery, dead-letter
state, replay, and kill/restart evidence. Current `AckWait` redelivery is not a
finished retry system.

## Health, lifecycle, and deployment

- API liveness has no dependency call.
- API readiness probes only PostgreSQL because the API's contract is durable
  acceptance, not synchronous delivery.
- Compose health gates startup ordering but does not continuously supervise
  dependencies.
- API, publisher, and worker each dispose their database engine.
- Publisher and worker drain NATS connections during cooperative shutdown with
  a bounded default drain timeout of five seconds.
- Worker closes its long-lived HTTP client.
- Compose gives the worker a 25-second stop grace period, longer than the
  default ten-second HTTP deadline plus shutdown cleanup margin.
- NATS/PostgreSQL named volumes survive `docker compose down`; `--volumes`
  deletes them.

Environment variables are configuration delivery, not automatic secret
management. Local credentials are examples; production needs managed secrets,
TLS/authentication for NATS/PostgreSQL/HTTP boundaries, least privilege,
rotation, backup, monitoring, and reviewed deployment controls.

## Verification boundaries

| Evidence | Proves | Does not prove |
| --- | --- | --- |
| Pure unit vectors | Deterministic body/signature, strict envelope, desired topology, local concurrency bound | Real database, broker, HTTP, or process behavior |
| Settings tests | Cross-setting safety constraints and local gate | Safe DNS resolution or trustworthy deployment input |
| PostgreSQL integration | Constraints, leases, transactions, attempt/delivery state | Every crash schedule or long-running recovery |
| Real JetStream integration | Topology, PubAck, durable pull/ACK behavior | Cluster quorum, disaster recovery, production disk behavior |
| End-to-end happy path | API -> DB -> publisher -> NATS -> worker -> receiver interoperability | Exactly once, Stage 4 recovery, Stage 5 hostile-network safety, HA, capacity |
| Compose/image checks | Local topology and packaged Linux artifact | Continuous supervision or production operations |

## Roadmap boundary after Stage 3

```text
Producer
  -> API + PostgreSQL acceptance                 implemented
  -> outbox leases + JetStream publication       implemented
  -> bounded signed delivery + local receiver    implemented
  -> retry/backoff/crash recovery/dead letters   Stage 4
  -> full SSRF/rate/circuit controls              Stage 5
  -> telemetry and operations console            Stage 6
  -> fault/scale evidence and release             Stage 7
```

## Decision index

See [Architecture Decision Records](decisions/README.md), especially:

- [0004: at-least-once delivery](decisions/0004-at-least-once-delivery.md)
- [0005: transactional outbox](decisions/0005-transactional-outbox.md)
- [0008: NATS JetStream dispatch](decisions/0008-nats-jetstream-dispatch.md)
- [0009: versioned webhook signature](decisions/0009-versioned-webhook-signature.md)
- [0010: Stage 3 local outbound gate](decisions/0010-stage3-local-outbound-gate.md)
