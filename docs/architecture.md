# HookRelay architecture

This document is cumulative through Stage 4 (`0.4.0`). It describes current
behavior and labels roadmap work explicitly. HookRelay provides at-least-once
delivery with persistent bounded recovery; it does not claim exactly once,
high availability, production security, or benchmark scale.

## Current system: Stage 4 failure recovery

```text
Deployment operator                         Producer
        |                                      |
        | bootstrap token                      | tenant API key
        v                                      v
+-------------------------- FastAPI / Uvicorn ---------------------------+
| tenant bootstrap | endpoints | idempotent events | replay | health    |
+--------------------------------+---------------------------------------+
                                 |
                                 | event + N delivery snapshots
                                 | + N generation-1 outbox rows
                                 | one PostgreSQL transaction
                                 v
             +---------------- PostgreSQL ----------------+
             | source of truth                            |
             | event / delivery / attempts / outbox       |
             | generation / due time / claim / terminal   |
             +--------^-------------------------^---------+
                      |                         |
             claim / publish / mark      claim / finish / recover
                      |                         |
              +-------+--------+                |
              | outbox publisher|               |
              +-------+--------+                |
                      | strict ID-only schema v1 |
                      v                          |
              +-----------------+ durable pull  |
              | NATS JetStream  |--------------+
              | work queue      |               v
              +-------^---------+       +------------------+
                      |                 | bounded delivery |
                      | delayed NAK     | worker           |
                      +-----------------+--------+---------+
                                                 | HMAC-signed HTTP
                                                 v
                                        +------------------+
                                        | local receiver   |
                                        | exact-byte ledger|
                                        +------------------+

dead_lettered -- authenticated replay transaction --> generation + 1
               + fresh outbox UUID --> publisher --> JetStream
```

The API, publisher, worker, and receiver are independent processes. Broker,
destination, or worker failure does not become a synchronous dependency of
`POST /v1/events`. PostgreSQL remains the durable acceptance and recovery
boundary.

## Runtime components and ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| FastAPI application | Authentication, tenant scope, validation, idempotent ingestion, current-state reads, dead-letter replay, PostgreSQL readiness | Broker publication, HTTP execution, migrations, NATS health |
| PostgreSQL | Authoritative event/delivery/attempt/outbox state, retry due time, generation, claims, terminal reason | Remote side effects or NATS cursor state |
| Outbox publisher | Short expiring claims, ID-only publication, PubAck-before-finalize | Ingestion, attempt/retry policy, HTTP |
| NATS JetStream | Durable dispatch message, delayed wake-up, shared consumer ACK state | Event bodies, URLs, secrets, retry budget, terminal domain state |
| Delivery worker | Durable pull, authoritative reconciliation, attempt lease/recovery, signing, classification, ACK/NAK/TERM decision | Full production SSRF/rate/circuit controls |
| Test receiver | Configurable local response and bounded exact-byte capture | Durable audit, signature enforcement, customer behavior |
| SQLAlchemy async engine | One connection pool per database-using process | A shared global session |
| Alembic | Ordered reviewed schema transitions and data backfill | Automatic process-start migration |

Database operations use short `AsyncSession` units. No transaction spans NATS
or HTTP I/O. Outbox and delivery claims commit before their external operation;
later conditional updates prove ownership.

## Public and local HTTP surfaces

Tenant identity always comes from a verified API key. Mutation routes with JSON
bodies require `Content-Type: application/json`; stable failures use sanitized
`application/problem+json`.

| Route | Scope | Meaning of success |
| --- | --- | --- |
| `GET /health/live` | public | Process/event loop responds; no dependency call |
| `GET /health/ready` | public | Bounded PostgreSQL `SELECT 1` succeeds |
| `POST /v1/bootstrap/tenants` | deployment bootstrap bearer | Tenant and one-time initial key committed |
| `GET /v1/tenant` | tenant key | Authenticated tenant metadata |
| `POST /v1/endpoints` | tenant key | Endpoint and one-time signing secret committed |
| `GET /v1/endpoints/{id}` | tenant key | Secret-free tenant-owned endpoint metadata |
| `POST /v1/events` | tenant key + idempotency key | Event, delivery snapshots, and generation-1 outbox rows committed |
| `GET /v1/events/{id}` | tenant key | Current status, dispatch generation, retry due time, and terminal reason |
| `POST /v1/deliveries/{id}/replay` | tenant key | `202`; dead-lettered delivery reset to pending in a fresh generation/outbox transaction |

Replay requires JSON `expected_dispatch_generation` and includes
`Location: /v1/events/{event_id}`. Missing/cross-tenant delivery IDs are
indistinguishable `404`; changed generation returns
`409 delivery_generation_conflict`; a non-dead-lettered delivery returns
`409 delivery_not_replayable`.

The local receiver exposes `/health/live`, `POST /webhooks`, `GET /requests`,
`GET /requests/{delivery_id}`, and `DELETE /requests`. It is not a product
service and loses its bounded in-memory evidence on restart.

## Durable ingestion and idempotency

Stage 2's boundary remains:

1. API-key authentication derives tenant context.
2. A versioned canonical fingerprint covers event type, payload, and endpoint
   set.
3. `(tenant_id, idempotency_key)` uniqueness plus PostgreSQL
   `ON CONFLICT DO NOTHING RETURNING` closes concurrent races.
4. Matching retry returns the original `201` creation representation and
   replay header; changed input returns `409`.
5. One transaction creates the immutable event, one pending delivery per
   endpoint, and one generation-1 outbox row per delivery.
6. Each delivery snapshots its target URL and exact signing-secret version.

`201` proves only that PostgreSQL committed acceptance and dispatch intent. The
creation representation remains the original pending view; authenticated event
GET returns later `delivering`, `retry_scheduled`, `succeeded`, or
`dead_lettered` state.

## Transactional outbox and dispatch generations

### Envelope compatibility

The internal command remains strict and ID-only. Version 1 contains:

```text
type, schema_version, message_id, tenant_id,
event_id, endpoint_id, delivery_id
```

Stage 4 does not add a broker field or version. Positive
`dispatch_generation` lives on delivery, attempt, and outbox database rows. The
worker first reconciles the seven message IDs/fields with the exact outbox row,
then derives that row's generation. Event payload, target URL, secret, and
generation remain outside NATS.

`message_id` equals the fresh outbox UUID and becomes `Nats-Msg-Id`. Outbox
uniqueness is `(delivery_id, topic, dispatch_generation)`: initial ingestion
creates generation 1, and each manual replay creates another row/UUID.

Keeping strict schema v1 prevents an old Stage 3 worker from TERMing a replay
message for an unknown field/version. This is only payload compatibility.
Manual replay should wait for Stage 4 worker cutover because an old worker can
parse the envelope but lacks generation fencing and may send a stale request.

### Publication

The publisher still:

1. scans eligible unpublished rows in stable order;
2. claims a bounded `FOR UPDATE SKIP LOCKED` batch with token/expiry;
3. validates the row/envelope schema and IDs while retaining generation on the
   PostgreSQL row;
4. commits before NATS I/O;
5. publishes sequentially and waits for the expected stream PubAck;
6. conditionally sets `published_at` using row ID and claim token;
7. releases still-owned claims after a handled failure or cooperative stop.

Claim TTL must exceed `batch_size * publish_timeout`, the aggregate configured
broker-wait budget. PubAck/finalization ambiguity still allows duplicate
publication. Finite-window `Nats-Msg-Id` deduplication reduces but cannot remove
it.

## JetStream topology and broker roles

Local Compose runs `nats:2.14.3-alpine3.22` with one named file volume.

### Stream

| Setting | Current value |
| --- | --- |
| Name | `HOOKRELAY_DELIVERIES_V1` |
| Subject | `hookrelay.delivery.requested.v1` |
| Retention/storage | Work queue / file |
| Full behavior | Reject new messages (`DiscardNew`) |
| Maximum message bytes | 16,384 |
| Default byte limit | 1 GiB |
| Duplicate window | 600 seconds |
| Replicas | One |

### Consumer

| Setting | Current value |
| --- | --- |
| Durable name | `HOOKRELAY_DELIVERY_WORKERS_V1` |
| Delivery | Pull, deliver-all, instant replay |
| Acknowledgment | Explicit |
| Default `AckWait` | 30 seconds |
| `MaxDeliver` | Unlimited (`-1`) |
| Default `MaxAckPending` | 32 |
| Filter | Exact delivery-requested subject |

`MaxDeliver=-1` is deliberate. Broker deliveries include due-time deferrals,
active-lease deferrals, and lost ACKs. PostgreSQL counts outbound or ambiguous
attempts and owns the maximum. The broker supplies durable transport and delayed
wake-ups, not the business retry ledger.

Every publisher/worker idempotently creates absent assets and rejects important
topology drift. One server/replica/volume provides local persistence, not
quorum, failover, backup, or HA.

## Worker state machine and attempt ownership

For each broker message, the worker locks the tenant-owned delivery and
reconciles the complete envelope with the outbox and domain rows.

### Generation and terminal gates

- Message generation below the delivery is stale: skip HTTP and ACK.
- Message generation ahead of PostgreSQL is poison: TERM.
- `succeeded` skips HTTP and ACKs.
- `dead_lettered` skips HTTP and ACKs.

### Due-time gate

For `retry_scheduled`, PostgreSQL `clock_timestamp()` is compared with
`next_attempt_at`.
An early message creates no attempt and receives a delayed NAK for the remaining
time. Due work proceeds only if current-generation attempts remain below the
configured maximum.

### Attempt lease

One short transaction creates an attempt with lifetime number, current
generation, and random claim token; copies the token to the delivery; sets
database-time expiry; changes state to `delivering`; and commits. A partial
unique index allows one unfinished attempt per delivery.

Default delivery claim TTL is 20 seconds and must exceed the ten-second HTTP
timeout plus the explicit five-second finalization margin. Claim and due checks
use PostgreSQL `clock_timestamp()` so wall time is not frozen at transaction
start. An unexpired claim causes a delayed NAK for its remaining lease rather
than another HTTP request. Attempt duration is `BIGINT` so long-stale recovery
cannot overflow 32-bit milliseconds.

After expiry, recovery marks the unfinished row `abandoned` with
`worker_lease_expired`, counts it against the current-generation budget, clears
the claim, and schedules or dead-letters. A finalizer must match attempt,
generation, token, delivering state, and unexpired lease. A late worker is
fenced with no stale-handle broker disposition.

## Retry, classification, and terminal policy

### Classification

| Observation | Outcome |
| --- | --- |
| `2xx` | Success |
| `408`, `425`, `429`, `5xx` | Transient failure |
| Request timeout or async HTTP transport error | Transient failure |
| Other HTTP status | Permanent failure |
| Pre-Stage-5 target policy rejection | Terminal `target_blocked`, without HTTP attempt |

Stage 4 does not follow redirects or honor `Retry-After`.

### Backoff

For current-generation attempt `n`:

```text
ceiling    = min(retry_max, retry_base * 2^(n - 1))
multiplier = (1 - jitter_ratio) + jitter_ratio * U  # U uniform [0, 1]
delay      = max(0.1, ceiling * multiplier)
```

Defaults are base 1 second, cap 60 seconds, ratio 0.25, and maximum five
attempts per dispatch generation. The resulting due time and failed attempt
commit together before delayed NAK. A generation-specific count drives policy;
lifetime attempt number never resets.

### Terminal state

Permanent response dead-letters immediately. Transient/abandoned work
dead-letters when attempts are exhausted. A blocked target dead-letters as
`target_blocked` before any HTTP attempt. Terminal state requires
`dead_lettered_at` and one of:

```text
permanent_failure | attempts_exhausted | target_blocked
```

The broker message is ACKed after the terminal transaction commits. A separate
dead-letter stream does not exist.

## Broker disposition matrix

| Executor/boundary | Disposition |
| --- | --- |
| `succeeded`, `already_succeeded` | Synchronous ACK |
| `dead_lettered` | Synchronous ACK after terminal commit |
| Old-generation `stale` | Synchronous ACK |
| `retry_scheduled` | Delayed NAK using policy delay |
| Active-lease `in_progress` | Delayed NAK using remaining lease |
| Target-blocked persisted by executor | Terminal ACK |
| Target-block exception before persistence | Fixed delayed-NAK fallback |
| Malformed/contradictory internal message | TERM |
| Lost claim / stale finalizer | No stale-handle disposition |
| Unexpected interruption | No disposition; broker recovery remains available |

This ordering chooses duplicate observation over ACK-before-state silent loss.

## Versioned webhook wire contract

Stage 4 preserves webhook contract version 1. The compact sorted-key UTF-8 body
contains original event time, stable event ID, delivery ID, payload, schema
version, and type. The HMAC remains:

```text
v1=hex(HMAC-SHA256(secret, ASCII(unix_seconds) + b"." + exact_body_bytes))
```

Headers remain content type, service `User-Agent`, delivery ID, stable event
ID, signature, timestamp, and webhook version. `User-Agent` now reports
`HookRelay/0.4.0`; it is not part of the signed content.

HMAC authenticates exact bytes and possession of the snapshotted secret. It
does not encrypt payloads, prove freshness alone, or deduplicate receiver side
effects. A receiver must verify raw bytes with constant-time comparison, apply
a timestamp window, and atomically deduplicate the stable event ID.

## Manual dead-letter replay

`POST /v1/deliveries/{id}/replay` locks the delivery within authenticated tenant
scope. Only `dead_lettered` is eligible. In one transaction it:

1. increments `dispatch_generation`;
2. sets `pending` and clears schedule/terminal/claim fields;
3. creates a new schema-v1 outbox row/UUID whose row stores the new generation;
4. commits before returning `202` and the event `Location`.

The fresh UUID avoids reusing an ACKed message and JetStream's duplicate window.
Old-generation messages are ACKed as stale. Lifetime attempts and old outbox
rows remain intact; the new generation receives a fresh bounded budget.

Replay is not idempotency-keyed. Its required observed-generation precondition
and row lock ensure one ambiguous operator intent advances at most once. A
duplicate/stale body returns `409 delivery_generation_conflict`; current event
detail exposes generation, due time, and terminal reason for reconciliation.
`202` is dispatch acceptance, not delivery success.

## Outbound safety boundary

Workers still run only in `local` and `test`. Normalized hostnames must be in
`HOOKRELAY_DELIVERY_ALLOWED_HOSTS`; redirects and environment proxies remain
disabled.

The executor persists a blocked target as `dead_lettered` with reason
`target_blocked`, then the worker ACKs. This avoids an infinite policy retry
loop while preserving operator recovery through replay after a reviewed
allowlist change. A fixed delayed NAK remains only a fallback if policy rejection
escapes before a terminal decision can commit.

This is not complete SSRF defense. It does not resolve/classify addresses,
handle DNS rebinding, cover special IPv4/IPv6 ranges, or enforce egress. Stage 5
must replace the temporary boundary before production execution is enabled.

## Database constraints and migration behavior

Migration `20260804_0003` adds:

- positive delivery, attempt, and outbox dispatch generations;
- delivery retry time, terminal timestamp/reason, claim token/expiry;
- attempt generation and claim token;
- `BIGINT` attempt duration for safe long-stale recovery;
- exact state/schedule/terminal/claim consistency constraints;
- one unfinished attempt per delivery;
- partial due-time index;
- per-generation outbox uniqueness.

Upgrade preserves Stage 3 evidence: unfinished attempts become `abandoned` with
`stage4_migration_recovery`, old `delivering` rows become immediately
`retry_scheduled`, and existing rows are generation 1. Downgrade proceeds only
when every row remains representable in Stage 3: no delivery may be
`delivering`, `retry_scheduled`, or `dead_lettered`; every delivery, attempt, and
outbox generation must be 1; and attempt duration must fit the former 32-bit
integer. It refuses instead of silently mapping or truncating Stage 4 evidence.

## State and acknowledgment invariants

1. Event `201` follows event/delivery/outbox commit.
2. Outbox `published_at` follows expected-stream PubAck.
3. Broker envelopes remain ID-only and are reconciled with PostgreSQL.
4. The schema-v1 broker payload remains unchanged; generation comes from the
   exact reconciled outbox row.
5. One unfinished attempt and matching delivery claim may exist.
6. Attempt/claim commits before HTTP; no database transaction spans HTTP.
7. PostgreSQL `clock_timestamp()` owns claim expiry and retry due time.
8. Only due current-generation work can create a new attempt.
9. Retry failure and due time commit together before delayed NAK.
10. Permanent/exhausted/blocked terminal state commits before ACK.
11. Success attempt and delivery commit together before ACK.
12. A stale finalizer cannot mutate a recovered owner.
13. An old-generation message cannot execute a replay generation.
14. Replay generation/state/fresh outbox commit atomically before `202`.
15. Attempts/history remain; replay resets only generation-scoped budget.
16. No documentation or implementation claims exactly once.

## Failure boundaries

| Window | Result |
| --- | --- |
| API commit before publisher | Durable outbox remains discoverable |
| PubAck before outbox finalization | Possible duplicate publication |
| Attempt commit before worker death | Lease eventually abandons and schedules/terminates |
| Receiver acts before HookRelay finalization | Possible duplicate HTTP on recovery |
| Retry commit before NAK | Lost NAK leads to `AckWait`; DB still enforces due time |
| Success/dead-letter commit before ACK | Redelivery observes terminal state and ACKs without HTTP |
| Lease expiry before old worker finalizes | Old finalizer is fenced; remote side effect remains ambiguous |
| Replay commit before publisher | Fresh outbox intent survives broker outage |
| Old message after replay | Lower generation ACKs stale without HTTP |
| NATS volume loss | Published unacked transport may be lost; local topology is not DR |

## Health, lifecycle, and deployment

- API liveness has no dependency call.
- API readiness probes only PostgreSQL because API contract is durable
  acceptance/replay, not synchronous background completion.
- Compose health gates initial order, not continuous supervision.
- API, publisher, and worker dispose database engines.
- Publisher/worker drain NATS with a bounded default five-second timeout.
- Worker closes its long-lived HTTP client.
- Worker stop grace remains longer than the HTTP/drain cleanup budget.
- Named volumes survive `docker compose down`; `--volumes` destroys them.

Environment variables deliver configuration; they are not automatic secret
management. Production needs managed credentials, TLS, least privilege,
rotation, backups, monitoring, and reviewed deployment controls.

## Verification boundaries

| Evidence | Proves | Does not prove |
| --- | --- | --- |
| Pure policy tests | Exact jitter bounds/cap, classifier, strict schema-v1 envelope, result/disposition invariants | Real database, broker, HTTP, or process behavior |
| Settings tests | Claim/timeout and base/cap relationships | Operational tuning or safe arbitrary destinations |
| PostgreSQL integration | Constraints, due state, claim/fencing, generation, dead letter, replay transactions | Every crash schedule or remote side effect |
| Real JetStream/HTTP integration | PubAck, pull, delayed/terminal disposition where exercised, signed requests | Cluster quorum, production durability, hostile networks |
| API/tenant/concurrency tests | Replay response, isolation, state serialization, fresh outbox cardinality | Lost client response handling as an idempotent API |
| Automated subprocess hard-kill plus manual exercise | A worker is killed after receiver capture; an expired claim is abandoned and a replacement recovers the same body for that schedule | Every crash point, receiver exactly once, HA, capacity |
| Compose/image checks | Local topology and packaged Linux artifact | Continuous supervision or production operations |

The Stage 4 integration suite has twelve real-service recovery scenarios,
including fresh database-clock behavior after a row-lock wait and one that
starts and terminates a separate worker OS process. At the Stage 4 checkpoint,
the complete suite passed all 143 tests. Fixture-driven expired leases and task
cancellation remain different evidence; the subprocess test proves its encoded
schedule, not arbitrary process-kill timing.

## Roadmap boundary after Stage 4

```text
Producer
  -> API + PostgreSQL acceptance                    implemented
  -> outbox leases + JetStream publication          implemented
  -> bounded signed delivery                        implemented
  -> persistent retry/classification/crash recovery implemented
  -> dead-letter state + manual replay              implemented
  -> full SSRF/rate/circuit controls                Stage 5
  -> telemetry/history UI                           Stage 6
  -> fault/scale evidence and release               Stage 7
```

## Decision index

See [Architecture Decision Records](decisions/README.md), especially:

- [0004: at-least-once delivery](decisions/0004-at-least-once-delivery.md)
- [0005: transactional outbox](decisions/0005-transactional-outbox.md)
- [0008: NATS JetStream dispatch](decisions/0008-nats-jetstream-dispatch.md)
- [0009: versioned webhook signature](decisions/0009-versioned-webhook-signature.md)
- [0010: local outbound gate](decisions/0010-stage3-local-outbound-gate.md)
- [0011: PostgreSQL-authoritative retry schedule](decisions/0011-postgresql-authoritative-retry-schedule.md)
- [0012: leased attempt recovery and dead letters](decisions/0012-leased-attempt-recovery-and-dead-letters.md)
- [0013: versioned dispatch replay](decisions/0013-versioned-dispatch-replay.md)
