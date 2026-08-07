# Stage 3: durable delivery pipeline

Stage 3 turns HookRelay's durable acceptance record into one observable,
signed webhook delivery. The implementation is version `0.3.0` and runs four
separate application processes: the API, outbox publisher, delivery worker,
and local test receiver. PostgreSQL remains authoritative; NATS JetStream is a
durable work transport, not a second domain database.

Use this guide as a learning loop, not just a runbook. For each component:
identify the **problem**, inspect the **build**, observe it running, **break it
safely**, and **explain** the invariant in your own words.

## 1. The problem this stage solves

Stage 2 could accept an event safely, but it deliberately stopped at an
unpublished `outbox_messages` row. That boundary removed the API's database /
broker dual write, but it did not move work to a receiver. Stage 3 supplies the
missing bridge:

```text
POST /v1/events
  -> PostgreSQL event + delivery + outbox commit
  -> expiring outbox claim
  -> JetStream publish acknowledgment
  -> durable pull consumer
  -> bounded worker task
  -> exact-byte HMAC-signed HTTP request
  -> successful attempt + succeeded delivery commit
  -> JetStream acknowledgment
```

The important outcome is not merely "an HTTP request happened." It is a chain
of ordered durable facts:

1. The API returns `201` only after PostgreSQL accepts the event and dispatch
   intent.
2. The publisher marks the outbox row published only after JetStream confirms
   storage.
3. The worker creates an attempt before performing HTTP.
4. The worker records the successful attempt and delivery before acknowledging
   the broker message.
5. The receiver captures the exact bytes and headers so the wire contract can
   be checked independently.

These orderings reduce silent-loss windows, but they do not form one transaction
across PostgreSQL, NATS, HTTP, and the receiver. HookRelay remains an
**at-least-once** system. A duplicated request is possible when a downstream
action succeeds but its acknowledgment is lost.

## 2. Deliberately out of scope

Stage 3 proves the happy path and establishes safe local boundaries. It does
not claim a complete failure-recovery or production-security system.

Stage 4 owns:

- persistent retry scheduling;
- exponential backoff and randomized jitter;
- maximum-attempt policy;
- designed negative-acknowledgment and redelivery behavior;
- stale-attempt and worker-crash recovery;
- transient/permanent failure classification;
- dead-letter transitions and manual replay;
- tests that kill workers or hold destinations down.

The current worker records a timeout, transport error, or non-2xx response as a
`transient_failure`, returns the delivery to `pending`, and does not acknowledge
the NATS message. JetStream can expose it again after `AckWait`, but that raw
behavior is not the Stage 4 retry subsystem: there is no schedule, backoff,
jitter, cap, or dead-letter destination.

Stage 5 owns complete outbound security and traffic control:

- DNS and resolved-IP SSRF validation, including IPv4/IPv6 special ranges;
- redirect and DNS-rebinding policy plus network egress controls;
- per-endpoint rate limiting and circuit breaking;
- request/payload-size policy for the product boundary;
- signing-secret rotation workflows and further container hardening.

Stage 3 therefore fails closed: delivery execution is allowed only when
`HOOKRELAY_ENVIRONMENT` is `local` or `test`, and the URL hostname must match an
explicit allowlist. Redirects and environment proxy variables are disabled.
This gate makes the included receiver safe to exercise locally; it is not a
claim of full SSRF protection.

Also excluded are high availability, broker clustering, production capacity
claims, benchmarks, observability dashboards, the operations UI, and
general-purpose exactly-once delivery.

## 3. Architecture before and after

### Before Stage 3

```text
Producer
   |
   v
FastAPI API
   |
   | one PostgreSQL transaction
   v
event + pending delivery + unpublished outbox row

No publisher -> no broker -> no worker -> no outbound HTTP
```

The API/database boundary was durable, but an accepted event stayed pending.

### After Stage 3

```text
                    +---------------- PostgreSQL ----------------+
                    | events, deliveries, attempts, outbox rows |
                    +----------^----------------------^----------+
                               |                      |
Producer -> API --commit-------+                      |
                               |                      |
                     outbox publisher                 |
                     claim / publish / mark           |
                               |                      |
                               v                      |
             +---------- NATS JetStream ----------+   |
             | HOOKRELAY_DELIVERIES_V1            |   |
             | hookrelay.delivery.requested.v1    |   |
             | durable pull-consumer cursor       |   |
             +----------------+--------------------+   |
                              |                        |
                     bounded worker tasks              |
                              | load / attempt / finish+
                              v
                   signed HTTP POST
                              |
                              v
                     local test receiver
```

The processes are intentionally separate:

| Process | Owns | Why it is separate |
| --- | --- | --- |
| `hookrelay` | Producer-facing API and PostgreSQL acceptance | NATS or receiver failure must not turn durable ingestion into a synchronous delivery call |
| `hookrelay-outbox` | PostgreSQL claim leases and JetStream publication | It can pause/restart independently and scale with `SKIP LOCKED` claims |
| `hookrelay-worker` | Durable pulls, attempt state, signing, and outbound HTTP | Its concurrency and outbound risk have a different lifecycle from the API |
| `hookrelay-test-receiver` | Local capture of exact request evidence | It is a test instrument, not product functionality or a customer receiver |

Each process owns one SQLAlchemy engine if it uses PostgreSQL. Sessions are
short-lived. The publisher does not hold a database transaction open during
NATS I/O, and the worker does not hold one during HTTP I/O.

## 4. Repository and file tour

### Delivery implementation

| File | Responsibility |
| --- | --- |
| `src/hookrelay/broker.py` | Strict ID-only broker model, deterministic encoding, JetStream topology creation/drift validation, PubAck handling, durable pull binding |
| `src/hookrelay/outbox.py` | Claim/publish/finalize loop and dedicated publisher entry point |
| `src/hookrelay/delivery.py` | Authoritative database loading, attempt lifecycle, canonical body, HMAC, outbound client, and success/failure finalization |
| `src/hookrelay/worker.py` | Pull loop, hard local concurrency bound, poison-message handling, broker progress/ACK decisions, graceful resource closure |
| `src/hookrelay/test_receiver.py` | Configurable local receiver, bounded in-memory capture, exact body bytes as base64, header evidence, and inspection/reset routes |
| `src/hookrelay/runtime.py` | Cooperative `SIGINT`/`SIGTERM` stop-event wiring |

### Existing code extended by Stage 3

| File | Stage 3 change or dependency |
| --- | --- |
| `src/hookrelay/config.py` | Typed NATS, publisher, worker, timeout, and allowlist settings plus cross-setting safety validation |
| `src/hookrelay/database.py` | Process-scoped engine lifecycle shared by API, publisher, and worker |
| `src/hookrelay/models.py` | Outbox claim fields and the delivery/attempt state used by execution |
| `src/hookrelay/security.py` | Decrypts the exact snapshotted endpoint secret with its AES-GCM associated data |
| `src/hookrelay/ingestion.py` | Produces the existing version-1 ID-only outbox command |
| `src/hookrelay/api/events.py` | Keeps `201` as durable acceptance while background processes update later state |

### Schema, packaging, and local topology

| File | Responsibility |
| --- | --- |
| `migrations/versions/20260803_0002_outbox_publish_claims.py` | Adds nullable claim token/expiry, consistency constraints, and the unpublished-claim scan index |
| `pyproject.toml` | Version `0.3.0`, `nats-py`, runtime `httpx2`, four process entry points, and `nats`/`e2e` pytest markers |
| `compose.yaml` | API, PostgreSQL, NATS 2.14.3, publisher, worker, receiver, loopback ports, and persistent database/broker volumes |
| `.env.example` | Local defaults and the explicit delivery allowlist warning |
| `.github/workflows/ci.yml` | Frozen install, quality gates, PostgreSQL, file-backed JetStream, migrations, tests, Compose validation, and image build |
| `tests/unit/test_stage3_delivery.py` | Fixed wire vector, strict broker/topology contracts, concurrency bound, poison-message termination, and policy-blocked recovery |
| `tests/integration/test_stage3_pipeline.py` | Real API/PostgreSQL/JetStream/HTTP publication, ambiguity, timeout, redelivery, ACK-order, and happy-path evidence |

### Documentation

- `docs/architecture.md` is the cumulative current-system view.
- `docs/glossary.md` defines Stage 1-3 language.
- `docs/interview-guide.md` turns invariants into concise explanations.
- `docs/decisions/0008-nats-jetstream-dispatch.md` records the broker choice.
- `docs/decisions/0009-versioned-webhook-signature.md` fixes the wire contract.
- `docs/decisions/0010-stage3-local-outbound-gate.md` records the temporary
  local/test safety boundary.

## 5. Full event-flow trace

### A. API acceptance

1. A producer authenticates with its tenant API key and calls
   `POST /v1/events` with `Idempotency-Key`.
2. Stage 2 validation and tenant-scoped idempotency still apply.
3. One transaction inserts the event, one pending delivery per target, and one
   `delivery.requested` outbox row per delivery.
4. The outbox JSON contains only `message_id`, `tenant_id`, `event_id`,
   `endpoint_id`, `delivery_id`, `type`, and `schema_version`. Event payload,
   URL, and secret never enter NATS.
5. PostgreSQL commits; only then does the API return `201`.

The API's readiness probe still checks PostgreSQL only. NATS being unavailable
does not retroactively make an already committed event unaccepted.

### B. Short outbox claim transaction

1. `hookrelay-outbox` scans rows whose `published_at` is null and whose claim
   is absent or expired.
2. It orders by `(created_at, id)`, limits the scan to
   `HOOKRELAY_OUTBOX_BATCH_SIZE`, and uses `FOR UPDATE SKIP LOCKED`.
3. It validates the stored topic, schema, and authoritative row/payload IDs.
4. It assigns one random `claim_token` and database-time-based
   `claim_expires_at` to the batch.
5. It commits and detaches the broker-ready values before contacting NATS.

Configuration requires the claim TTL to exceed the aggregate configured broker
publish-timeout budget: `outbox_batch_size *
nats_publish_timeout_seconds`. With the defaults, 60 seconds exceeds `25 * 2`
seconds and leaves margin for database finalization and loop overhead. This
prevents a healthy publisher from normally losing the tail of its lease while
still processing the batch; the formula alone is not a total runtime bound.

This lease makes a process crash recoverable without holding PostgreSQL locks
through broker latency. A second publisher skips actively claimed rows; an
expired claim becomes eligible again.

### C. JetStream publication

1. The publisher serializes the strict message deterministically.
2. It publishes on `hookrelay.delivery.requested.v1` and sets
   `Nats-Msg-Id` to the outbox UUID.
3. It waits for a JetStream PubAck from `HOOKRELAY_DELIVERIES_V1`.
4. Only after that acknowledgment does a conditional update set
   `published_at`, requiring the same row and claim token.
5. A failure releases the current and remaining batch claims where ownership
   still matches. A hard crash leaves leases to expire.
6. Cooperative shutdown checks the stop event between items and releases every
   remaining owned claim before exiting.

The duplicate window can collapse a quick republish with the same
`Nats-Msg-Id`, but it is finite. It does not make publication exactly once.

### D. Durable pull and bounded execution

1. `hookrelay-worker` binds to the existing durable consumer
   `HOOKRELAY_DELIVERY_WORKERS_V1`.
2. It fetches at most one local concurrency window, waits for that window to
   complete, and only then fetches more.
3. An `asyncio.Semaphore` enforces the same hard bound even if a larger list is
   passed internally. The long-lived HTTP client's connection limits match the
   worker concurrency.
4. Invalid internal JSON/schema is terminated rather than executed. A valid
   broker message is still not trusted: its IDs are reconciled against the
   outbox and delivery rows in PostgreSQL.
5. A valid message blocked only by the temporary destination policy is not
   poison: the worker logs it and leaves it unacknowledged/recoverable.

The stream is file-backed with work-queue retention. The consumer is durable,
pull-based, explicit-ack, and shared by worker instances. Local Compose uses a
single NATS server and one stream replica; that is durable across normal
container recreation with the named volume, but it is not highly available.

### E. Attempt claim and authoritative loading

1. The worker locks the delivery row by tenant and delivery ID.
2. It verifies that the broker envelope exactly matches the persisted outbox,
   event, endpoint, and delivery identities.
3. A delivery already marked `succeeded` returns `already_succeeded`; the
   worker ACKs without a second HTTP call.
4. A delivery already `delivering` with an unfinished attempt returns
   `in_progress`; the worker extends the NATS acknowledgment deadline but does
   not send another request.
5. For a `pending` delivery, it loads the immutable event and exact snapshotted
   signing-secret row, checks the Stage 3 host allowlist, decrypts the secret,
   calculates the next attempt number under the delivery lock, inserts the
   unfinished attempt, and changes the delivery to `delivering`.
6. That transaction commits before HTTP begins.

The secret exists briefly in worker memory and is excluded from dataclass
representations. It is never added to NATS or receiver inspection output.

### F. Exact webhook wire contract

The worker produces deterministic compact UTF-8 JSON with sorted object keys:

```json
{"created_at":"2026-08-03T12:34:56.123456Z","delivery_id":"<delivery UUID>","id":"<event UUID>","payload":{"order_id":"ord_123"},"schema_version":1,"type":"order.created"}
```

`created_at` is the original event timestamp, not the attempt time. The worker
then records the current Unix timestamp in seconds and signs:

```text
signed bytes = ASCII(decimal timestamp) + ASCII(".") + exact body bytes
signature    = "v1=" + lowercase hex(HMAC-SHA256(endpoint secret, signed bytes))
```

Headers are:

```text
Content-Type: application/json
User-Agent: HookRelay/0.3.0
HookRelay-Delivery-Id: <delivery UUID>
HookRelay-Event-Id: <event UUID>
HookRelay-Signature: v1=<64 lowercase hexadecimal characters>
HookRelay-Timestamp: <Unix seconds>
HookRelay-Webhook-Version: 1
```

The exact bytes passed to HMAC are the exact bytes passed to the HTTP client.
Re-serializing parsed JSON at the receiver can change bytes and must not be used
for signature verification.

### G. HTTP, database finalization, and ACK

1. One outer `asyncio.timeout` bounds the whole request; the HTTP client also
   has explicit connect/read/write/pool timeouts.
2. Redirect following is disabled, environment proxy variables are ignored,
   and the response is streamed rather than buffered without limit.
3. Any `2xx` is success. The worker records the attempt's database finish
   timestamp, duration, outcome `succeeded`, and response status, then changes
   the delivery to `succeeded` in the same transaction.
4. Only after that commit does it call JetStream `ack_sync`.
5. The local receiver exposes the exact body as base64, a SHA-256 digest,
   lower-cased headers, sequence, and receive time for independent inspection.

If success commits but `ack_sync` is lost, JetStream may redeliver. The worker
then observes `succeeded` and ACKs without repeating HTTP. If the receiver acts
but HookRelay fails before committing success, the request may be repeated.
That second ambiguity requires receiver-side idempotency on the stable event ID.

## 6. Definitions of new technology and terms

**Acknowledgment (`ACK`)**

A consumer signal that processing completed and a work-queue message may be
removed. HookRelay uses `ack_sync` only after the success transaction commits.

**Acknowledgment wait (`AckWait`)**

How long JetStream waits for progress or an ACK before making a message
eligible for redelivery. It is not a retry schedule. Stage 3 validates that it
exceeds the HTTP timeout by at least five seconds.

**Bounded concurrency**

A fixed ceiling on simultaneous tasks. It protects memory, the HTTP pool,
PostgreSQL, and destinations from unbounded fan-out. Async I/O improves overlap;
it does not make CPU work parallel or make blocking calls nonblocking.

**Claim lease**

An expiring ownership token stored on an outbox row. It lets a publisher commit
its claim, release database locks, perform NATS I/O, and later prove it still
owns the row when finalizing.

**Canonical bytes**

One deterministic byte representation of a logical payload. Signatures cover
bytes, not an abstract JSON object; whitespace, key order, and Unicode encoding
therefore matter.

**Durable consumer**

A named JetStream cursor whose delivery/acknowledgment state survives client
disconnects. Multiple workers bind to the same consumer rather than creating
independent copies of each delivery.

**HMAC-SHA256**

A keyed message-authentication code using SHA-256. It lets a receiver detect
body/timestamp changes and authenticate possession of the shared endpoint
secret. It does not encrypt the body.

**JetStream**

NATS's persistence and acknowledgment layer. Stage 3 uses one file-backed
work-queue stream, not plain ephemeral Core NATS messaging.

**`Nats-Msg-Id`**

A publication header JetStream uses for time-window duplicate detection.
HookRelay supplies the stable outbox UUID. Finite-window deduplication is not an
exactly-once guarantee.

**PubAck**

JetStream's confirmation that a publish was accepted into the named stream.
The publisher requires it before setting `published_at`.

**Pull consumer**

A consumer where the worker requests a bounded batch when it has capacity.
This makes backpressure more explicit than a server continuously pushing work.

**Work-queue retention**

A stream policy that retains work until its one eligible consumer acknowledges
it, subject to configured limits. It is dispatch semantics, not event-history
retention.

**Webhook signature version**

The `v1` prefix and `HookRelay-Webhook-Version: 1` label the exact current wire
grammar so a later incompatible contract can coexist deliberately.

## 7. Why each technology and design was chosen

### NATS JetStream rather than Core NATS

Core NATS is intentionally transient. JetStream supplies file persistence,
server publish acknowledgments, durable consumers, explicit ACK state, and
pull-based flow control while keeping the learning environment small.

### One work-queue stream and one shared durable consumer

Each delivery should be processed by one worker at a time, not broadcast to
every worker. A shared durable pull consumer preserves one cursor and lets
workers request only bounded capacity. The versioned stream, subject, and
consumer names make incompatible future contracts visible.

### ID-only broker messages

PostgreSQL already owns event payloads, destination snapshots, and encrypted
secrets. Sending only identities avoids copying sensitive or mutable data into
another store and forces the worker to reconcile broker input with
authoritative state. The tradeoff is an extra database read for every attempt.

### PostgreSQL claim leases

Holding `FOR UPDATE` locks while awaiting NATS would couple broker latency to
database lock and connection pressure. A short transaction records an expiring
claim; `SKIP LOCKED` supports multiple publishers, and the TTL recovers claims
after process death. The tradeoff is duplicate publication around PubAck /
finalization ambiguity.

### Durable pull with two concurrency bounds

The worker fetch size limits each broker batch, while a semaphore protects the
execution method even if internal callers supply more items. Matching HTTP pool
limits prevents a large hidden socket queue. `MaxAckPending` provides a broker
side ceiling and must be at least the local worker concurrency.

### HMAC over timestamp plus exact body

The body is authenticated without exposing the secret. Prefixing an explicit
timestamp lets a real receiver apply a freshness window; the dot makes the
signed grammar unambiguous. The version prefix enables contract evolution.
Timestamp signing alone is not replay prevention: receivers also need a
freshness check and idempotency keyed by stable event ID.

### A long-lived async HTTP client

One client per worker process reuses connections and centralizes timeouts,
pool limits, redirect policy, and proxy policy. Constructing a client per
attempt would discard pooling and add connection overhead. `asyncio` overlaps
network waits but does not remove the need for explicit bounds.

### Separate local receiver

The receiver gives a deterministic destination with configurable status and
delay while preserving the exact bytes needed for verification. Keeping it in
a separate process exercises real container DNS and HTTP instead of replacing
the delivery boundary with a function call.

## 8. Serious alternatives and why they were not selected

### Kafka

Kafka is excellent for high-throughput partitioned logs, long retention, and
large data-platform ecosystems. HookRelay currently needs a compact durable
work queue with explicit acknowledgments and simple local operation. Kafka's
broker/controller, partition, consumer-group, retention, and rebalancing model
would add operational concepts before this project has measured a need for
them. Stage 7 may produce evidence that changes this decision.

### Redis Streams

Redis Streams provide persistence and consumer groups, but correctness depends
on Redis durability, pending-entry recovery, trimming, and deployment choices.
HookRelay would also introduce Redis solely for messaging. JetStream directly
matches durable pull/ACK semantics and keeps the broker responsibility explicit.

### Celery

Celery offers a mature Python task framework, retries, scheduling, and many
broker backends. Those abstractions would hide the publish acknowledgment,
consumer cursor, retry, and crash boundaries this project is intended to teach.
HookRelay keeps the async worker and state transitions explicit.

### Publish directly from the API

Publishing after the event commit leaves a crash gap; publishing before commit
can expose work that later rolls back. Waiting for NATS inside the acceptance
transaction also couples API availability and database locks to broker latency.
The transactional outbox preserves a PostgreSQL-only acceptance boundary.

### Hold row locks across NATS publication

This avoids a lease schema but consumes a transaction and connection while an
independent network service responds. Claim leases add state, yet bound lock
duration and let several publishers coordinate with `SKIP LOCKED`.

### Put event bodies and secrets in NATS

Self-contained messages reduce worker database reads, but duplicate sensitive
data, complicate erasure/rotation, and risk stale configuration. ID-only
messages preserve PostgreSQL as the source of truth.

### Push consumer

A push consumer can provide low-latency delivery, but the server can create a
client-side backlog unless flow control is carefully aligned. Pull lets each
worker request one available concurrency window.

### Sign parsed JSON fields independently

This creates canonicalization ambiguity between languages and serializers.
Signing one documented byte string and sending those exact bytes yields a
testable wire vector.

### Treat broker deduplication as exactly once

The duplicate window is finite, and an HTTP receiver is outside the broker
transaction. `Nats-Msg-Id` reduces quick duplicate publications; database state
and receiver idempotency remain the correctness mechanisms.

## 9. Failure modes and design tradeoffs

| Failure or boundary | Current behavior | Remaining limitation |
| --- | --- | --- |
| NATS unavailable during API ingestion | API can still commit event/delivery/outbox because readiness is PostgreSQL-only | Outbox backlog grows and needs monitoring/retention policy |
| Publisher dies after claiming | Claim becomes eligible after its TTL | Publication is delayed by the remaining lease time |
| Publish fails before PubAck | Row is not marked published; owned claims are released | Reconnect/polling cadence is fixed, not Stage 4 backoff |
| NATS stores the message but PubAck is lost | Row can be published again | `Nats-Msg-Id` only deduplicates within its window |
| PubAck succeeds, publisher dies before `published_at` | Expired claim causes republish | At-least-once publication, never exactly once |
| Stream reaches `max_bytes` | `DiscardNew` rejects new publishes instead of evicting old work | PostgreSQL backlog grows; no automated capacity response |
| Invalid broker envelope | Worker calls `term()` and logs the internal contract error | No Stage 4 dead-letter record yet |
| Broker IDs disagree with PostgreSQL | Worker refuses execution and terminates the poison message | Operational alerting/history is still limited |
| Valid destination is blocked by the Stage 3 policy | Worker performs no HTTP and leaves the message unacknowledged/recoverable | It may reappear after `AckWait` without backoff until Stage 4/5 policy exists |
| Duplicate arrives after delivery success | Worker sees `succeeded`, skips HTTP, and ACKs | This does not cover receiver success before DB success commit |
| Receiver succeeds, worker fails before success commit | Message may be delivered again | Receiver must deduplicate the stable event ID |
| Success commits, `ack_sync` is lost | Redelivery is suppressed by database success state, then ACKed | Still an observable duplicate broker delivery |
| Timeout, transport error, or non-2xx | Attempt becomes `transient_failure`, delivery returns to `pending`, message stays unacknowledged | No backoff, jitter, cap, classification, or dead letter until Stage 4 |
| Worker dies after inserting unfinished attempt | Redelivery sees `delivering` and reports progress without another HTTP call | No stale-attempt recovery; work can remain stuck until Stage 4 |
| Slow operation exceeds `AckWait` | JetStream may expose the message again | Settings only constrain HTTP timeout plus margin; Stage 4 must harden crash/lease behavior |
| Destination redirects | Client does not follow it | Full resolved-IP and redirect-target SSRF policy arrives in Stage 5 |
| Allowed hostname resolves unexpectedly | Stage 3 compares hostname text only | DNS/IP classification and egress controls are not implemented |
| NATS data volume is lost | Local stream state is lost | Single-node Compose is reproducible, not HA or disaster recovery |
| Receiver restarts | In-memory captures disappear | Receiver evidence is a local test aid, not an audit store |

Backpressure must be reasoned about as one budget: publisher batch size, claim
TTL, NATS stream bytes, consumer `MaxAckPending`, worker semaphore, HTTP pool,
and the database connection pools. No single setting proves system capacity.

## 10. Exact commands for running and testing

The commands below use Windows PowerShell and bind PostgreSQL to host port
`55432` to avoid a common local `5432` collision. Containers still reach it as
`postgres:5432`.

### Prepare a clean local environment

```powershell
git clone --branch codex/stage-03-delivery-pipeline --single-branch https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
Copy-Item .env.example .env
$env:POSTGRES_HOST_PORT = "55432"
```

Edit the ignored `.env` and replace the local database password, encryption
key, and bootstrap token before storing anything important. Never paste real
credentials into issue logs or chat.

Create the host Python environment when running checks outside containers:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

### Build, start dependencies, migrate, then start processes

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose build
docker compose up --detach --wait postgres nats receiver
docker compose run --rm api alembic upgrade head
docker compose up --detach --wait api
docker compose up --detach outbox-publisher worker
docker compose ps
```

Migrations are explicit. Neither the API nor a background process calls
`metadata.create_all()` or upgrades the schema at startup.

Check the local surfaces:

```powershell
curl.exe --fail http://127.0.0.1:8000/health/live
curl.exe --fail http://127.0.0.1:8000/health/ready
curl.exe --fail http://127.0.0.1:9000/health/live
Invoke-RestMethod 'http://127.0.0.1:8222/healthz?js-enabled-only=true'
```

### Create a tenant and receiver endpoint

The endpoint URL uses Compose service-name DNS because the worker runs inside
the Compose network:

```powershell
$bootstrapHeaders = @{
  Authorization = "Bearer replace-this-local-bootstrap-token-before-use"
}
$bootstrapBody = @{
  name = "Stage 3 demo"
  initial_api_key_name = "developer"
} | ConvertTo-Json
$bootstrap = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/bootstrap/tenants `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body $bootstrapBody
$apiKey = $bootstrap.api_key.key

$authHeaders = @{ Authorization = "Bearer $apiKey" }
$endpointBody = @{
  name = "Local inspection receiver"
  url = "http://receiver:9000/webhooks"
} | ConvertTo-Json
$endpoint = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/endpoints `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $endpointBody
$signingSecret = $endpoint.signing_secret
```

The raw API key and signing secret are returned once. Keep these shell
variables only for the local exercise.

### Submit an event and observe asynchronous success

```powershell
$eventHeaders = @{
  Authorization = "Bearer $apiKey"
  "Idempotency-Key" = "stage3-demo-0001"
}
$eventBody = @{
  type = "order.created"
  payload = @{ order_id = "ord_123"; total = 42 }
  endpoint_ids = @($endpoint.id)
} | ConvertTo-Json -Depth 5
$event = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $eventHeaders `
  -ContentType "application/json" `
  -Body $eventBody
$deliveryId = $event.deliveries[0].id

do {
  Start-Sleep -Milliseconds 250
  $captures = @(
    Invoke-RestMethod "http://127.0.0.1:9000/requests/$deliveryId"
  )
} while ($captures.Count -eq 0)

$current = Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/v1/events/$($event.id)" `
  -Headers $authHeaders
$current.deliveries
```

The create response may show the original `pending` creation representation;
the GET route is the current-state view and should eventually show
`succeeded`.

### Verify exact body bytes and HMAC independently

```powershell
$capture = $captures[-1]
$bodyBytes = [Convert]::FromBase64String($capture.body_base64)
$timestamp = $capture.headers.'hookrelay-timestamp'
$prefixBytes = [Text.Encoding]::ASCII.GetBytes("$timestamp.")
$signedBytes = [byte[]]::new($prefixBytes.Length + $bodyBytes.Length)
[Buffer]::BlockCopy($prefixBytes, 0, $signedBytes, 0, $prefixBytes.Length)
[Buffer]::BlockCopy($bodyBytes, 0, $signedBytes, $prefixBytes.Length, $bodyBytes.Length)
$secretBytes = [Text.Encoding]::UTF8.GetBytes($signingSecret)
$hmac = [Security.Cryptography.HMACSHA256]::new($secretBytes)
try {
  $digest = $hmac.ComputeHash($signedBytes)
} finally {
  $hmac.Dispose()
}
$expectedSignature = "v1=" + ([BitConverter]::ToString($digest)).Replace('-', '').ToLowerInvariant()
$capture.headers.'hookrelay-signature' -eq $expectedSignature
[Text.Encoding]::UTF8.GetString($bodyBytes)
```

The comparison should be `True`. A real receiver must also reject stale
timestamps and atomically deduplicate the stable event ID with its side effect.

### Inspect durable evidence

```powershell
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT id, published_at, claim_token, claim_expires_at FROM outbox_messages ORDER BY created_at DESC LIMIT 5;"
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT delivery_id, attempt_number, outcome, response_status_code, error_code, duration_ms FROM delivery_attempts ORDER BY started_at DESC LIMIT 5;"
Invoke-RestMethod 'http://127.0.0.1:8222/jsz?streams=true&consumers=true'
docker compose logs --no-color --tail 100 outbox-publisher worker receiver
```

Do not query secret ciphertext or print the raw secret as part of routine
inspection.

### Run the quality and test gates

Fast checks:

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\mypy.exe
.\.venv\Scripts\pytest.exe -m "not integration"
```

PostgreSQL, JetStream, migration, and end-to-end checks:

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose stop api outbox-publisher worker receiver
docker compose up --detach --wait postgres nats
$env:HOOKRELAY_TEST_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:55432/hookrelay"
$env:HOOKRELAY_DATABASE_URL = $env:HOOKRELAY_TEST_DATABASE_URL
$env:HOOKRELAY_NATS_URL = "nats://127.0.0.1:4222"
$env:HOOKRELAY_TEST_NATS_URL = "nats://127.0.0.1:4222"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\pytest.exe -m "integration and not concurrency"
.\.venv\Scripts\pytest.exe -m concurrency
.\.venv\Scripts\alembic.exe check
docker compose config --quiet
docker build --pull --tag hookrelay:stage3 .
```

Stopping the long-lived application processes prevents them from claiming rows
created by the deterministic integration harness. Substitute the password you
chose in `.env`. The suite applies migrations and adds domain rows, so it must
use only a disposable local/test database.

### Stop safely

```powershell
docker compose down
```

This preserves `postgres_data` and `nats_data`. The destructive variant below
deletes both durable local stores and should be used only for an intentional
fresh start:

```powershell
docker compose down --volumes
```

## 11. How each test works—and what it cannot prove

### Pure contract tests

`tests/unit/test_stage3_delivery.py` fixes a complete body/signature vector,
proves timestamp/body/secret changes and a non-ASCII presented signature fail
verification safely, checks the exact
headers, rejects extra broker fields, inspects desired JetStream topology, and
drives more fake messages than the configured semaphore limit.

These tests are fast and deterministic. They do not prove NATS persistence,
PostgreSQL transactions, real HTTP behavior, container DNS, or shutdown.

### Settings tests

`tests/unit/test_config.py` checks masked NATS credentials, literal/versioned
asset names, timeout relationships, broker/worker concurrency relationships,
explicit host allowlists, and the local/test-only runtime gate.

They prove validation logic, not that a deployment supplied trustworthy values
or that an allowed hostname resolves safely.

### PostgreSQL and publisher integration

`tests/integration/test_stage3_pipeline.py` begins through the real API, applies
the migrated Stage 3 model, exercises claim
ownership/expiry, and checks that only a JetStream-acknowledged row becomes
published. It also checks delivery/attempt state transitions against real
constraints and transactions.

This can prove database and broker behavior for the exercised schedules. It
cannot enumerate every crash point, prove exactly once, or replace long-running
fault tests.

### Real JetStream integration

The same NATS-marked integration module connects to a real file-backed
JetStream server,
creates or validates the versioned stream/consumer, obtains a PubAck, and pulls
through the durable consumer. It injects failures both before and after a real
PubAck and proves the row remains recoverable.

One local broker cannot prove clustered replication, quorum behavior,
production disk durability, disaster recovery, or sustained throughput.

### End-to-end happy path

The end-to-end scenario begins with the same API ingestion boundary, lets the
publisher move the ID-only row through JetStream, lets a worker perform real
HTTP, verifies captured exact bytes and HMAC, and inspects one succeeded attempt
and delivery before the message ACK.

Adjacent real-service scenarios prove that a bounded HTTP deadline records an
unacknowledged failure and that real redelivery of already-succeeded work skips
a second HTTP attempt. They still do not constitute the Stage 4 retry/crash
recovery system.

It proves the components interoperate on the tested success path. It does not
prove retry scheduling, stale-attempt recovery, hostile-destination safety,
exactly once, HA, or benchmark scale.

### Component learning loops

#### Transactional outbox publisher

- **Problem:** a PostgreSQL commit and NATS publish cannot be one local
  transaction.
- **Build:** ingestion writes an outbox row; the publisher claims it, awaits
  PubAck, and conditionally marks it published.
- **Inspect:** see non-null `published_at`, cleared claim fields, and one stream
  message/consumer acknowledgment.
- **Break it safely:** stop NATS, submit a local event, and observe an unpublished
  row before restarting NATS.
- **Explain:** draw both crash gaps and say why `Nats-Msg-Id` reduces but cannot
  eliminate duplicates.

#### Durable worker and ACK ordering

- **Problem:** acknowledging before durable success can lose work.
- **Build:** create attempt -> HTTP -> commit success -> `ack_sync`.
- **Inspect:** correlate receiver capture, attempt row, delivery state, and
  consumer state.
- **Break it safely:** use the receiver's configurable non-2xx status briefly;
  do not call the resulting behavior a finished retry policy.
- **Explain:** state what happens if the receiver acts before HookRelay records
  success.

#### Exact-byte signing

- **Problem:** a receiver must authenticate payload integrity without receiving
  the shared secret in every request.
- **Build:** deterministic body, Unix timestamp, HMAC-SHA256 over
  `timestamp.body`, and a `v1` prefix.
- **Inspect:** recompute the signature from captured base64 bytes.
- **Break it safely:** append one space to a local copy of the body and observe
  verification fail.
- **Explain:** distinguish HMAC authenticity from encryption, freshness, and
  idempotency.

#### Bounded async concurrency

- **Problem:** unbounded task/socket creation converts backlog into resource
  exhaustion.
- **Build:** pull one bounded window, semaphore every execution, and align the
  HTTP pool and `MaxAckPending`.
- **Inspect:** run the gated unit test and review configured ceilings.
- **Break it safely:** lower concurrency to one and observe serialization on
  local requests.
- **Explain:** why `async def` enables overlapping waits but not CPU parallelism.

#### Outbound safety gate

- **Problem:** Stage 3 introduces server-side HTTP before full SSRF controls.
- **Build:** local/test runtime restriction, explicit hostname allowlist, no
  redirects, and no environment proxies.
- **Inspect:** the Compose target `receiver` works while an unlisted hostname is
  rejected.
- **Break it safely:** configure an unlisted harmless hostname and confirm no
  request leaves the worker.
- **Explain:** name the DNS/IP/rebinding/egress checks still absent until Stage 5.

## 12. Safe “break it intentionally” exercise

### Goal

Prove that NATS unavailability does not erase accepted work and that the
publisher can later drain PostgreSQL intent. This exercise stays local and does
not delete either volume.

### Preconditions

Complete the happy path once, keep `$apiKey` and `$endpoint`, and confirm every
service is running. Then stop only NATS:

```powershell
docker compose stop nats
curl.exe --fail http://127.0.0.1:8000/health/live
curl.exe --fail http://127.0.0.1:8000/health/ready
```

Both API health calls should remain `200` because the API's dependency-backed
readiness contract is PostgreSQL, not broker delivery health.

### Accept work while the broker is down

```powershell
$outageHeaders = @{
  Authorization = "Bearer $apiKey"
  "Idempotency-Key" = "stage3-nats-outage-0001"
}
$outageBody = @{
  type = "order.created"
  payload = @{ order_id = "ord_during_outage" }
  endpoint_ids = @($endpoint.id)
} | ConvertTo-Json -Depth 5
$outageEvent = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $outageHeaders `
  -ContentType "application/json" `
  -Body $outageBody
$outageEvent
```

Inspect the unpublished row and sanitized publisher logs:

```powershell
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT delivery_id, published_at, claim_token, claim_expires_at FROM outbox_messages WHERE delivery_id = '$($outageEvent.deliveries[0].id)';"
docker compose logs --no-color --tail 50 outbox-publisher
```

The request is durably accepted even though `published_at` remains null. A
claim may be absent, released, or waiting to expire depending on the exact
failure instant; none of those states is evidence of data loss.

### Recover

```powershell
docker compose up --detach --wait nats
```

Poll the receiver and current event state as in section 10. The publisher's
existing NATS client reconnects, an eligible claim is published, and the worker
can complete the request.

### Explain the result

Answer aloud:

1. Why could the API accept work without NATS?
2. Which PostgreSQL row preserved the obligation?
3. Why might recovery wait for the claim TTL?
4. Why can publication still duplicate after recovery?
5. Why does this exercise not prove Stage 4 retry or worker-crash recovery?

Do not run `docker compose down --volumes`; volume deletion tests backup
behavior, not this invariant.

## 13. Troubleshooting

### A process reports NATS topology drift

The stream or consumer exists under the configured name with incompatible
retention, storage, limits, subject, acknowledgment, or durability settings.
The code fails rather than silently mutating the contract. For disposable local
data, stop the stack and deliberately remove the NATS volume. For valued data,
inspect `/jsz`, plan a versioned stream migration, and do not delete the volume.

### Publisher logs failures and rows remain unpublished

Check NATS health, `HOOKRELAY_NATS_URL`, the stream byte limit, and claim expiry.
A PubAck timeout must not set `published_at`. If a process died, wait for
`HOOKRELAY_OUTBOX_CLAIM_TTL_SECONDS` or inspect the claim fields; do not manually
set `published_at`.

If settings reject the claim TTL, compare it with
`HOOKRELAY_OUTBOX_BATCH_SIZE * HOOKRELAY_NATS_PUBLISH_TIMEOUT_SECONDS`. The TTL
must be strictly larger because publication is sequential. Reducing the TTL
without reducing the batch/timeout can let a healthy batch outlive its claim.

### The API is ready but no delivery happens

This is possible by design. API readiness covers PostgreSQL acceptance, not
the background pipeline. Inspect `docker compose ps`, publisher/worker logs,
unpublished rows, NATS health/topology, and receiver health separately.

### Worker refuses to start outside local/test

That is the Stage 3 fail-closed boundary. Do not bypass it with a wildcard
allowlist. Implement and review Stage 5's full SSRF/egress controls before
enabling staging or production delivery.

### Worker rejects the receiver hostname

The URL hostname, normalized to lowercase without a trailing dot, must appear
in `HOOKRELAY_DELIVERY_ALLOWED_HOSTS`. A worker inside Compose should use
`http://receiver:9000/webhooks`; a host-run worker should use
`http://127.0.0.1:9000/webhooks`. Adding a name to this list does not prove its
resolved addresses are safe.

### Delivery remains `delivering`

An unfinished attempt may have been committed before a worker crash. Stage 3
suppresses a second concurrent HTTP call and sends broker progress, but it has
no stale-attempt ownership/abandonment recovery. Preserve the row for diagnosis;
Stage 4 must implement and test recovery rather than a manual status edit.

### Delivery returns to `pending` repeatedly

Inspect attempt `error_code` and response status. `request_timeout`,
`transport_error`, or `http_status` records a failed Stage 3 attempt. The
message remains unacknowledged and may reappear after `AckWait`; there is no
backoff or maximum attempt count yet. Restore the local receiver, then continue
only in a disposable environment.

### Receiver captured nothing

Confirm its `/health/live`, use the Compose service hostname in the endpoint,
check the allowlist, and inspect worker logs. The route is `/webhooks`, not the
older Stage 2 placeholder `/hooks`.

### Signature verification fails

Use `body_base64`, not parsed/reformatted JSON. Read the timestamp as ASCII,
insert exactly one dot byte, use the one-time secret from that endpoint version,
compute HMAC-SHA256, lowercase the hex digest, and prefix `v1=`. Confirm no
newline or PowerShell string encoding was added.

### NATS data disappears after cleanup

`docker compose down` preserves the named volume;
`docker compose down --volumes` deletes it. Local single-node persistence is
not a backup or HA strategy.

### PostgreSQL connection on `5432` is already in use

Keep the container port unchanged and set the host mapping before Compose:

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose up --detach --wait postgres
```

Host Python/test URLs must then use `127.0.0.1:55432`; container URLs continue
to use `postgres:5432`.

### Fast tests pass but the pipeline fails

Unit fakes do not prove PostgreSQL locking/constraints, JetStream storage,
Compose DNS, HTTP bytes, migration state, or process lifecycle. Run the marked
integration/end-to-end suite against disposable real services and inspect each
boundary rather than weakening a test.

## 14. Recruiter and interviewer questions

### “Walk me through one delivery.”

Start with the API's PostgreSQL commit, not NATS. Trace the expiring outbox
claim, PubAck-before-`published_at`, ID-only durable message, worker's
authoritative database check, attempt-before-HTTP, exact-byte HMAC, success
commit, and ACK-after-commit. End by naming the receiver-success/database-fail
ambiguity and receiver-side idempotency.

### “Why use a transactional outbox if NATS is durable?”

Broker durability begins only after a successful publish. The API still cannot
atomically commit PostgreSQL and NATS. The outbox makes event plus dispatch
intent one local transaction; the publisher can resume later. Publish/mark
ambiguity remains and is handled as at least once.

### “How can several publishers avoid duplicate work?”

They select eligible rows with `FOR UPDATE SKIP LOCKED`, attach an expiring
claim token in a short transaction, then publish outside the transaction. A
conditional final update proves ownership. This reduces concurrent duplication,
while crashes around PubAck can still duplicate publication.

### “Why JetStream instead of Kafka?”

JetStream supplies the persistence, server acknowledgments, durable cursor,
and pull-based work-queue semantics needed now with a smaller local operational
surface. Kafka is stronger when retained partitioned logs, large ecosystems,
and measured scale justify its partition/rebalance/cluster model. The choice is
scope-based, not a claim that one technology is universally better.

### “How is worker concurrency bounded?”

The worker pulls at most its configured concurrency, waits for the batch, and
uses a semaphore as a second hard guard. Its HTTP pool uses the same limit and
the durable consumer caps unacknowledged messages. Async overlaps I/O waits; it
does not create CPU parallelism or unlimited capacity.

### “What exactly is signed?”

The worker creates deterministic compact UTF-8 JSON, then HMAC-SHA256 signs the
ASCII Unix timestamp, one dot, and those exact body bytes. It sends the same
bytes and a versioned `v1=` digest. A receiver verifies raw bytes, timestamp
freshness, and stable event-ID idempotency; HMAC does not encrypt the payload.

### “Why ACK after the database commit?”

ACK-before-commit could remove the only broker work item while the delivery
still lacks durable success. Commit-before-ACK may cause a broker redelivery if
the ACK is lost, but database state lets the worker suppress a second HTTP call.
The ordering chooses duplicates over silent loss.

### “What happens when the receiver times out?”

Stage 3 records a transient-failure attempt, returns the delivery to `pending`,
and leaves the broker message unacknowledged. JetStream may expose it again
after `AckWait`, but there is no designed retry schedule, backoff, jitter,
classification, maximum, or dead letter until Stage 4.

### “What security boundary did outbound HTTP introduce?”

SSRF. Stage 3 limits execution to local/test, requires an explicit hostname
allowlist, disables redirects, and ignores environment proxies. Those controls
make the local receiver exercise safer but do not validate resolved IPs, DNS
rebinding, or egress. Full protection is explicitly Stage 5.

### “What did your tests prove?”

Name evidence by boundary: deterministic unit vectors, settings validation,
real PostgreSQL leases/state, real JetStream PubAck/durable pull, real HTTP
capture, and the end-to-end state chain. Then state what remains unproved:
exactly once, arbitrary crashes, HA, hostile networks, and benchmark capacity.

## 15. Hands-on modification for Tarun

Add a diagnostic `HookRelay-Attempt-Id` header using the attempt UUID that the
worker already creates. Do not copy a supplied solution; trace the value from
the database claim to the exact receiver capture.

Acceptance criteria:

1. Add the header in the signed request builder using the existing
   `DeliveryWork.attempt_id`.
2. Keep the body and HMAC version-1 fixed vector unchanged; this header is
   diagnostic metadata and is not currently covered by the HMAC.
3. Add a unit assertion for the exact header name/value.
4. Extend receiver filtering with a route that returns captures for one attempt
   ID, without removing the delivery filter.
5. Add an integration assertion that the captured attempt ID equals the
   succeeded `delivery_attempts.id` row.
6. Preserve bounded capture size and do not log or expose any secret.
7. Add no migration: the attempt UUID already exists.
8. Update the signature ADR to state explicitly that this header is not
   authenticated and must not be trusted for authorization or idempotency.
9. Run Ruff, format, mypy, fast tests, real integration/end-to-end tests,
   `alembic check`, Compose validation, and the image build.

Questions to answer before coding:

- Is attempt identity part of the stable logical event or one execution try?
- Why is it useful for diagnostics but wrong for receiver-side business
  idempotency?
- What would be required to authenticate this header?
- Why would adding it to the body require a deliberate wire-contract decision?
- Which test proves the header and database row refer to the same attempt?

## 16. Comprehension quiz and teach-back checklist

### Quiz

Answer without looking, then verify against code and tests.

1. What does event `201` prove, and what does it not prove?
2. Why is NATS absent from API readiness?
3. Which three application processes were added beside the API?
4. What seven fields are permitted in the ID-only broker payload?
5. Why are event payload, target URL, and secret omitted from NATS?
6. What selects and coordinates a publisher claim batch?
7. Why does the claim transaction commit before NATS I/O?
8. How can a claim recover after publisher death?
9. What must happen before `published_at` is set?
10. What value becomes `Nats-Msg-Id`, and what is its limit?
11. Name the stream, subject, and durable consumer.
12. What do work-queue retention and explicit ACK mean here?
13. What three layers bound worker concurrency/backlog?
14. Which PostgreSQL identities are cross-checked before HTTP?
15. What state suppresses duplicate HTTP after success?
16. Why is an attempt inserted before HTTP?
17. List every field in the version-1 webhook body.
18. Write the exact HMAC input grammar.
19. Why must a receiver verify raw bytes rather than re-serialized JSON?
20. What does the timestamp enable, and what does it not guarantee alone?
21. Why are redirects and environment proxies disabled?
22. What happens on a non-2xx response in Stage 3?
23. Why is that behavior not a complete retry policy?
24. What crash can leave a delivery stuck in `delivering`?
25. What are the two major duplicate-delivery ambiguity windows?
26. Why is local file-backed single-node JetStream not an HA claim?
27. Which SSRF controls are present, and which remain for Stage 5?
28. What does the local receiver retain, and what disappears on restart?
29. Why is `ack_sync` ordered after the success commit?
30. Name one thing each test layer cannot prove.

<details>
<summary>Self-check answer ingredients</summary>

1. PostgreSQL committed event/delivery/outbox; no publish, attempt, HTTP, or
   receiver success is implied.
2. The outbox decouples durable acceptance from broker availability.
3. Outbox publisher, delivery worker, and local test receiver.
4. Type, schema version, message, tenant, event, endpoint, and delivery IDs.
5. PostgreSQL is authoritative and NATS should not duplicate sensitive/mutable
   data.
6. Ordered eligible scan, `FOR UPDATE SKIP LOCKED`, batch limit, and claim
   token/expiry.
7. To release connection/row locks before broker latency.
8. Database-time claim expiry.
9. A PubAck from the expected stream and a matching conditional claim update.
10. Outbox UUID; deduplication lasts only the configured window.
11. `HOOKRELAY_DELIVERIES_V1`, `hookrelay.delivery.requested.v1`, and
    `HOOKRELAY_DELIVERY_WORKERS_V1`.
12. One eligible consumer retains work until explicit successful processing
    acknowledgment.
13. Fetch window, semaphore/HTTP pool, and consumer `MaxAckPending`.
14. Message/outbox IDs plus tenant, delivery, event, and endpoint relationships.
15. Delivery `succeeded`.
16. It creates a durable audit/state claim before an external side effect.
17. `created_at`, `delivery_id`, `id`, `payload`, `schema_version`, and `type`.
18. ASCII decimal Unix seconds, one dot byte, exact UTF-8 body bytes.
19. Parsing/serialization may change whitespace, key ordering, numbers, or
    encoding.
20. A receiver can enforce freshness; timestamp alone does not prevent replay.
21. To avoid destination changes and hidden proxy routing before complete SSRF
    controls.
22. Record transient failure, return pending, do not ACK.
23. No schedule, backoff, jitter, classification, maximum, or dead letter.
24. Worker death after attempt/delivering commit and before finalization.
25. PubAck before outbox finalization; receiver side effect before HookRelay
    success commit/response certainty.
26. One server/replica and one local volume provide no quorum or failover.
27. Local/test gate, explicit hostname allowlist, redirects/proxies disabled;
    resolved-IP, rebinding, IPv6/special-range, and egress controls remain.
28. Bounded in-memory exact bytes, hash, headers, sequence, receive time; all
    captures disappear on restart.
29. ACK-before-commit could silently lose broker work.
30. Unit: real services; integration: untested schedules/production; E2E:
    failure recovery/HA/security/scale.

</details>

### Three-to-five-minute teach-back

Use a blank page and this timing:

1. **0:00-0:30 — Boundary:** Stage 2's accepted outbox row versus Stage 3's
   successful local receiver delivery; no exactly-once claim.
2. **0:30-1:15 — Publish:** short PostgreSQL lease, `SKIP LOCKED`, ID-only
   message, `Nats-Msg-Id`, PubAck, and publish/mark ambiguity.
3. **1:15-2:00 — Broker/backpressure:** file-backed work-queue stream, shared
   durable pull consumer, explicit ACK, fetch/semaphore/pool/`MaxAckPending`.
4. **2:00-3:00 — Worker:** authoritative identity check, attempt-before-HTTP,
   snapshotted secret, exact canonical body, timestamped HMAC, timeout.
5. **3:00-3:40 — Ordering:** success transaction before `ack_sync`, duplicate
   suppression after DB success, receiver-success ambiguity.
6. **3:40-4:25 — Evidence:** unit vector, PostgreSQL lease/state, real
   JetStream, real receiver, and one limitation of each.
7. **4:25-5:00 — Boundaries:** unfinished-attempt recovery/retries in Stage 4;
   complete SSRF/traffic control in Stage 5; no HA or scale claim.

### Teach-back checklist

- [ ] I can draw the four processes and both durable stores.
- [ ] I can explain why API readiness remains PostgreSQL-only.
- [ ] I can trace claim, PubAck, finalization, pull, HTTP, success commit, and
      broker ACK in order.
- [ ] I can explain why leases avoid database locks over NATS I/O.
- [ ] I can name the exact stream, subject, consumer, and message-ID header.
- [ ] I can state why the broker payload is ID-only.
- [ ] I can distinguish PubAck deduplication from exactly once.
- [ ] I can state every worker concurrency/backpressure bound.
- [ ] I can write the exact canonical body and HMAC grammar.
- [ ] I can distinguish integrity/authenticity, freshness, encryption, and
      idempotency.
- [ ] I can explain attempt-before-HTTP and ACK-after-success-commit.
- [ ] I can describe both duplicate ambiguity windows honestly.
- [ ] I can explain the Stage 3 local/test safety gate without calling it full
      SSRF protection.
- [ ] I can name the unfinished-attempt failure that Stage 4 must repair.
- [ ] I can complete the attempt-ID header exercise and explain its trust limit.
- [ ] I can name what the happy-path test proves and what it cannot prove.
