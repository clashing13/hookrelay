# Stage 4: failure recovery

Stage 4 turns HookRelay's Stage 3 happy path into a bounded, persistent
failure-recovery system. Version `0.4.0` classifies receiver outcomes, schedules
transient retries with exponential backoff and downward jitter, fences crashed
workers with expiring PostgreSQL claims, dead-letters terminal work, and lets an
authenticated tenant replay a dead-lettered delivery as a fresh dispatch
generation.

PostgreSQL remains authoritative. JetStream's delayed negative acknowledgment
is a wake-up mechanism, not the retry database, and broker delivery count is not
the number of HTTP attempts. The end-to-end guarantee remains **at least once**.

Use this guide as a learning loop: identify the **problem**, inspect the
**build**, observe the state, **break it safely**, and **explain** the invariant
in your own words.

## 1. The problem this stage solves

Stage 3 could send one signed request and record a failure, but its recovery
behavior was intentionally incomplete. A timeout or non-2xx response returned
the delivery to `pending` and relied on raw JetStream `AckWait` redelivery. A
worker that died after committing an unfinished attempt left the delivery in
`delivering`, and every later worker merely extended the acknowledgment
deadline. There was no durable due time, retry budget, failure classification,
dead-letter transition, or safe operator replay.

Stage 4 adds an explicit state machine:

```text
pending
  -> delivering + attempt lease
       -> succeeded --------------------------------------> ACK
       -> permanent failure -> dead_lettered ------------> ACK
       -> final transient failure -> dead_lettered ------> ACK
       -> transient failure -> retry_scheduled + due time -> delayed NAK

delivering + expired lease
  -> abandoned attempt
       -> retry_scheduled + due time ---------------------> delayed NAK
       -> attempts exhausted -> dead_lettered -----------> ACK

dead_lettered
  -> authenticated manual replay
  -> generation + 1 + fresh transactional outbox row
  -> pending
```

The important facts are ordered:

1. The worker commits an attempt claim before outbound HTTP.
2. A transient outcome and its next due time commit before the delayed NAK.
3. A terminal reason commits before the broker ACK.
4. A worker can finalize only while its attempt token and lease still match.
5. A replay increments the dispatch generation and creates a fresh outbox row
   in one PostgreSQL transaction before returning `202`.

These rules prefer recoverable duplication over silent loss. They do not join
PostgreSQL, JetStream, HTTP, and a customer database into one transaction. A
receiver may act before HookRelay loses the response or a worker is killed, so
stable event-ID idempotency is still required at the receiver.

## 2. Deliberately out of scope

Stage 4 owns recovery policy, not every production control.

Stage 5 still owns:

- complete DNS and resolved-IP SSRF validation;
- IPv4/IPv6 private, loopback, link-local, and metadata-address blocking;
- DNS-rebinding and network-egress strategy;
- per-endpoint rate limiting;
- circuit-breaker cooldown and recovery probes;
- request/payload byte limits and secret rotation;
- enabling workers outside controlled local/test environments.

The Stage 3 outbound gate therefore remains. Workers run only in `local` or
`test`, target hostnames must appear in an explicit allowlist, redirects are
disabled, and environment proxies are ignored. A blocked target creates no HTTP
attempt; the executor persists replayable terminal reason `target_blocked` and
the worker ACKs after commit. A fixed delayed NAK exists only as a fallback if
blocking escapes before terminal state can be persisted. This is not complete
SSRF protection.

Stage 6 owns full delivery-history APIs, observability, metrics, tracing,
dashboards, and the operations console. Stage 7 owns controlled fault/load
measurements, clustered/scale evidence, and release claims. Stage 4 does not
claim high availability, disaster recovery, global ordering, production
capacity, or general-purpose exactly-once delivery.

The dead-letter state is currently a durable PostgreSQL domain state. There is
no separate dead-letter JetStream stream, so this guide does not call it a
dead-letter queue. The API supplies a manual replay operation, not an automatic
operator workflow or UI.

## 3. Architecture before and after the stage

### Before Stage 4

```text
worker -> attempt -> HTTP failure -> pending + unfinished broker message
                                      |
                                      +-- raw AckWait redelivery

worker crash after attempt commit -> delivering forever

No due time, classification, cap, terminal reason, or replay dispatch
```

### After Stage 4

```text
                              PostgreSQL
             +------------------------------------------+
             | delivery status + dispatch generation    |
             | next_attempt_at + dead-letter reason      |
             | claim token/expiry + attempt history      |
             +-----------^-------------------^----------+
                         |                   |
             claim/finalize/recover          | replay transaction
                         |                   |
JetStream durable pull -> worker             API POST /replay
        ^                |                   |
        |                | classified HTTP   +-> fresh schema-v1 outbox row
        |                v                       -> publisher -> JetStream
        |          local receiver
        |
        +-- delayed NAK after durable retry decision
        +-- ACK after success/dead-letter/stale decision
```

PostgreSQL owns *whether* work may execute. JetStream owns durable transport and
the wake-up cursor. A redelivery that arrives early is checked against
`next_attempt_at` and delayed again; a redelivery for an active lease is delayed
until that lease expires; and an old-generation message is ACKed as stale.

The same four application processes remain: API, outbox publisher, worker, and
local receiver. Stage 4 adds no scheduler process. The existing worker uses the
database due time and JetStream delayed NAK together.

## 4. Repository and file tour

### Recovery implementation

| File | Stage 4 responsibility |
| --- | --- |
| `src/hookrelay/delivery.py` | Retry formula, HTTP classification, database-time due checks, attempt leases, stale recovery, terminal transitions, and fenced finalization |
| `src/hookrelay/worker.py` | Translates durable results into ACK, delayed NAK, TERM, or no stale-owner disposition |
| `src/hookrelay/replay.py` | Locks a tenant-owned dead-lettered delivery, increments its generation, resets terminal state, and creates a fresh outbox row |
| `src/hookrelay/api/deliveries.py` | Authenticated `POST /v1/deliveries/{delivery_id}/replay` contract |
| `src/hookrelay/broker.py` | Preserves the strict Stage 3 schema-v1 ID-only envelope and JetStream topology |
| `src/hookrelay/ingestion.py` | Builds a fresh schema-v1 outbox identity and stores generation on its database row |

### Schema and configuration

| File | Stage 4 responsibility |
| --- | --- |
| `src/hookrelay/models.py` | Delivery generation/schedule/terminal/lease state, attempt generation/token, and per-generation outbox uniqueness |
| `src/hookrelay/schemas.py` | Replay response and current delivery-status vocabulary |
| `src/hookrelay/config.py` | Claim TTL/finalization margin, max attempts, backoff base/cap/jitter, and policy fallback-delay validation |
| `migrations/versions/20260804_0003_failure_recovery.py` | Adds and backfills Stage 4 state, constraints, indexes, and replay-safe outbox uniqueness |
| `.env.example` | Documents the local recovery-policy defaults |
| `compose.yaml` | Supplies the Stage 4 settings to every relevant process and tags the Stage 4 image |

### Evidence

| File | Boundary exercised |
| --- | --- |
| `tests/unit/test_stage4_recovery.py` | Exact backoff bounds, classification table, strict schema-v1 envelope, result invariants, settings, and worker dispositions |
| `tests/unit/test_stage3_delivery.py` | Signing, strict messages, concurrency, poison handling, and policy fallback disposition |
| `tests/integration/test_alembic.py` | Seeded Stage 3 active-attempt backfill, `BIGINT` duration, replay downgrade guard, and invalid dead-letter constraint rejection |
| `tests/integration/test_stage3_pipeline.py` | Existing real PostgreSQL/JetStream/HTTP pipeline, including durable timeout scheduling and lost-success-ACK suppression |
| `tests/integration/test_stage4_recovery.py` | Twelve real-service scenarios: transient recovery, stopped-destination recovery, permanent and exhausted dead letters, per-generation replay budgets, lease expiry/fencing, fresh database time after a row-lock wait, fresh replay and stale-message suppression, replay concurrency/isolation/preconditions, target blocking, and an actual killed/replacement worker process |

The cumulative current view lives in `docs/architecture.md`; terminology lives
in `docs/glossary.md`; interview answers live in `docs/interview-guide.md`; and
ADRs 0011-0013 record the Stage 4 policy decisions.

## 5. Full failure and recovery flow

### A. Versioned dispatch identity with an unchanged broker envelope

Initial ingestion and replay keep Stage 3's strict schema-version-1, seven-field
ID-only broker envelope:

```json
{
  "delivery_id": "<delivery UUID>",
  "endpoint_id": "<endpoint UUID>",
  "event_id": "<event UUID>",
  "message_id": "<fresh outbox UUID>",
  "schema_version": 1,
  "tenant_id": "<tenant UUID>",
  "type": "delivery.requested"
}
```

`dispatch_generation` is authoritative database metadata on the delivery,
attempt, and outbox rows. It is deliberately absent from NATS. The worker first
reconciles the broker `message_id` and all identities with the exact outbox row,
then reads that row's generation and compares it with the delivery.

Keeping schema v1 prevents an old strict Stage 3 worker from TERMing a replay
message merely because it contains a new field or version. It is payload
compatibility, not complete mixed-worker safety: manual replay should wait until
all Stage 3 workers have drained/cut over. An old worker can parse the unchanged
message but lacks generation fencing and may send an extra stale request.

Payload, destination URL, signing secret, and generation stay out of NATS. A
row generation behind the delivery is stale and safely ACKed by a current
worker. A row generation ahead of authoritative delivery state is poison and
terminated.

### B. Due-time and attempt-budget gate

The worker locks the delivery and reads PostgreSQL's current time. For
`retry_scheduled` work:

- if `next_attempt_at` is in the future, no attempt is created and the worker
  NAKs for the remaining delay;
- if the due time has arrived, execution may continue;
- if the current generation already has the configured maximum number of
  attempts, the worker dead-letters without another HTTP request.

The default maximum is five HTTP/ambiguous attempts **per dispatch
generation**. `attempt_number` remains lifetime-monotonic for audit, while the
generation-specific count resets after a manual replay. JetStream's
`metadata.num_delivered` is deliberately not used as the budget because early
due-time checks, active-lease deferrals, and broker ambiguity can redeliver a
message without creating an HTTP attempt.

### C. Attempt claim and lease

Before HTTP, one short transaction:

1. creates a globally next-numbered `delivery_attempts` row;
2. records the current `dispatch_generation` and a random `claim_token`;
3. sets the delivery to `delivering` with the same token;
4. sets `claim_expires_at` using PostgreSQL `clock_timestamp()`;
5. commits before outbound I/O.

The default delivery claim TTL is 20 seconds. Configuration requires it to be
strictly greater than the ten-second HTTP timeout plus the explicit five-second
`delivery_finalization_margin_seconds`. The margin reserves time for response
classification and the second database transaction; it is not a proof that a
remote side effect cannot outlive the worker.

`clock_timestamp()` is intentional: PostgreSQL `now()` is fixed at transaction
start, while claim/due decisions need the wall clock at the point of each query.
Attempt duration is stored as `BIGINT`, so recovering a very old stale row does
not overflow a 32-bit millisecond field.

The partial unique index on unfinished attempts permits at most one unfinished
attempt for a delivery. The delivery check constraint requires token and
expiry exactly while status is `delivering`.

### D. Exact HTTP and classification

Stage 4 does not change webhook body/signature version 1. The worker still
sends the same compact sorted-key UTF-8 body and signs:

```text
ASCII(unix timestamp) + b"." + exact body bytes
v1=<lowercase hex HMAC-SHA256 using the snapshotted endpoint secret>
```

The service `User-Agent` advances to `HookRelay/0.4.0`; that diagnostic header
is not part of the signed grammar.

Classification is explicit:

| Observation | Attempt outcome | Next action |
| --- | --- | --- |
| HTTP `200`-`299` | `succeeded` | Commit success, then ACK |
| HTTP `408`, `425`, or `429` | `transient_failure` | Schedule retry unless budget exhausted |
| HTTP `500`-`599` | `transient_failure` | Schedule retry unless budget exhausted |
| Timeout | `transient_failure`, `request_timeout` | Schedule retry unless budget exhausted |
| Async HTTP transport error | `transient_failure`, `transport_error` | Schedule retry unless budget exhausted |
| Other `1xx`, `3xx`, or `4xx` | `permanent_failure` | Dead-letter immediately, then ACK |
| Invalid numeric status outside `100`-`599` | `permanent_failure` | Defensive terminal classification |

Redirects remain disabled, so a `3xx` response is not followed. HookRelay does
not currently honor `Retry-After`; adding it requires an explicit bounded
policy rather than trusting an arbitrary remote delay.

### E. Exponential backoff with bounded downward jitter

For generation attempt number `n`, the unjittered ceiling is:

```text
ceiling(n) = min(retry_max, retry_base * 2^(n - 1))
```

Rather than synchronizing every failed delivery at that ceiling, the worker
draws `U` uniformly from `[0, 1]` and applies:

```text
multiplier = (1 - jitter_ratio) + jitter_ratio * U
delay      = max(0.1, ceiling(n) * multiplier)
```

With defaults `base=1`, `max=60`, and `ratio=0.25`, attempt-one failure waits
between 0.75 and 1 second. The default five-attempt generation uses ceilings
1, 2, 4, 8, and 16 seconds; if the attempt budget is configured higher, later
ceilings continue through 32 and then cap at 60 seconds. Downward jitter stays
in `[75%, 100%]` of the ceiling and never exceeds the configured cap. Tests
inject the random value to prove both bounds; they do not depend on statistical
luck.

The failure completion and exact `next_attempt_at` commit in one transaction.
Only afterward does the worker NAK with the corresponding delay. On an early or
ambiguous broker redelivery, PostgreSQL recomputes the remaining wait.

### F. Broker disposition

| Durable/execution result | Broker action |
| --- | --- |
| `succeeded` or `already_succeeded` | Synchronous ACK |
| `dead_lettered` | Synchronous ACK after terminal commit |
| Old dispatch generation (`stale`) | Synchronous ACK |
| `retry_scheduled` | Delayed NAK using persisted policy delay |
| Active unexpired claim (`in_progress`) | Delayed NAK until lease expiry |
| Policy-blocked target | Persist `target_blocked`, then ACK; fixed delayed NAK only if terminal persistence was not reached |
| Malformed or authoritative-state-contradictory message | TERM as poison |
| Stale worker loses its claim | No ACK/NAK/TERM from that stale handle |
| Unexpected internal interruption | No disposition; normal broker recovery remains available |

The consumer intentionally retains `MaxDeliver=-1`. PostgreSQL, not a broker
delivery counter, decides when business attempts are exhausted.

### G. Worker-crash recovery and fencing

If a worker disconnects while its claim is still live, a redelivery is delayed
for the remaining lease. Once the database lease expires, a worker holding the
delivery lock:

1. finishes the old attempt as `abandoned`;
2. records `worker_lease_expired` and a database-time duration;
3. clears the old delivery claim;
4. counts that ambiguous attempt against the generation budget;
5. either persists a retry due time or dead-letters as exhausted.

Counting an abandoned attempt is conservative: the old worker may have sent
HTTP before dying. Ignoring it would permit an unlimited crash loop and would
understate possible receiver side effects.

Every finalizer presents attempt ID, generation, and claim token. It also
requires an unexpired matching delivery lease. A late old worker therefore
raises `DeliveryClaimLost` instead of overwriting a newer attempt or terminal
state. Fencing protects HookRelay's database state; it cannot retract an HTTP
request already observed by the receiver.

### H. Dead letter and manual replay

Permanent failures dead-letter immediately. A transient or abandoned attempt
dead-letters when the current generation reaches `delivery_max_attempts`. A
pre-HTTP allowlist rejection dead-letters without creating an attempt.
PostgreSQL records `dead_lettered_at` plus exactly one reason:
`permanent_failure`, `attempts_exhausted`, or `target_blocked`. The broker
message is ACKed only after that terminal transaction commits.

An authenticated tenant can call:

```text
POST /v1/deliveries/{delivery_id}/replay
```

The JSON request supplies the generation the operator observed:

```json
{"expected_dispatch_generation": 1}
```

Only a tenant-owned `dead_lettered` delivery at that exact generation is
eligible. Cross-tenant and missing IDs return the same opaque `404`; a changed
generation returns `409 delivery_generation_conflict`; any other non-dead-
lettered state returns `409 delivery_not_replayable`.

Under a delivery row lock, replay increments `dispatch_generation`, clears the
terminal/schedule/claim fields, returns the delivery to `pending`, and creates a
fresh schema-v1 outbox row/UUID whose database row carries the new generation.
The transaction then commits and returns `202` with
`Location: /v1/events/{event_id}`. The response contains delivery ID, event ID,
`pending`, and the new generation.

Fresh outbox identity matters because the previous broker message was ACKed and
its UUID may still be inside JetStream's duplicate window. Uniqueness is now
`(delivery_id, topic, dispatch_generation)`. Historical attempts remain; their
lifetime numbers are never reset. Replay itself is not idempotency-keyed, but
the optimistic expected-generation precondition prevents one ambiguous
operator intent from advancing twice. Retrying the same body after a lost
response receives `409 delivery_generation_conflict`; event detail exposes the
current generation, due time, and terminal reason for reconciliation.

## 6. Definitions of new technology and terms

**Abandoned attempt**

An unfinished attempt whose delivery lease expired. Its remote outcome is
unknown, so Stage 4 records it terminally as `abandoned`, counts it against the
generation budget, and schedules recovery or dead-letters.

**Attempt lease**

A random claim token and database-time expiry proving which attempt may
finalize a delivery. The attempt and delivery share the token; only the
delivery stores the expiry.

**Backoff cap**

The maximum unjittered retry delay. Exponential growth stops at this value so a
long failure history does not overflow or create an unbounded wait.

**Database wall clock (`clock_timestamp()`)**

PostgreSQL's current wall time at the instant the function is evaluated. Unlike
transaction-start `now()`, it advances during a transaction, so Stage 4 uses it
for lease-expiry and retry-due decisions shared by every worker.

**Dead-lettered delivery**

A terminal PostgreSQL state for work that received a permanent failure,
exhausted its current dispatch-generation attempt budget, or was blocked by the
temporary outbound policy. It remains inspectable and manually replayable; it
is not deleted.

**Dead-letter reason**

The required terminal explanation stored with a dead-lettered delivery:
`permanent_failure`, `attempts_exhausted`, or `target_blocked`.

**Delayed negative acknowledgment (`NAK`)**

A JetStream consumer response asking for the message again after a delay. It
is a transport wake-up hint. The durable PostgreSQL due time remains the
authority if delivery is early, duplicated, or delayed differently.

**Dispatch generation**

A positive integer identifying one bounded retry cycle for a delivery.
Initial work is generation 1; each accepted manual replay increments it and
creates a new outbox/message identity. It is stored on PostgreSQL rows and is
not a broker-envelope field.

**Downward jitter**

A random reduction from an exponential ceiling. Stage 4 samples uniformly
between `(1 - ratio) * ceiling` and `ceiling`, spreading retries without ever
exceeding the cap.

**Exponential backoff**

A retry policy whose unjittered delay doubles after each failed attempt until a
configured cap. It reduces repeated pressure on an unhealthy destination.

**Failure classification**

A documented mapping from observed HTTP/transport outcomes to success,
transient failure, or permanent failure. Classification is policy, not a fact
inherent in every customer's application.

**Fencing**

Rejecting work from an old owner after a newer owner can recover it. HookRelay
matches attempt ID, generation, token, state, and unexpired lease before
finalization.

**Generation attempt number**

The number of attempt rows in the current dispatch generation. It drives
backoff and maximum-attempt policy. It differs from the lifetime
`attempt_number` and JetStream delivery count.

**Manual replay**

An authenticated operator action that moves one dead-lettered delivery into a
new dispatch generation with a fresh transactional-outbox message. Its JSON
precondition names the generation the operator observed so one ambiguous intent
cannot advance twice. It does not erase attempts or promise immediate success.

**Optimistic replay precondition**

The request field `expected_dispatch_generation`. Replay succeeds only if it
still matches the locked delivery, so a duplicate request after a lost response
cannot silently grant another generation.

**Next attempt time (`next_attempt_at`)**

The PostgreSQL timestamp at or after which a scheduled delivery may create its
next HTTP attempt. Broker delivery before this time causes another delayed NAK.

**Permanent failure**

An outcome that the current policy does not expect to improve by repeating the
same request, such as most `4xx` responses. It dead-letters immediately.

**Policy-blocked delivery**

Valid durable work whose target is outside the temporary local/test allowlist.
The executor creates no attempt or HTTP request; it normally persists terminal
reason `target_blocked` before the worker ACKs. A fixed delayed NAK is only the
fallback if that terminal decision could not be stored.

**Retry budget**

The maximum number of HTTP or ambiguous abandoned attempts permitted in one
dispatch generation. Broker redeliveries that create no attempt do not spend it.

**Retry schedule**

The persistent delivery state `retry_scheduled` plus a non-null due time. A
database constraint requires both together.

**Stale dispatch**

A broker message whose reconciled outbox row belongs to a lower generation than
the current delivery. A Stage 4 worker ACKs it without another HTTP request.

**Transient failure**

An outcome that may improve later, such as a timeout, async transport error,
`408`, `425`, `429`, or `5xx`. It is retried only while budget remains.

## 7. Why each technology and design was chosen

### PostgreSQL-authoritative schedules

Retry intent must survive worker restarts and be inspectable with domain state.
Persisting status and due time in the same transaction as attempt completion
prevents a successful database update from depending on an in-memory timer or
one NAK call. JetStream still transports work efficiently.

### Delayed NAK rather than sleeping inside a worker

Sleeping would hold one broker handle and one concurrency slot while doing no
work. A delayed NAK returns capacity to the worker. Rechecking PostgreSQL on
redelivery makes an early or duplicated wake-up harmless.

### Database-owned maximum rather than `MaxDeliver`

JetStream counts every broker delivery. HookRelay needs to count only outbound
or ambiguous attempts within the current generation. Keeping `MaxDeliver=-1`
prevents the broker from silently exhausting a message during due-time or
active-lease deferrals.

### Bounded downward jitter

Pure exponential backoff makes every delivery that failed together retry on the
same boundaries. Downward jitter spreads those attempts while retaining a
simple documented upper bound. A random source is injected so tests can prove
both edges deterministically.

### Database-time expiring leases

Holding a database transaction across HTTP would consume locks and connections.
An expiring claim permits short transactions and crash recovery. Database time
keeps all workers comparing against one clock. Tokens fence late finalizers.

### Generation-scoped retry budgets

A manual replay should provide a deliberate new budget after an operator fixes
the cause, but historical attempts must remain immutable. Dispatch generation
separates those concerns without resetting lifetime attempt numbers.

### Fresh outbox row on replay

A terminal broker message has been ACKed, and reusing its message UUID can be
suppressed by finite-window deduplication. A new outbox identity restores the
same transactional handoff used by ingestion and makes each replay auditable.

### PostgreSQL dead-letter state

The delivery and attempt rows already hold authoritative operational history.
A terminal state plus reason is enough for Stage 4 replay without introducing
another stream, consumer, retention policy, and reconciliation problem.

## 8. Serious alternatives and why they were not selected

### Raw `AckWait` redelivery

It requires no application policy, but supplies no durable due time, jitter,
business attempt cap, or permanent classification. Repeated failures can form a
tight synchronized loop, and Stage 3's unfinished attempts remain stuck.

### In-memory timers or an `asyncio.sleep` per delivery

Timers disappear on process death and consume memory proportional to the
backlog. Sleeping tasks also occupy worker capacity. Persistent due state plus
delayed NAK survives normal worker restarts.

### JetStream `MaxDeliver` as the retry limit

Broker deliveries include early schedules, active leases, lost ACKs, and other
events that are not HTTP attempts. A broker limit could discard work before
PostgreSQL records the configured business attempts.

### Fixed delay without jitter

It is easy to explain but keeps a fleet synchronized and does not reduce
pressure progressively during a long outage. Exponential ceilings plus bounded
jitter provide both relief and a reviewable maximum.

### Full jitter from zero

Sampling from zero to the ceiling spreads traffic more broadly, but can retry
almost immediately even during a sustained failure. Stage 4 chooses downward
jitter within a configurable band so every retry retains a meaningful fraction
of the backoff ceiling.

### Retry every HTTP failure

Repeatedly sending a malformed or unauthorized request wastes capacity and can
harm the receiver. The default classifier treats most `4xx` and redirects as
permanent while preserving known temporary statuses. A future product may need
per-endpoint policy, but silent customization is avoided now.

### Separate dead-letter stream

It can be useful for independent retention and operational consumers, but would
create a second message/state reconciliation problem. Stage 4 needs a durable
terminal domain fact and safe replay, which PostgreSQL already supplies.

### Reset or delete attempt history on replay

That would hide evidence and make lifetime ordering ambiguous. Generations
reset only the budget; attempts remain append-only identities.

### Reuse the original outbox row

The old message may be acknowledged and its UUID may still be deduplicated by
JetStream. A new generation and outbox UUID avoid false replay success and
preserve an audit trail.

## 9. Failure modes and design tradeoffs

| Failure or boundary | Stage 4 behavior | Remaining limitation |
| --- | --- | --- |
| Receiver timeout or transport error | Persist transient attempt and due time, then delayed NAK | Receiver may already have acted before uncertainty |
| `408`, `425`, `429`, or `5xx` | Retry with capped exponential backoff and jitter | Policy is global, not per endpoint |
| Other non-2xx | Persist permanent attempt, dead-letter, then ACK | Some customer APIs may consider a specific `4xx` retryable |
| NAK lost after retry commit | `AckWait` can redeliver; PostgreSQL rechecks due time | Wake-up may be earlier/later than requested |
| ACK lost after success/dead-letter commit | Redelivery observes terminal state and ACKs again | Duplicate broker delivery remains observable |
| Worker dies before attempt commit | Broker redelivery can claim normally | Exact recovery time depends on broker state |
| Worker dies after attempt commit | Active lease delays; expired attempt becomes abandoned and retryable/terminal | Receiver side effect may be duplicated |
| Old worker returns after lease recovery | Token/generation/expiry checks reject finalization | Cannot cancel a remote side effect already performed |
| Every transient attempt fails | Current generation dead-letters at the configured maximum | Manual intervention is required |
| Permanent response | Immediate dead letter | Classification may need product-specific evolution |
| Policy-blocked target | Persist `target_blocked` without an attempt, then ACK | Operator must fix policy and replay deliberately |
| Replay succeeds but response is lost | Delivery is pending in the next generation; retrying the same expected generation gets a conflict | Client reads exposed current generation/state |
| Concurrent replay calls | Row lock plus expected-generation precondition advances one generation once | No general replay idempotency-key record |
| Old-generation message appears after replay | Worker ACKs it as stale without HTTP | Broker duplication still consumes a small read/check |
| New replay publish is ambiguous | Fresh outbox follows normal PubAck/finalization recovery | Publication remains at least once |
| NATS unavailable | API/replay can commit fresh outbox intent; publisher waits/reconnects | Backlog grows and needs Stage 6 monitoring |
| NATS volume lost | PostgreSQL retains domain/retry facts, but published unacked transport may be gone | Local single-replica topology is not disaster recovery |
| PostgreSQL unavailable | API/worker cannot make an authoritative transition | Liveness remains process-local; readiness covers API PostgreSQL only |
| Process clock differs | Due/lease state uses PostgreSQL `clock_timestamp()` | HMAC timestamp still uses the worker clock |
| Retry storm | Jitter and bounded concurrency spread/limit attempts | Stage 5 rate/circuit controls and Stage 7 measurement remain absent |

The retry policy is a capacity budget as well as a correctness rule. Worker
concurrency, HTTP pool size, `MaxAckPending`, claim TTL, `AckWait`, retry cap,
and destination recovery behavior must be reasoned about together.

## 10. Exact commands for running and testing

The examples use Windows PowerShell and host PostgreSQL port `55432`. Container
processes still use `postgres:5432`.

### Prepare a clean checkout

```powershell
git clone --branch codex/stage-04-failure-recovery --single-branch https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
Copy-Item .env.example .env
$env:POSTGRES_HOST_PORT = "55432"
```

Before starting services, edit the ignored `.env` and replace the example
database password, encryption key, and bootstrap token. The retry defaults are:

```text
claim TTL       20 seconds
HTTP timeout    10 seconds
finalize margin 5 seconds
max attempts    5 per dispatch generation
retry base      1 second
retry cap       60 seconds
jitter ratio    0.25
policy delay    30 seconds
```

Create the locked host environment if needed:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

### Build, migrate, and start

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose build
docker compose up --detach --wait postgres nats receiver
docker compose run --rm api alembic upgrade head
docker compose up --detach --wait api
docker compose up --detach outbox-publisher worker
docker compose ps
```

Migrations remain an explicit release step. Upgrade `20260804_0003` recovers
any Stage 3 unfinished attempts as `abandoned`, makes the corresponding
deliveries immediately scheduled, and adds the new constraints. Its downgrade
refuses any data Stage 3 cannot represent: Stage 4 active/scheduled/terminal
delivery state, a non-1 generation on any relevant row, or an attempt duration
outside the former 32-bit range.

### Bootstrap one local tenant and endpoint

```powershell
$bootstrapHeaders = @{
  Authorization = "Bearer replace-this-local-bootstrap-token-before-use"
}
$bootstrapBody = @{
  name = "Stage 4 demo"
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
  name = "Local recovery receiver"
  url = "http://receiver:9000/webhooks"
} | ConvertTo-Json
$endpoint = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/endpoints `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $endpointBody
```

### Observe transient retry then success

Make the receiver return `503`, recreating only the disposable receiver:

```powershell
$env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE = "503"
docker compose up --detach --wait --force-recreate receiver
```

Submit one event:

```powershell
$transientHeaders = @{
  Authorization = "Bearer $apiKey"
  "Idempotency-Key" = "stage4-transient-0001"
}
$transientBody = @{
  type = "order.created"
  payload = @{ order_id = "ord_retry" }
  endpoint_ids = @($endpoint.id)
} | ConvertTo-Json -Depth 5
$transientEvent = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $transientHeaders `
  -ContentType "application/json" `
  -Body $transientBody
$transientDeliveryId = $transientEvent.deliveries[0].id
```

Inspect the durable schedule and attempts:

```powershell
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT id, status, dispatch_generation, next_attempt_at, dead_letter_reason FROM deliveries WHERE id = '$transientDeliveryId';"
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT attempt_number, dispatch_generation, outcome, response_status_code, error_code FROM delivery_attempts WHERE delivery_id = '$transientDeliveryId' ORDER BY attempt_number;"
```

Restore the receiver before the five-attempt budget is exhausted:

```powershell
$env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE = "204"
docker compose up --detach --wait --force-recreate receiver

do {
  Start-Sleep -Milliseconds 500
  $currentTransient = Invoke-RestMethod `
    -Uri "http://127.0.0.1:8000/v1/events/$($transientEvent.id)" `
    -Headers $authHeaders
  $transientStatus = $currentTransient.deliveries[0].status
} while ($transientStatus -notin @("succeeded", "dead_lettered"))
$currentTransient.deliveries
```

### Observe permanent dead letter and replay

Return `400` and submit a separate event:

```powershell
$env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE = "400"
docker compose up --detach --wait --force-recreate receiver

$permanentHeaders = @{
  Authorization = "Bearer $apiKey"
  "Idempotency-Key" = "stage4-permanent-0001"
}
$permanentBody = @{
  type = "order.created"
  payload = @{ order_id = "ord_fix_then_replay" }
  endpoint_ids = @($endpoint.id)
} | ConvertTo-Json -Depth 5
$permanentEvent = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $permanentHeaders `
  -ContentType "application/json" `
  -Body $permanentBody
$permanentDeliveryId = $permanentEvent.deliveries[0].id

do {
  Start-Sleep -Milliseconds 250
  $currentPermanent = Invoke-RestMethod `
    -Uri "http://127.0.0.1:8000/v1/events/$($permanentEvent.id)" `
    -Headers $authHeaders
} while ($currentPermanent.deliveries[0].status -ne "dead_lettered")
```

Repair the destination and enqueue generation 2:

```powershell
$env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE = "204"
docker compose up --detach --wait --force-recreate receiver

$replayBody = @{
  expected_dispatch_generation = $currentPermanent.deliveries[0].dispatch_generation
} | ConvertTo-Json
$replay = Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/v1/deliveries/$permanentDeliveryId/replay" `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $replayBody
$replay
```

The response is `pending` generation 2. Poll the event until `succeeded`, then
inspect both generations:

```powershell
do {
  Start-Sleep -Milliseconds 250
  $afterReplay = Invoke-RestMethod `
    -Uri "http://127.0.0.1:8000/v1/events/$($permanentEvent.id)" `
    -Headers $authHeaders
} while ($afterReplay.deliveries[0].status -ne "succeeded")

docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT id, dispatch_generation, published_at FROM outbox_messages WHERE delivery_id = '$permanentDeliveryId' ORDER BY dispatch_generation;"
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT attempt_number, dispatch_generation, outcome FROM delivery_attempts WHERE delivery_id = '$permanentDeliveryId' ORDER BY attempt_number;"
```

Expect two outbox rows with different IDs/generations and preserved attempts.

### Run quality and test gates

Fast checks:

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\mypy.exe
.\.venv\Scripts\pytest.exe -m "not integration"
```

Real PostgreSQL, JetStream, HTTP, migration, and package checks:

```powershell
$postgresHostPort = "55432"
$env:POSTGRES_HOST_PORT = $postgresHostPort
docker compose stop api outbox-publisher worker receiver
docker compose up --detach --wait postgres nats

# Enter the values from the current untracked .env used by this PostgreSQL
# container. The password prompt is masked.
$postgresUser = Read-Host "POSTGRES_USER from .env"
$securePostgresPassword = Read-Host "POSTGRES_PASSWORD from .env" -AsSecureString
$postgresCredential = [System.Net.NetworkCredential]::new(
  $postgresUser,
  $securePostgresPassword
)
if ([string]::IsNullOrWhiteSpace($postgresCredential.UserName) -or
    [string]::IsNullOrEmpty($postgresCredential.Password)) {
  throw "PostgreSQL user and password are required."
}

$testDatabase = "hookrelay_test"
$databaseExists = docker compose exec -T postgres psql `
  --username $postgresCredential.UserName `
  --dbname postgres `
  --tuples-only `
  --no-align `
  --command "SELECT 1 FROM pg_database WHERE datname = '$testDatabase';"
if ($LASTEXITCODE -ne 0) {
  throw "Could not inspect the PostgreSQL databases."
}
if (($databaseExists -join "").Trim() -eq "1") {
  $disposableConfirmation = Read-Host `
    "$testDatabase already exists; type its exact name to confirm it is disposable"
  if ($disposableConfirmation -cne $testDatabase) {
    throw "Refusing to run integration tests against an unconfirmed database."
  }
} else {
  docker compose exec -T postgres createdb `
    --username $postgresCredential.UserName `
    --owner $postgresCredential.UserName `
    $testDatabase
  if ($LASTEXITCODE -ne 0) {
    throw "Could not create the disposable $testDatabase database."
  }
}

$encodedPostgresUser = [Uri]::EscapeDataString($postgresCredential.UserName)
$encodedPostgresPassword = [Uri]::EscapeDataString($postgresCredential.Password)
$env:HOOKRELAY_TEST_DATABASE_URL = `
  "postgresql+asyncpg://${encodedPostgresUser}:${encodedPostgresPassword}@127.0.0.1:$postgresHostPort/$testDatabase"
$env:HOOKRELAY_DATABASE_URL = $env:HOOKRELAY_TEST_DATABASE_URL
$env:HOOKRELAY_NATS_URL = "nats://127.0.0.1:4222"
$env:HOOKRELAY_TEST_NATS_URL = "nats://127.0.0.1:4222"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\pytest.exe -m "integration and not concurrency"
.\.venv\Scripts\pytest.exe -m concurrency
.\.venv\Scripts\alembic.exe check
docker compose config --quiet
docker build --pull --tag hookrelay:stage4 .
```

The commands create or reuse only the explicitly named `hookrelay_test`; they
never point tests at the normal `POSTGRES_DB` or drop a database. Reserve it for
disposable test data and stop if it contains anything valuable. A Stage 4
downgrade succeeds only while the data remains representable by Stage 3. The
migration intentionally refuses active/scheduled/dead-lettered Stage 4 state,
non-1 generations, and attempt durations that would overflow Stage 3's integer
column; validate those guards with seeded fixtures rather than deleting valued
evidence.

### Stop without deleting durable local data

```powershell
docker compose down
```

`docker compose down --volumes` destroys PostgreSQL and NATS local data and is
not part of a recovery test.

## 11. How each test works and what it cannot prove

### Pure policy and contract tests

`tests/unit/test_stage4_recovery.py` injects jitter edges, checks exponential
growth/capping, fixes the classification table, proves the strict schema-v1
envelope still rejects a generation field/version change, validates retry-result
shape and settings relationships, and verifies ACK versus delayed-NAK decisions.

These tests prove deterministic policy functions. They do not prove row locks,
database time, a real delayed NAK, HTTP, or process recovery.

### Existing unit/API boundaries

Stage 2/3 tests continue to prove exact HTTP contracts, authentication,
idempotency, HMAC bytes, strict broker data, bounded concurrency, poison
termination, and fallback disposition behavior. OpenAPI must expose the
authenticated JSON replay route without revealing another tenant's resources.

In-process fakes do not prove PostgreSQL constraints, commit ordering,
JetStream persistence, or cross-process behavior.

### PostgreSQL and real-service recovery

The twelve-scenario Stage 4 integration suite exercises transient delayed-NAK
recovery, destination outage/restart, permanent failure, exact max-attempt
exhaustion, per-generation replay budgets with global history, expired-lease
abandonment and stale fencing, fresh database time after a row-lock wait, fresh
replay with stale-broker suppression, concurrent and cross-tenant replay, stale
replay preconditions, terminal target blocking, and an actual killed/replacement
worker process. The Stage 3 pipeline suite still verifies real PubAck, durable
pull, HTTP capture, timeout scheduling, and success-commit-before-ACK behavior.

The migration suite also seeds Stage 3 active state and verifies abandoned-row
backfill, `BIGINT` duration, the replay-history downgrade guard, and rejection of
invalid dead-letter timestamp/reason combinations. A row-lock timing regression
starts its transaction, waits on the delivery lock until scheduled work becomes
due, then proves the attempt proceeds with a fresh `clock_timestamp()`-based
start rather than the stale transaction-start `now()` value.

At the Stage 4 checkpoint, the complete suite passed all 143 tests.

This evidence covers the exact schedules encoded by those tests. It cannot
enumerate every crash point, prove a remote receiver's transaction outcome, or
prove clustered durability and production capacity.

### Hard process-kill boundary

The Stage 4 suite starts a separate worker OS process, waits until the receiver
captures its request, forcibly kills that worker, then starts a replacement.
The replacement abandons the expired claim and recovers the same delivery. Two
receiver captures with identical bodies demonstrate the real at-least-once
ambiguity at that specific crash boundary.

That is materially stronger evidence than task cancellation or an expired-row
fixture, but it proves only the encoded schedule. It does not enumerate every
instruction-level crash point, prove exactly-once receiver effects, or establish
HA and production capacity. The manual exercise in section 12 makes the same
boundary observable during local learning.

### Component learning loops

#### Persistent retry schedule

- **Problem:** an in-memory delay disappears with the worker.
- **Build:** attempt completion and database-time `next_attempt_at` commit
  together; delayed NAK only requests a later wake-up.
- **Inspect:** compare attempt finish, due time, and NAK log fields.
- **Break it safely:** stop the worker after a transient schedule and restart it
  after the due time.
- **Explain:** why PostgreSQL remains authoritative even though JetStream holds
  the message.

#### Classification and budget

- **Problem:** retrying every response forever wastes capacity.
- **Build:** one explicit status table and a five-attempt per-generation cap.
- **Inspect:** compare a `503` schedule with a `400` immediate dead letter.
- **Break it safely:** configure the local receiver, never a real endpoint.
- **Explain:** why classification is policy and why broker deliveries are not
  attempts.

#### Lease recovery and fencing

- **Problem:** a crash can leave an unfinished attempt while its HTTP outcome is
  unknown.
- **Build:** shared token, expiry, abandonment, and conditional finalization.
- **Inspect:** observe `delivering`, the unfinished row, then `abandoned` after
  expiry.
- **Break it safely:** kill only the disposable local worker process.
- **Explain:** why fencing protects database state but cannot provide exactly
  once at the receiver.

#### Dead letter and replay generation

- **Problem:** bounded retry needs an inspectable terminal state and controlled
  recovery after an operator fix.
- **Build:** terminal reason plus authenticated row-locked replay with a fresh
  outbox identity.
- **Inspect:** see two generations and lifetime-monotonic attempts.
- **Break it safely:** replay before repairing the receiver and observe that the
  fresh generation can fail independently.
- **Explain:** why replay resets budget but never erases history.

## 12. Safe "break it intentionally" exercise

### Goal

Observe the real worker-crash ambiguity: the receiver may see a request while
HookRelay retains an unfinished attempt; an expired database lease then fences
the dead worker and permits at-least-once recovery.

Use only the local receiver and preserve both named volumes. Start from the
running environment and tenant/endpoint variables in section 10.

### Make the receiver hold the response

```powershell
$env:HOOKRELAY_RECEIVER_DELAY_SECONDS = "15"
$env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE = "204"
docker compose up --detach --wait --force-recreate receiver
```

Submit one event:

```powershell
$crashHeaders = @{
  Authorization = "Bearer $apiKey"
  "Idempotency-Key" = "stage4-worker-crash-0001"
}
$crashBody = @{
  type = "order.created"
  payload = @{ order_id = "ord_crash_window" }
  endpoint_ids = @($endpoint.id)
} | ConvertTo-Json -Depth 5
$crashEvent = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $crashHeaders `
  -ContentType "application/json" `
  -Body $crashBody
$crashDeliveryId = $crashEvent.deliveries[0].id

do {
  Start-Sleep -Milliseconds 100
  $firstCaptures = @(
    Invoke-RestMethod "http://127.0.0.1:9000/requests/$crashDeliveryId"
  )
} while ($firstCaptures.Count -eq 0)
$firstEvidence = $firstCaptures[-1]
```

The receiver has accepted the bytes but is still delaying its response. Kill
only the worker, then inspect the unfinished database claim:

```powershell
docker compose kill worker
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT status, claim_token, claim_expires_at FROM deliveries WHERE id = '$crashDeliveryId';"
docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT attempt_number, finished_at, outcome FROM delivery_attempts WHERE delivery_id = '$crashDeliveryId' ORDER BY attempt_number;"
```

Expect `delivering`, a claim, and an unfinished attempt. This does **not** tell
HookRelay whether the receiver's business side effect committed.

### Restore and recover

Recreate the disposable receiver without delay. Its in-memory ledger resets,
so retain `$firstEvidence` as proof of the first request.

```powershell
$env:HOOKRELAY_RECEIVER_DELAY_SECONDS = "0"
docker compose up --detach --wait --force-recreate receiver
docker compose up --detach worker
```

The durable consumer may wait for `AckWait`; the delivery lease must also
expire. With defaults this can take a little over 30 seconds. Poll current state:

```powershell
do {
  Start-Sleep -Seconds 1
  $afterCrash = Invoke-RestMethod `
    -Uri "http://127.0.0.1:8000/v1/events/$($crashEvent.id)" `
    -Headers $authHeaders
} while ($afterCrash.deliveries[0].status -notin @("succeeded", "dead_lettered"))
$afterCrash.deliveries

docker compose exec -T postgres psql `
  --username hookrelay `
  --dbname hookrelay `
  --command "SELECT attempt_number, dispatch_generation, outcome, error_code FROM delivery_attempts WHERE delivery_id = '$crashDeliveryId' ORDER BY attempt_number;"

$recoveredCaptures = @(
  Invoke-RestMethod "http://127.0.0.1:9000/requests/$crashDeliveryId"
)
$recoveredEvidence = $recoveredCaptures[-1]
[pscustomobject]@{
  SameBody = $firstEvidence.body_sha256 -eq $recoveredEvidence.body_sha256
  SameEventId = `
    $firstEvidence.headers.'hookrelay-event-id' -eq `
    $recoveredEvidence.headers.'hookrelay-event-id'
  FirstBodySha256 = $firstEvidence.body_sha256
  RecoveredBodySha256 = $recoveredEvidence.body_sha256
}
```

The first attempt should become `abandoned` with
`worker_lease_expired`; a later attempt can succeed. Compare the saved first
capture with the recovered request: `SameBody` and `SameEventId` should both be
`True`. Both requests carry the same stable event ID, which is why a real
receiver must deduplicate it atomically with its business effect.

### Explain the result

Answer aloud:

1. Why was the first attempt not immediately retried?
2. Which clock decides when ownership expires?
3. Why does the abandoned attempt spend retry budget?
4. What prevents the killed worker from overwriting a recovered attempt if it
   somehow returns late?
5. Why can the receiver still observe a duplicate?
6. Which part was preserved by JetStream, and which part by PostgreSQL?
7. Why does this manual run not prove every kill schedule or production HA?

Do not run `docker compose down --volumes` during the exercise.

## 13. Troubleshooting

### A retry happens earlier or later than the configured ceiling

The ceiling is not the exact delay. Downward jitter selects between
`ceiling * (1 - ratio)` and `ceiling`, and broker wake-up timing is approximate.
Inspect the persisted `next_attempt_at`; it is authoritative.

### A message redelivers but no new attempt appears

This is often correct. The delivery may not be due, may have an unexpired
attempt lease, may belong to an older generation, or may be policy blocked.
Compare delivery status, generation, due time, claim expiry, and worker logs.
Broker delivery count alone is not attempt count.

### A delivery remains `delivering`

Check `claim_expires_at`, NATS health, and whether a worker is connected. A
worker redelivery before expiry receives a delayed NAK. After expiry, recovery
marks the old attempt abandoned. Do not manually clear tokens in valued data.

### The worker reports `delivery_claim_lost`

Another worker or later redelivery fenced the old finalizer after its lease was
no longer authoritative. The stale handle deliberately performs no broker
disposition. Inspect the current delivery/attempt owner instead of extending
the old claim.

### A delivery dead-letters after fewer than five responses

A permanent response dead-letters immediately. An expired unfinished attempt
also counts, even though no response is known. Count attempts in the current
`dispatch_generation`, not only rows with response status.

### Replay returns `409 delivery_not_replayable`

Only `dead_lettered` is accepted. The delivery may already be pending in the
new generation. Read event detail and inspect generation/outbox rows.

### Replay returns `409 delivery_generation_conflict`

The JSON `expected_dispatch_generation` is stale. This commonly means an earlier
ambiguous replay already advanced the delivery. Do not increment the value and
blindly resubmit; inspect exposed current state and decide whether a later
terminal generation really needs a new operator action.

### Replay returns `404` for an ID that exists

The authenticated API key may belong to another tenant. Missing and cross-
tenant IDs intentionally share one opaque response.

### Replayed work never reaches JetStream

Inspect the fresh generation's outbox row, publisher claim, `published_at`, and
publisher/NATS logs. Replay commits durable dispatch intent; `202` does not
mean publication or HTTP success.

### Alembic downgrade refuses current data

This is deliberate. Stage 3 cannot represent Stage 4 active/scheduled/terminal
state, replay generations, or an attempt duration above its former 32-bit
column. The downgrade refuses to discard, remap, or truncate that evidence.
Restore from a compatible pre-upgrade backup or keep the Stage 4 schema; do not
delete rows just to force a downgrade on valued data.

### A delivery dead-lettered as `target_blocked`

The pre-Stage-5 allowlist rejected it before an HTTP attempt. Correct only a
reviewed local/test configuration, observe the delivery's current generation,
then use manual replay with that expected generation. Do not add a wildcard or
enable production workers. Repeated fixed delayed NAK indicates the fallback
path ran before terminal state could be persisted; inspect database errors.

### `MaxDeliver` is unlimited even though max attempts is five

That is intentional. The broker counts deliveries; PostgreSQL counts actual or
ambiguous attempts per generation. Setting both to five could exhaust work on
deferrals without making five HTTP attempts.

### Tests pass but restart behavior differs

Unit tests do not start a separate process, and a local single-node NATS server
is not clustered evidence. Run the real-service suite and the manual exercise,
then inspect durable rows and consumer state at every boundary.

## 14. Recruiter and interviewer questions

### "How did you implement retries without making the broker the database?"

The attempt result, delivery status, and exact database-time due timestamp
commit together in PostgreSQL. Only then does the worker send a delayed NAK.
Every redelivery reloads and locks authoritative state, so an early or duplicate
wake-up cannot bypass the schedule.

### "What is your backoff formula?"

For current-generation attempt `n`, cap `base * 2^(n-1)` at the configured
maximum. Sample uniformly from `[ceiling * (1-ratio), ceiling]`, with a 100 ms
floor. Defaults use a one-second base, 60-second cap, and 25% downward jitter.
The random source is injected in tests.

### "Why not use JetStream MaxDeliver?"

Broker delivery count includes due-time deferrals, active-lease deferrals, and
lost ACKs. The business policy counts HTTP and ambiguous abandoned attempts.
PostgreSQL therefore owns the five-attempt per-generation budget while the
consumer keeps unlimited broker redelivery.

### "Which failures are retryable?"

Timeouts, async transport errors, `408`, `425`, `429`, and `5xx` are transient.
`2xx` succeeds; other HTTP statuses are permanent by the current policy. The
table is explicit and tested, but it remains a product policy that may need
per-endpoint evolution.

### "How do you recover after a worker crash?"

The attempt and delivery share a random claim token, and the delivery stores a
database-time lease expiry. Before expiry, redelivery is delayed. After expiry,
a worker under the row lock records the old attempt as abandoned, spends its
budget, and schedules or dead-letters. Late finalization is fenced by attempt,
generation, token, state, and expiry checks.

### "Can crash recovery duplicate a webhook?"

Yes. The first worker may have reached the receiver before dying. The lease can
protect only HookRelay's ownership state, not an independent receiver
transaction. Both attempts carry the stable event ID so the receiver can
deduplicate it atomically with its side effect.

### "What does dead-letter mean here?"

It is a terminal PostgreSQL delivery state with a timestamp and either
`permanent_failure`, `attempts_exhausted`, or `target_blocked`. The original
broker message is ACKed after that commit. There is no separate DLQ in Stage 4.

### "How does replay avoid stale broker work?"

Replay row-locks a tenant-owned dead-lettered delivery, increments its
generation only if the JSON expected-generation precondition still matches,
clears terminal state, and creates a fresh schema-v1 outbox UUID in the same
transaction. Current workers derive generation from the reconciled outbox row;
old-generation rows are ACKed as stale. The fresh UUID avoids the old
`Nats-Msg-Id` deduplication window.

### "What did the tests prove?"

Name exact boundaries: deterministic policy vectors, real database
constraints/leases/generations, real broker delayed disposition where
exercised, real HTTP classification/recovery, replay authorization/concurrency,
and migration behavior. The suite also kills a separate worker after receiver
capture and proves replacement-worker recovery for that one schedule. Then
state the boundary honestly: fixture-driven recovery alone would not be
hard-kill proof, and even the real process-kill scenario does not prove every
crash point, exactly once, HA, hostile-network safety, or production scale.

## 15. Hands-on modification for Tarun

Add a diagnostic `HookRelay-Attempt-Number` header containing the lifetime
attempt number already present in `DeliveryWork`. Do not copy a supplied
solution; trace the value from the database claim to the exact receiver
capture.

Acceptance criteria:

1. Add the header without changing the version-1 body or HMAC input.
2. Use the lifetime `attempt_number`, not broker delivery count or generation
   attempt count.
3. Add a fixed unit assertion for the exact header name and value.
4. Add a recovery integration assertion showing the value increases after a
   retry.
5. Preserve `HookRelay-Event-Id` as the receiver's business idempotency key.
6. State in ADR 0009 that this diagnostic header is not authenticated and must
   not be trusted for authorization, policy, or idempotency.
7. Add no migration; the attempt number already exists.
8. Do not log the signing secret or add it to broker/receiver output.
9. Run Ruff, format, mypy, fast tests, real-service tests, `alembic check`,
   Compose validation, and the image build.

Questions to answer before coding:

- Why is an attempt number different from a stable event ID?
- Why can a replay generation contain lifetime attempt 6 but generation attempt
  1?
- Why is an unsigned diagnostic header useful but untrusted?
- What would require a versioned change to the signed grammar?
- Which test proves the header corresponds to the persisted attempt?

## 16. Comprehension quiz and teach-back checklist

### Quiz

Answer without looking, then verify against code and tests.

1. What failure-recovery gaps remained after Stage 3?
2. Which store is authoritative for retry eligibility?
3. What is JetStream's delayed NAK responsible for?
4. Write the exact unjittered retry-ceiling formula.
5. Write the downward-jitter interval.
6. Why can the delay never exceed the configured cap?
7. Which attempt number drives backoff?
8. How does lifetime `attempt_number` differ from generation attempt number?
9. Why is broker delivery count not the retry budget?
10. What is the default maximum per generation?
11. Which HTTP statuses are transient?
12. What happens to most `4xx` responses?
13. How are timeouts and async transport errors classified?
14. Why is `Retry-After` not currently used?
15. Which fields prove an active attempt owns the delivery?
16. Which clock creates and checks lease/due timestamps?
17. Why must claim TTL exceed the HTTP timeout?
18. What happens when a live claim receives a broker redelivery?
19. What happens when the lease has expired?
20. Why does an abandoned attempt spend budget?
21. Which conditions fence a late finalizer?
22. Why can fencing not prevent receiver duplicates?
23. Which durable results ACK the broker message?
24. Which results use delayed NAK?
25. Why does policy-blocked work not consume an attempt, and how is it stored?
26. Why does the consumer keep `MaxDeliver=-1`?
27. What three dead-letter reasons exist?
28. What status, authentication, content-type, and precondition does replay use?
29. Why must replay create a fresh outbox UUID?
30. Where is dispatch generation stored, and how does a worker derive it?
31. What happens to an old-generation message after replay?
32. Does replay erase attempt history?
33. What can `202` replay prove, and what can it not prove?
34. Why can the Stage 4 migration refuse downgrade?
35. What Stage 5 protections remain absent?
36. What evidence distinguishes a fixture-driven stale lease from a real killed
    worker process?
37. Why should manual replay wait until Stage 4 workers have cut over?

<details>
<summary>Self-check answer ingredients</summary>

1. No persistent due time/backoff/jitter/cap/classification, stale recovery,
   terminal reason, or replay.
2. PostgreSQL.
3. Requesting a later broker wake-up; it is not the domain schedule.
4. `min(max, base * 2^(n-1))`.
5. `[ceiling * (1-ratio), ceiling]`, with a 100 ms floor.
6. Jitter is only downward and the ceiling is capped first.
7. Count of attempts in the current dispatch generation.
8. Lifetime numbering never resets; generation count resets on replay.
9. Broker deferrals/redeliveries may create no HTTP attempt.
10. Five.
11. `408`, `425`, `429`, `5xx`, timeout, and async transport error.
12. Immediate permanent attempt and dead letter.
13. Transient, with sanitized error codes.
14. No bounded trusted policy has been designed for it.
15. Attempt ID/generation/token plus matching delivery state/token/unexpired
    expiry.
16. PostgreSQL.
17. A healthy bounded request needs time to finalize before another owner can
    recover it.
18. Delayed NAK for the remaining lease.
19. Mark old attempt abandoned, spend budget, then schedule or dead-letter.
20. The receiver side effect is unknown and crash loops must be bounded.
21. Exact attempt, generation, token, delivering state, and unexpired lease.
22. A remote side effect cannot be rolled back by HookRelay's database fence.
23. Success, already-success, dead-letter, and stale generation.
24. Scheduled retry and active lease; policy block uses fixed delayed NAK only
    if terminal persistence was not reached.
25. The allowlist check happens before an attempt/HTTP call; it normally commits
    dead letter reason `target_blocked` before ACK.
26. Broker deliveries are not business attempts.
27. `permanent_failure`, `attempts_exhausted`, and `target_blocked`.
28. Tenant bearer auth, JSON `POST`, `202`, and observed
    `expected_dispatch_generation`; opaque `404`, state/generation `409`.
29. Old message was ACKed and old UUID may still be broker-deduplicated.
30. On delivery, attempt, and outbox rows; after `message_id` and all envelope
    IDs reconcile, the worker reads the exact outbox row's generation.
31. It is ACKed as stale without HTTP.
32. No; it increments generation and retains lifetime attempt rows.
33. New dispatch intent committed; no publish, attempt, or receiver success.
34. Stage 3 cannot represent active/scheduled/terminal Stage 4 state,
    generations other than 1, or attempt duration beyond its old 32-bit range;
    downgrade refuses instead of discarding, remapping, or truncating evidence.
35. Complete SSRF/DNS/IP/egress, rate limiting, circuit breaking, size policy,
    rotation, and production enablement.
36. The latter starts and forcibly terminates a separate OS process/container;
    direct state construction or task cancellation does not.
37. Old Stage 3 workers parse the compatible schema-v1 message but lack
    generation fencing and could execute one stale request.

</details>

### Three-to-five-minute teach-back

Use a blank page and this timing:

1. **0:00-0:35 — Boundary:** Stage 3 raw redelivery/stuck attempt versus Stage
   4 persistent recovery; at least once remains.
2. **0:35-1:15 — Policy:** classification, five-attempt generation budget,
   capped exponential ceiling, and downward jitter.
3. **1:15-2:00 — Scheduling:** attempt completion plus due time commit, delayed
   NAK afterward, early redelivery recheck, and why `MaxDeliver` is unlimited.
4. **2:00-2:50 — Crash:** attempt/delivery token, database-time expiry,
   abandonment, budget, and stale-finalizer fencing.
5. **2:50-3:35 — Terminal/replay:** three dead-letter reasons,
   ACK-after-commit, expected-generation precondition, and fresh outbox UUID.
6. **3:35-4:20 — Compatibility/ambiguity:** unchanged schema-v1 payload,
   generation from the outbox row, worker cutover, receiver uncertainty, and
   stable event-ID idempotency.
7. **4:20-5:00 — Evidence/boundaries:** name unit, database, broker, HTTP, and
   process-kill evidence separately; Stage 5 security and Stage 7 scale remain.

### Teach-back checklist

- [ ] I can draw all five delivery states and every allowed transition.
- [ ] I can write the exact exponential/jitter formula and default bounds.
- [ ] I can reproduce the complete HTTP classification table.
- [ ] I can distinguish broker redelivery, lifetime attempt, and generation
      attempt count.
- [ ] I can explain why PostgreSQL owns the schedule and JetStream supplies the
      wake-up.
- [ ] I can explain why retry state commits before delayed NAK.
- [ ] I can explain why `MaxDeliver=-1` is compatible with max attempts five.
- [ ] I can name every attempt-lease/fencing check.
- [ ] I can explain why abandoned work counts and may have duplicated remotely.
- [ ] I can distinguish ACK, delayed NAK, TERM, and stale-owner no-disposition.
- [ ] I can state all three dead-letter reasons.
- [ ] I can trace manual replay's row lock, generation, fresh outbox, commit,
      and `202`.
- [ ] I can explain why schema v1 stays unchanged and generation lives on the
      reconciled PostgreSQL rows.
- [ ] I can explain the mixed-worker replay cutover caveat.
- [ ] I can state why old-generation messages ACK without HTTP.
- [ ] I can explain why replay preserves history and resets only policy budget.
- [ ] I can distinguish fixture-driven recovery from a hard process-kill test.
- [ ] I can name the Stage 5, 6, and 7 boundaries without claiming them now.
- [ ] I can explain why Stage 4 is still at least once.
