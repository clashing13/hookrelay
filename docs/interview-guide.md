# HookRelay interview guide

This guide turns the implemented system into precise interview explanations.
Lead with an invariant and evidence, then state the boundary. Avoid reciting a
tool list or claiming roadmap behavior as complete.

## Thirty-second Stage 5 pitch

> HookRelay `0.5.0` accepts tenant-authenticated, byte-bounded events
> idempotently and commits dispatch intent in PostgreSQL before publishing an
> ID-only command through NATS JetStream. PostgreSQL owns retry timing, attempt
> leases, per-endpoint fixed-window admission, circuit state, and a fenced
> single recovery probe. Endpoint creation rejects unsafe destinations, while
> each outbound connection re-resolves, validates, pins, and verifies its peer.
> Signing-secret rotation preserves delivery snapshots. The system remains at
> least once, and its application controls do not replace production egress,
> quota, capacity, or key-management controls.

## Ninety-second architecture answer

1. Stage 1 created the FastAPI lifecycle, validated configuration, separate
   liveness/readiness probes, process-scoped async engine, explicit Alembic
   boundary, Compose topology, and layered tests.
2. A bounded ASGI layer counts actual request-body bytes before routing,
   authentication, or JSON parsing; event ingestion also caps the compact
   validated payload.
3. A deployment-only bootstrap route creates a tenant and one raw API key. The
   raw key is returned once; only its public ID, SHA-256 secret digest, and hint
   are stored.
4. Tenant routes authenticate bearer keys and derive the tenant internally.
   Callers never choose a tenant ID.
5. Endpoint creation validates the URL and, for non-exempt public destinations,
   every resolved address before persistence. It generates an
   AES-256-GCM-encrypted signing secret and returns it once. Optimistically
   locked rotation retires that version and returns a replacement once;
   existing deliveries retain their secret-row snapshots.
6. Event submission requires an idempotency header. HookRelay hashes a
   canonical versioned request and uses a tenant/key unique constraint plus
   PostgreSQL `ON CONFLICT` to make retries safe across concurrent API replicas.
7. The successful transaction creates one event, one pending delivery per
   endpoint, and one generation-1 outbox row per delivery. Only after commit
   does the route return `201`.
8. A separate publisher leases eligible outbox rows in short PostgreSQL
   transactions, publishes ID-only commands to a file-backed JetStream stream,
   waits for PubAck, then conditionally records `published_at`.
9. Workers share a durable pull consumer and bound concurrency with a fetch
   window, semaphore, HTTP pool, and broker `MaxAckPending`.
10. A worker reconciles the exact outbox row and locks the endpoint's traffic
    row. Rate denial, an open circuit, or another half-open probe schedules a
    database-time deferral without creating an attempt or spending retry budget.
11. Admitted work claims an attempt with a token and expiry. Each new outbound
    connection re-resolves all addresses, rejects unsafe answers, connects to a
    validated numeric IP, preserves the original host for HTTP/TLS, and verifies
    the peer; redirects, environment proxies, and Unix sockets are disabled.
12. Success commits before ACK. Transient failure commits a capped
    exponential/jittered due time before delayed NAK; permanent, exhausted, or
    policy-blocked work commits a dead letter before ACK.
13. An expired worker claim becomes an abandoned attempt and is scheduled or
    dead-lettered. Token/generation/expiry checks fence late finalizers.
14. Authenticated manual replay increments generation and creates a fresh
    outbox UUID in one transaction. The unchanged schema-v1 broker envelope
    remains ID-only; old-generation fencing comes from the reconciled outbox
    row. The guarantee remains at least once.
15. HookRelay's own containers run non-root with read-only filesystems, dropped
    capabilities, `no-new-privileges`, PID ceilings, and restricted `/tmp`; the
    PostgreSQL and NATS images retain service-specific profiles.

## Whiteboard trace

Draw the current boundary and keep external production controls outside it:

```text
Producer
  -> ASGI: count actual body bytes -> 413 if over configured maximum
  -> FastAPI: validate JSON + headers + compact event payload size
  -> API-key lookup/digest verification -> tenant context
  -> canonical request fingerprint
  -> tenant-scoped endpoint/secret lookup
  -> PostgreSQL transaction
       event
       + N pending delivery snapshots
       + N unpublished outbox rows
  -> commit
  -> 201 Created
  -> outbox lease -> JetStream PubAck -> published_at
  -> durable pull consumer -> bounded worker
  -> PostgreSQL endpoint traffic row
       -> rate/circuit/probe deferral -> due time + delayed NAK, no attempt
  -> attempt + delivering claim/lease commit
  -> DNS validate -> numeric-IP connect -> peer verify
  -> timestamped exact-byte HMAC HTTP (original Host + TLS identity)
       -> 2xx -> success commit -> ACK
       -> transient -> retry_scheduled + due time commit -> delayed NAK
       -> permanent/exhausted -> dead_lettered + reason commit -> ACK
  -> expired attempt lease -> abandoned -> retry/dead letter

Operator -> POST /v1/deliveries/{id}/replay
         -> generation + 1 + fresh outbox UUID, one transaction -> 202

Operator -> POST /v1/endpoints/{id}/signing-secret/rotate
         -> expected active version -> retire old + create new -> 200

Ambiguity: PubAck before outbox finalization; receiver action before
HookRelay success certainty. Stable IDs + idempotency, not exactly once.
Residual boundary: application SSRF checks still require external egress policy.
```

## Stage 1 foundation questions

### Why separate liveness and readiness?

- Liveness asks whether the process/event loop can answer and touches no
  external dependency.
- Readiness executes a timeout-bounded real PostgreSQL `SELECT 1`.
- If liveness depended on PostgreSQL, a database outage could trigger mass API
  restarts that do not repair the database and may add a connection storm.
- Readiness returns sanitized `503` during the outage and rechecks on every
  request, so it can recover without process restart.

### Why one engine but one session per request?

- The async engine and pool are process-scoped reusable infrastructure.
- A session contains mutable identity-map, transaction, pending-write, commit,
  and rollback state.
- A global session could interleave unrelated requests and let one request
  commit or roll back another's work.
- Each domain request therefore receives a short-lived session and returns the
  borrowed connection promptly.

### Why PostgreSQL rather than SQLite or MongoDB?

- The current design relies on concurrent multi-process transactions,
  `ON CONFLICT`, JSONB, partial indexes, relational constraints, composite
  tenant foreign keys, and a transactional outbox.
- SQLite is excellent for embedded or small local workloads, but it would not
  exercise the production concurrency, locking, type, and SQL behavior.
- MongoDB supports transactions in suitable deployments, but this domain is
  strongly relational and no document-model requirement offsets a second set
  of consistency/query tradeoffs.

### Why Alembic rather than `metadata.create_all()`?

- ORM models describe what code expects now; a live database contains history
  and data.
- `create_all()` can create missing objects but does not express an ordered
  rename, backfill, staged constraint, or reviewed transition.
- Stage 2 has an explicit first domain revision; `alembic check` also looks for
  model/migration drift.
- Migrations run before traffic as a release step, not concurrently inside
  every API replica's startup.

### What does async buy, and what does it not buy?

- Async drivers yield the event loop while network I/O waits, allowing other
  requests to progress without a thread per wait.
- `async def` is not parallel execution and does not make blocking code safe.
- Database pools and background-worker concurrency still need explicit bounds.

## Stage 2 design questions

### What does `201 Created` mean for an event?

- PostgreSQL committed the logical event, all requested delivery snapshots, and
  one unpublished outbox message per delivery.
- The response is emitted only after commit.
- It does not mean an outbox message was published, a worker ran, an HTTP
  request was made, or the destination acknowledged anything.
- A matching replay is also `201`, with the original representation and an
  explicit replay header.

### Why return `201` again on an idempotent replay?

- The route reproduces the result of the original create operation rather than
  exposing a second resource or switching response schema.
- Stable status/body/`Location` make lost-response retries simple for clients.
- `Idempotency-Replayed: true` provides observability without changing the
  representation.
- Other APIs sometimes choose `200`; the important part is a documented,
  tested contract. HookRelay's exact choice is `201`.

### How is idempotency defined?

- Scope is `(authenticated tenant, Idempotency-Key)`.
- The logical request includes operation, type, payload, and endpoint set under
  a versioned canonicalization rule.
- JSON object-key order and endpoint-list order do not change the fingerprint;
  payload array order does.
- Same key and same fingerprint returns original event/delivery IDs.
- Same key and different fingerprint returns
  `409 idempotency_key_reused` and creates nothing.

### Why store a request fingerprint instead of only the key?

- A key alone cannot distinguish a safe network retry from a caller bug that
  accidentally reuses the key for different work.
- Silently returning the old event for changed input would make the producer
  believe new work was accepted when it was not.
- The versioned SHA-256 fingerprint makes comparison compact and lets the
  canonicalization contract evolve deliberately.

### What closes the concurrent idempotency race?

- The preliminary lookup is not enough: two transactions can both observe no
  row.
- PostgreSQL uniquely constrains `(tenant_id, idempotency_key)` across all
  processes.
- `INSERT ... ON CONFLICT DO NOTHING RETURNING` elects one winner.
- A loser loads the committed winner and compares fingerprints, producing a
  replay or `409`.
- Correctness therefore lives at the shared serialization point, not in a
  process-local lock or Python conditional.

### Why a transactional outbox?

- Committing PostgreSQL and publishing directly to a broker are two independent
  writes.
- Database-first can crash before publish and lose dispatch; broker-first can
  expose work whose database transaction rolls back.
- Writing an outbox row in the same PostgreSQL transaction makes accepted
  domain work and publish intent atomic.
- A later publisher still may publish twice if it crashes after broker success
  but before recording `published_at`, so consumers must remain idempotent.
- Stage 2 has no publisher or broker; outbox rows remain unpublished.

### Why one outbox row per delivery rather than one per event?

- One event can target several independently deliverable endpoints.
- Per-delivery messages give the Stage 3 worker one scheduling/retry identity per
  destination.
- The unique `(delivery_id, topic)` constraint prevents duplicate dispatch facts
  for the current topic.
- The versioned ID-only payload avoids putting event bodies or signing secrets
  on the broker.

### Why snapshot URL and signing-secret version on the delivery?

- Endpoint configuration can change after an event is accepted.
- Historical work should preserve the destination and credential version that
  were selected at acceptance time.
- A foreign key to the versioned secret keeps the encrypted material in one
  place while preventing retirement/deletion from invalidating accepted work.
- Snapshotting all secret plaintext into each row would enlarge exposure and
  duplicate sensitive material.

### Why hash API keys but encrypt endpoint signing secrets?

- An inbound API key needs only one-way verification, so storing a digest
  avoids retaining recoverable raw credentials.
- The API-key secret has 256 random bits, so SHA-256 digest storage is not
  relying on low-entropy password hashing.
- The Stage 3 worker must recover an endpoint signing secret to compute an
  HMAC, so one-way hashing would make delivery impossible.
- AES-256-GCM supplies confidentiality and integrity; associated data binds the
  ciphertext to its tenant/endpoint/secret/version context.
- The encryption key remains a production secret-management responsibility.

### How is tenant isolation enforced?

- Authentication derives `tenant_id` from the verified API key; callers do not
  submit it.
- Every resource query includes the derived tenant scope.
- Missing and cross-tenant resources share an opaque `404` response.
- Composite foreign keys repeat tenant identity across relationships so the
  database rejects a cross-tenant event/API-key, delivery/endpoint, or
  delivery/secret association.
- The Stage 5 endpoint traffic row is likewise keyed and constrained by
  `(tenant_id, endpoint_id)`.
- This is application scope plus relational integrity, not a claim that the
  current schema uses PostgreSQL row-level security.
- Application checks and database constraints are defense in depth, not
  interchangeable layers.

### Why return endpoint signing secrets only once?

- Repeated plaintext reads expand the number of places and requests that can
  leak the credential.
- Creation returns it under no-store headers so the client can provision its
  receiver.
- Later reads return only non-secret metadata.
- HookRelay still stores encrypted ciphertext for future HMAC computation; the
  phrase “only once” applies to the public API response, not internal
  recoverability.

### Why use stable Problem Details errors?

- Clients need machine-readable failure categories without parsing prose.
- RFC-style media type and fields are conventional; a stable `code` expresses
  HookRelay-specific meaning.
- Validation pointers identify the field while omitting submitted values.
- Authentication and cross-tenant errors deliberately avoid details that could
  become enumeration or secret oracles.

### Why create a delivery-attempt table before attempts exist?

- It records the intended separation between a logical delivery and each
  future execution try, avoiding a later overloaded mutable row.
- The schema establishes constraints and audit vocabulary that Stage 3/4 can
  use.
- It does not justify claiming attempts exist: Stage 2 creates zero rows and no
  delivery leaves `pending`.

### What failure causes the whole ingestion transaction to roll back?

- Any database failure while inserting the event, a delivery, or an outbox row
  invalidates the acceptance invariant.
- The route rolls the request session back and returns sanitized `503`.
- The producer can retry with the same idempotency key because no `201` was
  promised before commit.
- Targeted rollback tests should force failure inside the transaction and prove
  every table count remains unchanged.

## Stage 3 delivery-pipeline questions

### Why does the API not publish directly to NATS?

- PostgreSQL and NATS cannot share the API's local transaction.
- Database-first publishing has a crash gap; broker-first can expose rolled-back
  work; awaiting both couples API availability to the broker.
- Event, deliveries, and outbox intent commit together. A separate publisher
  can resume from PostgreSQL after NATS recovers.
- Publish/finalize can still duplicate, so the answer is at least once, not
  exactly once.

### How do multiple publishers coordinate?

- They select eligible unpublished rows in stable order with a bounded limit
  and `FOR UPDATE SKIP LOCKED`.
- A short transaction writes a random claim token and database-time expiry.
- Broker I/O happens after commit, so no row lock or connection is held across
  NATS latency.
- Claim TTL must exceed `batch_size * publish_timeout`, covering aggregate
  broker publish waits; database finalization/loop overhead still need margin,
  and cooperative shutdown releases the unprocessed remainder.
- Finalization requires the same token; a dead publisher's lease eventually
  expires.
- A crash after PubAck can still cause republishing.

### Why require a JetStream PubAck before `published_at`?

- A successful client call without server persistence evidence is not a safe
  handoff.
- `published_at` tells future scans to stop; setting it early could lose work.
- If PubAck is ambiguous, leaving the row unpublished risks a duplicate rather
  than silent loss.
- `Nats-Msg-Id` uses the outbox UUID to reduce quick duplicates, but only inside
  the broker's configured window.

### Why NATS JetStream instead of Kafka, Redis Streams, or Celery?

- HookRelay needs durable server acknowledgments, a shared cursor, explicit
  consumer ACKs, and pull-based work-queue flow control.
- JetStream provides those with a small local topology.
- Kafka's partitioned retained-log/consumer-group model adds operational
  concepts not yet justified by measurements.
- Redis Streams would introduce Redis-specific pending/claim/durability
  operations; Celery would hide the retry/ACK state machine this project is
  meant to expose.
- This is a scope choice, not a universal performance claim.

### Why keep the broker payload ID-only?

- PostgreSQL is authoritative for payload, URL, delivery state, and encrypted
  secret.
- ID-only messages avoid duplicating sensitive or stale data in the broker.
- The worker must reconcile every identity against PostgreSQL before executing.
- The cost is one authoritative database load for each attempt.

### How is worker concurrency bounded?

- Pull only one configured local window.
- Use an `asyncio.Semaphore` even if an internal caller presents more messages.
- Align the long-lived HTTP client's connection pool with worker concurrency.
- Require consumer `MaxAckPending` to be at least local concurrency and treat
  it as a global broker-side budget.
- Async overlaps I/O waits; it is not CPU parallelism or infinite capacity.

### What is the exact signature contract?

- Serialize a version-1 envelope to compact, sorted-key UTF-8 JSON.
- Use the original event timestamp inside the body and current Unix seconds in
  `HookRelay-Timestamp`.
- HMAC-SHA256 signs `ASCII(timestamp) + b"." + exact_body_bytes`.
- Send the same bytes and `HookRelay-Signature: v1=<lowercase hex>`.
- HMAC authenticates integrity/secret possession; a receiver separately needs
  timestamp freshness and stable-event-ID idempotency.

### Why insert an attempt before HTTP and ACK after success commit?

- Attempt-before-HTTP creates an audit/ownership record before an external side
  effect.
- No database transaction is held during network waiting.
- A second transaction finishes the attempt and delivery together.
- ACK-before-commit could remove the only work item while durable state still
  says incomplete.
- Commit-before-ACK can redeliver, but `succeeded` lets the worker skip a second
  HTTP request and ACK safely.

### What failure boundary did Stage 3 intentionally leave?

- Timeout, transport error, or non-2xx recorded a transient attempt and relied
  on raw `AckWait` redelivery.
- It had no persistent due time, backoff/jitter, classification, maximum, or
  terminal transition.
- A crash after attempt commit could leave `delivering` indefinitely.
- Stage 4 replaces that historical boundary; do not describe the Stage 3
  behavior as current Stage 4 or Stage 5 behavior.

## Stage 4 failure-recovery questions

### Why is PostgreSQL authoritative for retry timing?

- Attempt outcome, status, and exact due time commit atomically.
- The schedule survives worker restart or a lost NAK.
- Every redelivery locks and rechecks due state; an early wake-up cannot execute.
- JetStream remains the durable transport and delayed wake-up, not a second
  domain database.

### What is the exact retry formula?

- For current-generation attempt `n`, calculate
  `min(max, base * 2^(n-1))`.
- Draw uniformly between `(1-ratio) * ceiling` and `ceiling`, with a 100 ms
  floor.
- Defaults are base 1 second, cap 60 seconds, and 25% downward jitter.
- Injecting the random source makes edge tests deterministic.

### Why is `MaxDeliver=-1` while max attempts is five?

- JetStream counts every presentation: early due-time wake-up, active-lease
  deferral, lost ACK, and actual attempt.
- PostgreSQL counts only actual or ambiguous abandoned attempts in the current
  dispatch generation.
- A broker limit of five could exhaust work without five HTTP attempts.

### How are receiver outcomes classified?

- `2xx` succeeds.
- Timeout, async transport error, `408`, `425`, `429`, and `5xx` are transient.
- Other HTTP statuses are permanent and dead-letter immediately.
- This is explicit global policy, not a claim that every customer API uses the
  same semantics.

### How does attempt-lease recovery work?

- Attempt and delivery share a random claim token; the delivery stores an
  expiry from PostgreSQL `clock_timestamp()`.
- Claim TTL must exceed HTTP timeout plus the explicit finalization margin so a
  healthy response has time for its second transaction.
- Before expiry, redelivery receives a delayed NAK rather than another request.
- After expiry, the old attempt becomes `abandoned` with
  `worker_lease_expired` and spends generation budget.
- Exact attempt/generation/token/state/expiry checks fence a late finalizer.
- The fence protects HookRelay state, not a remote side effect already made.

### What exactly is dead-lettered?

- The existing delivery enters a terminal PostgreSQL state.
- It records `dead_lettered_at` and reason `permanent_failure`,
  `attempts_exhausted`, or `target_blocked`.
- Its broker message is ACKed after that transaction commits.
- There is no separate dead-letter stream/queue in Stage 4.

### How does manual replay work?

- `POST /v1/deliveries/{id}/replay` requires the tenant API key, JSON content
  type, body `{"expected_dispatch_generation": N}`, and current
  `dead_lettered` state.
- Under a row lock, it increments database dispatch generation, clears terminal
  state, sets `pending`, and inserts a fresh outbox UUID in one transaction.
- It returns `202` plus the event `Location`; publication/delivery is not
  implied.
- Missing/cross-tenant IDs share opaque `404`; a stale expected generation is
  `409 delivery_generation_conflict`; another state is
  `409 delivery_not_replayable`.
- Lifetime attempt history remains; the generation receives a fresh bounded
  budget.

### Why does the broker envelope stay schema v1?

- Generation is authoritative on the exact outbox row found by `message_id`,
  not duplicated into the broker payload.
- Keeping the strict seven-field envelope avoids old Stage 3 workers TERMing a
  replay message as unknown schema/data.
- Current workers derive generation after complete outbox reconciliation and
  ACK messages from an older row generation as stale.
- During deployment, finish worker cutover before manual replay: an old worker
  can parse the compatible envelope but lacks generation fencing and may send an
  extra stale request.

### Which messages terminate, ACK, or remain recoverable?

- Malformed or authoritative-state-contradictory commands TERM as poison.
- Success, already-success, dead-letter, and stale generation ACK after durable
  state.
- Scheduled retry and active lease use delayed NAK.
- Stage 5 URL-policy rejection commits `target_blocked` before an attempt;
  connection-time DNS or peer rejection records the already-claimed attempt.
  Both ACK only after terminal state commits.
- A stale worker that loses its claim performs no broker action from its old
  handle.

## Stage 5 security and traffic-control questions

### How does destination validation address DNS rebinding?

- Endpoint creation accepts only HTTP(S), rejects credentials/fragments, and
  requires HTTPS in staging/production. Public hostnames must resolve, and
  every A/AAAA answer must be safe before the endpoint is persisted.
- That lookup is not reused as authorization. For every new connection the
  worker resolves again with a bounded timeout and answer count, rejects any
  unsafe or mixed answer set, and chooses one validated numeric IP.
- It connects to that IP, preserves the original hostname for `Host` and TLS
  SNI/certificate checks, and verifies that the actual peer is the selected IP.
- Redirects and environment proxies are disabled, connection reuse is disabled,
  and Unix-domain transports are forbidden, so those paths cannot bypass the
  per-connection decision.

### Which addresses are rejected, and what is still outside the guarantee?

- Non-global, private, loopback, link-local, multicast, reserved, unspecified,
  mapped-IPv6, metadata, deprecated IPv4 transition `192.88.99.0/24`, NAT64,
  Teredo, ORCHIDv2, and 6to4 destinations are rejected.
- Exact `delivery_allowed_hosts` exemptions support controlled local/test
  receivers. They skip creation-time DNS preflight but still resolve and pin at
  connection time; staging/production require the exemption list to be empty.
- These are application-layer controls. Production must still restrict DNS and
  network egress with firewall/orchestrator policy and monitor that boundary.

### Why store rate and circuit state in PostgreSQL?

- A process-local counter or breaker would multiply with every worker replica
  and forget state on restart.
- Each endpoint has one `(tenant_id, endpoint_id)` traffic-control row. A row
  lock plus a fresh PostgreSQL time read after any lock wait serializes the
  fixed window, circuit transition, and recovery-probe lease across workers.
- The endpoint creation transaction and Stage 5 migration both ensure the row
  exists; composite ownership keeps it inside the tenant boundary.
- This design chooses shared correctness and inspectability over the latency of
  a purely in-memory limiter.

### When does a traffic-control deferral spend an attempt?

- Never. A full fixed window, an open circuit still in cooldown, or a competing
  half-open probe moves the delivery to `retry_scheduled` with a database due
  time before any attempt row, retry budget, or outbound socket exists.
- The worker then sends a delayed NAK. PostgreSQL remains authoritative if that
  wake-up is early, late, or lost.
- The default fixed window is 10 admitted requests per endpoint per one second;
  it is intentionally a simple burst control, not a tenant quota or proof of
  production capacity.

### How is exactly one half-open recovery probe enforced?

- After the default 30-second cooldown, the locked traffic row grants one
  probe token/expiry fenced to the delivery's claim. Competing workers observe
  the probe and defer without an attempt.
- The probe consumes a normal rate-limit slot. Success closes and resets the
  circuit; a transient failure or abandoned probe reopens it for another full
  cooldown. Permanent or policy-blocked outcomes close/reset it because retrying
  that same destination condition is not the circuit's job.
- The default transient-failure threshold is five. The circuit reduces futile
  traffic; it does not promise endpoint health or eliminate all concurrency.

### How is signing-secret rotation snapshot-safe?

- `POST /v1/endpoints/{id}/signing-secret/rotate` takes
  `{"expected_active_version": N}` and locks the tenant-owned endpoint and
  active secret.
- One request atomically retires the old row using database time and inserts
  version `N + 1`; it returns the replacement plaintext once with `200` and
  `Cache-Control: no-store` plus `Pragma: no-cache`.
- A stale writer receives `409 signing_secret_version_conflict` and
  `HookRelay-Active-Secret-Version`; missing and cross-tenant IDs share opaque
  `404` behavior.
- Old deliveries keep their signing-secret row ID, while events accepted after
  rotation snapshot the new row. Retention is what preserves retry and replay
  correctness.

### Does endpoint-secret rotation rotate the encryption master key?

- No. Endpoint rotation creates a new encrypted endpoint secret under the
  configured AES master key/version.
- The current runtime loads only one master key. Replacing it without first
  rewrapping retained rows, or without a keyring capable of old versions, makes
  those delivery snapshots undecryptable.
- Key custody, rotation, audit, backup, and restore remain deployment concerns.

### Where are request and payload sizes enforced?

- ASGI middleware counts actual streamed body bytes, treating
  `Content-Length` only as an early hint. A single all-digit value is compared
  without parsing an unbounded integer; a declared overage receives immediate
  `413`, while duplicate/malformed hints still fall through to streamed-byte
  counting. More than the default 1 MiB receives `413` before routing,
  authentication, or JSON parsing.
- Event ingestion then measures compact, sorted-key UTF-8 bytes after Pydantic
  validation and rejects a payload above the default 256 KiB with `413`.
- The settings are bounded and cross-validated, but neither limit is a tenant
  storage quota or a substitute for an ingress limit.

### What does least privilege mean for the Compose application containers?

- API, publisher, worker, and receiver run as UID/GID `10001:10001` with
  root-owned code and migrations, a read-only root filesystem, all capabilities
  dropped, `no-new-privileges`, a 256-PID ceiling, and a 16 MiB `/tmp` tmpfs
  mounted `noexec,nosuid,nodev`.
- PostgreSQL and NATS keep image-appropriate permissions instead of inheriting
  an untested generic profile. Loopback-published ports still do not replace
  deployment network policy.

Primary references: the
[Stage 5 guide](stages/05-security-traffic-control.md),
[ADR 0014](decisions/0014-resolved-address-ssrf-policy.md),
[ADR 0015](decisions/0015-database-authoritative-endpoint-traffic-controls.md),
[ADR 0016](decisions/0016-request-byte-limits-before-parsing.md),
[ADR 0017](decisions/0017-versioned-signing-secret-rotation.md), and
[ADR 0018](decisions/0018-least-privilege-app-containers.md).

## Security and limitation questions

### Does requiring `HttpUrl` or HTTPS solve SSRF?

- No. Syntax and TLS-scheme checks alone say nothing about where DNS resolves or
  which peer receives the connection.
- Stage 5 additionally rejects unsafe literal/resolved IPs at creation,
  re-resolves and validates all answers on every new connection, pins a chosen
  numeric IP, checks the connected peer, preserves hostname TLS validation, and
  disables redirects, environment proxies, connection reuse, and Unix sockets.
- Exact private-host exemptions are local/test only; staging/production require
  the exemption list to be empty.
- Application validation still cannot replace external DNS and egress policy,
  so do not describe it as complete infrastructure isolation.

### Is producer traffic protected by TLS?

- Not by the application itself. Uvicorn/Compose serve HTTP and local Compose
  binds only to loopback.
- A deployed environment needs TLS termination at a trusted ingress/proxy and a
  protected hop to the app.
- Requiring `https://` for destination URLs protects neither API keys nor event
  payloads on the producer-to-HookRelay connection.

### Is request size bounded?

- Yes. Actual streamed request bodies are capped at 1 MiB by default before
  routing/parsing, and compact validated event payloads are capped at 256 KiB.
  Both oversize paths return `413`.
- Individual names, types, URLs, endpoint count, key grammar, and top-level body
  shapes remain separately bounded.
- There is still no tenant storage quota or production capacity claim; an
  external ingress should enforce its own limit too.

### Does encryption at rest make secret storage complete?

- No. It reduces exposure if the database alone is copied.
- The key must be stored separately with access control, rotation, audit,
  backup, restore, and incident-recovery procedures.
- The checked-in local key is intentionally not a production key, and settings
  reject it in staging/production.
- Endpoint signing-secret rotation does not rotate that master key. The current
  one-key runtime needs a deliberate rewrap/keyring workflow before an old key
  can be retired without breaking retained delivery snapshots.
- Memory, logs, client handling, and authorized application access remain part
  of the threat model.

## Test-evidence questions

### How do you know Stage 5 works?

Name the evidence and the boundary it crosses:

- `tests/unit/test_stage5_security.py` covers address classification, bounded
  all-answer DNS policy, numeric-IP connection pinning, original Host/SNI,
  peer verification, no connection reuse, and safe error mapping.
- `tests/unit/test_stage5_traffic_control.py` drives deterministic fixed-window,
  circuit, probe-lease, expiry, and outcome transitions.
- `tests/api/test_stage5_security.py` exercises actual-stream and canonical
  payload caps, secret-rotation HTTP contracts, cross-tenant opacity, and
  blocked literal endpoint creation.
- `tests/integration/test_stage5_security.py` uses real PostgreSQL for
  traffic-row creation, immutable secret snapshots, concurrent rotation, and
  cross-tenant non-mutation.
- `tests/integration/test_stage5_traffic_control.py` uses concurrent workers and
  real PostgreSQL to show one shared fixed-window limit and exactly one leased
  recovery probe.
- Migration `20260805_0004` creates and backfills traffic rows and adds
  `is_circuit_probe` attempt evidence; migration tests and `alembic check` cover
  schema/model alignment. Compose/image checks cover the declared
  non-root/read-only application profile.

Then state the limit: constructed DNS/network doubles establish deterministic
policy behavior but are not an external egress audit; one PostgreSQL concurrency
schedule is not a throughput, fairness, HA, or capacity benchmark; container
configuration checks do not prove host isolation; and the system remains at
least once.

### How do you know Stage 4 works?

Name evidence by boundary:

- Pure policy tests: exact exponential/jitter edges, status classification,
  strict unchanged schema-v1 envelope, settings, and ACK/NAK dispositions.
- PostgreSQL integration: schedule/due constraints, attempt claims, expired
  abandonment, stale-token fencing, fresh database time after a row-lock wait,
  generation-specific counting, and terminal reasons.
- Real service paths: transient failure can schedule and later succeed;
  permanent and exhausted work dead-letter; success state suppresses lost-ACK
  duplicate HTTP.
- Replay API/database: tenant isolation, expected-generation precondition,
  concurrency, new outbox UUID/generation, preserved history, and stale-row
  suppression.
- Process recovery: a separate worker is forcibly killed after receiver
  capture; a replacement abandons the expired claim, retries, and produces a
  second identical capture for that encoded schedule.
- Migrations/packaging: upgrade/backfill, guarded downgrade, `alembic check`,
  Compose validation, and Linux image build.

The Stage 4 integration file contributes twelve recovery scenarios, and the
complete checkpoint suite passed all 143 tests. Then state the limit: a real
subprocess kill is stronger than fixture-driven expiry, but one schedule still
does not prove every crash point, receiver exactly once, HA, hostile-network
safety, or production capacity.

### How do you know Stage 3 works?

Name evidence by boundary:

- Unit vectors: strict ID-only broker envelope, exact body bytes, fixed HMAC,
  topology configuration, timeout settings, and semaphore ceiling.
- PostgreSQL integration: claim ownership/expiry, conditional
  publish-finalization, attempt-before-HTTP, and attempt/delivery success state.
- Real JetStream: file-backed stream, durable consumer, PubAck, pull, and ACK.
- End to end: API acceptance through publisher, broker, worker, real HTTP
  capture, independent HMAC verification, and final database state.
- Migrations/packaging: upgrade/downgrade/re-upgrade, `alembic check`, Compose
  validation, and Linux image build.

Then state its historical limit: Stage 3 evidence did not prove the later Stage
4 retry/recovery policy, the Stage 5 address/traffic controls, exactly once,
clustered HA, production egress isolation, or production throughput.

### How do you know Stage 2 works?

Name evidence by boundary:

- Unit tests: configuration, canonical fingerprints, key parsing/digest checks,
  and authenticated-encryption behavior.
- In-process API tests: exact `201`/header/body contracts, problem media type,
  content type, validation, credential rejection, and secret redaction.
- Real PostgreSQL integration tests: migration, composite constraints,
  transaction rollback, endpoint/event persistence, same-request replay,
  different-request conflict, tenant isolation, and concurrent duplicates.
- Alembic checks: revision application and model/schema alignment.
- Compose/Docker checks: local topology and packaged Linux artifact.

Then state the limit: even all of them together do not prove production load,
long-lived reliability, hostile-input resilience, TLS/proxy correctness, SSRF
defense, broker behavior, or receiver compatibility.

### Why not use SQLite for the integration suite?

- It would bypass exactly the PostgreSQL behaviors under test: JSONB, partial
  indexes, regex/check constraints, `ON CONFLICT`, transaction visibility, and
  concurrent uniqueness.
- Fast substitute tests can complement the suite but cannot be the evidence for
  a PostgreSQL-specific correctness claim.

### What is a valuable safe failure demonstration?

- Try to create an endpoint for `http://169.254.169.254/` and observe a
  `422 destination_not_allowed` with no endpoint or secret row persisted.
- Send more requests than the fixed-window allowance to a controlled endpoint
  and show that excess deliveries receive a durable due time without an attempt
  row; repeat with a tripped circuit and its single recovery probe.
- Configure only the local receiver to return `503` and observe a transient
  attempt plus a persistent due time before delayed NAK.
- Restore `204` and observe later success without creating unbounded work.
- Separately use `400` to show an immediate permanent dead letter, then replay
  with the observed expected generation after repairing the receiver.
- For crash ambiguity, delay the local receiver, save its first capture, kill
  the disposable worker, and observe abandoned-attempt recovery after the
  database lease expires.
- Never delete named volumes or send the exercise to a real third party.

## Recruiter-oriented story prompts

### “What was the most important design decision?”

Use the transaction/outbox invariant:

- State the dual-write failure window.
- Draw event + deliveries + outbox inside one PostgreSQL commit.
- Explain why NATS is intentionally outside Stage 2.
- Admit duplicate publication remains possible later.
- Point to rollback/integration evidence rather than saying “transactions are
  reliable.”

An equally strong alternative is database-enforced idempotency: explain why a
process-local precheck loses under concurrency and how the unique constraint
elects one winner.

### “Tell me about a security tradeoff.”

Contrast credential storage:

- hash API-key secrets because they need verification only;
- encrypt signing secrets because later HMAC generation needs recovery;
- bind AES-GCM ciphertext to row context with AAD;
- return both raw values only once;
- rotate endpoint-secret rows without rewriting accepted delivery snapshots;
- distinguish that operation from unsupported one-key master-key rotation; and
- identify key management and TLS as remaining operational dependencies.

### “Tell me about a failure you tested.”

Use one concrete loop:

- **Concurrency:** many same-key requests converge to one durable event.
- **Conflict:** changed input under the same key becomes a stable `409` without
  new rows.
- **Rollback:** injected outbox failure leaves no event or delivery residue.
- **Dependency outage:** readiness becomes `503` while liveness remains `200`
  and later recovers.
- **Broker outage:** the API still commits an outbox row while NATS is stopped;
  after restart the publisher drains it and the local receiver gets the event.
- **Ambiguous ACK:** a duplicated broker message observes `succeeded`, skips a
  second HTTP call, and ACKs from durable database state.
- **Transient destination:** `503` commits an attempt and database due time,
  then later succeeds after destination recovery.
- **Expired worker lease:** an unfinished attempt becomes `abandoned`, spends
  budget, and a stale token cannot finalize over the recovered owner.
- **Permanent/exhausted work:** a durable dead-letter reason commits before
  broker ACK.
- **Policy block:** a URL rejected before traffic admission commits
  `target_blocked` without an attempt; a connection-time DNS/peer block records
  the claimed attempt. Replay remains available after a reviewed policy fix.
- **Traffic pressure:** concurrent workers share one endpoint row, and rate or
  open-circuit deferral consumes neither attempt evidence nor retry budget.
- **Circuit recovery:** after cooldown, one worker owns the fenced probe while
  competitors defer; transient probe failure reopens the circuit.
- **Secret rotation:** concurrent expected-version requests produce one winner,
  while older deliveries continue to reference their original secret row.
- **Replay:** expected generation, tenant scope, and row locking produce one
  fresh outbox UUID without erasing earlier attempts.
- **Real process death:** kill a separate worker after receiver capture, then
  observe expired-claim abandonment and replacement-worker recovery with the
  same body.

For each, say what was observed and one thing the exercise cannot prove.

### “What would you build next?”

Stage 6 should add telemetry, history APIs, and the operations console without
weakening the tenant and secret-redaction boundaries. Stage 7 should expand the
single Stage 4 subprocess-kill schedule into a broader DNS/TLS/process fault
matrix and add load, soak, HA, fairness, and capacity evidence. Production work
also needs external egress policy, ingress/tenant quotas, and a safe master-key
keyring/rewrap lifecycle; Stage 5 intentionally does not claim those outcomes.

## Claims to avoid

- “A `201` means the webhook was delivered.”
- “Stage 2 uses NATS.”
- “Stage 2 creates delivery attempts.”
- “The transactional outbox gives exactly-once publishing.”
- “An application precheck makes idempotency concurrency-safe.”
- “JSON key order makes these two requests different.”
- “SHA-256 is how every password should be stored.”
- “Encryption means the application cannot reveal the secret.”
- “HTTPS URL validation solves SSRF.”
- “Destination HTTPS protects producer API keys.”
- “Pydantic validation imposes a request byte limit.”
- “A global SQLAlchemy session saves connections safely.”
- “Changing the ORM model migrates PostgreSQL.”
- “Async code is parallel.”
- “All tests passed, therefore the system is production scale.”
- “At least once means the receiver's side effect occurs exactly once.”
- “`Nats-Msg-Id` makes publication exactly once.”
- “A PubAck and database update are one transaction.”
- “File-backed single-node JetStream is highly available.”
- “`AckWait` is our complete retry policy.”
- “JetStream `MaxDeliver` is our HTTP attempt count.”
- “Every broker redelivery spends retry budget.”
- “Jitter means retry can exceed the cap.”
- “Every `4xx` is transient.”
- “A lease proves the receiver did not process the old request.”
- “Fencing makes worker-crash recovery exactly once.”
- “Dead letter means a separate queue.”
- “Manual replay erases failed attempts.”
- “`202` replay means the receiver got the webhook.”
- “Dispatch generation is carried in the broker payload.”
- “Keeping broker schema v1 makes mixed old/new workers fully safe for replay.”
- “A fixture with an expired lease proves OS-level worker-kill recovery.”
- “The hostname allowlist is complete SSRF protection.”
- “Application DNS/IP validation makes an external egress policy unnecessary.”
- “A fixed-window limiter is a tenant quota or production capacity guarantee.”
- “A traffic-control deferral is an HTTP attempt.”
- “Endpoint signing-secret rotation also rotates the AES master key.”
- “Non-root and read-only containers are a complete security boundary.”
- “HMAC encrypts the webhook body.”
- “A timestamp alone prevents replay.”
- “Async means the worker has unlimited concurrency.”
- “The happy-path test proves production scale.”

## Three-to-five-minute Stage 5 teach-back

Aim for this timing:

1. **0:00-0:35 — Boundary:** Stage 5 adds ingress, destination, traffic,
   rotation, and container controls while at-least-once delivery remains.
2. **0:35-1:15 — Ingress and tenant:** actual-stream 1 MiB cap, canonical
   256 KiB payload cap, bearer-derived tenant, opaque lookup behavior.
3. **1:15-2:05 — Destination:** creation preflight, all-answer classification,
   per-connection re-resolution, numeric-IP pinning, peer/Host/TLS checks, and
   why external egress policy remains required.
4. **2:05-3:00 — Traffic:** PostgreSQL fixed window and circuit state, no-attempt
   deferrals, cooldown, and exactly one fenced half-open probe.
5. **3:00-3:45 — Rotation:** expected active version, row locks, immutable
   delivery snapshots, one-time response, and the one-master-key limitation.
6. **3:45-4:25 — Runtime:** non-root/read-only app containers and why official
   database/broker images need service-specific profiles.
7. **4:25-5:00 — Evidence/boundaries:** name the five Stage 5 test files and
   migration `20260805_0004`; deny exactly-once, egress-isolation, quota, scale,
   and production-security claims.

## Historical Stage 4 teach-back

Aim for this timing:

1. **0:00-0:35 — Boundary:** Stage 3 raw redelivery/stuck attempts versus Stage
   4 persistent recovery; at least once remains.
2. **0:35-1:15 — Policy:** exact classifier, five-attempt generation budget,
   exponential ceiling, and bounded downward jitter.
3. **1:15-2:00 — Scheduling:** failure plus due-time commit, delayed NAK,
   early-wake recheck, and why broker `MaxDeliver` is unlimited.
4. **2:00-2:50 — Crash:** token/expiry, abandonment, budget, and stale-finalizer
   fencing; remote ambiguity remains.
5. **2:50-3:35 — Terminal/replay:** three dead-letter reasons,
   ACK-after-commit, expected-generation replay, and fresh outbox UUID.
6. **3:35-4:20 — Compatibility:** unchanged ID-only schema v1, generation from
   the reconciled outbox row, stale suppression, and worker-cutover caveat.
7. **4:20-5:00 — Evidence/boundaries:** distinguish unit, database, real
   service, and hard-process-kill evidence; at that checkpoint Stage 5 security
   work was still pending.

If any sentence depends on “FastAPI handles it” or “the database guarantees it”
without naming the route, transaction, constraint, or test, trace one concrete
request again before using the answer in an interview.
