# HookRelay interview guide

This guide turns the implemented system into precise interview explanations.
Lead with an invariant and evidence, then state the boundary. Avoid reciting a
tool list or claiming roadmap behavior as complete.

## Thirty-second Stage 4 pitch

> HookRelay `0.4.0` accepts events idempotently and commits dispatch intent in
> PostgreSQL before an outbox publisher sends an ID-only command through NATS
> JetStream to a bounded signed-webhook worker. PostgreSQL owns retry due time,
> exponential backoff with jitter, a five-attempt generation budget, expiring
> worker claims, and terminal dead-letter reasons. The worker persists a retry
> or terminal decision before delayed NAK or ACK. An authenticated replay creates
> a fresh outbox UUID and generation without erasing attempts. It remains at
> least once; Stage 5 still owns full SSRF, rate, and circuit controls.

## Ninety-second architecture answer

1. Stage 1 created the FastAPI lifecycle, validated configuration, separate
   liveness/readiness probes, process-scoped async engine, explicit Alembic
   boundary, Compose topology, and layered tests.
2. A deployment-only bootstrap route creates a tenant and one raw API key. The
   raw key is returned once; only its public ID, SHA-256 secret digest, and hint
   are stored.
3. Tenant routes authenticate bearer keys and derive the tenant internally.
   Callers never choose a tenant ID.
4. Endpoint creation generates a signing secret, encrypts it with AES-256-GCM,
   and returns it once. The worker recovers the exact snapshotted version to
   sign HTTP.
5. Event submission requires an idempotency header. HookRelay hashes a
   canonical versioned request and uses a tenant/key unique constraint plus
   PostgreSQL `ON CONFLICT` to make retries safe across concurrent API replicas.
6. The successful transaction creates one event, one pending delivery per
   endpoint, and one generation-1 outbox row per delivery. Only after commit
   does the route return `201`.
7. A separate publisher leases eligible outbox rows in short PostgreSQL
   transactions, publishes ID-only commands to a file-backed JetStream stream,
   waits for PubAck, then conditionally records `published_at`.
8. Workers share a durable pull consumer and bound concurrency with a fetch
   window, semaphore, HTTP pool, and broker `MaxAckPending`.
9. A worker reconciles the broker message with its exact PostgreSQL outbox row,
   derives dispatch generation there, and claims an attempt with a token and
   database-time expiry before timeout-bounded HTTP.
10. Success commits before ACK. Transient failure commits a capped
    exponential/jittered due time before delayed NAK; permanent, exhausted, or
    policy-blocked work commits a dead letter before ACK.
11. An expired worker claim becomes an abandoned attempt and is scheduled or
    dead-lettered. Token/generation/expiry checks fence late finalizers.
12. Authenticated manual replay increments generation and creates a fresh
    outbox UUID in one transaction. The unchanged schema-v1 broker envelope
    remains ID-only; old-generation fencing comes from the reconciled outbox
    row. The guarantee remains at least once.

## Whiteboard trace

Draw the current boundary and keep later roadmap controls outside it:

```text
Producer
  -> FastAPI: validate JSON + headers
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
  -> attempt + delivering claim/lease commit
  -> timestamped exact-byte HMAC HTTP
       -> 2xx -> success commit -> ACK
       -> transient -> retry_scheduled + due time commit -> delayed NAK
       -> permanent/exhausted -> dead_lettered + reason commit -> ACK
  -> expired attempt lease -> abandoned -> retry/dead letter

Operator -> POST /v1/deliveries/{id}/replay
         -> generation + 1 + fresh outbox UUID, one transaction -> 202

Ambiguity: PubAck before outbox finalization; receiver action before
HookRelay success certainty. Stable IDs + idempotency, not exactly once.
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
  behavior as current `0.4.0` behavior.

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
- A blocked target normally commits `target_blocked` and ACKs; a fixed delayed
  NAK is only the pre-persistence fallback.
- A stale worker that loses its claim performs no broker action from its old
  handle.

## Security and limitation questions

### Does requiring `HttpUrl`, HTTPS, or the local/test allowlist solve SSRF?

- No. Schema validation accepts only HTTP(S) syntax and rejects
  userinfo/fragments. The endpoint API additionally requires HTTPS in
  staging/production, while delivery workers refuse to run there at
  all.
- The current worker restricts execution to local/test, requires an explicit
  hostname allowlist, disables redirects, and ignores environment proxies. A
  blocked target becomes replayable terminal `target_blocked` state.
- Complete SSRF defense must still handle loopback/private/link-local/metadata
  IPs, DNS resolution/rebinding, IPv6, and network egress policy.
- The current gate permits a controlled local receiver; it is not safe
  arbitrary-destination production delivery. Stage 5 owns that boundary.

### Is producer traffic protected by TLS?

- Not by the application itself. Uvicorn/Compose serve HTTP and local Compose
  binds only to loopback.
- A deployed environment needs TLS termination at a trusted ingress/proxy and a
  protected hop to the app.
- Requiring `https://` for destination URLs protects neither API keys nor event
  payloads on the producer-to-HookRelay connection.

### Is request size bounded?

- Individual names, types, URLs, endpoint count, key grammar, and top-level body
  shapes are bounded.
- There is no explicit whole-request byte cap or tenant payload/storage quota
  through Stage 4.
- A production design needs ingress and application limits, rate limits, and
  clear `413`/quota contracts before accepting hostile traffic.

### Does encryption at rest make secret storage complete?

- No. It reduces exposure if the database alone is copied.
- The key must be stored separately with access control, rotation, audit,
  backup, restore, and incident-recovery procedures.
- The checked-in local key is intentionally not a production key, and settings
  reject it in staging/production.
- Memory, logs, client handling, and authorized application access remain part
  of the threat model.

## Test-evidence questions

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

Then state its historical limit: Stage 3 evidence did not prove the Stage 4
retry/recovery policy, exactly once, Stage 5 hostile-network safety, clustered
HA, or production throughput.

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
- **Policy block:** `target_blocked` commits without an HTTP attempt, then ACKs;
  replay is available after a reviewed allowlist fix.
- **Replay:** expected generation, tenant scope, and row locking produce one
  fresh outbox UUID without erasing earlier attempts.
- **Real process death:** kill a separate worker after receiver capture, then
  observe expired-claim abandonment and replacement-worker recovery with the
  same body.

For each, say what was observed and one thing the exercise cannot prove.

### “What would you build next?”

Stage 5 should replace the local/test hostname gate with complete
DNS/IP/rebinding/egress SSRF controls, add per-endpoint rate limiting and
circuit breaking, define size limits, and design secret rotation. Stage 6 then
adds telemetry, history APIs, and the operations console. Stage 7 should expand
the single Stage 4 subprocess-kill schedule into a broader fault matrix and add
load, soak, HA, and capacity evidence.

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
- “HMAC encrypts the webhook body.”
- “A timestamp alone prevents replay.”
- “Async means the worker has unlimited concurrency.”
- “The happy-path test proves production scale.”

## Three-to-five-minute Stage 4 teach-back

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
   service, and hard-process-kill evidence; Stage 5 security remains.

If any sentence depends on “FastAPI handles it” or “the database guarantees it”
without naming the route, transaction, constraint, or test, trace one concrete
request again before using the answer in an interview.
