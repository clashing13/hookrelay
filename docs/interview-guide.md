# HookRelay interview guide

This guide turns the implemented system into precise interview explanations.
Lead with an invariant and evidence, then state the boundary. Avoid reciting a
tool list or claiming roadmap behavior as complete.

## Thirty-second Stage 3 pitch

> HookRelay `0.3.0` accepts authenticated events idempotently, commits each
> event and its dispatch intent atomically in PostgreSQL, and asynchronously
> sends a signed webhook through a transactional-outbox publisher, NATS
> JetStream, and a bounded Python worker. The publisher uses expiring database
> claims and marks rows only after a JetStream PubAck. The worker loads
> authoritative state, records an attempt, signs exact JSON bytes with a
> timestamped HMAC, commits success, and only then ACKs the durable message. The
> guarantee remains at least once; Stage 4 adds designed retry/crash recovery,
> and Stage 5 replaces the current local-only outbound gate with full SSRF
> controls.

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
   endpoint, and one unpublished outbox row per delivery. Only after commit does
   the route return `201`.
7. A separate publisher leases eligible outbox rows in short PostgreSQL
   transactions, publishes ID-only commands to a file-backed JetStream stream,
   waits for PubAck, then conditionally records `published_at`.
8. Workers share a durable pull consumer and bound concurrency with a fetch
   window, semaphore, HTTP pool, and broker `MaxAckPending`.
9. A worker reconciles broker IDs with PostgreSQL, commits an unfinished attempt,
   signs deterministic bytes as `v1=HMAC-SHA256(secret, timestamp.body)`, sends
   timeout-bounded HTTP, commits success, and then calls `ack_sync`.
10. The local receiver captures exact bytes and headers for verification. The
    transport is at least once; Stage 4 owns recovery policy and Stage 5 owns
    complete outbound security.

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
  -> attempt + delivering commit
  -> timestamped exact-byte HMAC HTTP
  -> succeeded attempt + delivery commit
  -> JetStream ack_sync

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

### What happens on failure today?

- Timeout, transport error, or non-2xx becomes a `transient_failure`; delivery
  returns to `pending`; the NATS message remains unacknowledged.
- `AckWait` can expose it again, but there is no persistent schedule, backoff,
  jitter, classification, maximum, or dead letter yet.
- A crash after the unfinished-attempt commit can leave delivery `delivering`;
  Stage 3 reports progress rather than duplicating HTTP, but cannot recover the
  stale attempt.
- Stage 4 owns both designed retry and crash recovery.

### Which messages terminate and which stay recoverable?

- Malformed internal envelopes and identities that contradict authoritative
  PostgreSQL state are poison; the worker terminates them rather than performing
  HTTP.
- A valid delivery blocked by the temporary local/test hostname policy is not
  poison. It remains unacknowledged so a later reviewed policy/configuration can
  recover it.
- Stage 4 will add dead-letter and operator recovery semantics.

## Security and limitation questions

### Does requiring `HttpUrl`, HTTPS, or the Stage 3 allowlist solve SSRF?

- No. Schema validation accepts only HTTP(S) syntax and rejects
  userinfo/fragments. The endpoint API additionally requires HTTPS in
  staging/production, while Stage 3 delivery workers refuse to run there at
  all.
- Stage 3 restricts workers to local/test, requires an explicit hostname
  allowlist, disables redirects, and ignores environment proxies.
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
- There is no explicit whole-request byte cap or tenant payload/storage quota in
  Stage 2.
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

Then state the limit: this does not prove every crash schedule, exactly once,
Stage 4 retry/recovery, Stage 5 hostile-network safety, clustered HA, or
production throughput.

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

- Submit many concurrent requests with the same tenant, key, and body.
- Observe one event, one delivery per endpoint, one outbox per delivery, zero
  attempts, stable IDs, and replay responses.
- Then reuse the same key with a changed payload and observe `409` with unchanged
  row counts.
- This tests an intended correctness boundary without exposing a secret,
  deleting a volume, or sending traffic to a real third party.

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

For each, say what was observed and one thing the exercise cannot prove.

### “What would you build next?”

Stage 4 should replace raw `AckWait` redelivery with persistent retry state,
classification, exponential backoff and jitter, maximum attempts, stale-attempt
worker-crash recovery, dead letters, and replay. It must test killed workers and
unavailable destinations. Stage 5 then replaces the current local/test
hostname gate with complete DNS/IP/rebinding/egress SSRF controls plus rate and
circuit protection.

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
- “An unfinished Stage 3 attempt always recovers after worker death.”
- “The hostname allowlist is complete SSRF protection.”
- “HMAC encrypts the webhook body.”
- “A timestamp alone prevents replay.”
- “Async means the worker has unlimited concurrency.”
- “The happy-path test proves production scale.”

## Three-to-five-minute Stage 3 teach-back

Aim for this timing:

1. **0:00-0:30 — Boundary:** PostgreSQL `201` acceptance versus asynchronous
   receiver success and the at-least-once guarantee.
2. **0:30-1:15 — Publisher:** expiring claim, `SKIP LOCKED`, ID-only message,
   PubAck, conditional `published_at`, and duplicate window.
3. **1:15-2:00 — JetStream/backpressure:** file-backed work queue, shared
   durable pull cursor, explicit ACK, fetch/semaphore/pool/`MaxAckPending`.
4. **2:00-3:00 — Worker/wire:** authoritative database reconciliation,
   attempt-before-HTTP, snapshotted secret, exact JSON, timestamped HMAC, and
   timeout.
5. **3:00-3:40 — Ordering/ambiguity:** success commit before `ack_sync`,
   suppression after DB success, and receiver-success ambiguity.
6. **3:40-4:25 — Evidence:** deterministic vector, real PostgreSQL, real NATS,
   real HTTP, full happy path, and one limitation of each.
7. **4:25-5:00 — Boundaries:** retry/crash recovery in Stage 4, full SSRF in
   Stage 5, and no exactly-once/HA/scale claim.

If any sentence depends on “FastAPI handles it” or “the database guarantees it”
without naming the route, transaction, constraint, or test, trace one concrete
request again before using the answer in an interview.
