# HookRelay interview guide

This guide turns the implemented system into precise interview explanations.
Lead with an invariant and evidence, then state the boundary. Avoid reciting a
tool list or claiming roadmap behavior as complete.

## Thirty-second Stage 2 pitch

> HookRelay is a webhook-delivery learning project. Through Stage 2 it accepts
> authenticated tenant events idempotently and commits each event, its per-target
> delivery snapshots, and transactional-outbox rows atomically in PostgreSQL.
> Matching retries return the original IDs with `201` and an explicit replay
> header; conflicting key reuse is `409`, and a database uniqueness constraint
> closes concurrent races. API keys are hash-only, signing secrets are encrypted
> because they must be recoverable, and tenant relationships are constrained in
> the schema. It does not publish to NATS or send webhooks yet, so I call this
> durable ingestion, not delivery.

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
   and returns it once. A future worker must recover that secret to sign HTTP.
5. Event submission requires an idempotency header. HookRelay hashes a
   canonical versioned request and uses a tenant/key unique constraint plus
   PostgreSQL `ON CONFLICT` to make retries safe across concurrent API replicas.
6. The successful transaction creates one event, one pending delivery per
   endpoint, and one unpublished outbox row per delivery. Only after commit does
   the route return `201`.
7. There is no publisher, NATS, worker, outbound HTTP, or delivery attempt yet.
   That deliberate boundary makes the durable handoff inspectable before
   introducing another system.

## Whiteboard trace

Draw this without adding roadmap components inside the current boundary:

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

Matching retry -> original response + Idempotency-Replayed: true
Changed retry  -> 409 idempotency_key_reused

Outbox publisher / NATS / HTTP worker: not implemented
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
- Database pools and future worker concurrency still need explicit bounds.

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
- Per-delivery messages give a future worker one scheduling/retry identity per
  destination.
- The unique `(delivery_id, topic)` constraint prevents duplicate dispatch facts
  for the current topic.
- The versioned ID-only payload avoids putting event bodies or signing secrets
  on a future broker.

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
- A future sender must recover an endpoint signing secret to compute an HMAC,
  so one-way hashing would make delivery impossible.
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

## Security and limitation questions

### Does requiring `HttpUrl` or HTTPS solve SSRF?

- No. Syntax validation rejects malformed/userinfo/fragment forms, and
  staging/production require an HTTPS scheme.
- Complete SSRF defense must handle loopback/private/link-local/metadata IPs,
  DNS resolution and rebinding, redirects, IPv6, and network egress policy.
- Stage 2 performs no outbound request, so it stores risk but does not exercise
  it. A worker must not be enabled against untrusted URLs until those defenses
  are in place.

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

For each, say what was observed and one thing the exercise cannot prove.

### “What would you build next?”

Stage 3 should publish durable outbox rows to NATS JetStream and add a
bounded-concurrency worker that signs and sends webhook requests. Before that
worker is exposed to untrusted endpoint URLs, add or deliberately gate SSRF
defenses. Preserve idempotent message consumption because publish/mark and
HTTP acknowledgment still have crash ambiguity. Do not jump directly to retry
polish while the handoff boundary is unverified.

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

## Three-to-five-minute Stage 2 teach-back

Aim for this timing:

1. **0:00-0:30 — Boundary:** Product goal, durable-ingestion outcome, and the
   explicit absence of NATS/outbound attempts.
2. **0:30-1:10 — Identity:** Bootstrap, one-time API key, bearer verification,
   derived tenant scope, and composite database enforcement.
3. **1:10-1:50 — Secrets:** One-time signing secret, hashing versus AES-GCM,
   associated data, and key-management limit.
4. **1:50-2:50 — Transaction:** Trace event validation through event + delivery
   snapshots + outbox and commit-before-`201`.
5. **2:50-3:40 — Idempotency:** Canonical fingerprint, database race, `201`
   replay/header, and exact `409` conflict semantics.
6. **3:40-4:30 — Evidence:** Name the unit/API/PostgreSQL/concurrency/rollback
   tests and one limit of each layer.
7. **4:30-5:00 — Risks/next step:** Request-size, TLS, SSRF, quotas, and why the
   future pipeline remains at least once.

If any sentence depends on “FastAPI handles it” or “the database guarantees it”
without naming the route, transaction, constraint, or test, trace one concrete
request again before using the answer in an interview.
