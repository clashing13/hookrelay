# HookRelay glossary

This glossary is cumulative through Stage 5 (`0.5.0`). Entries marked
**future** describe planned behavior, not a capability of the current
repository.

## Product and HTTP terms

**Accepted event**

An event for which HookRelay committed the event, delivery snapshots, and
outbox messages to PostgreSQL. "Accepted" still does not mean published,
attempted, acknowledged, or delivered; those are later asynchronous states.

**API (Application Programming Interface)**

A contract through which software communicates. HookRelay exposes health,
protected bootstrap, authenticated tenant/endpoint inspection, endpoint
creation/signing-secret rotation, idempotent event submission, delivery replay,
and health routes.

**Application factory**

A function that constructs and configures an application. `create_app()` makes
settings, database ownership, cryptography, error handlers, and routes explicit
and lets tests create isolated app instances.

**ASGI (Asynchronous Server Gateway Interface)**

The Python interface between an asynchronous web server and an application.
Uvicorn is HookRelay's ASGI server and FastAPI supplies the application.

**Bearer authentication**

An HTTP authentication scheme in which the holder presents a credential in
`Authorization: Bearer <credential>`. Possession grants the credential's
authority, so it must be protected in transit, storage, logs, shell history,
and client code.

**Bootstrap token**

A deployment-level bearer secret that authorizes creation of the first tenant
and initial API key. The endpoint is explicit, defaults to disabled in process
settings, and should be disabled after provisioning. A bootstrap token is not a
tenant API key.

**Content type**

The media type of a request or response. HookRelay mutation routes require
`Content-Type: application/json`; deliberate error responses use
`application/problem+json`.

**Dependency injection**

Supplying a component from outside an operation. FastAPI dependencies create a
request session, authenticate a tenant, validate headers, and supply the
configured secret cipher without hidden global transaction state.

**HTTP `201 Created`**

The success status for tenant, endpoint, and event creation. Event replays also
return `201`: they reproduce the result of the original create operation rather
than creating additional rows.

**HTTP `400 Bad Request`**

Used when event submission omits `Idempotency-Key`, supplies an invalid key
grammar, or sends malformed JSON.

**HTTP `401 Unauthorized`**

Used for missing or invalid bearer credentials. HookRelay intentionally gives
the same response for malformed, unknown, revoked, expired, and wrong-secret
API keys.

**HTTP `404 Not Found`**

Used for a missing resource, a resource owned by another tenant, an inactive or
unusable requested endpoint, or a disabled bootstrap surface. Opaque responses
avoid revealing whether another tenant's identifier exists.

**HTTP `409 Conflict`**

Used when a tenant reuses an `Idempotency-Key` for a different versioned request
fingerprint, submits a stale signing-secret version during rotation, submits a
stale replay generation, or tries to replay a non-dead-lettered delivery. No
second event/rotation/generation is silently created.

**HTTP `413 Content Too Large`**

Used when actual ASGI request-body bytes exceed the configured whole-request
limit or a validated event's canonical UTF-8 payload exceeds its smaller event
limit. `Content-Length` is only an early hint; streamed bytes are authoritative.

**HTTP `415 Unsupported Media Type`**

Used when a JSON mutation route is called without
`Content-Type: application/json`.

**HTTP `422 Unprocessable Content`**

Used when syntactically valid JSON violates the request schema, such as a bad
URL, duplicate endpoint IDs, an unexpected field, or an invalid event type.

**HTTP `503 Service Unavailable`**

A temporary dependency failure. Readiness and domain work return sanitized
`503` responses when PostgreSQL cannot currently support the operation.

**`Idempotency-Key`**

A required event-submission header chosen by the producer. Stage 2 accepts 8-128
characters from letters, digits, `.`, `_`, `:`, and `-`, and scopes uniqueness
to the authenticated tenant.

**`Idempotency-Replayed`**

A response header present with value `true` when an event-create response came
from a matching existing idempotency record. It is absent on the first
acceptance.

**`Location`**

A response header identifying the created or replayed resource. Event creation
uses `/v1/events/{event_id}`.

**Lifespan**

ASGI's startup/shutdown context. HookRelay uses it to attach process-scoped
resources and dispose the database engine during graceful shutdown.

**One-time secret response**

A response that is the only API opportunity to capture raw credential material.
Tenant bootstrap returns the initial API key once; endpoint creation returns
the first signing secret once; endpoint rotation returns the replacement once.
All carry no-store headers. Later inspection returns metadata, not plaintext.

**OpenAPI**

A machine-readable HTTP description generated by FastAPI at `/openapi.json`.
It describes contracts but does not prove the implementation, authorization,
or database behavior is correct.

**Problem Details**

The RFC 9457 family of error objects. HookRelay extends the standard fields with
a stable machine-readable `code` and optional sanitized field errors. It does
not echo secret or internal values.

**Response contract**

The status, headers, media type, and body a client may rely on. Idempotent
behavior includes all four: a matching replay is `201`, uses the original IDs,
sets `Location`, and adds `Idempotency-Replayed: true`.

**Strict request model**

A schema that rejects undeclared fields instead of silently discarding them.
This reduces accidental mass assignment and makes API evolution visible.

## Domain and persistence terms

**API key**

A tenant credential shaped as `hrk_<public-id>.<secret>`. The public ID supports
indexed lookup; the 256-bit random secret proves possession. The raw token is
returned once and only a digest and short hint are persisted.

**API-key ID**

The internal UUID of an API-key row. Events record it for attribution. It is
not the public ID embedded in the presented token and is never accepted as
authentication input.

**Check constraint**

A database rule each row must satisfy, such as valid delivery states, positive
versions, JSON-object payloads, bounded URL length, or consistent attempt
timestamps.

**Commit**

The point at which a database transaction becomes durable and visible to other
transactions. HookRelay sends an event `201` only after commit succeeds.

**Composite foreign key**

A relationship enforced across multiple columns. Stage 2 includes `tenant_id`
in domain relationships so a delivery cannot reference an endpoint or event
from another tenant even if application code makes a mistake.

**Delivery**

One intended dispatch of one event to one endpoint. Ingestion creates it in
`pending` state with a target-URL and signing-secret-version snapshot. It can
move through `delivering` and `retry_scheduled` to `succeeded`, or become
`dead_lettered`. Manual replay keeps the delivery ID but increments its dispatch
generation.

**Delivery attempt**

A durable record of one worker try, including its lifetime number, dispatch
generation, claim token, start/finish time, outcome, response status or
sanitized error code, and duration. It is created unfinished before HTTP and
finished as succeeded, transient, permanent, or abandoned.

**Endpoint signing secret**

A generated `whsec_...` value the delivery worker recovers to authenticate
webhook requests with an HMAC. Unlike an API key, it must be recoverable, so it
is encrypted rather than irreversibly hashed. Stage 5 can atomically retire the
active version and return a new version once; retained delivery snapshots keep
referencing their original encrypted row.

**Endpoint traffic-control row**

The PostgreSQL row keyed by `(tenant_id, endpoint_id)` that holds one endpoint's
fixed-window counter, circuit state/failure count/open time, and optional probe
token/expiry. Locking it makes admission shared across worker processes rather
than multiplying limits per process.

**Event**

The immutable tenant-owned logical fact submitted by a producer. It records a
type, JSON-object payload, accepting API key, creation time, idempotency key,
and versioned request fingerprint.

**Foreign key**

A database constraint requiring a referenced parent row. It protects relational
integrity independently of whether every application query is correct.

**JSONB**

PostgreSQL's binary JSON type. Stage 2 uses it for event payloads and outbox
payloads while requiring their top-level value to be an object.

**Migration**

An ordered, reviewable schema transition. Stage 2 creates the domain schema;
Stage 3 adds recoverable outbox claims; Stage 4 revision `20260804_0003` adds
retry, delivery-claim, generation, terminal, and replay state; Stage 5 revision
`20260805_0004` creates/backfills per-endpoint traffic-control rows and adds
probe evidence to attempts. Editing an ORM model does not update a database.

**Outbox message**

A durable record that dispatch work needs publishing. Initial delivery and
each manual replay generation have one strict schema-v1, ID-only
`delivery.requested` row with a fresh UUID. The database row, not the broker
payload, carries authoritative dispatch generation. The publisher waits for a
NATS PubAck before setting `published_at`.

**Partial index**

An index covering only rows that match a predicate. Stage 2 indexes unpublished
outbox rows and enforces one non-retired signing secret per endpoint with
partial indexes.

**Primary key**

A value that uniquely identifies a database row. Stage 2 uses UUID primary keys
for domain entities.

**Relational source of truth**

The authoritative durable state. PostgreSQL, not a future broker or in-memory
cache, determines whether an event was accepted and which deliveries exist.

**Rollback**

Discarding every uncommitted change in a transaction. If delivery or outbox
creation fails, the event must roll back too; a partial acceptance is invalid.

**Snapshot**

A copy of mutable configuration captured when work is accepted. A delivery
stores its target URL and signing-secret version ID so later endpoint changes do
not silently rewrite historical intent.

**Tenant**

HookRelay's customer isolation boundary. Tenant identity is derived from a
verified API key, not accepted from a request body or query parameter. Resource
queries carry tenant predicates, missing and cross-tenant identifiers share
opaque responses, and composite foreign keys protect relational ownership.
The current schema does not claim PostgreSQL row-level security.

**Transaction**

An atomic database unit of work. Stage 2 uses one transaction for event,
delivery, and outbox creation so all rows commit or none do.

**Unique constraint**

A database guarantee that a value or tuple cannot appear twice. The unique
`(tenant_id, idempotency_key)` rule is the final concurrency-safe idempotency
guard across all API replicas.

**Webhook endpoint**

A tenant-owned destination name and URL. Stage 5 validates it at creation and
again through the worker's IP-pinned transport. Exact private-host exemptions
exist only for local/test; blocked work becomes replayable `target_blocked`
terminal state. The current `enabled` response field maps to stored active
state.

## Idempotency and concurrency terms

**Canonicalization**

Turning semantically equivalent input into one deterministic representation.
HookRelay sorts JSON object keys and endpoint UUIDs before hashing; endpoint
list order therefore does not change the fingerprint.

**Concurrent race**

Two overlapping operations whose result depends on timing. Two API processes
may both fail to find an idempotency row before either inserts. PostgreSQL's
unique constraint and `ON CONFLICT` behavior resolve this race.

**Idempotency**

The property that repeating the same logical operation does not create more
effects. Stage 2 returns the original event/delivery IDs rather than inserting
duplicates.

**Idempotency scope**

The namespace in which a key must be unique. HookRelay uses the authenticated
tenant plus the key, so two tenants may independently use the same text.

**Logical request**

The fields included by the versioned fingerprint: operation, type, payload,
and endpoint set. HTTP JSON whitespace and object-member order are not logical
differences; payload array order is.

**`ON CONFLICT DO NOTHING RETURNING`**

A PostgreSQL insert pattern that lets one concurrent request create the event
while another observes that it lost the unique-key race and loads the winner.
An earlier application read remains an optimization, not the correctness guard.

**Request fingerprint**

A SHA-256 digest of the canonical, versioned logical event request. Comparing
it distinguishes a safe replay from accidental reuse of a key for different
work. The version lets future canonicalization rules evolve deliberately.

**Ingestion replay**

Returning the original creation result for a matching
tenant/key/fingerprint. It creates no new event, delivery, or outbox row. It is
different from broker redelivery and dead-letter replay.

**Dead-letter replay**

An authenticated `202` operation that increments a dead-lettered delivery's
dispatch generation and atomically creates a fresh outbox row. Its JSON request
must match the generation the operator observed, preventing one ambiguous
intent from advancing twice. It preserves attempt history and grants the new
generation a bounded retry budget. Missing/cross-tenant IDs use opaque `404`;
a valid tenant-owned delivery outside `dead_lettered` returns
`409 delivery_not_replayable`.

**Signing-secret rotation precondition**

The rotation request's `expected_active_version`. HookRelay compares it while
holding the tenant-owned endpoint/active-secret lock. One concurrent request
wins; a stale request receives `409 signing_secret_version_conflict` plus the
safe current-version header, without retiring or inserting a row.

## Security and cryptography terms

**AAD (Additional Authenticated Data)**

Unencrypted context covered by an authenticated cipher's integrity check.
HookRelay binds a signing-secret ciphertext to its tenant, endpoint, row,
secret version, envelope version, and key version. Substituting the ciphertext
into another context causes decryption to fail.

**AES-256-GCM**

An authenticated-encryption algorithm using a 256-bit key. It protects endpoint
signing-secret confidentiality and detects ciphertext/AAD modification. Its
security depends on unique nonces, protected keys, and correct rotation and
backup procedures.

**Authentication versus authorization**

Authentication proves which credential is presented. Authorization determines
which resources it may access. HookRelay authenticates an API key and then
authorizes every lookup within the derived tenant.

**Constant-time comparison**

A comparison designed not to stop at the first differing byte, reducing timing
information about a secret. HookRelay uses it for bootstrap tokens, API-key
digests, and idempotency fingerprints.

**Encryption**

A reversible transformation using a protected key. It is appropriate for
signing secrets because the delivery worker must recover them. Encryption is
not the same as hashing.

**Encryption-key version**

Metadata identifying which configured key encrypted a secret. It makes a
deliberate master-key rotation/re-encryption workflow possible. Stage 5 rotates
endpoint signing-secret versions, not this master key. The current process
loads one AES key/version, so replacing it without a keyring or rewrapping
retained secret rows makes those snapshots undecryptable.

**Entropy**

Unpredictability in a generated secret. API-key secrets and endpoint signing
secrets use 256 random bits, making offline guessing impractical when generation
and storage remain sound.

**Hashing**

A one-way digest calculation. HookRelay hashes API-key secrets because
authentication needs only verification, not recovery. The high-entropy random
input is important; hashing alone does not make human-chosen passwords safe.

**HMAC (Hash-based Message Authentication Code)**

A keyed integrity/authenticity value. Stage 3 computes HMAC-SHA256 over the
ASCII Unix timestamp, one dot byte, and the exact request body bytes. HMAC does
not encrypt the payload or provide receiver idempotency.

**Nonce**

A per-encryption value that must not repeat under the same AES-GCM key. Stage 2
generates a random 96-bit nonce and stores it in the ciphertext envelope.

**Secret hint**

The last four characters stored to help a human distinguish credentials
without revealing the complete value. A hint is not sufficient to authenticate.

**`SecretStr`**

A Pydantic wrapper that reduces accidental secret display in representations
and errors. Code can still deliberately reveal the value; it is not encryption,
access control, or a secret manager.

**DNS rebinding**

Returning a safe address during validation and a different, unsafe address
when the client connects. HookRelay does not trust the creation-time lookup as
a connection authorization: each new connection resolves again, validates all
answers, selects a validated numeric address, and verifies the connected peer.

**Egress control**

Network policy outside the application that restricts where a workload can
connect. HookRelay's application checks reduce SSRF risk, but production still
needs firewall, DNS, proxy, and orchestrator policy as an independent boundary.

**IP pinning**

Connecting to a validated numeric address while preserving the original
hostname for the HTTP `Host` header and TLS SNI/certificate verification.
HookRelay also confirms that the actual peer address is the selected address.

**Local destination exemption**

An exact hostname in `delivery_allowed_hosts` that permits a private local/test
receiver. Exemptions skip endpoint-creation DNS preflight but are still
resolved and pinned when a worker connects. Staging/production settings require
this list to be empty.

**SSRF (Server-Side Request Forgery)**

Abusing a server's outbound fetch to reach unintended internal or privileged
targets. Stage 5 accepts only policy-compliant HTTP(S) URLs, requires HTTPS in
staging/production, rejects unsafe literal or resolved addresses, repeats DNS
validation at each new connection, pins the chosen IP, verifies the peer,
preserves hostname TLS validation, disables redirects and environment proxies,
and forbids Unix sockets. This is defense in depth, not a claim that application
code replaces production egress controls.

**TLS (Transport Layer Security)**

Encryption and peer authentication for network traffic, normally expressed as
HTTPS. Uvicorn/Compose currently serve HTTP on loopback. A production ingress
must terminate TLS; requiring HTTPS destination URLs does not secure the
producer-facing connection.

## Async and lifecycle terms

**Async I/O**

I/O APIs that yield control while waiting so one event-loop thread can progress
other tasks. `async def` does not make CPU-heavy or blocking library calls
parallel.

**Connection pool**

A process-scoped manager of reusable database connections. Pool size multiplied
by every API/worker process must fit the PostgreSQL connection budget.

**Coroutine**

A suspendable computation produced by calling an async function. It progresses
only when the event loop schedules it.

**Event loop**

The scheduler that resumes coroutines when awaited I/O can progress. Blocking
the loop delays unrelated requests using the same process.

**SQLAlchemy async engine**

The process-scoped database infrastructure and pool. It is safe to reuse across
requests and is disposed during application shutdown.

**SQLAlchemy `AsyncSession`**

A mutable transaction/unit-of-work boundary. HookRelay creates one per request;
sharing one global session could interleave pending state, commits, and
rollbacks across callers.

## Reliability and messaging terms

**Acknowledgment (`ACK`)**

A consumer signal that one broker message completed successfully. HookRelay
uses synchronous JetStream acknowledgment only after PostgreSQL commits the
successful attempt and delivery state.

**Acknowledgment wait (`AckWait`)**

The period JetStream waits for an ACK or progress signal before a message is
eligible for redelivery. It is a fallback wake-up, not HookRelay's authoritative
retry schedule; Stage 4 persists due time and uses explicit delayed NAK.

**At-least-once delivery**

A logical event may be attempted more than once so transient or ambiguous
failures do not silently lose it. Stages 4 and 5 provide bounded retry, crash
recovery, and traffic admission but retain duplicate-publication/HTTP
ambiguity; receiver idempotency remains required.

**Bounded concurrency**

A hard ceiling on simultaneous work. Stage 3 aligns a worker fetch window,
`asyncio.Semaphore`, HTTP connection pool, and JetStream `MaxAckPending` rather
than creating an unbounded task/socket backlog.

**Circuit breaker**

Per-endpoint PostgreSQL state that stops new attempts after repeated transient
failures. `closed` admits normal work, `open` defers until its cooldown, and
`half_open` admits exactly one fenced recovery probe. A success or permanent
outcome closes/resets it; a transient or abandoned probe reopens it.

**Canonical body bytes**

The deterministic compact, sorted-key UTF-8 representation used for both HMAC
and HTTP. A receiver must verify these raw bytes rather than re-serializing JSON.

**Claim lease**

An expiring `claim_token` plus `claim_expires_at` on an outbox row. It lets a
publisher commit ownership, release its database locks before broker I/O, and
later finalize only while its token still matches. Expiry recovers abandoned
claims. The TTL must exceed the configured sequential batch size multiplied by
the per-publish timeout.

**Fixed-window rate limit**

A per-endpoint request counter and database-time window stored in PostgreSQL.
Admission locks the endpoint traffic-control row and reads fresh database time
after any lock wait, so concurrent workers share one default limit of 10
requests per one second. It is a simple burst-control policy, not a production
quota or capacity guarantee.

**Dead letter**

A terminal PostgreSQL delivery state with timestamp and reason
`permanent_failure`, `attempts_exhausted`, or `target_blocked`. It remains
inspectable and manually replayable. Stage 4 has no separate dead-letter stream
or queue.

**Durable consumer**

A named JetStream cursor whose delivery/acknowledgment state survives client
disconnects. All Stage 3 workers bind to
`HOOKRELAY_DELIVERY_WORKERS_V1` rather than receiving independent copies.

**Dual write**

Trying to update two independent systems as one logical action, such as
committing PostgreSQL and publishing to a broker. A crash between them can leave
inconsistent state.

**Exactly once**

A guarantee that a logical side effect occurs one time. HookRelay cannot make
this general claim across its database, a broker, HTTP, and an independent
receiver database. Receiver-side idempotency can provide effectively-once
business behavior.

**`Nats-Msg-Id`**

A JetStream publication header used for duplicate detection within a finite
window. HookRelay sets it to the outbox UUID. It reduces quick duplicates but
does not guarantee exactly once.

**NATS JetStream**

The durable message transport between the outbox publisher and delivery
workers. HookRelay uses one file-backed work-queue stream and an explicit-ACK
durable pull consumer. Delayed NAK requests a later wake-up; PostgreSQL remains
the domain/retry source of truth.

**Poison message**

An internally malformed command or one whose identities contradict
authoritative PostgreSQL state. The worker terminates it rather than performing
HTTP. A destination blocked by the outbound destination policy is valid domain
work, not poison; HookRelay persists a replayable `target_blocked` dead letter.

**Policy-blocked delivery**

Valid durable work whose destination URL, DNS answers, or connected peer is not
permitted by the current policy. A URL blocked before admission records
terminal reason `target_blocked` without an attempt; a connection-time block is
recorded on the already-claimed attempt. HookRelay ACKs after terminal state
commits and permits manual replay after a reviewed policy/configuration change.

**Publish acknowledgment (`PubAck`)**

JetStream confirmation that a message was accepted into the expected stream.
The outbox publisher requires it before marking the PostgreSQL row published.

**Pull consumer**

A consumer whose client requests a bounded batch when it has capacity. Stage 3
uses pull delivery to align broker flow with worker concurrency.

**Recovery probe**

The single attempt admitted after an open circuit's cooldown. Its probe token
and expiry are fenced to the delivery claim; competing workers defer without
creating attempts. The probe consumes a normal rate-limit slot.

**Transactional outbox**

Writing a domain change and a to-be-published message in the same database
transaction. It closes the event-commit/message-not-created gap. It does not
make later broker publication exactly once.

**Work-queue retention**

A JetStream policy that keeps a message for one eligible consumer until it is
acknowledged, subject to configured limits. It is a dispatch queue, not a
permanent event-history log.

**Webhook signature version**

The `v1=` signature prefix plus `HookRelay-Webhook-Version: 1`. It labels the
current timestamp/body grammar so incompatible changes can be versioned rather
than silently breaking receivers.

**Abandoned attempt**

An unfinished attempt whose delivery lease expired. Its HTTP outcome is
unknown, so HookRelay finishes it as `abandoned`, counts it against the current
generation, and schedules recovery or dead-letters.

**Attempt lease**

A random claim token shared by the attempt and delivery plus a database-time
expiry stored on the delivery. It permits HTTP outside a database transaction
while allowing later workers to recover ownership and fence stale finalizers.
The TTL must exceed HTTP timeout plus an explicit finalization margin; Stage 4
uses PostgreSQL `clock_timestamp()` for wall-clock comparisons.

**Backoff cap**

The maximum unjittered retry delay. Stage 4 doubles from the base but stops at
this bound before applying downward jitter.

**Broker delivery count**

How many times JetStream has presented a message. It includes due-time and
active-lease deferrals, so it is not the number of outbound HTTP attempts and
does not enforce HookRelay's business attempt maximum.

**Delayed negative acknowledgment (`NAK`)**

A consumer instruction asking JetStream to present a message after a delay.
HookRelay sends it after a durable retry decision or active-lease check.
PostgreSQL due state remains authoritative if wake-up timing differs.

**Dispatch generation**

A positive PostgreSQL integer identifying one bounded retry cycle for a
delivery. Initial work is generation 1; manual replay increments it. The strict
schema-v1 broker envelope does not carry generation: the worker derives it from
the exact outbox row after reconciling `message_id`.

**Downward jitter**

A uniform random reduction from an exponential ceiling. With ratio `r`, Stage
4 selects from `[ceiling * (1-r), ceiling]`, so the delay spreads retries
without exceeding the cap.

**Exponential backoff**

A retry delay whose ceiling doubles with each attempt in the current dispatch
generation until a configured maximum. The Stage 4 ceiling is
`min(max, base * 2^(n-1))`.

**Failure classification**

The explicit policy mapping an observation to success, transient failure, or
permanent failure. Stage 4 retries timeout/transport errors, `408`, `425`,
`429`, and `5xx`; it treats other non-2xx statuses as permanent.

**Fencing**

Rejecting an operation from an owner whose lease has been superseded. A Stage
4 finalizer must match attempt identity, delivery state, dispatch generation,
claim token, and unexpired database lease.

**Generation attempt number**

The number of attempt rows associated with the current dispatch generation.
It drives backoff and maximum-attempt policy. Lifetime `attempt_number` remains
monotonic across manual replay.

**Next attempt time (`next_attempt_at`)**

The database timestamp at or after which a `retry_scheduled` delivery may
create another HTTP attempt. A constraint requires scheduled status and due
time together.

**Optimistic generation precondition**

The replay request's `expected_dispatch_generation`. HookRelay compares the
operator-observed value under a row lock before advancing it. A stale or
duplicate intent receives `409 delivery_generation_conflict` instead of
creating another generation.

**Permanent failure**

An outcome the current policy does not retry, such as most `4xx` and all
redirect responses. It records the attempt, moves the delivery immediately to
dead letter, and ACKs after the terminal commit.

**Retry budget**

The maximum actual or ambiguous abandoned attempts allowed in one dispatch
generation. Defaults permit five. Broker presentations that create no attempt
do not spend it.

**Retry schedule**

Persistent `retry_scheduled` delivery state plus a non-null PostgreSQL due time.
It survives worker restarts independently of an individual delayed NAK.

**Traffic deferral**

A database-authoritative decision to postpone work because the fixed window is
full, the circuit is cooling down, or another recovery probe owns admission.
The delivery becomes `retry_scheduled` with a PostgreSQL due time and receives a
delayed NAK; no attempt row, retry budget, or outbound socket is consumed.

**Stale dispatch**

A broker message whose exact outbox row belongs to an older dispatch generation
than the delivery. A current worker ACKs it without HTTP. During deployment,
manual replay should wait for Stage 3 workers to drain/cut over because an old
worker understands the unchanged v1 envelope but not generation fencing.

**Transient failure**

An outcome the current policy may retry, including timeout, async transport
error, `408`, `425`, `429`, and `5xx`, while the current generation has budget.

## Testing and operations terms

**`alembic check`**

A comparison between current ORM metadata and the schema Alembic would produce.
It catches some model/migration drift after the migration has been applied.

**Integration test**

A test crossing a real system boundary. Through Stage 5, integration coverage
uses PostgreSQL, NATS JetStream, and HTTP receiver behavior for migrations,
claims, schedules, fencing, publication, attempts, dead letters, replay, and
the delivery path, plus PostgreSQL-shared traffic control, rotation snapshots,
and tenant isolation.

**Least-privilege app container**

An application container running as a non-root identity with a read-only root
filesystem, all Linux capabilities dropped, `no-new-privileges`, a PID limit,
and a small restricted `/tmp` tmpfs. HookRelay applies this shared profile to
its own API, publisher, worker, and receiver services; it does not blindly
apply identical settings to the official PostgreSQL and NATS images.

**Test receiver**

A local-only configurable HTTP process that records a bounded in-memory list of
exact body bytes, SHA-256 body digest, lower-cased headers, sequence, and receive
time. It is inspection evidence, not a durable audit service or production
receiver.

**Liveness probe**

The dependency-free question “can this process and event loop respond?” It must
remain healthy during a PostgreSQL outage.

**Readiness probe**

The question “can this instance currently handle dependency-backed traffic?”
HookRelay executes a bounded `SELECT 1` and returns sanitized `503` on failure.

**Request-size limit**

A maximum number of bytes accepted for an HTTP request. Stage 5's ASGI boundary
counts actual streamed body bytes and rejects more than 1 MiB with `413` before
routing, authentication, or JSON parsing. One all-digit `Content-Length` is an
early hint compared without converting an unbounded decimal integer; duplicate
or malformed hints do not replace streamed-byte counting. Event ingestion
separately rejects a compact validated payload larger than 256 KiB. These caps
are not tenant storage quotas or substitutes for an external ingress limit.

**End-to-end test**

A test that crosses API, PostgreSQL, outbox publisher, JetStream, worker, and
receiver boundaries. It proves the tested success/recovery path interoperates,
not every process-kill schedule, production security, HA, or scale.

**Test marker**

Pytest metadata for selecting suites. `integration` requires real services;
`concurrency` highlights overlapping operations; `security` highlights
authentication/isolation/redaction plus request-limit, SSRF, and rotation
boundaries; `nats` and `e2e` identify broker and full-path coverage.

**Unit test**

A fast test of a small boundary without real external infrastructure. It cannot
prove PostgreSQL-specific constraints, isolation, SQL, or transaction behavior.
