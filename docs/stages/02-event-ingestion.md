# Stage 2: durable event ingestion

Stage 2 turns the service foundation into a durable intake boundary. A producer
can authenticate, choose one or more tenant-owned webhook endpoints, and submit
an event with an idempotency key. HookRelay commits the event, pending delivery
snapshots, and transactional-outbox messages together before returning `201`.

The learning goal is not merely “insert rows with FastAPI.” It is to understand
authentication versus tenant authorization, one-way versus recoverable secret
storage, database-enforced idempotency, transactional atomicity, the dual-write
problem, and what an acceptance response can honestly promise.

This guide is written against the Stage 2 repository. It uses the same learning
loop throughout:

> State the problem -> build the boundary -> inspect durable evidence -> break
> it safely -> explain the invariant and its limit.

The most important scope sentence is:

> Stage 2 durably records delivery intent. It has no NATS connection, outbox
> publisher, outbound webhook request, or delivery attempt.

## 1. The problem this stage solves

A producer can lose an HTTP response even when the server completed the
request. Consider four timelines:

```text
A. request never reaches HookRelay                 -> nothing committed
B. request reaches HookRelay; transaction fails    -> nothing committed
C. transaction commits; response reaches producer -> event accepted
D. transaction commits; response is lost           -> event accepted, caller unsure
```

Without an idempotency contract, retrying case D duplicates work while not
retrying it risks loss in cases A or B. Without an atomic durability boundary,
HookRelay might retain an event but lose the information that a worker must
eventually dispatch it.

Stage 2 answers six questions:

1. Which tenant is making the request, and how is that identity derived rather
   than trusted from input?
2. How can a producer safely retry after an ambiguous timeout?
3. How does the system distinguish a matching retry from accidental key reuse
   for changed work?
4. How are an event, its per-endpoint deliveries, and future publish intent
   committed as one all-or-nothing unit?
5. Which secrets need only verification, and which must be recoverable later?
6. What does `201 Created` prove—and what does it explicitly not prove?

The central database invariant is:

```text
For one accepted event targeting N endpoints:

1 event + N pending deliveries + N unpublished outbox messages + 0 attempts

all commit, or all roll back.
```

The central idempotency invariant is:

```text
same tenant + same key + same logical request
    -> original event/delivery IDs, HTTP 201, replay header

same tenant + same key + different logical request
    -> HTTP 409, no additional rows
```

Both invariants must survive several API processes and concurrent requests.
They cannot depend only on a Python dictionary, process lock, or read-before-
insert check.

## 2. Deliberately out of scope

Stage 2 does **not** implement:

- an outbox publisher or a change-data-capture process;
- NATS JetStream or any other message broker;
- an outbound HTTP client, webhook request, or HMAC header;
- delivery execution, concurrency control, timeouts, response classification,
  retry backoff, jitter, dead letters, or replay of dead letters;
- delivery-attempt creation—the table exists, but ingestion creates zero rows;
- endpoint update/disable/rotation APIs or API-key management after bootstrap;
- a public signup flow; bootstrap is a protected deployment operation;
- rate limiting, tenant storage quotas, payload quotas, or an explicit
  whole-request byte limit;
- complete SSRF protection, including IP classification, DNS rebinding,
  redirects, cloud-metadata targets, or egress policy;
- TLS termination in Uvicorn/Compose, proxy trust configuration, or production
  ingress;
- production key management, managed encryption keys, automatic rotation,
  audit, backup/restore, or recovery workflows;
- tracing, metrics dashboards, operations UI, load tests, or cloud deployment.

Field-level validation is not a whole-request limit. `HttpUrl` parsing and an
HTTPS requirement outside local/test are not SSRF defenses. AES-GCM encryption
with a process-supplied key is not a managed secret service. These are explicit
boundaries, not implied future behavior.

The schema includes future state vocabulary such as `delivering`,
`retry_scheduled`, `succeeded`, and `dead_lettered`. Stage 2 creates deliveries
only as `pending` and never advances them.

## 3. Architecture before and after

### Before Stage 2

Stage 1 has a deployable, observable process and PostgreSQL connection but no
domain model:

```text
monitor -> FastAPI -> /health/live
                    -> /health/ready -> bounded SELECT 1 -> PostgreSQL

no tenants -> no credentials -> no endpoints -> no events -> no outbox
```

The engine, pool, lifecycle, migration environment, Compose topology, tests, and
CI are ready. PostgreSQL contains no HookRelay domain tables.

### After Stage 2

```text
Deployment operator                           Producer
        |                                        |
        | bootstrap bearer                      | tenant API-key bearer
        v                                        v
+------------------------- FastAPI --------------------------------+
| /v1/bootstrap/tenants   /v1/tenant   /v1/endpoints   /v1/events  |
|                                                                |
| strict JSON and header validation                              |
|     -> credential verification                                 |
|     -> internally derived tenant scope                         |
|     -> one request-scoped AsyncSession                         |
+-----------------------------+----------------------------------+
                              |
                              v
                         PostgreSQL
  +-------------------------------------------------------------+
  | tenant -> API keys                                          |
  |        -> endpoints -> versioned encrypted signing secrets  |
  |        -> events -> delivery snapshots -> outbox messages   |
  |                                      \-> attempt schema only |
  +-------------------------------------------------------------+

Unpublished outbox --X--> NATS --X--> worker --X--> customer endpoint
                         not implemented in Stage 2
```

The API and PostgreSQL form the complete current acceptance boundary. A future
publisher starts from durable rows rather than being called inline by the event
route.

### Data relationships

```text
Tenant
  +-- ApiKey
  +-- WebhookEndpoint
  |     +-- EndpointSigningSecret (versions; one active)
  +-- Event (unique tenant + idempotency key)
        +-- Delivery (unique event + endpoint)
              +-- OutboxMessage (one delivery.requested)
              +-- DeliveryAttempt (zero rows in Stage 2)
```

Composite tenant foreign keys turn authorization assumptions into database
integrity rules. A delivery cannot point at an event, endpoint, or signing
secret from another tenant even if a future application bug tries.

## 4. Repository and file tour

Read files in this order. The sequence follows one request from public contract
to durable state.

### Configuration and application wiring

- [`src/hookrelay/config.py`](../../src/hookrelay/config.py) defines version
  `0.2.0`, validates the 32-byte URL-safe-base64 encryption key, tracks its key
  version, and controls the optional bootstrap surface. It rejects the
  documented development key in staging/production.
- [`src/hookrelay/main.py`](../../src/hookrelay/main.py) constructs one
  `SecretCipher` and one database component per application lifespan, registers
  the health/bootstrap/tenant/endpoint/event routers, and disposes the engine at
  shutdown.
- [`.env.example`](../../.env.example) and
  [`compose.yaml`](../../compose.yaml) expose local-only bootstrap/encryption
  examples. They are configuration documentation, not a production secret
  store.

### Public contracts and dependencies

- [`src/hookrelay/schemas.py`](../../src/hookrelay/schemas.py) contains strict
  Pydantic request/response models. Extra fields are rejected. Event payloads
  must be JSON objects with finite numbers; endpoint IDs must be unique and
  contain 1-100 UUIDs.
- [`src/hookrelay/api/dependencies.py`](../../src/hookrelay/api/dependencies.py)
  creates request sessions, enforces JSON media type, validates bootstrap and
  tenant bearer credentials, derives tenant context, and validates the
  `Idempotency-Key` grammar.
- [`src/hookrelay/api/errors.py`](../../src/hookrelay/api/errors.py) maps
  deliberate, validation, framework, and unexpected failures to sanitized
  `application/problem+json` responses.

### Domain routes

- [`src/hookrelay/api/tenants.py`](../../src/hookrelay/api/tenants.py) contains
  protected tenant bootstrap and authenticated current-tenant inspection.
- [`src/hookrelay/api/endpoints.py`](../../src/hookrelay/api/endpoints.py)
  creates a tenant endpoint and encrypted signing-secret version in one
  transaction, returns the raw secret once, and exposes secret-free metadata on
  later reads.
- [`src/hookrelay/api/events.py`](../../src/hookrelay/api/events.py) defines the
  exact `201` first/replay contract, adds `Location` and replay headers, commits
  only after the ingestion service succeeds, rolls back failures, and provides
  current event inspection.

### Security and ingestion logic

- [`src/hookrelay/security.py`](../../src/hookrelay/security.py) generates and
  parses API keys, hashes/verifies their random secrets, generates signing
  secrets, and implements the versioned AES-256-GCM envelope and AAD.
- [`src/hookrelay/ingestion.py`](../../src/hookrelay/ingestion.py) computes the
  request fingerprint, validates tenant endpoint/secret snapshots, resolves
  concurrent insert races, builds stable replay responses, and stages one
  outbox message per delivery. It deliberately leaves commit ownership with the
  route.

### Durable schema

- [`src/hookrelay/models.py`](../../src/hookrelay/models.py) describes ORM
  mappings, names constraints deterministically, and makes the domain
  relationships visible to Python and Alembic metadata.
- [`migrations/versions/20260802_0001_durable_event_ingestion.py`](../../migrations/versions/20260802_0001_durable_event_ingestion.py)
  is the first meaningful schema revision. Read its `upgrade()` in table order
  and `downgrade()` in reverse dependency order.
- [`migrations/env.py`](../../migrations/env.py) imports `Base.metadata` so
  `alembic check` can compare models with the migrated database.

An ORM model is not a migration. The model is the current Python expectation;
the revision is the ordered operation that changes an existing database.

### Tests, packaging, and CI

- [`tests/unit/`](../../tests/unit/) contains pure configuration, fingerprint,
  and cryptographic learning loops.
- [`tests/api/`](../../tests/api/) exercises exact HTTP behavior in-process,
  including response status/headers/media types and sanitization.
- [`tests/integration/`](../../tests/integration/) crosses the real PostgreSQL
  boundary for migrations, constraints, tenant isolation, transactions,
  idempotency, and concurrency.
- [`pyproject.toml`](../../pyproject.toml) adds `cryptography`, version `0.2.0`,
  and the `integration`, `concurrency`, and `security` pytest markers.
- [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) upgrades the real
  PostgreSQL schema before integration tests, checks model/migration alignment,
  and builds the image after static and behavioral checks pass.

## 5. Request and event-flow traces

### Exact route and header contract

| Method and route | Required input | Success | Important failures |
| --- | --- | --- | --- |
| `POST /v1/bootstrap/tenants` | bootstrap `Authorization: Bearer ...`; JSON body/content type | `201`; `Location: /v1/tenant`; raw API key once; no-store headers | `401`, disabled `404`, `415`, `422`, `503` |
| `GET /v1/tenant` | tenant API-key bearer | `200`; authenticated tenant | `401`, `404`, `503` |
| `POST /v1/endpoints` | tenant API-key bearer; JSON body/content type | `201`; `Location`; raw signing secret once; no-store headers | `401`, `415`, `422`, `503` |
| `GET /v1/endpoints/{id}` | tenant API-key bearer | `200`; no secret | `401`, opaque `404`, `503` |
| `POST /v1/events` | tenant API-key bearer; `Idempotency-Key`; JSON body/content type | first `201`; replay `201` plus `Idempotency-Replayed: true`; both `Location` | `400`, `401`, opaque `404`, conflict `409`, `415`, `422`, `503` |
| `GET /v1/events/{id}` | tenant API-key bearer | `200`; payload and current delivery state | `401`, opaque `404`, `503` |

All deliberate errors use `application/problem+json`. The current health routes
retain their Stage 1 response bodies rather than being forced into the domain
error shape.

### Protected tenant bootstrap

Request:

```http
POST /v1/bootstrap/tenants HTTP/1.1
Authorization: Bearer <deployment-bootstrap-token>
Content-Type: application/json

{"name":"Local demo","initial_api_key_name":"developer"}
```

Flow:

1. Reject the route with opaque `404 bootstrap_disabled` unless bootstrap is
   explicitly enabled.
2. Compare the supplied bearer token with the configured token in constant
   time; all invalid cases become `401 invalid_credentials`.
3. Generate a token shaped as `hrk_<12-char-public-id>.<43-char-secret>`.
4. Add the tenant and API-key row to one request transaction.
5. Commit before responding.
6. Return tenant metadata and the raw API key once with `Location: /v1/tenant`,
   `Cache-Control: no-store`, and `Pragma: no-cache`.

Only the API-key public ID, SHA-256 secret digest, last-four hint, ownership,
and lifecycle metadata remain in PostgreSQL.

### Tenant API-key authentication

For every tenant route:

1. Parse `Authorization: Bearer hrk_<public-id>.<secret>`.
2. Reject malformed grammar before attempting a secret comparison.
3. Look up the globally unique public ID while requiring an active tenant.
4. Reject a revoked or expired row.
5. Hash the presented 256-bit random secret and compare it in constant time.
6. Produce internal `AuthenticatedTenant(tenant_id, api_key_id)` context.
7. Apply that `tenant_id` to every domain lookup and insert.

The request never contains a trusted tenant ID. Missing, malformed, expired,
revoked, unknown, and wrong-secret inputs share the same `401` response and
`WWW-Authenticate: Bearer` header.

### Endpoint creation and secret storage

Request:

```http
POST /v1/endpoints HTTP/1.1
Authorization: Bearer <tenant-api-key>
Content-Type: application/json

{"name":"Billing receiver","url":"https://example.com/hooks"}
```

Flow:

1. Strict validation rejects blank/overlong names, unsupported URL forms,
   userinfo, fragments, and URLs over 2,048 characters.
2. Staging and production reject non-HTTPS destination schemes. Local/test may
   use HTTP for a loopback learning receiver.
3. Generate an endpoint UUID, secret UUID, and `whsec_` secret with 256 random
   bits.
4. Encrypt the secret with AES-256-GCM under the configured key/version. AAD
   binds tenant, endpoint, secret row, secret version, envelope version, and
   key version.
5. Commit the endpoint and encrypted secret row together.
6. Return endpoint metadata plus raw `signing_secret` once with `Location` and
   no-store headers.
7. A later `GET` returns endpoint metadata only.

The signing secret is encrypted rather than hashed because a future worker must
recover it to compute an HMAC. No HMAC is created in this stage.

### First event acceptance

Request:

```http
POST /v1/events HTTP/1.1
Authorization: Bearer <tenant-api-key>
Idempotency-Key: order-ord_123-v1
Content-Type: application/json

{
  "type": "order.created",
  "payload": {"order_id": "ord_123", "total": 42},
  "endpoint_ids": ["11111111-1111-1111-1111-111111111111"]
}
```

Flow:

1. Require `Idempotency-Key` with the exact 8-128 character
   `[A-Za-z0-9._:-]` grammar.
2. Strictly validate an event type of 1-100 allowed characters, a JSON-object
   payload with no non-finite floats, and 1-100 unique endpoint UUIDs.
3. Authenticate the API key and derive tenant/API-key IDs.
4. Canonicalize operation, type, payload, sorted endpoint set, and fingerprint
   version; hash the UTF-8 JSON with SHA-256.
5. Look for an existing `(tenant, key)` event.
6. Verify every requested endpoint is active, belongs to this tenant, and has
   one active signing-secret version. An incomplete set returns an opaque
   `404`; it does not identify which ID is missing or belongs elsewhere.
7. Insert the event with PostgreSQL
   `ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING id`.
8. For a new event, create one `pending` delivery per sorted endpoint. Snapshot
   `target_url` and `signing_secret_id`.
9. Create one version-1 `delivery.requested` outbox row per delivery. Its JSON
   contains IDs and schema/type metadata, not the event payload or secret.
10. Flush constraints and commit the request transaction.
11. Return `201 Created`, `Location: /v1/events/{id}`, and the event with
    pending delivery IDs. The first response omits `Idempotency-Replayed`.

If a database insert/constraint/commit step raises a SQLAlchemy failure, the
route rolls back and returns sanitized `503 database_unavailable`. An unexpected
non-database exception also rolls back and reaches the sanitized `500` boundary.
Neither path returns `201` for a partial transaction.

### Matching replay

A response can be lost after commit. When the producer repeats the same tenant,
key, type, payload, and endpoint set:

1. Load the existing event.
2. Compare stored fingerprint version and digest in constant time.
3. Load its original delivery IDs in deterministic endpoint order.
4. Rebuild the original creation representation.
5. Commit/close the read transaction normally.
6. Return **HTTP `201` again**, the same `Location`, the same event/delivery IDs,
   and `Idempotency-Replayed: true`.

The replay body intentionally reports the original creation state `pending`.
A future delivery state change belongs in `GET /v1/events/{id}`, not in a
response whose idempotency contract is stable reproduction.

No event, delivery, outbox, or attempt row is added on replay.

### Conflicting key reuse

If type, payload, or endpoint set changes while the tenant/key remains the same,
the fingerprint differs. HookRelay returns exactly:

```json
{
  "type": "urn:hookrelay:problem:idempotency-key-reused",
  "title": "Idempotency key reused",
  "status": 409,
  "code": "idempotency_key_reused",
  "detail": "The Idempotency-Key was already used for a different request."
}
```

The request creates no new rows. The producer must choose a new key for new
logical work.

### Concurrent same-key race

Two requests can both run the preliminary lookup before either inserts:

```text
request A: SELECT -> missing ---- INSERT wins ---- deliveries/outbox ---- COMMIT
request B: SELECT -> missing ---- INSERT conflicts/waits -------- load A -> replay
```

PostgreSQL's unique constraint is the shared arbiter. The loser's
`ON CONFLICT DO NOTHING` path loads the committed winner and then performs the
same fingerprint decision. A process-local lock would not protect a second API
process.

### Event inspection

`GET /v1/events/{event_id}` authenticates and filters by tenant. It returns the
event payload and current delivery states, but no idempotency key, request
fingerprint, outbox contents, target URL snapshot, or signing secret. A UUID
owned by another tenant is indistinguishable from a nonexistent one.

## 6. Definitions of new technology and terms

The cumulative [glossary](../glossary.md) has fuller definitions. These are the
Stage 2 concepts to be able to explain without reading code.

| Term | Working definition |
| --- | --- |
| Tenant | The customer isolation boundary derived from a verified API key |
| Bearer credential | A secret whose possession grants its authority; it requires protected transport/storage |
| API-key public ID | Non-secret token segment used for indexed credential lookup |
| Secret digest | One-way stored result used to verify a high-entropy API-key secret |
| Signing secret | Recoverable endpoint credential a future worker will use for HMAC |
| AES-256-GCM | Authenticated encryption that hides plaintext and detects modification |
| Nonce | Unique per-encryption value stored with AES-GCM ciphertext |
| AAD | Unencrypted row context covered by the ciphertext integrity check |
| Idempotency | Repeating one logical operation does not create additional effects |
| Request fingerprint | Versioned SHA-256 digest of canonical logical request fields |
| Canonicalization | Deterministic representation that removes irrelevant ordering/whitespace differences |
| Transaction | Atomic group of database changes that all commit or all roll back |
| Transactional outbox | Domain state and a durable to-be-published message written in one database transaction |
| Dual write | Updating two independent systems with a crash window between them |
| Delivery snapshot | Accepted copy of endpoint URL plus signing-secret version reference |
| Composite foreign key | Multi-column relationship that includes tenant identity to prevent cross-tenant links |
| JSONB | PostgreSQL JSON storage used for event and outbox objects |
| Partial index | Index covering only rows matching a predicate, such as unpublished outbox rows |
| Problem Details | Stable structured HTTP error object returned as `application/problem+json` |
| Replay | Reproduction of an earlier creation result; not a new event or a dead-letter retry |
| Delivery attempt | One future worker execution record; schema exists, Stage 2 creates none |

## 7. Why each design was chosen

### Tenant identity from authentication

Accepting `tenant_id` in the body would let a caller request another tenant's
scope and rely on every handler remembering an authorization comparison. A
verified API key produces tenant context once. Queries still filter by that
context, and composite foreign keys enforce it again at the database boundary.

### High-entropy API keys with digest-only storage

HookRelay needs to verify an inbound key, not recover it. A public lookup ID
avoids scanning every digest, and a 256-bit random secret makes offline guessing
impractical. Digest-only storage reduces the impact of a database-only leak.
This reasoning does not apply to low-entropy human passwords.

### AES-GCM for endpoint signing secrets

A future worker must recover a signing secret to calculate an HMAC. Hashing it
would destroy required information. AES-GCM provides confidentiality and
integrity; AAD makes ciphertext relocation to another tenant/endpoint/version
fail. Version metadata leaves room for an explicit rotation design.

### One-time raw-secret responses

Returning credentials only at creation narrows plaintext exposure and makes the
client responsible for secure capture. No-store headers discourage caches.
They cannot prevent client logging or screenshots, so this is exposure
reduction rather than a magical erasure guarantee.

### PostgreSQL-enforced idempotency

Only the shared database can serialize requests across processes. The unique
tenant/key constraint closes the race; a preliminary lookup only speeds up the
common replay path. A fingerprint prevents silent acceptance of changed work
under an old key.

### Versioned canonical fingerprints

Clients should not get a conflict because JSON object members or endpoint IDs
were reordered. Canonicalization removes those irrelevant differences. The
version is stored because changing identity rules later can otherwise reinterpret
historical keys silently.

### `201` for first acceptance and replay

Both responses represent the outcome of the same create operation. Keeping
status, body, IDs, and `Location` stable makes an ambiguous-response retry easy
for clients. The replay header makes the server's path observable without
inventing a second resource or a second response schema.

### One transaction for event, deliveries, and outbox

The API must never acknowledge an event that lacks durable dispatch intent.
Putting all rows in one PostgreSQL transaction gives a local atomicity boundary
that can be forced to fail and inspected. It also postpones broker availability
from the request path.

### One delivery and outbox row per endpoint

Targets fail and retry independently. Per-target delivery identity is the
natural unit for later scheduling and attempts. Per-delivery outbox messages
avoid another fan-out ambiguity after publication.

### ID-only outbox payload

The future broker does not need raw signing secrets or entire event bodies.
IDs keep the message small and reduce secret/data duplication; the worker can
load authoritative PostgreSQL state. The schema version lets consumers reject
or adapt to future shapes deliberately.

### Endpoint and secret-version snapshots

Configuration can change after acceptance. Snapshotting the URL and referencing
the exact secret version preserves historical delivery intent. Otherwise a
later edit could silently redirect already accepted work.

### Stable Problem Details and opaque `404`

Machine-readable error codes let clients respond without parsing prose.
Sanitized validation pointers help fix requests without echoing data. Treating
cross-tenant and missing resources identically avoids an identifier-enumeration
oracle.

## 8. Serious alternatives and why they were not selected

### Publish to NATS directly after database commit

It has a crash window in which the API has acknowledged the event but no
message exists. A later reconciliation scan would recreate an outbox-like
durable handoff. Stage 2 makes that handoff explicit now.

### Publish before database commit

A worker could see work that later rolls back. Holding the database transaction
open during broker I/O also consumes connections/locks without creating
cross-system atomicity.

### Distributed two-phase commit

It adds coordination, availability, and operational cost and does not extend
cleanly through the planned HTTP receiver boundary. The outbox accepts
at-least-once publication and designs consumers for duplicates.

### Poll the delivery table as the queue

PostgreSQL-backed queues can be valid. A separate outbox gives publish intent
its own immutable identity, index, schema version, and audit boundary while
leaving delivery state as domain state.

### Process-local idempotency cache or lock

It disappears on restart and cannot coordinate several processes/hosts. It may
be a performance layer later, but the unique database constraint remains the
correctness boundary.

### Return the old event for any reused key

This hides a client bug: changed input appears accepted even though it was
discarded. Fingerprint comparison and `409` make the conflict actionable.

### Create a new event automatically on fingerprint conflict

This makes a single operation key identify multiple effects. The producer must
choose a new key to communicate intent for new work.

### Return `202 Accepted`

`202` commonly means processing has been accepted for later completion. Stage 2
has already completed its stated operation—the durable database acceptance—when
it responds. It has not promised delivery. `201` plus precise documentation is
less ambiguous for this resource-creation contract.

### Store every secret in plaintext

It simplifies access but turns a database/backup read into immediately usable
credentials. The current split minimizes recoverability according to need.

### Hash signing secrets too

One-way storage would prevent the future worker from computing HMACs. A
receiver cannot verify a secret HookRelay no longer has.

### Encrypt API keys too

Inbound verification does not require recovery. Encrypting them would grant
unnecessary raw-key recovery power to the application and key holders.

### Password hashing for generated API keys

Argon2id is suitable for human passwords. HookRelay secrets are uniformly
random 256-bit values, so SHA-256 verification is efficient without enabling a
feasible guessing attack. Do not generalize this choice to passwords.

### One signing-secret column on each endpoint

It cannot cleanly represent rotation or bind historical deliveries to the
accepted version. Separate version rows preserve identity and allow one active
version through a partial unique index.

### Trust a tenant ID supplied in the body

It expands the confused-deputy surface and places authorization correctness in
every route. Deriving tenant scope from the credential makes the secure path the
normal path.

### SQLite substitutes in integration tests

They would not exercise PostgreSQL JSONB, partial indexes, regex checks,
`ON CONFLICT`, transaction visibility, or concurrent uniqueness. Unit tests can
remain infrastructure-free, but persistence claims need the real database.

## 9. Failure modes and design tradeoffs

### Lost success response

The producer times out after PostgreSQL commits. The correct response is to
retry the exact request with the exact key. HookRelay returns the original IDs,
`201`, and the replay header. A new key would intentionally create a new event.

### Conflicting retry

The caller changes type, payload, or target set but keeps the key. HookRelay
returns stable `409`; repeated retries remain `409`. The fix is to decide
whether the original work was intended and use a new key only for genuinely new
work.

### Concurrent first submissions

Several requests can reach the insert together. One wins the unique constraint;
matching losers replay, differing losers conflict. If losers instead return
`503` or create extra rows, the PostgreSQL race path is broken.

### Failure after event insert but before outbox insert

The transaction rolls back the event and any deliveries. A database failure
becomes sanitized `503`; an unexpected internal failure becomes sanitized
`500`. In either case no `201` was promised, so a same-key retry is safe.
Catching the exception and committing partial rows would violate the primary
invariant.

### Database outage or schema not migrated

Authentication itself needs PostgreSQL, so tenant routes return sanitized
`503`. Liveness remains `200`; readiness becomes `503`. If the database is
reachable but tables are missing, run the explicit Alembic upgrade rather than
calling `create_all()` or auto-migrating during API startup.

### Global session or overly broad transaction

A global session can mix tenants and rollbacks across requests. Conversely,
waiting for a broker or destination inside the ingestion transaction would hold
connections/locks across slow external I/O. The request session owns only the
local database unit of work.

### Bootstrap left enabled

A stolen deployment token could create unauthorized tenants and keys. Use a
high-entropy managed token, restrict the network surface, audit provisioning,
and disable bootstrap after initial setup. The local Compose default is a
teaching convenience, not deployment guidance.

### Raw API key or signing secret lost by the client

HookRelay cannot re-display an API key because it stores only a digest. It also
does not expose endpoint signing-secret recovery. Stage 2 lacks replacement and
rotation APIs, so protect the creation response or reprovision local data.

### Encryption key lost or changed without rotation

Existing endpoint ciphertext becomes undecryptable. The key must be backed up
and restored separately from the database. Key-version metadata makes a future
rotation workflow possible; changing the configured version/value alone is not
that workflow.

### Ciphertext copied to another row

AES-GCM AAD should make decryption fail because tenant/endpoint/secret/version
context changed. Integrity failure must not fall back to plaintext or a default
secret.

### Endpoint URL risk and SSRF

Stage 2 does not contact stored URLs. `HttpUrl` and production HTTPS validation
still allow hosts that may resolve to loopback, private, link-local, or metadata
addresses. A future worker must validate resolved destinations, redirects, and
egress before outbound traffic is safe.

### Plain HTTP producer connection

Local Compose binds HTTP to `127.0.0.1`; bearer keys and event bodies are not
safe over an untrusted network. Production requires TLS termination and trusted
proxy/network configuration. Destination HTTPS validation is unrelated to the
producer-to-HookRelay hop.

### Oversized payload or request flood

Names, URLs, type, endpoint count, and header length are bounded, but JSON
payload byte size and overall request size are not explicitly capped. There is
also no rate/storage quota. A valid key can consume API memory, database space,
and rows faster than intended. Ingress body limits, application quotas, `413`
contracts, rate limiting, and load measurements remain required.

### Outbox growth

Every accepted delivery creates an unpublished row, and Stage 2 has no
publisher or cleanup. This is expected in a small learning environment but is
not sustainable operations. Stage 3 needs publisher lag metrics, batch/locking
design, safe retention, and failure recovery.

### Canonicalization changes

Fingerprint rules are durable protocol semantics. Recomputing old requests
with changed rules can turn a valid replay into conflict. Persisted versions
require explicit compatibility logic and tests during future changes.

### Snapshot tradeoff

Snapshotting preserves historical intent but means a newly corrected endpoint
URL does not repair already accepted deliveries automatically. A later product
must define whether operators can retarget/replay work; it must not mutate
history silently.

### Database connection pressure

Authentication and ingestion both use PostgreSQL. Async code avoids blocking
the event loop during I/O but does not create unlimited database capacity.
Pools, process counts, statement latency, transaction duration, and future
worker connections share one PostgreSQL budget.

## 10. Exact commands for running and testing

Run commands from the repository root. PowerShell examples use the checked-in
local values only. Replace all example secrets before any non-local use and do
not paste raw API keys or signing secrets into issue trackers, commits, or chat.

### Inspect prerequisites

```powershell
git --version
py -3.12 --version
docker version
docker compose version
```

The complete path requires a working Docker engine, not only the Docker CLI.

### Prepare local configuration

```powershell
Copy-Item .env.example .env
```

Open the ignored `.env` and replace at least:

```text
POSTGRES_PASSWORD
HOOKRELAY_SECRET_ENCRYPTION_KEY
HOOKRELAY_BOOTSTRAP_TOKEN
```

A canonical encryption key is URL-safe base64 for exactly 32 bytes. Generate a
fresh local value without printing it later in logs:

```powershell
$keyBytes = New-Object byte[] 32
$random = [Security.Cryptography.RandomNumberGenerator]::Create()
$random.GetBytes($keyBytes)
$random.Dispose()
$generatedEncryptionKey = [Convert]::ToBase64String($keyBytes).Replace('+','-').Replace('/','_')
$generatedEncryptionKey
```

Copy it into `.env`, then clear the shell variables if desired. The printed
value is a key; do not use this command in recorded CI output.

### Build, migrate, and start Compose in release order

```powershell
docker compose build
docker compose up --detach --wait postgres
docker compose run --rm api alembic upgrade head
docker compose up --detach --wait api
docker compose ps
```

This makes migration an explicit step before the API receives domain traffic.
The API never calls `create_all()` or Alembic on startup.

Check both health boundaries:

```powershell
curl.exe --fail http://127.0.0.1:8000/health/live
curl.exe --fail http://127.0.0.1:8000/health/ready
```

Both healthy bodies contain version `0.2.0`. Liveness does not prove the Stage 2
tables exist; the explicit migration did that work.

### Bootstrap one local tenant

The following token is the `.env.example` value. Substitute the value from your
untracked `.env` if you replaced it:

```powershell
$bootstrapToken = "replace-this-local-bootstrap-token-before-use"
$bootstrapBody = @{
  name = "Stage 2 learning tenant"
  initial_api_key_name = "local-developer"
} | ConvertTo-Json -Compress
$bootstrap = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/bootstrap/tenants `
  -Headers @{ Authorization = "Bearer $bootstrapToken" } `
  -ContentType "application/json" `
  -Body $bootstrapBody
$tenantId = $bootstrap.tenant.id
$apiKey = $bootstrap.api_key.key
```

Assignment does not print the returned raw key. Keep `$apiKey` in this terminal
only for the walkthrough. Inspect the tenant without exposing it:

```powershell
$authHeaders = @{ Authorization = "Bearer $apiKey" }
Invoke-RestMethod `
  -Method Get `
  -Uri http://127.0.0.1:8000/v1/tenant `
  -Headers $authHeaders
```

The GET body contains tenant metadata and no API-key field.

### Create and inspect one endpoint

```powershell
$endpointBody = @{
  name = "Local learning receiver"
  url = "http://127.0.0.1:9000/hooks"
} | ConvertTo-Json -Compress
$endpointCreated = Invoke-WebRequest `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/endpoints `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $endpointBody
$endpoint = $endpointCreated.Content | ConvertFrom-Json
$endpointId = $endpoint.id
$signingSecret = $endpoint.signing_secret
$endpointCreated.StatusCode
$endpointCreated.Headers["Location"]
$endpointCreated.Headers["Cache-Control"]
$endpointCreated.Headers["Pragma"]
```

Expected observations are `201`, `/v1/endpoints/{id}`, `no-store`, and
`no-cache`. Do not print `$signingSecret`. Confirm it is absent from inspection:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://127.0.0.1:8000/v1/endpoints/$endpointId" `
  -Headers $authHeaders
```

Nothing needs to listen on port 9000: Stage 2 stores the URL and sends no HTTP.

### Submit and replay an event

```powershell
$idempotencyKey = "stage2-order-ord_123-v1"
$eventBody = @{
  type = "order.created"
  payload = @{
    order_id = "ord_123"
    total = 42
  }
  endpoint_ids = @($endpointId)
} | ConvertTo-Json -Depth 5 -Compress
$eventHeaders = @{
  Authorization = "Bearer $apiKey"
  "Idempotency-Key" = $idempotencyKey
}
$first = Invoke-WebRequest `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $eventHeaders `
  -ContentType "application/json" `
  -Body $eventBody
$replay = Invoke-WebRequest `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $eventHeaders `
  -ContentType "application/json" `
  -Body $eventBody
$firstBody = $first.Content | ConvertFrom-Json
$replayBody = $replay.Content | ConvertFrom-Json
$first.StatusCode
$first.Headers["Location"]
$first.Headers["Idempotency-Replayed"]
$replay.StatusCode
$replay.Headers["Location"]
$replay.Headers["Idempotency-Replayed"]
$first.Content -eq $replay.Content
```

Expected output:

- first status `201`, a `Location`, and no replay-header value;
- replay status `201`, the same `Location`, and replay value `true`;
- body equality `True`, including the same event and delivery IDs.

Inspect current state:

```powershell
$eventId = $firstBody.id
Invoke-RestMethod `
  -Method Get `
  -Uri "http://127.0.0.1:8000/v1/events/$eventId" `
  -Headers $authHeaders
```

The delivery remains `pending`. No receiver has been contacted.

### Observe an exact `409` conflict

Keep the same key but change the payload:

```powershell
$changedEventBody = @{
  type = "order.created"
  payload = @{
    order_id = "ord_123"
    total = 99
  }
  endpoint_ids = @($endpointId)
} | ConvertTo-Json -Depth 5 -Compress
curl.exe --silent --show-error --include `
  --request POST `
  --header "Authorization: Bearer $apiKey" `
  --header "Idempotency-Key: $idempotencyKey" `
  --header "Content-Type: application/json" `
  --data $changedEventBody `
  http://127.0.0.1:8000/v1/events
```

Observe HTTP `409`, media type `application/problem+json`, code
`idempotency_key_reused`, and the exact detail documented in section 5. The
response must not contain the API key, submitted payload, fingerprint, or
database error.

### Inspect durable cardinality without reading secrets

The following query selects counts only for the walkthrough event:

```powershell
$countQuery = @"
WITH chosen_event AS (
  SELECT id
  FROM events
  WHERE tenant_id = '$tenantId'::uuid
    AND idempotency_key = '$idempotencyKey'
), chosen_deliveries AS (
  SELECT d.id
  FROM deliveries d
  JOIN chosen_event e ON e.id = d.event_id
)
SELECT
  (SELECT count(*) FROM chosen_event) AS events,
  (SELECT count(*) FROM chosen_deliveries) AS deliveries,
  (SELECT count(*) FROM outbox_messages o JOIN chosen_deliveries d ON d.id = o.delivery_id)
    AS outbox_messages,
  (SELECT count(*) FROM delivery_attempts a JOIN chosen_deliveries d ON d.id = a.delivery_id)
    AS delivery_attempts;
"@
docker compose exec --no-TTY postgres `
  psql --username hookrelay --dbname hookrelay --command $countQuery
```

For one endpoint, expect `1 | 1 | 1 | 0`. The replay and conflict do not change
those counts. Do not query or print `secret_hash`, `ciphertext`, raw shell
variables, or `.env` values as part of a demonstration.

### Host Python setup

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

For host-run API/database commands, point SQLAlchemy at the published host port:

```powershell
$env:HOOKRELAY_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay"
$env:HOOKRELAY_BOOTSTRAP_ENABLED = "true"
$env:HOOKRELAY_BOOTSTRAP_TOKEN = "replace-this-local-bootstrap-token-before-use"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\hookrelay.exe
```

Use the password and port from your `.env` if changed. The host process also
needs the same encryption key that owns existing ciphertext.

### Fast checks without PostgreSQL

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\mypy.exe
.\.venv\Scripts\pytest.exe -m "not integration"
```

Run focused Stage 2 loops:

```powershell
.\.venv\Scripts\pytest.exe tests/unit/test_stage2_contracts.py tests/unit/test_security.py
.\.venv\Scripts\pytest.exe tests/api/test_stage2_api_contracts.py
```

These use pure/in-process doubles where appropriate. They do not prove the real
PostgreSQL constraints or concurrent transaction path.

### PostgreSQL integration and migration checks

Use only a disposable local or CI database. With the example Compose database:

```powershell
$env:HOOKRELAY_TEST_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay"
$env:HOOKRELAY_DATABASE_URL = $env:HOOKRELAY_TEST_DATABASE_URL
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\pytest.exe -m integration
.\.venv\Scripts\alembic.exe check
.\.venv\Scripts\alembic.exe current --check-heads
```

The integration modules also run `alembic upgrade head` so they do not depend
on test collection order. If `HOOKRELAY_TEST_DATABASE_URL` is absent, local
integration tests skip; in CI, absence is a hard failure.

Target only the Stage 2 concurrency/security paths:

```powershell
.\.venv\Scripts\pytest.exe tests/integration/test_event_ingestion.py -m "integration and concurrency"
.\.venv\Scripts\pytest.exe tests/integration/test_event_ingestion.py -m "integration and security"
```

### Container checks and shutdown

```powershell
docker compose config --quiet
docker build --pull --tag hookrelay:stage2 .
docker compose down
```

Normal shutdown preserves the named PostgreSQL volume. `docker compose down
--volumes` deletes local data and is intentionally not part of the learning
loop.

### CI order

The workflow runs:

1. locked dependency installation;
2. Ruff lint;
3. Ruff format check;
4. strict mypy;
5. non-integration unit/API tests;
6. `alembic upgrade head` against real PostgreSQL;
7. PostgreSQL integration tests;
8. `alembic check` for metadata drift;
9. Compose validation and Docker image build.

Each step protects a different boundary. Reordering integration tests before
migration would make failures depend on leftover schema state.

## 11. How each test works—and what it cannot prove

### Shared test harness

[`tests/conftest.py`](../../tests/conftest.py) enters ASGI lifespan and uses
HTTPX2's asynchronous in-process transport. This exercises route resolution,
dependencies, exception handlers, response serialization, and shutdown without
opening a TCP socket. It does not prove DNS, host-port publishing, proxy/TLS,
or container networking.

The Stage 2 integration module creates a real app pointed at the explicitly
configured PostgreSQL URL. It uses separate clients to overlap transactions and
a separate assertion engine so row checks observe committed state rather than a
request's session cache.

### Pure ingestion and schema contracts

[`tests/unit/test_stage2_contracts.py`](../../tests/unit/test_stage2_contracts.py)
contains:

- `test_request_fingerprint_is_stable_for_object_and_endpoint_order`: proves
  object member and endpoint list order do not change the digest; it does not
  prove historical compatibility with a future fingerprint version.
- `test_request_fingerprint_binds_type_payload_and_destination_set`: proves each
  logical identity component changes the digest; it does not prove collision
  impossibility in a mathematical sense.
- schema tests for invalid type/payload/fan-out/tenant injection, URL userinfo
  and fragments, finite JSON numbers, and name trimming; they do not impose a
  whole-request byte limit or resolve destination IPs.
- `test_event_response_uses_public_type_alias_and_separates_live_delivery_status`:
  proves the creation and current-state schemas have distinct status roles; it
  does not execute a worker.
- `test_outbox_builder_emits_one_id_only_message_per_delivery`: proves
  cardinality and absence of payload/secret keys in the in-memory builder; the
  integration test proves those rows actually commit.

### Credential and encryption contracts

[`tests/unit/test_security.py`](../../tests/unit/test_security.py) proves:

- generated API keys parse, verify, use exact prefixes/lengths, and avoid raw
  secret repr exposure;
- malformed token shapes fail and separately generated credentials differ;
- AES-GCM round trips with different nonces for the same plaintext;
- ciphertext tampering and tenant/endpoint/secret/version context swaps fail;
- the recorded encryption-key version and actual key are both required;
- invalid key sizes, versions, and envelopes are rejected.

These tests verify library use and local contracts. They do not prove operating-
system entropy health, remote key management, secure memory erasure, automatic
rotation, backup recovery, or resistance to a compromised application process.

### Fast API, error, and disclosure contracts

[`tests/api/test_stage2_api_contracts.py`](../../tests/api/test_stage2_api_contracts.py)
uses controlled session doubles to prove:

- missing, malformed, unknown, revoked, expired, and wrong-secret API keys share
  one sanitized `401` shape and do not echo presented tokens;
- bootstrap returns a raw API key once, persists digest material rather than raw
  text, commits once, sets no-store headers, and excludes the key from later
  tenant inspection;
- endpoint creation returns a raw signing secret once, retains ciphertext
  rather than plaintext, and excludes it from later endpoint inspection;
- missing/incorrect content type, malformed JSON, strict-field validation, and
  unknown routes use stable Problem Details without reflecting submitted values;
- event idempotency headers are required and grammar-checked;
- unexpected failures return/log a sanitized error type rather than the private
  exception text;
- OpenAPI describes bearer security, required headers, documented error media
  types, and distinct creation/inspection secret shapes.

Session doubles do not enforce PostgreSQL constraints, isolation, transaction
visibility, or rollback. They intentionally leave those claims to integration
tests.

### Real PostgreSQL ingestion contracts

[`tests/integration/test_event_ingestion.py`](../../tests/integration/test_event_ingestion.py)
contains the strongest Stage 2 evidence:

- `test_ingestion_atomically_commits_event_deliveries_and_outbox` submits to two
  endpoints and inspects one event, two URL/secret-version snapshots, two
  unpublished ID-only outbox rows, and zero attempts.
- `test_sequential_replay_and_conflict_preserve_one_row_set` locks the exact
  first `201`, replay `201`/header/body equality, `409` conflict, and unchanged
  `1/1/1/0` counts.
- `test_concurrent_identical_requests_collapse_to_one_event` uses 12 separate
  overlapping clients and a barrier that forces every request past the advisory
  precheck. Exactly one response is first acceptance, eleven are replay, and
  only one row set exists.
- `test_concurrent_conflicting_requests_have_one_winning_fingerprint` races two
  bodies under one key. One fingerprint wins; matching requests receive `201`,
  the other half receive `409`, and one row set remains.
- `test_outbox_build_failure_rolls_back_every_row_and_does_not_poison_key`
  injects a failure after event staging, observes zero rows, then proves the same
  key can succeed as a fresh acceptance.
- `test_idempotency_and_resource_access_are_tenant_scoped` proves foreign
  endpoints are opaque `404`, tenants may reuse the same key independently, and
  cross-tenant event reads remain opaque.
- `test_composite_foreign_keys_reject_cross_tenant_domain_rows` bypasses the API
  and proves PostgreSQL itself rejects cross-tenant delivery and attempt
  relationships.

These tests exercise one PostgreSQL instance and a bounded set of orchestrated
interleavings. They do not exhaust every scheduler timing, prove behavior during
network partitions/failover, measure load capacity, or test a broker/receiver.

### Alembic and Stage 1 continuity

[`tests/integration/test_alembic.py`](../../tests/integration/test_alembic.py)
applies every revision, checks model metadata for drift, and verifies the
database is at every head. Existing readiness tests still cross the real driver
and confirm liveness/readiness separation and recovery.

An upgrade test does not prove a future large-table migration is nonblocking,
that downgrade is appropriate for production data, or that backups restore.
Those require migration-specific rollout evidence.

### Component learning loops

#### Authentication and tenant isolation

1. **Problem:** a caller must not choose or enumerate another tenant.
2. **Build:** verify a bearer key, derive tenant context, filter every query, and
   add composite tenant constraints.
3. **Inspect:** read route queries and migration foreign keys; run security
   integration tests.
4. **Break safely:** present malformed/wrong keys and cross-tenant UUIDs.
5. **Explain:** uniform `401`/opaque `404` reduce disclosure; database
   constraints protect relationships even if a route regresses.

#### Credential-storage loop

1. **Problem:** raw stored credentials magnify a database leak, but signing
   secrets must be recoverable.
2. **Build:** hash random API-key secrets; encrypt signing secrets with bound
   AES-GCM context; return raw values once.
3. **Inspect:** verify stored digest/ciphertext and no-store/inspection responses.
4. **Break safely:** tamper with ciphertext or swap one AAD identifier in a unit
   test.
5. **Explain:** verification-only and recovery-required secrets need different
   storage; application/key compromise remains in scope.

#### Transaction and outbox loop

1. **Problem:** an accepted event without durable dispatch intent is stranded.
2. **Build:** stage event, deliveries, and outbox in one request transaction;
   commit before `201`.
3. **Inspect:** query `1/N/N/0` committed cardinality and unpublished rows.
4. **Break safely:** inject outbox construction failure after event staging.
5. **Explain:** rollback removes every uncommitted row; outbox atomicity closes
   one dual-write gap but does not make later publication exactly once.

#### Idempotency loop

1. **Problem:** response loss makes retry necessary and duplicate creation
   dangerous.
2. **Build:** canonical fingerprint, tenant/key unique constraint, `ON CONFLICT`
   winner, replay/`409` split.
3. **Inspect:** compare status, headers, IDs, and row counts.
4. **Break safely:** force 12 requests past the precheck simultaneously, then
   race two different fingerprints.
5. **Explain:** the unique database constraint—not a precheck—is the shared
   serialization point.

#### Model and migration loop

1. **Problem:** code expectations do not change an existing database.
2. **Build:** update ORM metadata and an explicit reviewed revision.
3. **Inspect:** run `alembic upgrade head`, `alembic check`, and
   `current --check-heads`.
4. **Break safely:** in a disposable database, change a model without a
   revision and observe drift detection.
5. **Explain:** autogeneration/checks help find differences; humans still own
   rollout, data transformation, locks, and rollback decisions.

## 12. Safe “break it intentionally” exercise

### Goal

Force the idempotency race with real concurrent HTTP requests and prove that
PostgreSQL converges them to one durable row set. Then reuse the key for changed
input and prove conflict does not mutate that set.

This exercise stores local rows only. It does not stop PostgreSQL, delete a
volume, reveal ciphertext, or contact the endpoint URL.

### Preconditions

Complete the Compose migration, tenant bootstrap, and endpoint creation from
section 10. Confirm these variables still exist in the current PowerShell
session:

```powershell
$tenantId
$apiKey.Length -gt 0
$endpointId
```

The boolean length check avoids printing the credential.

### Launch 12 matching requests together

Use a fresh key so an earlier run cannot affect observations:

```powershell
$raceKey = "race-" + [Guid]::NewGuid().ToString("N")
$raceBody = @{
  type = "learning.race"
  payload = @{ exercise = "same-request"; run = $raceKey }
  endpoint_ids = @($endpointId)
} | ConvertTo-Json -Depth 5 -Compress
$jobs = 1..12 | ForEach-Object {
  Start-Job -ArgumentList $apiKey, $raceKey, $raceBody -ScriptBlock {
    param($token, $key, $body)
    $response = Invoke-WebRequest `
      -UseBasicParsing `
      -Method Post `
      -Uri http://127.0.0.1:8000/v1/events `
      -Headers @{
        Authorization = "Bearer $token"
        "Idempotency-Key" = $key
      } `
      -ContentType "application/json" `
      -Body $body
    [PSCustomObject]@{
      status = [int]$response.StatusCode
      replay = [string]$response.Headers["Idempotency-Replayed"]
      body = $response.Content
    }
  }
}
$results = $jobs | Wait-Job | Receive-Job
$jobs | Remove-Job
$results | Group-Object status, replay | Select-Object Count, Name
($results.body | Select-Object -Unique).Count
```

Expected observations:

- all 12 statuses are `201`;
- one response has an empty replay value and 11 have `true`;
- there is exactly one unique body with one event/delivery ID set.

Unlike the deterministic integration test, shell job startup cannot guarantee
all requests pass the precheck simultaneously. It is still a useful black-box
exercise; the barrier-based test supplies the deterministic race evidence.

### Inspect committed counts

```powershell
$raceCountQuery = @"
WITH chosen_event AS (
  SELECT id
  FROM events
  WHERE tenant_id = '$tenantId'::uuid
    AND idempotency_key = '$raceKey'
), chosen_deliveries AS (
  SELECT d.id
  FROM deliveries d
  JOIN chosen_event e ON e.id = d.event_id
)
SELECT
  (SELECT count(*) FROM chosen_event) AS events,
  (SELECT count(*) FROM chosen_deliveries) AS deliveries,
  (SELECT count(*) FROM outbox_messages o JOIN chosen_deliveries d ON d.id = o.delivery_id)
    AS outbox_messages,
  (SELECT count(*) FROM delivery_attempts a JOIN chosen_deliveries d ON d.id = a.delivery_id)
    AS delivery_attempts;
"@
docker compose exec --no-TTY postgres `
  psql --username hookrelay --dbname hookrelay --command $raceCountQuery
```

Expect `1 | 1 | 1 | 0`, not `12 | 12 | 12 | 0`.

### Reuse the key for changed work

```powershell
$conflictingRaceBody = @{
  type = "learning.race"
  payload = @{ exercise = "different-request"; run = $raceKey }
  endpoint_ids = @($endpointId)
} | ConvertTo-Json -Depth 5 -Compress
curl.exe --silent --show-error --include `
  --request POST `
  --header "Authorization: Bearer $apiKey" `
  --header "Idempotency-Key: $raceKey" `
  --header "Content-Type: application/json" `
  --data $conflictingRaceBody `
  http://127.0.0.1:8000/v1/events
```

Observe `409 idempotency_key_reused`, then rerun the count query. It must remain
`1 | 1 | 1 | 0`.

### Run the deterministic version

```powershell
$env:HOOKRELAY_TEST_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay"
.\.venv\Scripts\pytest.exe tests/integration/test_event_ingestion.py `
  -m "integration and concurrency" `
  -vv
```

The test barrier forces all 12 requests beyond the advisory lookup, so it proves
the insert/unique-constraint path rather than merely sending requests near each
other.

### Explain the result

Be able to say all four sentences:

1. The preliminary SELECT is an optimization and loses a forced race.
2. `UNIQUE (tenant_id, idempotency_key)` is the shared invariant across API
   processes.
3. `ON CONFLICT DO NOTHING` lets losers load the committed winner and compare
   fingerprints.
4. This makes ingestion idempotent, but it says nothing yet about duplicate
   broker publications, webhook attempts, or receiver side effects.

### Safe cleanup

No cleanup is required; the rows are valid learning data and the endpoint was
never contacted. Stop services with `docker compose down` to preserve the named
volume. Do not add `--volumes` merely to reset this exercise.

## 13. Troubleshooting

### Bootstrap returns `404 bootstrap_disabled`

Application settings default bootstrap to false. For local Compose, confirm the
untracked `.env` contains:

```text
HOOKRELAY_BOOTSTRAP_ENABLED=true
HOOKRELAY_BOOTSTRAP_TOKEN=<your-token>
```

Recreate the API container after changing Compose environment:

```powershell
docker compose up --detach --force-recreate --wait api
```

In staging/production, do not enable the route casually. Provisioning authority
and network access must be deliberate, and the route should be disabled again.

### Bootstrap or tenant access returns `401 invalid_credentials`

Check that the header is exactly bearer authentication and that bootstrap and
tenant credentials are not mixed:

```text
POST /v1/bootstrap/tenants -> deployment bootstrap token
all other /v1 domain routes -> hrk_... tenant API key
```

Do not trim, split, or reconstruct the generated API token. Missing, malformed,
unknown, expired, revoked, and wrong-secret keys intentionally return the same
response, so the public error will not reveal which check failed.

### A JSON POST returns `415 unsupported_media_type`

Every mutation route requires a JSON content type. With `Invoke-RestMethod` or
`Invoke-WebRequest`, supply `-ContentType "application/json"`. With curl, add:

```text
--header "Content-Type: application/json"
```

A JSON-looking body without the header is deliberately rejected.

### Event submission returns `400 idempotency_key_required`

`Idempotency-Key` is a header, not a JSON body field. Add it to the event
request. It is not required on bootstrap or endpoint creation.

### Event submission returns `400 invalid_idempotency_key`

Use 8-128 characters from letters, digits, `.`, `_`, `:`, and `-`. Spaces,
slashes, Unicode, and shorter keys are rejected before PostgreSQL work.

### A request returns `422 validation_error`

Inspect sanitized `errors[].pointer` and `code`. Frequent causes are:

- an extra field such as `tenant_id`—request models are strict;
- `event_type` instead of the public JSON field `type`;
- a non-object `payload` or `NaN`/infinity;
- no endpoint IDs, more than 100, duplicates, or malformed UUIDs;
- a blank/overlong name;
- URL userinfo, password, fragment, unsupported scheme, or excessive length;
- an HTTP endpoint URL while `HOOKRELAY_ENVIRONMENT` is staging/production.

The error omits submitted values by design. Reproduce locally with a
non-sensitive body if more inspection is needed.

### Event submission returns opaque `404 resource_not_found`

At least one requested endpoint is missing, disabled, belongs to another
tenant, or lacks an active signing-secret version. HookRelay does not say which
condition occurred because that detail can reveal another tenant's identifiers.
Verify the endpoint with the same API key using
`GET /v1/endpoints/{endpoint_id}`.

### Event submission returns `409 idempotency_key_reused`

That tenant already used the key for a different type, payload, or endpoint
set. It is not transient. Retrieve the original event if its ID was retained,
or reconcile producer state. Use a new key only if the changed input represents
intentionally new work.

Changing JSON object-member or endpoint-list order alone should replay rather
than conflict. Payload array order is meaningful and can conflict.

### Replay returns `201` but the replay header appears missing

The first successful request intentionally omits the header. A matching later
request sets the exact value `Idempotency-Replayed: true`. Confirm that the same
API key, key header, type, payload, and endpoint set were sent. A new tenant or
new idempotency key is a first acceptance even if its body looks similar.

Some client libraries normalize response header names to lowercase; header
names are case-insensitive.

### Event stays `pending` and the receiver sees nothing

That is the correct Stage 2 outcome. There is no outbox publisher, NATS
integration, worker, outbound HTTP request, HMAC, or delivery attempt. Inspect
one unpublished outbox row per delivery and zero attempt rows. Delivery begins
in Stage 3.

### Domain routes return `503` or logs mention a missing relation

Check readiness, then apply migrations explicitly:

```powershell
curl.exe --include http://127.0.0.1:8000/health/ready
docker compose run --rm api alembic current
docker compose run --rm api alembic upgrade head
docker compose run --rm api alembic current --check-heads
```

Liveness can be `200` while the schema is missing because liveness deliberately
does not query PostgreSQL. Do not solve this by adding `create_all()` to startup.

### Readiness fails after changing `POSTGRES_PASSWORD`

The official PostgreSQL image uses bootstrap variables only when initializing a
new data directory. Changing `.env` does not rewrite credentials inside an
existing named volume. Restore the previous matching value or perform a
deliberate credential change inside PostgreSQL. Back up valuable data before
choosing any fresh-volume procedure; normal troubleshooting should not delete
the volume.

### Settings rejects the encryption key

`HOOKRELAY_SECRET_ENCRYPTION_KEY` must be canonical URL-safe base64 that decodes
to exactly 32 bytes. Staging/production reject the published development key.
`HOOKRELAY_SECRET_ENCRYPTION_KEY_VERSION` must be 1-32767.

Changing the key or version does not re-encrypt existing rows. Stage 2 has no
rotation workflow, so keep the owning key available and plan migrations before
any change.

### Settings rejects the bootstrap token

When enabled, the token must be 32-256 printable ASCII characters with no
spaces. Disable bootstrap or provide a valid managed token. Avoid passing it as
a command-line argument in environments where process listings or shell history
are collected.

### `alembic check` reports new upgrade operations

Model metadata and the reviewed revision differ. Inspect the diff; do not make
CI green by blindly autogenerating and accepting a migration. Decide whether
the model changed accidentally or a new reviewed schema/data transition is
required.

### Integration tests skip

Set an explicit disposable PostgreSQL URL:

```powershell
$env:HOOKRELAY_TEST_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay"
```

The suite skips locally without it and fails in CI if CI forgot it. A skipped
test is not evidence that concurrency or constraints passed.

### Concurrency tests hang or fail with connection timeouts

They open 12 overlapping requests and the fixture configures additional pool
capacity. Confirm the test uses the Stage 2 fixture settings, PostgreSQL is
healthy, and no unrelated host process exhausted the server connection budget.
Do not “fix” the test by removing the barrier or serializing requests; that
would erase the behavior it is meant to prove.

### Host-run API uses unexpected settings

Process environment variables take precedence over `.env` for the host process,
and shell variables can also affect Compose interpolation. Inspect only
non-secret names/values and clear stale overrides in the current terminal:

```powershell
Remove-Item Env:HOOKRELAY_DATABASE_URL -ErrorAction SilentlyContinue
Remove-Item Env:HOOKRELAY_BOOTSTRAP_ENABLED -ErrorAction SilentlyContinue
Remove-Item Env:HOOKRELAY_BOOTSTRAP_TOKEN -ErrorAction SilentlyContinue
```

Restart the process after changing settings; they are cached for process
lifetime.

### Curl reports malformed JSON on Windows

PowerShell/native quoting can alter JSON arguments. Build the body with
`ConvertTo-Json -Compress` as shown in section 10, or use
`Invoke-RestMethod`/`Invoke-WebRequest` with `-Body $json` and the explicit
content type. Do not debug by pasting a real bearer token into a public command
transcript.

### A large payload is rejected by a proxy but not locally

Stage 2 has no application-level whole-request byte contract. A proxy may apply
its own limit and status. Production needs one documented ingress/application
policy so clients receive predictable `413`/quota behavior. Do not infer that a
large local success is safe under hostile traffic.

### A stored URL resolves to an internal address

Do not add a quick worker that fetches it. Stage 2's URL syntax and HTTPS checks
are not SSRF protection. Implement resolved-IP, redirect, DNS-rebinding, IPv6,
metadata-address, and egress policy before enabling untrusted outbound delivery.

## 14. Recruiter and interviewer questions

The cumulative [interview guide](../interview-guide.md) contains longer answers.
Practice these Stage 2 versions with one architecture sketch.

### “What did Stage 2 actually deliver?”

It delivered authenticated, tenant-isolated, idempotent durable ingestion. A
successful request atomically commits an event, per-endpoint pending deliveries,
and unpublished outbox rows. It did not deliver the webhooks; NATS, publisher,
worker, HTTP attempts, and retries remain later stages.

### “How did you make retries safe?”

Require a producer operation key, scope it to the authenticated tenant, and
store a versioned fingerprint of type/payload/endpoint set. PostgreSQL uniquely
constrains tenant/key and resolves concurrent inserts. Matching retries return
the original IDs with `201` and a replay header; changed retries return `409`.

### “Why is the pre-insert lookup not enough?”

Two transactions can both read “missing” before either inserts. A process lock
also fails across replicas. The database uniqueness constraint is the shared
serialization point; `ON CONFLICT` lets a loser load and classify the winner.

### “Why use a transactional outbox?”

A database commit and broker publish are independent writes with a crash gap.
Writing dispatch intent alongside the event makes local acceptance atomic.
Later publish/mark is still at least once and may duplicate, so the outbox does
not justify exactly-once claims.

### “Why hash one secret and encrypt another?”

Inbound API keys need verification only, so one-way digest storage removes
unnecessary recovery. A future worker must recover endpoint signing secrets to
compute HMACs, so they use authenticated encryption and row-bound AAD. Both raw
values are returned only at creation.

### “How did you enforce tenant isolation?”

Derive tenant identity from a verified key, filter all queries by it, return
opaque cross-tenant `404`, and carry tenant ID through composite foreign keys.
The API and database layers reinforce rather than replace each other.

### “Tell me about a failure you proved.”

Choose one:

- force 12 requests past the idempotency precheck; observe one winner, 11
  replays, and one row set;
- inject outbox construction failure after event staging; observe complete
  rollback and successful fresh retry with the same key;
- submit a cross-tenant endpoint and bypass the API with a cross-tenant foreign
  key; observe opaque `404` and PostgreSQL rejection respectively.

State what the test cannot prove: no current test demonstrates NATS, receiver
delivery, production capacity, or every network-partition schedule.

### “What are the largest remaining security risks?”

Before untrusted production traffic: TLS ingress/proxy configuration,
whole-request and tenant quotas, rate limits, managed encryption/bootstrap keys,
credential rotation/revocation workflows, and complete SSRF/egress protection
before a worker sends HTTP.

### “Why not claim exactly once?”

The current database transaction is exactly one local atomic boundary. Future
publication can be duplicated around broker acknowledgment, and an HTTP
receiver can commit a side effect before its response is lost. Stable IDs and
consumer/receiver idempotency provide effectively-once business behavior; the
transport guarantee remains at least once.

## 15. Hands-on modification for Tarun

Add an authenticated, soft-disable operation for endpoints while preserving
accepted delivery snapshots.

Proposed contract:

```http
PATCH /v1/endpoints/{endpoint_id} HTTP/1.1
Authorization: Bearer <tenant-api-key>
Content-Type: application/json

{"enabled":false}
```

Acceptance criteria:

1. Add a strict request model that accepts only `enabled: false`; extra fields,
   missing values, and `true` are rejected with the existing validation shape.
2. Require tenant API-key authentication and JSON content type.
3. Filter the update by authenticated tenant. Missing and cross-tenant endpoint
   IDs return the existing opaque `404`.
4. Set `is_active = false` and commit in the request-scoped session. Repeating
   the same disable is idempotent and returns the same secret-free endpoint
   representation with HTTP `200`.
5. Never return/decrypt/re-encrypt the signing secret and never expose
   ciphertext or its hint.
6. Add an integration scenario that first accepts an event, disables the
   endpoint, and proves the existing delivery still retains its URL and
   signing-secret version snapshot.
7. Prove a new event selecting the disabled endpoint receives opaque `404` and
   creates no event, delivery, outbox, or attempt rows.
8. Prove a different tenant cannot disable or distinguish the endpoint.
9. Add API/OpenAPI and real PostgreSQL tests. Explain why a mocked route test
   alone cannot prove update isolation or snapshot persistence.
10. Do not add a migration: `webhook_endpoints.is_active` already exists. Be
    able to explain why “no migration needed” follows from schema inspection,
    not from forgetting migrations.
11. Update README/API documentation and describe the state transition without
    implying deletion, secret rotation, or cancellation of accepted work.
12. Run Ruff, formatting, mypy, fast tests, full integration, Alembic drift,
    Compose validation, and Docker build.

Questions to answer before coding:

- Should the route be `PATCH`, `DELETE`, or a command-style endpoint, and what
  does each choice communicate?
- Why must disabling configuration not mutate already accepted delivery
  snapshots?
- If disable races with event acceptance, which outcomes are legal under
  PostgreSQL's default isolation, and does the product need stronger ordering?
- Why should an inactive endpoint remain an opaque `404` during new ingestion?
- What future re-enable/rotation policy would require a new decision rather than
  silently reusing the old secret?

This task is intentionally small in schema surface but deep in state semantics.
Do not solve it with a global session or by deleting the endpoint row.

## 16. Comprehension quiz and teach-back checklist

### Quiz

Answer without looking at the guide, then verify against code and tests.

1. What exact durable rows does one accepted event targeting three endpoints
   create in Stage 2?
2. Which component does Stage 2 use to publish outbox rows to NATS?
3. Why is event creation `201` rather than evidence of webhook delivery?
4. What three request changes cause same-key reuse to become `409`?
5. Why do endpoint-list and JSON object-member order not cause a conflict?
6. What is the exact scope of an idempotency key?
7. Why is the read-before-insert idempotency check not sufficient?
8. What PostgreSQL statement/constraint path resolves concurrent winners?
9. What status/header/body behavior distinguishes first acceptance from a
   matching replay?
10. Why does a replay creation body report `pending` even if future current
    delivery state changes?
11. Why is the API-key public ID stored separately from its secret digest?
12. Why is SHA-256 suitable for this generated API-key secret but not a general
    password-storage recommendation?
13. Why can the signing secret not use the same irreversible storage?
14. What does AES-GCM AAD bind, and which attack does that help detect?
15. Which raw secrets appear once in which responses?
16. How does tenant identity enter an event row if it is absent from the body?
17. What do composite tenant foreign keys protect after application
    authorization has already run?
18. Why is there one delivery/outbox row per endpoint?
19. Why is the outbox payload ID-only, and what tradeoff follows?
20. Which injected test proves transaction rollback does not poison an
    idempotency key?
21. Which test deliberately forces 12 requests beyond the advisory precheck?
22. What do unit/session-double tests fail to prove about PostgreSQL?
23. Why is `HttpUrl` plus HTTPS not a complete SSRF defense?
24. Which producer-facing TLS protection does local Uvicorn provide?
25. Which input dimensions are bounded, and which whole-request control is
    absent?
26. Why can at-least-once delivery still duplicate a receiver side effect after
    this transactional outbox is complete?

<details>
<summary>Self-check answer ingredients</summary>

1. One event, three pending deliveries, three unpublished outbox messages, zero
   delivery attempts—all in one transaction.
2. None; publisher and NATS are not implemented.
3. It proves the local database acceptance transaction committed, not that any
   transport/worker/receiver action occurred.
4. Type, payload, or endpoint set under the same tenant/key.
5. Version-1 canonicalization sorts JSON object keys and endpoint UUIDs.
6. Authenticated tenant plus the 8-128-character key.
7. Two transactions/processes can both observe no row before either insert.
8. Unique `(tenant_id, idempotency_key)` plus PostgreSQL
   `INSERT ... ON CONFLICT DO NOTHING RETURNING`; the loser loads the winner.
9. Both are `201` with the same IDs/body/`Location`; replay alone adds
   `Idempotency-Replayed: true`.
10. It reproduces original creation state; GET event is the current-state view.
11. Public ID enables indexed lookup without making the secret itself a lookup
    or stored raw credential.
12. The input has 256 random bits, so offline guessing is infeasible; human
    passwords need a salted, slow password KDF such as Argon2id.
13. A future worker must recover it to compute an HMAC.
14. Tenant, endpoint, secret row, secret version, envelope version, and key
    version; ciphertext tampering/context substitution fails authentication.
15. Initial API key in bootstrap `201`; signing secret in endpoint-create `201`.
16. Verified bearer authentication produces internal tenant/API-key context.
17. They reject cross-tenant relationships even if application code is bypassed
    or later regresses.
18. Destinations execute/retry independently and each needs a worker identity.
19. It avoids duplicating payload/secrets on a broker; a worker must load
    authoritative database state.
20. `test_outbox_build_failure_rolls_back_every_row_and_does_not_poison_key`.
21. `test_concurrent_identical_requests_collapse_to_one_event` with an asyncio
    barrier and 12 separate clients.
22. Real constraints, JSONB/index behavior, transaction visibility, rollback,
    `ON CONFLICT`, and concurrent serialization.
23. It does not classify resolved IPs, redirects, rebinding, IPv6, metadata
    destinations, or egress paths.
24. None; local Compose is plain HTTP bound to loopback. Production needs TLS
    termination and a trusted network/proxy path.
25. Names, type, URL, endpoint count/uniqueness, idempotency grammar, and schema
    shapes are bounded; explicit request-body bytes/payload quota is absent.
26. Publisher or HTTP acknowledgment can be lost after the downstream system
    acted, making a retry necessary and duplicate observation possible.

</details>

### Three-to-five-minute teach-back

Use a blank page, not the repository, and cover:

- **0:00-0:30 — Problem and boundary:** response-loss ambiguity, durable
  acceptance, and no NATS/outbound attempts.
- **0:30-1:10 — Identity:** bootstrap token, one-time API key, digest
  verification, derived tenant scope, and opaque errors.
- **1:10-1:50 — Secret storage:** hashing versus encryption, AES-GCM/AAD,
  one-time signing-secret response, and managed-key limitation.
- **1:50-2:40 — First request:** strict validation, endpoint snapshots, event +
  N deliveries + N outbox rows, commit-before-`201`.
- **2:40-3:30 — Retry and race:** canonical fingerprint, matching `201` replay,
  different `409`, unique constraint, and `ON CONFLICT` winner.
- **3:30-4:20 — Evidence:** unit/API versus real PostgreSQL rollback,
  concurrency, tenant, and migration tests; one limitation per layer.
- **4:20-5:00 — Risks and next stage:** request size/quotas, TLS, SSRF, key
  management, outbox growth, and why future delivery remains at least once.

### Teach-back checklist

- [ ] I can draw only Stage 2 components and place publisher/NATS/worker outside
      the implemented boundary.
- [ ] I can state `1 event + N deliveries + N outbox + 0 attempts` and identify
      the one transaction.
- [ ] I can reproduce the first/replay `201` headers and exact `409` meaning.
- [ ] I can explain canonicalization and name what changes a fingerprint.
- [ ] I can explain why the unique constraint—not the preliminary query—closes
      the concurrency race.
- [ ] I can contrast API-key digest storage with signing-secret AES-GCM storage.
- [ ] I can name the AAD fields and the remaining key-management responsibility.
- [ ] I can trace tenant scope from bearer credential through queries and
      composite foreign keys.
- [ ] I can explain why URL and signing-secret snapshots preserve accepted
      intent.
- [ ] I can explain the dual-write problem and why an outbox still publishes at
      least once later.
- [ ] I can name the rollback, identical-race, conflicting-race, tenant, and
      cross-tenant-constraint tests.
- [ ] I can distinguish model metadata, Alembic revision, and migrated database.
- [ ] I can state the exact request-size, SSRF, and TLS limitations without
      claiming a partial defense is complete.
- [ ] I can implement the endpoint-disable exercise without mutating historical
      deliveries or exposing secrets.
- [ ] I can say honestly that no endpoint has been contacted in Stage 2.

## Stage 2 completion evidence checklist

Documentation is not evidence that an external check passed. Before declaring
Stage 2 complete, confirm:

- [ ] clean frozen dependency installation succeeds;
- [ ] the first Alembic revision upgrades a disposable PostgreSQL database;
- [ ] `alembic check` reports no unreviewed metadata drift and the database is at
      every head;
- [ ] Ruff lint/format, strict mypy, fast API/unit tests, and full PostgreSQL
      integration tests pass;
- [ ] concurrency tests actually run rather than skip and preserve one row set;
- [ ] rollback, tenant isolation, composite foreign-key, ciphertext, and
      redaction contracts pass;
- [ ] Compose validation and Docker build pass;
- [ ] the manual first/replay/conflict loop produces `201`/`201`/`409` with
      `1/N/N/0` durable counts;
- [ ] no raw API key, bootstrap token, signing secret, encryption key,
      ciphertext, or database password was committed or copied into logs;
- [ ] CI is green on the reviewed commit rather than inferred from a local run;
- [ ] Tarun can complete the three-to-five-minute teach-back and state the
      current scope limitations before Stage 3 begins.
