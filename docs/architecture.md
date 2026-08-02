# HookRelay architecture

This document is cumulative. It describes what the repository implements at
the end of the current stage and labels roadmap components explicitly so a
diagram is never mistaken for running behavior.

## Current system: Stage 2 durable ingestion

```text
Deployment operator                    Producer
        |                                 |
        | bootstrap bearer token          | tenant API key
        v                                 v
+----------------------- FastAPI / Uvicorn ------------------------+
| POST /v1/bootstrap/tenants   POST /v1/endpoints   POST /v1/events |
| GET  /v1/tenant              GET  /v1/endpoints   GET  /v1/events |
|                                                                |
| strict JSON -> authentication -> tenant scope -> unit of work  |
|                                                |               |
| /health/live -> process only                   |               |
| /health/ready -> bounded SELECT 1              |               |
+-----------------------------+------------------+---------------+
                              |
                              | one AsyncSession per request
                              | explicit transaction commit
                              v
                    SQLAlchemy async engine/pool
                              |
                              v
                         PostgreSQL
       +-------------------------------------------------+
       | tenants, api_keys, webhook_endpoints, secrets   |
       | events + deliveries + unpublished outbox rows  |
       | delivery_attempts schema, currently zero rows   |
       +-------------------------------------------------+

No outbox publisher -> no NATS -> no delivery worker -> no webhook HTTP
```

Stage 2 turns the Stage 1 service foundation into a durable intake boundary.
The word **accepted** has one precise meaning: PostgreSQL committed an event,
one pending delivery per requested endpoint, and one unpublished
`delivery.requested` outbox message per delivery. It does not mean a customer
endpoint was contacted.

## Runtime components

| Component | Responsibility | Explicit non-responsibility |
| --- | --- | --- |
| FastAPI application factory | Builds one app and wires settings, database, cipher, errors, and routers | It does not start a server or migrate the database |
| Pydantic request models | Enforce strict body shapes, bounded names/types, URL syntax, JSON-object payloads, and unique endpoint IDs | They do not impose a whole-request byte limit |
| Authentication dependency | Parses a bearer API key, finds its public ID, verifies the secret digest, and derives tenant context | It never trusts a caller-supplied tenant ID |
| Request-scoped `AsyncSession` | Owns one request's database unit of work | It is not shared globally between concurrent requests |
| `SecretCipher` | Encrypts and authenticates endpoint signing secrets with AES-256-GCM and versioned associated data | It is not a secret manager or automatic rotation service |
| Ingestion service | Canonicalizes the request, enforces idempotency, snapshots endpoints, and stages domain/outbox rows | It does not publish or send HTTP |
| PostgreSQL constraints | Enforce tenant relationships, idempotency uniqueness, valid states, and outbox cardinality during races | They do not replace application validation or authorization |
| Alembic | Applies the explicit Stage 2 schema transition | It never runs as an API startup side effect |
| Liveness/readiness routes | Separate process health from PostgreSQL availability | Readiness does not prove the domain schema is current or the service has production capacity |

## Public HTTP surface

All mutation routes require `Content-Type: application/json`. Deliberate API
failures use `application/problem+json` with stable `type`, `title`, `status`,
`code`, and `detail` fields. Validation failures may add sanitized JSON-pointer
entries under `errors`; submitted values and internal exceptions are not
echoed.

| Route | Scope and important headers | Success |
| --- | --- | --- |
| `GET /health/live` | public | `200`, no dependency call |
| `GET /health/ready` | public | `200` after `SELECT 1`; sanitized `503` on dependency failure |
| `POST /v1/bootstrap/tenants` | `Authorization: Bearer <bootstrap-token>`; bootstrap must be enabled | `201`, `Location: /v1/tenant`, `Cache-Control: no-store`, `Pragma: no-cache` |
| `GET /v1/tenant` | tenant API-key bearer authentication | `200`, authenticated tenant only |
| `POST /v1/endpoints` | tenant API-key bearer authentication | `201`, endpoint plus one-time secret and no-store headers |
| `GET /v1/endpoints/{endpoint_id}` | tenant-scoped lookup | `200`, never returns secret material |
| `POST /v1/events` | tenant API key and `Idempotency-Key` | `201`, `Location: /v1/events/{id}`; replay also adds `Idempotency-Replayed: true` |
| `GET /v1/events/{event_id}` | tenant-scoped lookup | `200`, payload and current delivery states |

Resources outside the authenticated tenant appear as the same opaque `404` as
missing resources. This prevents a lookup endpoint from becoming a tenant-ID
oracle.

## Provisioning and credential flows

### Tenant bootstrap

1. A deployment operator explicitly enables bootstrap and configures a
   32-256-character printable-ASCII token.
2. The operator sends that token as a bearer credential to
   `POST /v1/bootstrap/tenants`.
3. The API compares the supplied token in constant time.
4. One transaction creates the tenant and an initial API-key record.
5. The response returns the complete `hrk_<public-id>.<secret>` token once and
   forbids caching.
6. The database retains the public ID, a SHA-256 digest of the 256-bit random
   secret, the last four characters for identification, and revocation/expiry
   metadata. It never retains the raw secret.

Bootstrap is a deployment escape hatch for the first tenant, not a public
signup flow. It defaults to disabled in application settings and should be
disabled again after provisioning. The Compose example enables it only for the
local learning workflow.

### Tenant API-key authentication

1. The caller sends `Authorization: Bearer hrk_<public-id>.<secret>`.
2. Syntax is checked before database work: 12 URL-safe public-ID characters and
   43 URL-safe secret characters.
3. The API finds the key by globally unique public ID while requiring an active
   tenant.
4. Revoked or expired keys are rejected.
5. A SHA-256 digest of the presented high-entropy secret is compared with the
   stored digest using `hmac.compare_digest`.
6. Success produces internal `tenant_id` and `api_key_id` context. Domain
   routes never accept either value from the request body.

Malformed, missing, revoked, expired, and incorrect credentials all return the
same `401 invalid_credentials` response with `WWW-Authenticate: Bearer`.

### Endpoint signing-secret creation

An API key authenticates a caller; a signing secret will later authenticate a
HookRelay webhook to the receiver. They need different storage properties:

- an API key only needs verification, so its raw secret is irreversibly hashed;
- a signing secret will be needed to compute an HMAC later, so it must be
  recoverable and is encrypted instead.

`POST /v1/endpoints` creates the endpoint and secret in one transaction. The
secret is `whsec_` plus 256 random bits. AES-256-GCM supplies confidentiality
and integrity. The ciphertext envelope contains an envelope version, a unique
96-bit nonce, and the authenticated ciphertext/tag. Associated data binds the
ciphertext to tenant ID, endpoint ID, secret ID, secret version, envelope
version, and encryption-key version, so moving a ciphertext to another row
causes decryption to fail.

Only ciphertext, key version, secret version, and a four-character hint are
stored. The raw secret appears in the creation response once, with
`Cache-Control: no-store` and `Pragma: no-cache`. Later endpoint reads omit it.
The encryption key itself comes from process configuration; production still
needs an external secret manager, rotation procedure, access control, and
audit.

## Event ingestion flow

The request body is:

```json
{
  "type": "order.created",
  "payload": {"order_id": "ord_123", "total": 42},
  "endpoint_ids": ["11111111-1111-1111-1111-111111111111"]
}
```

The `Idempotency-Key` is an HTTP header, not a body field. Its grammar is 8-128
ASCII characters from letters, digits, `.`, `_`, `:`, and `-`.

### First successful submission

```text
producer
   |
   | POST /v1/events
   | Authorization: Bearer <tenant API key>
   | Idempotency-Key: demo-event-0001
   v
strict body and header validation
   -> authenticate key and derive tenant
   -> compute canonical request fingerprint
   -> verify every endpoint is active, tenant-owned, and has an active secret
   -> INSERT event ON CONFLICT DO NOTHING
   -> INSERT one pending delivery snapshot per endpoint
   -> INSERT one unpublished delivery.requested outbox row per delivery
   -> COMMIT
   -> 201 Created + Location
```

The response is not sent until commit succeeds. A database failure rolls the
transaction back and returns sanitized `503 database_unavailable`; the caller
can safely retry with the same idempotency key.

Each delivery snapshots the endpoint URL and signing-secret version ID. A later
endpoint change must not silently rewrite already accepted work. The outbox
payload is deliberately ID-only and versioned; it contains identifiers needed
to load current durable state but no endpoint signing secret or event payload.

### Same-key replay

The idempotency scope is `(tenant_id, Idempotency-Key)`. Version 1 of the
fingerprint hashes a canonical JSON representation containing:

- operation name `POST /v1/events`;
- event type;
- payload;
- endpoint IDs sorted by UUID text;
- fingerprint version.

Sorting JSON object keys makes object member order irrelevant. Sorting endpoint
IDs makes target-list order irrelevant. Payload array order remains meaningful.

If the tenant submits the same key and same versioned fingerprint, HookRelay
loads the original event and deliveries and returns:

- HTTP `201 Created`, not `200` or `202`;
- the same creation representation and stable IDs;
- the same `Location` header;
- `Idempotency-Replayed: true`.

The replay representation reports each original delivery as `pending`, even if
a future worker later changes current state. This keeps the creation response
stable. `GET /v1/events/{id}` is the separate current-state view.

### Same-key conflict

If the key already belongs to a different fingerprint, HookRelay creates no
new rows and returns:

```json
{
  "type": "urn:hookrelay:problem:idempotency-key-reused",
  "title": "Idempotency key reused",
  "status": 409,
  "code": "idempotency_key_reused",
  "detail": "The Idempotency-Key was already used for a different request."
}
```

Changing the event type, payload, or endpoint set is a different logical
request. A caller that intends new work must use a new key.

### Concurrent duplicate submissions

An application-level read before insert is only an optimization; two requests
can both observe “missing.” PostgreSQL closes the race with a unique constraint
on `(tenant_id, idempotency_key)` and
`INSERT ... ON CONFLICT DO NOTHING RETURNING`. One transaction wins. The loser
then loads the committed winner, compares the fingerprint, and replays or
returns `409`.

This is why idempotency is a database invariant rather than a Python `if`
statement. It works across processes and replicas that share PostgreSQL.

## Transactional outbox boundary

Without an outbox, this tempting sequence has a dual-write gap:

```text
commit event to PostgreSQL
publish delivery message to broker
```

If the process crashes between the two operations, the accepted event has no
dispatch message. Reversing the order creates the opposite bug: a consumer can
see work whose database transaction later rolls back.

Stage 2 writes the event, deliveries, and outbox messages in one PostgreSQL
transaction:

```text
BEGIN
  event
  + N delivery snapshots
  + N unpublished outbox messages
COMMIT
```

The invariant is all-or-nothing. A future Stage 3 publisher can repeatedly scan
the partial index on unpublished rows, publish to NATS JetStream, and mark a row
published. Publishing and marking still cannot be one cross-system atomic
transaction, so duplicate publication remains possible and consumers must be
idempotent.

There is no publisher in Stage 2. Every accepted outbox row remains
`published_at IS NULL`.

## Relational model and tenant isolation

```text
tenant
  +-- api_key
  +-- webhook_endpoint
  |     +-- endpoint_signing_secret (versioned; one active)
  +-- event (unique tenant + idempotency key)
        +-- delivery (one per selected endpoint)
              +-- outbox_message (one delivery.requested row)
              +-- delivery_attempt (schema only; none created in Stage 2)
```

Important database invariants include:

1. Composite foreign keys carry `tenant_id` through API keys, endpoints,
   secrets, events, deliveries, attempts, and outbox rows. Cross-tenant links
   are invalid even if an application query is wrong.
2. `(tenant_id, idempotency_key)` is unique for events.
3. `(event_id, endpoint_id)` is unique for deliveries.
4. `(delivery_id, topic)` is unique for outbox messages.
5. One partial unique index permits only one non-retired signing secret for an
   endpoint.
6. Payloads and outbox bodies must be JSON objects; digests have fixed lengths;
   state strings and timestamp relationships are checked.
7. Deletes use `RESTRICT` so an operator cannot casually erase an audit chain
   through cascading deletion.

Models make these relationships usable from Python. The Alembic revision is
what changes an existing database. Neither artifact substitutes for the other.

## Process, session, and database lifecycle

The Stage 1 ownership model remains:

```text
one application process
  -> one SQLAlchemy async engine and connection pool
  -> many short request-scoped AsyncSessions
  -> each session borrows connections for its transaction
  -> shutdown disposes the engine/pool
```

A session carries mutable unit-of-work and transaction state. Making it global
would allow concurrent requests to share pending objects, rollback one
another's work, or commit outside the intended boundary. The engine is the
long-lived concurrency-safe infrastructure; sessions are not.

## Health and deployment topology

Liveness remains dependency-free. Readiness still executes a bounded
`SELECT 1` through the real async engine. An unavailable database makes the API
unready but does not make the process appear dead, and readiness can recover on
the next request.

Docker Compose publishes API and PostgreSQL ports only on host loopback and
connects containers over a private network where the database hostname is
`postgres`. A named volume preserves PostgreSQL data across `docker compose
down`. Migrations remain an explicit operator/CI step before domain traffic.

Compose serves plain HTTP and is a local topology, not a production ingress.
Production requires TLS termination, trusted proxy configuration, managed
secrets, backups, restore drills, deployment ordering, and access controls.

## Verification boundaries

Stage 2 deliberately uses multiple evidence layers:

- Unit tests check deterministic fingerprinting, configuration, cryptographic
  envelope behavior, and small pure contracts without a network.
- In-process API tests check headers, status codes, problem bodies,
  authentication failures, and response redaction.
- Real PostgreSQL integration tests check the actual migration, constraints,
  transactions, rollback, tenant isolation, idempotent replay, and concurrent
  inserts.
- `alembic check` compares model metadata with the migrated schema.
- Compose validation and a Docker build check the packaged topology.

Mocks cannot prove PostgreSQL `ON CONFLICT`, JSONB, partial indexes, foreign
keys, or transaction visibility. A passing PostgreSQL suite cannot prove
production load, long-lived race coverage, proxy/TLS behavior, hostile payload
resistance, or receiver compatibility.

## Reliability and security invariants through Stage 2

1. Liveness has no external dependency; readiness touches real PostgreSQL.
2. A public `201` is emitted only after durable commit.
3. Event, deliveries, and outbox rows commit or roll back together.
4. One tenant/key pair identifies at most one logical event.
5. Same-request replay does not create extra events, deliveries, or outbox rows.
6. Different-request key reuse is an explicit `409`, not silent aliasing.
7. Tenant identity comes from verified credentials, never from a body field.
8. Composite constraints reject cross-tenant relationships at the database
   boundary.
9. Raw API-key secrets are not persisted; raw endpoint signing secrets are
   encrypted and returned only at creation.
10. Public errors do not expose submitted secrets, database URLs, ciphertext,
    or internal exception text.
11. No Stage 2 code claims a queued row was actually attempted or delivered.

## Current limitations and required follow-up

- **No message transport:** NATS JetStream and the outbox publisher arrive in
  Stage 3. Outbox rows remain unpublished.
- **No outbound delivery:** there is no HTTP client, HMAC request, worker,
  concurrency limiter, timeout policy, or receiver acknowledgment.
- **No attempts:** the `delivery_attempts` schema exists, but Stage 2 creates
  zero rows and changes no delivery out of `pending`.
- **No complete SSRF defense:** URL parsing and non-local HTTPS requirements do
  not block loopback, private, link-local, cloud-metadata, redirect, DNS
  rebinding, or resolved-IP attacks. No outbound fetch occurs yet. Those checks
  must exist before a worker contacts untrusted destinations.
- **No end-to-end TLS in the app:** Uvicorn serves HTTP. Local Compose is bound
  to `127.0.0.1`; a deployment needs trusted TLS termination. Requiring an
  HTTPS destination does not secure producer-to-API traffic.
- **No whole-request limit:** fields and target count are bounded, but the API
  has no explicit request-body byte cap or tenant payload quota. Ingress and
  application enforcement are required before hostile public traffic.
- **No rate or storage quota:** a valid key can currently submit work as fast as
  infrastructure allows. Rate limits and per-tenant quotas are future work.
- **No credential-management API:** the schema can represent expiration,
  revocation, and secret versions, but rotation/revocation workflows are not
  exposed yet.
- **No production secret manager:** configuration validation and encryption at
  rest reduce exposure; they do not provide managed-key access control,
  rotation, backup, audit, or recovery.

## Planned architecture, not current behavior

```text
Producer
  -> HookRelay API
  -> PostgreSQL event + transactional outbox       (implemented in Stage 2)
  -> outbox publisher
  -> NATS JetStream                                (Stage 3)
  -> bounded-concurrency delivery worker           (Stage 3)
  -> HMAC-signed HTTP request
  -> customer endpoint
     -> retries / jitter / dead letter / replay     (Stage 4)
     -> SSRF and traffic controls                   (Stage 5)
     -> observability and operations UI             (Stage 6)
     -> measured performance and deployment         (Stage 7)
```

Future delivery is at least once. PostgreSQL/outbox atomicity prevents one
important loss window, but it cannot atomically combine HookRelay state, broker
acknowledgment, an HTTP response, and an independent receiver database.
Duplicate delivery remains a normal possibility.

## Decision index

- [Python instead of Go](decisions/0001-python-over-go.md)
- [PostgreSQL instead of SQLite or MongoDB](decisions/0002-postgresql-over-sqlite-or-mongodb.md)
- [Explicit Alembic migrations](decisions/0003-explicit-alembic-migrations.md)
- [At-least-once delivery semantics](decisions/0004-at-least-once-delivery.md)
- [Transactional outbox](decisions/0005-transactional-outbox.md)
- [Tenant-scoped idempotency](decisions/0006-tenant-scoped-idempotency.md)
- [Credential and signing-secret storage](decisions/0007-credential-and-signing-secret-storage.md)
