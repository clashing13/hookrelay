# ADR 0006: Tenant-scoped idempotency with versioned request fingerprints

- Status: Accepted
- Date: 2026-08-02

## Context

Producers cannot always observe the response to event creation. A timeout can
occur before the request reaches HookRelay, during the database transaction, or
after commit while the successful response is lost. Telling a producer never
to retry risks event loss; blindly inserting on every retry duplicates work.

A key by itself is insufficient. A client can accidentally reuse the same key
for a different type, payload, or endpoint set. Silently returning an earlier
event would falsely imply that the changed request was accepted.

Correctness must also survive overlapping requests and multiple API processes.
An application lookup followed by insert has a race because two transactions
can both observe that no row exists.

## Decision

Require an `Idempotency-Key` header on `POST /v1/events`. Accept 8-128 ASCII
characters from letters, digits, `.`, `_`, `:`, and `-`. Scope the key to the
authenticated tenant and enforce uniqueness with PostgreSQL's
`UNIQUE (tenant_id, idempotency_key)` constraint.

Store a SHA-256 fingerprint of a versioned canonical request containing:

- the operation name `POST /v1/events`;
- event type;
- the complete JSON-object payload;
- the set of endpoint UUIDs;
- fingerprint version.

Canonical JSON sorts object members, and endpoint UUIDs are sorted before
encoding. JSON whitespace, object-key order, and endpoint-list order therefore
do not change identity. Payload array order remains meaningful.

Use PostgreSQL `INSERT ... ON CONFLICT DO NOTHING RETURNING` as the race arbiter:
one transaction inserts the event, while a loser loads the committed row and
compares the stored fingerprint in constant time.

Define the public results exactly:

- first accepted request: `201 Created`, creation body, and
  `Location: /v1/events/{id}`;
- same tenant/key/fingerprint: `201 Created`, the original creation
  representation and IDs, the same `Location`, and
  `Idempotency-Replayed: true`;
- same tenant/key but different fingerprint: `409 Conflict` with code
  `idempotency_key_reused` and no new rows.

The stable replay representation reports the original deliveries as `pending`.
The authenticated `GET /v1/events/{id}` route is the separate current-state
view.

## Serious alternatives

### Always return the old event for a reused key

This makes retries simple but hides accidental key reuse. A producer changing
the payload could receive success for work HookRelay never stored. Fingerprint
conflicts make that error explicit.

### Treat a changed request as new work

Automatically creating a second event under a new server identity makes the
same client key ambiguous and defeats the point of a retry token. The producer
must choose a new key when it intends new work.

### Application precheck or process-local lock only

Neither coordinates several processes or replicas, and both can lose a
read-then-insert race. The shared database uniqueness constraint is the actual
serialization point. The early lookup remains a latency optimization.

### Server-generated deduplication only

HookRelay cannot reliably infer which similar events are deliberate repeats.
Payload equality over a time window would collapse valid repeated business
events. A producer-supplied operation key communicates intent.

### Make the client event ID the primary key

This can be a sound API design. HookRelay instead keeps an opaque server event
UUID distinct from the operation key so key format/retention can evolve and
stable event identity is not coupled to a producer's namespace. The unique
tenant/key record still gives equivalent retry safety.

### Return `200` or `202` on replay

Either can be designed coherently. `202` would misleadingly suggest merely
deferred acceptance when the rows are already committed. Switching between
`201` and `200` would give clients another status branch for the same create
result. HookRelay chooses stable `201` and a replay header; the tests lock this
contract.

## Consequences

- Safe retries converge on one event and one set of delivery/outbox IDs.
- Two tenants may use the same textual key independently.
- The database constraint, not request timing, decides concurrent winners.
- Fingerprint version is durable data. Future changes need compatibility and
  replay rules rather than silently recomputing old rows under new semantics.
- Idempotency records consume storage for as long as their retry contract is
  honored; retention/expiry policy remains future work.
- The stored digest does not reveal the original payload by itself, but the
  event row intentionally retains the payload as the system of record.
- A `409` is a caller-correctable semantic conflict, not a transient database
  failure. Retrying unchanged changed input will not help.
- Idempotent ingestion does not make future broker publication, HTTP attempts,
  or receiver side effects exactly once.
