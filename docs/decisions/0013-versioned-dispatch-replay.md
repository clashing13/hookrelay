# ADR 0013: Replay with versioned dispatch generations and fresh outbox identity

- Status: Accepted
- Date: 2026-08-04

## Context

A dead-lettered delivery may become viable after an operator fixes the
destination or policy. Reusing the original message is unsafe: its work-queue
entry was ACKed at the terminal transition, and republishing its UUID as
`Nats-Msg-Id` can be suppressed inside JetStream's finite duplicate window.

Resetting attempt history would erase evidence, while applying a lifetime
attempt maximum would cause an exhausted delivery to dead-letter immediately
again. Old broker messages can also arrive after a replay and must not execute
the new retry cycle.

The Stage 3 broker schema has no generation field. Adding it would cause an old
strict Stage 3 worker to reject/terminate replay work during a rolling cutover.
Compatibility and stale-generation fencing must therefore be explicit without
mutating the broker contract.

## Decision

Give every delivery a positive `dispatch_generation`.

- Initial ingestion creates generation 1.
- Each accepted manual replay increments the generation under a delivery row
  lock.
- Attempt lifetime numbers remain monotonic, while retry count and backoff are
  calculated within the current generation.
- Outbox uniqueness becomes `(delivery_id, topic, dispatch_generation)`.

Keep the broker envelope at strict schema version 1 with the original seven
ID-only fields. Persist generation on delivery, attempt, and outbox rows. After
the worker reconciles `message_id` to the exact authoritative outbox row, it
derives that row's generation and compares it with the delivery.

Expose authenticated:

```text
POST /v1/deliveries/{delivery_id}/replay
```

Require JSON body:

```json
{"expected_dispatch_generation": 1}
```

Only a tenant-owned `dead_lettered` delivery at that exact generation is
eligible. Missing/cross-tenant IDs return opaque `404`; a changed generation
returns `409 delivery_generation_conflict`; another state returns
`409 delivery_not_replayable`.

In one transaction, increment the generation, clear retry/terminal/claim state,
set `pending`, and create a fresh schema-v1 outbox row with a new UUID. Commit
before returning `202 Accepted`, a pending replay representation, and
`Location: /v1/events/{event_id}`.

Workers reconcile broker and PostgreSQL generation. A message behind current
state is stale and ACKed without HTTP; a message ahead of state is poison and
terminated.

## Serious alternatives

### Reuse the original outbox row and UUID

The old broker work was acknowledged, and finite-window deduplication can
silently suppress a quick republish. Rewriting historical outbox identity also
damages auditability.

### Clear all attempt rows or reset lifetime numbering

This hides the cause and shape of earlier failure. Generation-specific counting
provides a fresh budget without destroying history.

### Keep one lifetime maximum

An exhausted delivery would have no capacity after an operator repair. A
generation expresses the deliberate decision to grant another bounded cycle.

### Create a separate delivery row

A new row would split one logical event/endpoint obligation and complicate
current-state reads. Generation is the retry-cycle identity; delivery identity
remains stable.

### Add a version-2 broker envelope with generation

This makes generation explicit in transport, but an old strict Stage 3 worker
would reject the unknown schema/field. Keeping v1 and deriving generation from
the exact outbox row supports payload compatibility while PostgreSQL remains
authoritative.

### Idempotency-key the replay endpoint

This can improve lost-response ergonomics but requires another durable
operation/fingerprint contract. Stage 4 instead requires the operator's
observed `expected_dispatch_generation`. Reusing the same intent after a lost
response cannot advance a second generation: it receives
`409 delivery_generation_conflict`, and the caller inspects current event state.

## Consequences

- Every replay obtains a fresh `Nats-Msg-Id` and transactional-outbox handoff.
- Historical attempts and earlier outbox rows remain inspectable.
- Retry maximum resets per generation without resetting lifetime attempt order.
- Old broker messages cannot execute a newer replay cycle.
- Stage 3 and replay messages use the same strict schema-v1 payload. Current
  workers recover generation from the matching outbox row.
- `202` proves only that PostgreSQL committed pending state and fresh outbox
  intent; it does not prove broker publication or webhook success.
- Concurrent/duplicate replay calls serialize through the row lock and
  expected-generation precondition; one operator intent advances at most once.
- Payload compatibility is not full mixed-worker replay safety. Manual replay
  should wait for Stage 4 worker cutover because an old worker can parse the
  message but lacks generation fencing and may send one extra stale request.
- The Stage 4 downgrade refuses to collapse multiple outbox generations into
  Stage 3's one-row uniqueness model.
- Replay remains at least once and can itself encounter the normal outbox
  publish/finalization ambiguity.
