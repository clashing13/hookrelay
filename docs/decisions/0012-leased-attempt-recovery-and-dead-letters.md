# ADR 0012: Lease attempts and dead-letter bounded failures

- Status: Accepted
- Date: 2026-08-04

## Context

Stage 3 committed an unfinished attempt and `delivering` before HTTP so an
external side effect never preceded all local evidence. If the worker died
after that commit, later deliveries saw an unfinished attempt and reported
progress forever. HookRelay needed a recoverable ownership boundary without
holding a database transaction across network I/O.

Retries also need a terminal outcome. Retrying a permanent response wastes
capacity, while retrying transient failures forever creates an unbounded retry
storm. A terminal decision must remain inspectable and replayable without
silently discarding the broker message first.

## Decision

Lease each outbound attempt.

Before HTTP, one short transaction writes a random `claim_token` to both the
attempt and delivery, records a database-time `claim_expires_at` on the
delivery, sets status `delivering`, and commits. The delivery claim TTL must be
strictly greater than the configured HTTP timeout plus the explicit
finalization margin (defaults: 10 + 5 < 20 seconds).

Use PostgreSQL `clock_timestamp()` for claim and due decisions rather than
transaction-start `now()`. Store attempt duration as `BIGINT` so recovering a
multi-week stale row cannot overflow 32-bit milliseconds.

A redelivery before expiry is delayed without another attempt. After expiry, a
worker holding the delivery row lock marks the unfinished attempt `abandoned`
with error `worker_lease_expired`, clears ownership, and counts that ambiguous
attempt against the current generation's maximum. It then schedules a retry or
dead-letters as exhausted.

An attempt finalizer must match attempt identity, generation, claim token,
delivery state/token, and an unexpired lease. A late worker that no longer owns
the attempt is fenced and performs no broker disposition from its stale handle.

Classify outcomes explicitly:

- `2xx`: success;
- timeout, async HTTP transport error, `408`, `425`, `429`, and `5xx`:
  transient failure;
- other HTTP responses: permanent failure.

A permanent failure moves directly to `dead_lettered`. A transient or abandoned
attempt dead-letters when the generation reaches its configured maximum.
PostgreSQL records a terminal timestamp and reason. The worker ACKs only after
the terminal transaction commits.

A target rejected by the temporary pre-Stage-5 outbound policy is persisted as
`dead_lettered` with reason `target_blocked` and then ACKed. No HTTP attempt is
created. Manual replay is available after a reviewed allowlist/policy change.
The worker retains a fixed delayed-NAK fallback only if blocking is raised
before the executor can persist that terminal decision.

## Serious alternatives

### Hold the row lock through HTTP

This prevents ownership changes but consumes a transaction, connection, and
lock while an independent service responds or stalls. It couples database
capacity directly to destination latency.

### Use attempt start time without a persisted lease

A runtime threshold applied to `started_at` changes meaning when configuration
changes and does not record the ownership deadline selected at claim time. A
persisted expiry makes the decision explicit.

### Heartbeat indefinitely

Heartbeats can extend truly long work, but the HTTP request already has a hard
deadline. Indefinite progress would recreate Stage 3's stuck-attempt behavior.

### Ignore abandoned attempts in the budget

The remote request may have completed. Ignoring it understates potential side
effects and permits an unlimited crash/retry loop.

### Retry all statuses

This is operationally simple but repeats requests that the default policy does
not expect to improve. Explicit classification makes the tradeoff reviewable.

### Separate dead-letter queue

A second stream would add publication, acknowledgment, retention, and
reconciliation boundaries. The authoritative delivery and attempt records
already support Stage 4 terminal inspection and replay.

## Consequences

- A crashed worker no longer leaves a delivery permanently `delivering`.
- At most one unfinished attempt is permitted by a partial unique index.
- Late finalizers cannot overwrite recovered state.
- Recovery time includes broker redelivery and the remaining database lease.
- Abandonment is explicitly recorded rather than pretending the HTTP outcome
  is known.
- Permanent, exhausted, and policy-blocked work becomes a durable terminal
  state before its broker message is ACKed.
- A dead letter is a PostgreSQL state, not a separate queue.
- Fencing does not retract a request already observed by the receiver; stable
  event-ID idempotency remains required.
- The global classification table is deliberately small and may need
  per-endpoint policy later.
- Downgrade refuses attempt duration outside Stage 3's former 32-bit range and
  active/scheduled/terminal Stage 4 delivery state rather than truncating or
  remapping recovery evidence.
