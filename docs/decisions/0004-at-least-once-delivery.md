# ADR 0004: At-least-once delivery instead of exactly-once claims

- Status: Accepted for the future delivery pipeline
- Date: 2026-08-01

## Context

A webhook receiver can finish a side effect and then lose the HTTP response
before HookRelay observes it. HookRelay cannot atomically commit both its own
database state and an arbitrary customer's database transaction. On timeout,
it cannot know whether the receiver did nothing, is still working, or
completed successfully.

Retrying protects against loss but may duplicate delivery. Not retrying avoids
some duplicates but can lose successfully queued work after transient failure.

## Decision

The future delivery pipeline will provide **at-least-once** delivery: an
accepted event remains eligible for retry until it is acknowledged or reaches
a documented terminal state. A destination may receive the same logical event
more than once.

Use a stable event ID on every attempt. Document and demonstrate receiver-side
idempotency, such as recording that ID under a unique constraint in the same
transaction as the receiver's side effect. Never describe HookRelay as a
general-purpose exactly-once system.

This ADR defines future semantics. Stage 1 has no ingestion endpoint, outbox,
broker, worker, or webhook delivery behavior yet.

## Serious alternatives

### At-most-once

Marking work complete before an attempt, or never retrying ambiguous failures,
reduces duplicate attempts but can silently lose events. That conflicts with
HookRelay's reliability goal.

### Exactly-once delivery

Exactly-once processing can be approximated within a tightly controlled single
transactional system, or the effect can be made effectively-once through
idempotency. It cannot generally be guaranteed across HTTP and an independent
customer database. Claiming it would hide the unavoidable ambiguity rather
than solve it.

## Consequences

- Retries and duplicate attempts are normal correctness behavior, not
  necessarily bugs.
- Receivers need an idempotency strategy based on stable event identity.
- Delivery attempts and logical events must be modeled separately.
- Metrics must distinguish events, attempts, successful acknowledgments, and
  duplicate observations.
- The transactional outbox planned for Stage 2 and broker/worker planned for
  Stage 3 reduce loss windows, but neither removes HTTP acknowledgment
  ambiguity.
