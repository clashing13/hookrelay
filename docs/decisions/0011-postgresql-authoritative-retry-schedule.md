# ADR 0011: PostgreSQL-authoritative retry schedules with delayed NAK

- Status: Accepted
- Date: 2026-08-04

## Context

Stage 3 left a failed delivery message unacknowledged and relied on JetStream's
`AckWait` to expose it again. That retained work, but it was not a retry policy:
there was no durable next-attempt time, exponential delay, jitter, or business
attempt budget. An in-memory timer would disappear on worker restart, while
sleeping inside the worker would occupy a bounded concurrency slot.

JetStream and PostgreSQL also count different things. The broker's delivery
counter increases for an early wake-up, active-attempt deferral, or lost ACK
even when HookRelay sends no new HTTP request. It cannot safely represent the
number of destination attempts.

## Decision

Make PostgreSQL authoritative for retry eligibility.

When a transient attempt finishes, one transaction records the attempt outcome,
sets delivery status `retry_scheduled`, and stores a database-time
`next_attempt_at`. Only after that commit does the worker issue a JetStream
negative acknowledgment with the calculated delay.

On every broker delivery, the worker locks and reloads PostgreSQL state. If the
due time is still in the future, it creates no attempt and requests redelivery
after the remaining interval. The NAK is therefore a wake-up optimization, not
the durable schedule.

Use capped exponential backoff with bounded downward jitter. For attempt `n`
within the current dispatch generation:

```text
ceiling    = min(maximum, base * 2^(n - 1))
multiplier = (1 - ratio) + ratio * U, where U is uniform in [0, 1]
delay      = max(0.1 seconds, ceiling * multiplier)
```

Defaults are a one-second base, 60-second cap, 0.25 jitter ratio, and five
attempts per dispatch generation. Inject the random source in tests so both
bounds and the cap are deterministic.

Keep the JetStream durable consumer's `MaxDeliver` unlimited. PostgreSQL counts
attempt rows for the current generation and enforces the business maximum.

## Serious alternatives

### Raw `AckWait`

This is simple but provides no application-visible due time, backoff, jitter,
classification, or attempt budget. A broad outage can synchronize rapid
redelivery.

### Sleep in the worker

Sleeping retains a message/concurrency slot, couples recovery to one process,
and loses the timer on process death. It scales with waiting work rather than
active I/O.

### In-memory scheduler

A heap or timer wheel can be efficient, but must be rebuilt after restart and
coordinated across workers. PostgreSQL already owns the delivery state and
transactional attempt result.

### JetStream `MaxDeliver`

It limits broker deliveries, not HTTP attempts. Early due-time delivery,
active-lease deferral, and lost ACK can exhaust it without spending the intended
business budget.

### Separate scheduler service

A dedicated due-row poller can be appropriate at scale. Stage 4 can preserve
the same correctness boundary with the existing durable message and worker,
avoiding another process and dispatch handoff before measurement justifies it.

## Consequences

- Retry state survives normal worker restarts because it is committed with the
  attempt result.
- A lost NAK cannot erase the schedule; `AckWait` redelivery rechecks due time.
- A delayed NAK that wakes early cannot bypass the database gate.
- Retry timing is approximate at the broker but exact eligibility is governed
  by PostgreSQL `clock_timestamp()` rather than transaction-start `now()`.
- The attempt cap is stable across broker redelivery noise.
- Jitter reduces synchronized retry pressure but is not rate limiting or a
  circuit breaker; those remain Stage 5.
- PostgreSQL availability is required to decide every attempt.
- The design remains at least once because retry cannot determine whether an
  ambiguous remote side effect committed.
