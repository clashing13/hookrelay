# ADR 0015: Serialize per-endpoint traffic controls in PostgreSQL

- Status: Accepted
- Date: 2026-08-05

## Context

Several worker processes can receive deliveries for the same endpoint concurrently.
An in-memory rate limiter or circuit breaker would give every process an independent
view and allow the aggregate fleet to exceed the intended limit. HookRelay already
uses PostgreSQL as the durable source of truth for delivery state, leases, retry due
times, and replay generations.

Traffic deferral is not an HTTP attempt. Counting a rate-limited or open-circuit
decision as an attempt would exhaust the delivery budget without contacting the
receiver. Recovery also needs exactly one half-open probe across all workers, with a
lease so a crashed probe cannot hold the endpoint forever.

## Decision

Create one `endpoint_traffic_controls` row for each `(tenant_id, endpoint_id)` and
lock it with `SELECT ... FOR UPDATE` when a worker considers a due delivery.
Endpoint creation inserts the row atomically; the Stage 5 migration backfills rows
for existing endpoints. Database constraints enforce valid counters and coherent
closed, open, and half-open state.

Use these default controls:

- fixed-window rate limit: 10 admitted requests per one-second window;
- circuit threshold: five consecutive transient failures;
- open cooldown: 30 seconds;
- half-open recovery: exactly one leased probe;
- probe lease: the delivery claim lease, 20 seconds by default.

The admission transaction locks the delivery row and then its endpoint control row.
Because another worker can make it wait for that second lock, it refreshes PostgreSQL
`clock_timestamp()` **after** acquiring the control row and uses that fresh value for
the rate/circuit decision. Finalization likewise refreshes database time after the
traffic-row lock before recording an outcome. The admission logic applies this order:

1. An open circuit before its cooldown ends defers to that boundary.
2. An expired open circuit becomes eligible for one probe.
3. An active half-open probe defers competitors to the probe lease expiry.
4. An expired probe lease makes a replacement probe eligible.
5. The fixed rate window resets at its exact boundary.
6. A full window defers to the window end without incrementing the counter.
7. An admitted request increments the counter; a recovery candidate atomically
   becomes the sole half-open probe.
8. Only after admission does HookRelay create a `DeliveryAttempt` and consume an
   attempt-budget slot.

A success closes and clears the circuit. A permanent HTTP failure or outbound policy
block also clears it because the receiver was reachable or the failure is not a
transient availability signal. A transient failure increments the consecutive count;
the fifth opens the circuit. A transient probe failure reopens it and starts a new
cooldown. Worker lease recovery treats an abandoned current probe as transient and
releases it through the normal state transition.

Every deferral persists `retry_scheduled` and `next_attempt_at`; JetStream delayed
NAK remains only a wake-up mechanism. No attempt row is created, no HTTP request is
sent, and the delivery's attempt budget is unchanged.

## Serious alternatives

### Keep counters in each worker

This is fast but not a fleet-wide limit. Restarting a process erases state, and N
workers can admit approximately N times the configured traffic.

### Add Redis solely for traffic controls

Redis can implement efficient atomic limits, but it adds another operational source
of truth and failure mode to the current learning stage. PostgreSQL transactions are
already required for each claim. Redis may become appropriate after measured row-lock
contention justifies it.

### Use a token bucket or sliding window

Those algorithms smooth boundary bursts better than a fixed window, but require more
state and arithmetic. The fixed window makes the first durable implementation easy to
audit. Its boundary-burst tradeoff is explicit.

### Let every worker send a half-open probe

That creates a thundering herd precisely when a receiver is recovering. A token and
expiry on the shared row lease one probe across the fleet.

### Count deferrals as attempts

An endpoint could consume all retries without one network operation. HookRelay keeps
admission decisions separate from attempt evidence.

### Hold the database lock during HTTP

This would serialize remote latency inside a database transaction, expand contention,
and risk long-lived locks. HookRelay commits the short claim/admission transaction,
sends outside it, then locks again to finalize with claim fencing.

## Consequences

- All workers observe one authoritative per-endpoint control state.
- Different endpoints have independent rows and can proceed concurrently.
- The same hot endpoint is intentionally serialized for short claim/finalize updates.
- A fixed window can admit up to twice its nominal count around a boundary.
- Database availability and row-lock latency now affect admission.
- A lock wait does not leave rate-window, cooldown, probe-expiry, or circuit-open time
  decisions based on the timestamp captured before that wait.
- Circuit state survives worker restarts and is auditable, but Stage 5 does not expose
  a public traffic-control inspection or reset API.
- The probe lease prevents a permanent half-open state after a crash; it does not
  prevent at-least-once duplicate HTTP effects around lost acknowledgements.
- The cooldown setting must be at least the delivery claim TTL.
- PostgreSQL is authoritative for scheduling; JetStream timing remains advisory.

## Primary references

- [PostgreSQL explicit and row-level locking](https://www.postgresql.org/docs/17/explicit-locking.html)
- [PostgreSQL date/time functions and `clock_timestamp()`](https://www.postgresql.org/docs/17/functions-datetime.html)
- [Martin Fowler: Circuit Breaker](https://martinfowler.com/bliki/CircuitBreaker.html)
