# ADR 0019: Preserve observability context beside the outbox payload

Status: Accepted

Date: 2026-08-06

## Context

HookRelay's outbox decouples the API transaction from later NATS publication. In-memory trace
context disappears between those processes, while the existing broker schema is a strict,
versioned, ID-only contract. Adding trace fields to that JSON would create a wire-contract change
for an operational concern.

## Decision

Store a canonical correlation UUID and optional canonical W3C `traceparent` in dedicated
`outbox_messages` columns. The publisher restores that parent, creates a producer span, and injects
the active context plus correlation UUID into optional NATS headers. The worker extracts valid
headers and creates a consumer span. Missing or malformed telemetry headers create a new safe
context and never reject durable work.

Do not add baggage, `tracestate`, payload fields, or receiver-facing headers. The webhook body,
signature vector, and broker schema version 1 remain unchanged.

## Serious alternatives

- Add trace fields to broker JSON: rejected because it changes the durable strict contract.
- Keep context only in process memory: rejected because API and publisher are independent and work
  may be delayed or recovered after restart.
- Use IDs alone and no trace parent: useful for log lookup, but it loses trace parent/child timing.

## Consequences

Trace continuity survives outbox delay and process restarts. Telemetry storage adds two bounded
columns and a backfill, but does not become a product dependency. A trace may still be absent when
export is disabled, sampled, queued beyond capacity, or the collector is unavailable.

Primary reference: [W3C Trace Context](https://www.w3.org/TR/trace-context/).
