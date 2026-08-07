# ADR 0020: Use bounded-cardinality process-local metrics

Status: Accepted

Date: 2026-08-06

## Context

The API, outbox publisher, and delivery worker are separate processes. A global in-memory registry
cannot describe them all, and labels derived from tenant or delivery data would create an
unbounded number of Prometheus time series.

## Decision

Each process owns a custom Prometheus registry. The API exposes `/metrics`; the non-HTTP processes
use small internal scrape listeners. Metric labels come only from reviewed closed vocabularies
such as route template, status class, outcome, circuit-probe flag, and broker disposition.

Never label metrics with tenant, event, endpoint, delivery, URL, event type, raw path, exception
text, or unrestricted error code. Record metrics only at defined durability boundaries.

## Serious alternatives

- Push metrics from application code: rejected because it adds an availability dependency and
  complicates retry semantics.
- Use Prometheus' default global registry: rejected because repeated app factories in tests can
  accumulate collectors and because process ownership becomes implicit.
- Label by delivery ID for easy search: rejected because logs, traces, and authorized APIs provide
  that lookup without cardinality exhaustion.

## Consequences

Prometheus can independently scrape every role and restarts cause expected counter resets. The
dashboard provides bounded operational summaries, not a durable audit or per-tenant billing
system.

Primary reference: [Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/).
