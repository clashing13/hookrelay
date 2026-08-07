# ADR 0002: PostgreSQL as the durable source of truth

- Status: Accepted
- Date: 2026-08-01

## Context

Later HookRelay stages need atomic writes across events, deliveries, and a
transactional outbox; unique constraints for idempotency; safe concurrent
claiming; ordered migrations; and behavior that remains valid across multiple
processes. Stage 1 must exercise the same database category rather than hide
those concerns behind a test-only substitute.

## Decision

Use PostgreSQL as the durable source of truth in development, integration
tests, CI, and deployment. Access it through SQLAlchemy 2's asynchronous APIs
and the `asyncpg` driver. Keep one long-lived async engine and connection pool
per process; create short-lived sessions for later units of work.

## Serious alternative: SQLite

SQLite is excellent for embedded applications, local tools, and very fast
tests. It requires no server. However, its concurrency and locking model,
supported SQL, data types, and transaction behavior differ from PostgreSQL.
Tests passing against SQLite would not validate the PostgreSQL behavior on
which HookRelay's outbox and concurrent workers will rely. SQLite may still be
appropriate for unrelated pure unit tests, but it is not a substitute for the
integration database.

## Serious alternative: MongoDB

MongoDB offers flexible documents, useful horizontal-scaling features, and
transactions in supported topologies. HookRelay's core data is relational:
tenants own endpoints, events produce deliveries, deliveries have attempts,
and the outbox must be written atomically with event state. PostgreSQL provides
natural constraints, joins, transactions, and mature operational tooling for
that model. Choosing MongoDB here would add data-model and operational
complexity without a demonstrated document-oriented requirement.

## Consequences

- Local integration work needs a real PostgreSQL server, normally through
  Docker Compose.
- CI needs a PostgreSQL service and must verify migrations.
- Tests can expose real transaction, driver, networking, and packaging faults.
- PostgreSQL is a dependency and can be unavailable; liveness therefore stays
  independent while readiness reports dependency usability.
- Connection limits and pool sizing become operational responsibilities.
