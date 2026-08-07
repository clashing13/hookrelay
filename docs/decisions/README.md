# Architecture decision records

Architecture Decision Records (ADRs) capture important choices, their context,
and their consequences. A decision can later be superseded, but its original
reasoning remains useful.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-python-over-go.md) | Use Python rather than Go for the core MVP | Accepted |
| [0002](0002-postgresql-over-sqlite-or-mongodb.md) | Use PostgreSQL as the durable source of truth | Accepted |
| [0003](0003-explicit-alembic-migrations.md) | Use explicit Alembic migrations rather than `create_all()` | Accepted |
| [0004](0004-at-least-once-delivery.md) | Design for at-least-once delivery, not exactly-once claims | Accepted; execution begins in a later stage |
| [0005](0005-transactional-outbox.md) | Commit delivery intent through a transactional outbox | Accepted |
| [0006](0006-tenant-scoped-idempotency.md) | Use tenant-scoped keys plus versioned request fingerprints | Accepted |
| [0007](0007-credential-and-signing-secret-storage.md) | Hash API-key secrets and encrypt endpoint signing secrets | Accepted |

Stage 2 contains the event/delivery model and transactional outbox, but no
outbox publisher, NATS dependency, outbound HTTP worker, or delivery attempts.
ADR 0004 fixes the semantics the later execution stages must preserve; it does
not claim that durable acceptance is delivery.
