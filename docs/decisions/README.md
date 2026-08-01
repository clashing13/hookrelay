# Architecture decision records

Architecture Decision Records (ADRs) capture important choices, their context,
and their consequences. A decision can later be superseded, but its original
reasoning remains useful.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-python-over-go.md) | Use Python rather than Go for the core MVP | Accepted |
| [0002](0002-postgresql-over-sqlite-or-mongodb.md) | Use PostgreSQL as the durable source of truth | Accepted |
| [0003](0003-explicit-alembic-migrations.md) | Use explicit Alembic migrations rather than `create_all()` | Accepted |
| [0004](0004-at-least-once-delivery.md) | Design for at-least-once delivery, not exactly-once claims | Accepted for future delivery stages |

The Stage 1 repository does not yet contain an event model, transactional
outbox, message broker, or delivery worker. ADR 0004 fixes the semantics those
later stages must preserve; it does not claim that delivery exists today.
