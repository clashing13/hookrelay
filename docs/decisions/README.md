# Architecture decision records

Architecture Decision Records (ADRs) capture important choices, their context,
and their consequences. A decision can later be superseded, but its original
reasoning remains useful.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-python-over-go.md) | Use Python rather than Go for the core MVP | Accepted |
| [0002](0002-postgresql-over-sqlite-or-mongodb.md) | Use PostgreSQL as the durable source of truth | Accepted |
| [0003](0003-explicit-alembic-migrations.md) | Use explicit Alembic migrations rather than `create_all()` | Accepted |
| [0004](0004-at-least-once-delivery.md) | Design for at-least-once delivery, not exactly-once claims | Accepted; happy-path execution implemented in Stage 3 |
| [0005](0005-transactional-outbox.md) | Commit delivery intent through a transactional outbox | Accepted |
| [0006](0006-tenant-scoped-idempotency.md) | Use tenant-scoped keys plus versioned request fingerprints | Accepted |
| [0007](0007-credential-and-signing-secret-storage.md) | Hash API-key secrets and encrypt endpoint signing secrets | Accepted |
| [0008](0008-nats-jetstream-dispatch.md) | Use NATS JetStream for durable delivery dispatch | Accepted |
| [0009](0009-versioned-webhook-signature.md) | Sign a versioned exact-byte webhook contract with HMAC-SHA256 | Accepted |
| [0010](0010-stage3-local-outbound-gate.md) | Restrict Stage 3 outbound delivery to controlled local/test targets | Accepted; temporary until Stage 5 |

Stage 3 implements the first successful local execution path through outbox
leases, JetStream, a bounded worker, exact-byte HMAC, and the test receiver.
ADR 0004 still governs the guarantee: durable acceptance is not delivery, and
neither PubAck deduplication nor success-state suppression makes the system
exactly once. Stage 4 owns complete retry/crash recovery; Stage 5 owns complete
outbound security.
