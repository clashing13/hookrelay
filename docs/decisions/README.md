# Architecture decision records

Architecture Decision Records (ADRs) capture important choices, their context,
and their consequences. A decision can later be superseded, but its original
reasoning remains useful.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-python-over-go.md) | Use Python rather than Go for the core MVP | Accepted |
| [0002](0002-postgresql-over-sqlite-or-mongodb.md) | Use PostgreSQL as the durable source of truth | Accepted |
| [0003](0003-explicit-alembic-migrations.md) | Use explicit Alembic migrations rather than `create_all()` | Accepted |
| [0004](0004-at-least-once-delivery.md) | Design for at-least-once delivery, not exactly-once claims | Accepted; bounded failure recovery implemented in Stage 4 |
| [0005](0005-transactional-outbox.md) | Commit delivery intent through a transactional outbox | Accepted |
| [0006](0006-tenant-scoped-idempotency.md) | Use tenant-scoped keys plus versioned request fingerprints | Accepted |
| [0007](0007-credential-and-signing-secret-storage.md) | Hash API-key secrets and encrypt endpoint signing secrets | Accepted |
| [0008](0008-nats-jetstream-dispatch.md) | Use NATS JetStream for durable delivery dispatch | Accepted |
| [0009](0009-versioned-webhook-signature.md) | Sign a versioned exact-byte webhook contract with HMAC-SHA256 | Accepted |
| [0010](0010-stage3-local-outbound-gate.md) | Restrict Stage 3 outbound delivery to controlled local/test targets | Superseded by ADR 0014 |
| [0011](0011-postgresql-authoritative-retry-schedule.md) | Persist retry eligibility in PostgreSQL and use delayed NAK as a wake-up | Accepted |
| [0012](0012-leased-attempt-recovery-and-dead-letters.md) | Lease/fence attempts and dead-letter bounded or blocked failures | Accepted |
| [0013](0013-versioned-dispatch-replay.md) | Replay through database dispatch generations and fresh outbox identity | Accepted |
| [0014](0014-resolved-address-ssrf-policy.md) | Validate all resolved addresses and pin each outbound connection | Accepted; supersedes ADR 0010 |
| [0015](0015-database-authoritative-endpoint-traffic-controls.md) | Serialize per-endpoint rate limits and circuit breakers in PostgreSQL | Accepted |
| [0016](0016-request-byte-limits-before-parsing.md) | Enforce raw request byte limits before routing and parsing | Accepted |
| [0017](0017-versioned-signing-secret-rotation.md) | Rotate endpoint signing secrets by immutable version and delivery snapshot | Accepted |
| [0018](0018-least-privilege-app-containers.md) | Run application containers with a least-privilege baseline | Accepted |

Stage 5 adds connection-time outbound destination enforcement, shared endpoint
traffic controls, bounded request bodies, endpoint signing-secret rotation, and
least-privilege application containers. ADR 0004 still governs the guarantee:
security controls, retries, claim fencing, PubAck deduplication, and
success-state suppression do not make a remote receiver exactly once. PostgreSQL
tenant filters are application-enforced in Stage 5; database row-level security and
production egress firewalling remain future defense-in-depth work.
