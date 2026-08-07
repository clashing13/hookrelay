# HookRelay architecture

This document is cumulative. It describes what the repository implements at
the end of the current stage and labels roadmap components explicitly so a
diagram is never mistaken for running behavior.

## Current system: Stage 1 foundation

```text
Developer or health monitor
          |
          | HTTP on 127.0.0.1:8000
          v
  +---------------------------+
  | FastAPI / Uvicorn process |
  |                           |
  | GET /health/live ---------+----> process-local response
  |                           |
  | GET /health/ready         |
  +-------------+-------------+
                | bounded SELECT 1
                v
      SQLAlchemy async engine
      and connection pool
                |
                | asyncpg / PostgreSQL protocol
                v
        PostgreSQL service
        + named data volume
```

Stage 1 is an operational skeleton, not yet a webhook product. It contains no
tenant, endpoint, event, delivery, attempt, or outbox tables. It accepts no
events and sends no webhooks. Alembic is configured, but the first revision is
deliberately deferred until Stage 2 introduces a meaningful model.

### Runtime components

| Component | Responsibility | Explicit non-responsibility |
| --- | --- | --- |
| FastAPI application factory | Constructs an isolated application and wires dependencies | It does not start a server or migrate the database |
| Uvicorn | Runs the ASGI application and event loop | It does not make blocking code asynchronous |
| Pydantic Settings | Parses and validates `HOOKRELAY_*` process configuration | Environment variables are not automatically secret storage |
| JSON logging foundation | Writes machine-parseable process events | Correlation IDs, traces, and delivery metrics are later-stage work |
| `PostgresDatabase` | Owns one SQLAlchemy async engine and its pool for an application process | A database session must not be shared globally across future requests |
| Liveness endpoint | Proves that the API process can answer | It does not contact PostgreSQL |
| Readiness endpoint | Runs a bounded `SELECT 1` and maps dependency failure to a sanitized response | It does not reveal raw exceptions or connection URLs |
| Alembic environment | Provides an ordered migration mechanism | It does not run automatically at API startup |
| Docker Compose | Reproduces the local API and PostgreSQL topology | `depends_on` is not lifetime supervision of database health |

## Health request flows

### Liveness

1. A monitor sends `GET /health/live`.
2. FastAPI routes the request to the liveness handler.
3. The handler reads process-local service name and version settings.
4. It returns the stable JSON contract with HTTP `200`.
5. No database connection is acquired.

This separation matters during a database outage. Restarting a healthy API
process cannot repair PostgreSQL and can amplify an incident through connection
storms.

### Readiness

1. A monitor sends `GET /health/ready`.
2. FastAPI resolves the application-scoped database health dependency.
3. The database component checks out a pooled connection and executes
   `SELECT 1` through SQLAlchemy and `asyncpg`.
4. `asyncio.wait_for` sets the configured deadline for the complete probe and
   requests cancellation if it expires. The async database stack is expected to
   cooperate; elapsed wall time can exceed the value while cancellation finishes.
5. A successful probe returns HTTP `200` with a stable public contract.
6. A timeout or database/driver failure is logged internally and returned as a
   sanitized HTTP `503`; the raw exception and URL never enter the response.
7. Each request performs a fresh probe, so readiness can recover after
   PostgreSQL returns without restarting the API.

## Process and database lifecycle

The application factory wires how process-scoped resources belong to one app
instance. FastAPI's lifespan context creates and closes them at explicit times:

```text
create app
   -> enter application lifespan
   -> create one async engine / pool
   -> serve many requests, borrowing connections briefly
   -> stop accepting work
   -> dispose engine and pooled connections
   -> exit lifespan and process
```

The engine is safe to keep for the process lifetime and owns connection
pooling. A future SQLAlchemy `AsyncSession` represents a short unit of work and
must be created per request or background operation. A global session would
mix transaction state across unrelated concurrent work.

## Deployment topology

Docker Compose starts two services on a private Compose network:

```text
Windows host
  |
  | 127.0.0.1 published ports
  v
Docker Desktop / WSL 2 Linux VM
  |
  +-- api container -------- DNS name `postgres` --------+
  |                                                    |
  +-- postgres container <-----------------------------+
              |
              v
       named PostgreSQL volume
```

Inside the Compose network, the API uses `postgres`, not `localhost`, as the
database hostname. `localhost` in a container refers to that same container.
The host bindings use `127.0.0.1` so development services are not deliberately
published to every network interface.

The PostgreSQL named volume outlives replacement containers. `docker compose
down` preserves it; `docker compose down --volumes` deletes it and is therefore
intentionally absent from normal reset instructions.

## Configuration boundary

Configuration comes from validated `HOOKRELAY_*` environment variables. The
checked-in `.env.example` is documentation and contains only development
examples. A real `.env` file is ignored. This prevents accidental commits but
does not turn environment variables into secure secrets: production still
needs an access-controlled secret manager, rotation, audit, and least
privilege.

The database URL is represented as a Pydantic `SecretStr` to reduce accidental
display, rejected input is hidden from validation errors, and the validator
requires the `postgresql+asyncpg://` driver scheme. Code must still avoid
deliberately calling `get_secret_value()` in logs. Service name and version are
literal-valued so environment configuration cannot silently mutate the stable
health identity.

## Migration boundary

Alembic records ordered schema transitions. Migration execution is separate
from API startup:

```text
deployment operator or CI -> alembic upgrade head -> PostgreSQL
application startup        -> serve requests only
```

This avoids multiple replicas racing to change a schema and keeps privileged,
potentially slow DDL observable as its own release step. An ORM model change
alone never changes an existing database.

## Verification boundaries

Stage 1 uses several test layers because each catches a different class of
mistake:

- Unit tests validate settings without network dependencies.
- In-process API tests validate routing, lifespan, response contracts, and
  failure sanitization through HTTPX2's ASGI transport.
- PostgreSQL integration tests validate a successful probe through the real
  driver, SQL, pooling, and server. Injected API tests cover recovery and engine
  disposal; the safe Compose exercise observes a real outage and recovery.
- Alembic validation proves the migration environment can connect and upgrade
  a disposable database even though there is no empty Stage 1 revision.
- A Docker build checks the packaged Linux runtime and non-root image setup.

Passing the fastest tests does not prove networking, PostgreSQL, migrations, or
container packaging. Passing all Stage 1 checks still does not prove
production scale; performance evidence belongs to Stage 7.

## Reliability invariants established in Stage 1

1. Liveness has no external dependency.
2. Readiness touches real PostgreSQL and has a bounded wait.
3. Dependency errors are useful internally but sanitized publicly.
4. Readiness is re-evaluated and can recover without process restart.
5. One engine/pool belongs to one application process and is disposed during
   shutdown.
6. Schema changes are explicit and ordered rather than side effects of startup.
7. Dependency versions are resolved in `uv.lock` for reproducible installs.

## Planned architecture, not current behavior

The intended later-stage flow is included for orientation only:

```text
Producer
  -> HookRelay API
  -> PostgreSQL event + transactional outbox       (Stage 2)
  -> outbox publisher
  -> NATS JetStream
  -> bounded-concurrency delivery worker           (Stage 3)
  -> HMAC-signed request
  -> customer endpoint
     -> retries / dead letter / replay              (Stage 4)
```

The transactional outbox and NATS are not Stage 1 dependencies and are not
implemented here. Future delivery uses at-least-once semantics: acknowledgment
loss can cause another attempt, so stable event IDs and receiver-side
idempotency are required. See [ADR 0004](decisions/0004-at-least-once-delivery.md).

## Decision index

- [Python instead of Go](decisions/0001-python-over-go.md)
- [PostgreSQL instead of SQLite or MongoDB](decisions/0002-postgresql-over-sqlite-or-mongodb.md)
- [Explicit Alembic migrations](decisions/0003-explicit-alembic-migrations.md)
- [At-least-once delivery semantics](decisions/0004-at-least-once-delivery.md)
