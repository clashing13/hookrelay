# HookRelay glossary

This glossary is cumulative. Entries marked **future** describe planned stages,
not Stage 1 behavior.

## Application and HTTP terms

**API (Application Programming Interface)**

A contract through which software communicates. HookRelay's current HTTP API
contains health endpoints; event submission arrives in Stage 2.

**Application factory**

A function that constructs and configures an application instead of relying
only on one import-time global. `create_app()` makes settings and database
dependencies explicit and allows tests to construct isolated app instances.

**ASGI (Asynchronous Server Gateway Interface)**

The Python interface between an asynchronous web server and an application.
Uvicorn is the ASGI server and FastAPI produces the ASGI application.

**Dependency injection**

Supplying a component's dependency from outside rather than constructing it
inside every operation. The readiness route can depend on a database-health
interface, which lets API tests substitute a controlled success or failure.

**Event loop**

The scheduler that runs ready coroutines and resumes them when awaited I/O can
progress. Blocking CPU or synchronous I/O on this thread delays every other
coroutine using the same loop.

**FastAPI**

A Python ASGI web framework that uses type hints and Pydantic models for request
and response contracts and generates an OpenAPI description.

**HTTP 200 OK**

The success response used when a health probe's condition is satisfied.

**HTTP 503 Service Unavailable**

A temporary failure response. Readiness uses it when the API is alive but
PostgreSQL cannot currently support dependency-backed work.

**HTTPX2**

A maintained asynchronous HTTP client used by Stage 1 tests. With
`ASGITransport`, it can call the FastAPI app in-process without opening a TCP
port. The same client family is intended for later outbound webhook requests,
but delivery does not exist in Stage 1.

**Lifespan**

ASGI's application startup/shutdown context. HookRelay uses it to make resource
ownership visible and to dispose the database engine on shutdown.

**LifespanManager**

A test helper that enters and exits an ASGI application's lifespan. An
in-process transport alone may not run startup/shutdown behavior, so this helper
ensures tests exercise it.

**Liveness probe**

A dependency-free question: "Can this API process and event loop respond?"
HookRelay's `GET /health/live` must stay healthy during a PostgreSQL outage.

**OpenAPI**

A machine-readable description of HTTP routes and schemas. FastAPI serves it at
`/openapi.json`; Stage 1 tests ensure liveness is included.

**Readiness probe**

A question about whether an instance can currently handle dependency-backed
traffic. HookRelay's `GET /health/ready` performs a bounded real PostgreSQL
query and returns `503` on failure.

**Response contract**

The documented status code and body shape clients may depend upon. Tests assert
health responses exactly so accidental changes are visible.

**Sanitized error**

A public error stripped of internal implementation and secret details. A
readiness failure returns a stable `503` without the raw exception, credentials,
or database URL; diagnostics belong in internal logs.

**Uvicorn**

The ASGI server that binds a host/port, owns the event loop, and invokes the
FastAPI application. It is distinct from FastAPI itself.

**Webhook (future product behavior)**

An HTTP callback sent when an event occurs. HookRelay will send webhooks in
Stage 3; Stage 1 exposes health endpoints only.

## Python and concurrency terms

**`asyncio`**

Python's event-loop and cooperative-concurrency library. It overlaps work while
tasks await nonblocking I/O. It does not automatically make CPU work parallel.

**`async def` / `await`**

Syntax for a coroutine and a cooperative suspension point. Calling a blocking
database or HTTP library inside `async def` still blocks the event loop; the
library must expose genuine async I/O.

**Bounded timeout**

A configured deadline after which waiting is cancelled or treated as failure.
Readiness uses `asyncio.wait_for`; it requests cancellation at the deadline but
waits for cancellation to finish, so a strict wall-clock bound still depends on
the awaited I/O cooperating.

**Concurrency**

Making progress on multiple tasks during overlapping time. It is not identical
to parallelism, which means executing work at the same instant on different
cores.

**Coroutine**

A suspendable Python operation produced by an `async def` function. The event
loop schedules coroutines and resumes them around awaited I/O.

**Protocol**

A Python structural typing interface: an object is compatible if it has the
required methods, without inheriting a base class. `DatabaseHealth` keeps the
health handler coupled to a capability rather than a concrete database class.

## Configuration and observability terms

**`.env` file**

A local text file containing environment-style key/value configuration. The
real file is ignored by Git and Docker; `.env.example` documents safe
development names. This convenience is not a production secret manager.

**Correlation ID**

An identifier used to connect logs and traces for one logical operation.
Full correlation propagation is planned for Stage 6; Stage 1 only establishes
the JSON logging foundation.

**Environment variable**

A key/value supplied to a process. Pydantic Settings reads `HOOKRELAY_*`
variables. Environment variables are configuration transport, not inherently
secure secret storage.

**JSON logging / structured logging**

Writing each log record as named fields in a JSON object rather than free-form
text. Machines can filter fields such as `level`, `event`, and `service`.

**Pydantic**

A Python validation library that turns typed declarations into validated
runtime models. Health response models use it to preserve stable shapes.

**Pydantic Settings**

Pydantic's configuration package. It reads environment-backed values, converts
types, applies constraints, and rejects invalid configuration early.

**Secret manager**

An access-controlled system for storing, distributing, auditing, and rotating
secrets. Ignoring `.env` and masking a `SecretStr` reduce accidental exposure
but do not replace one.

**`SecretStr`**

A Pydantic wrapper whose normal string/repr display is masked. Code can still
extract the value, so it is a guardrail rather than encryption or authorization.
HookRelay also hides rejected configuration inputs in validation errors because
a malformed database URL can still contain a password before conversion to
`SecretStr` succeeds.

## Database and migration terms

**Alembic**

SQLAlchemy's migration tool. It tracks ordered schema revisions and applies
them with commands such as `alembic upgrade head`.

**`asyncpg`**

An asynchronous PostgreSQL driver. SQLAlchemy uses it to speak PostgreSQL
without blocking the event loop during network I/O.

**Async engine**

SQLAlchemy's process-scoped entry point for async database connectivity. It
owns the connection pool and creates connection/session resources; it is not a
single permanently open connection.

**Connection**

One live conversation with PostgreSQL. Code borrows a connection from the pool
for a bounded operation and then returns it.

**Connection pool**

A managed set of reusable database connections. Reuse avoids opening a new TCP
and authenticated PostgreSQL connection for every request. Pool size must be
budgeted against the database's connection limit.

**`max_overflow`**

The maximum number of temporary SQLAlchemy connections allowed above the base
pool size under pressure. It is a ceiling, not reserved capacity, and it
multiplies across processes.

**Database URL**

A driver and connection locator such as
`postgresql+asyncpg://user:password@host:5432/database`. It can contain secrets
and must not appear in public errors or logs.

**DDL (Data Definition Language)**

SQL that changes schema objects, such as `CREATE TABLE` or `ALTER TABLE`.
Alembic revisions make DDL ordered and reviewable.

**Downgrade**

An Alembic revision's reverse transition. A downgrade is not always safe when
data would be destroyed, so it must be designed and reviewed rather than
assumed.

**Engine disposal**

Closing the engine's pooled connections during application shutdown. Disposal
prevents resource leakage and makes lifecycle ownership explicit.

**Migration**

An ordered transformation from one schema version to another. Editing an ORM
model does not migrate an existing database.

**`NullPool`**

A SQLAlchemy pool policy that does not retain connections for reuse. Alembic's
short-lived migration engine uses it because migration execution is an explicit
operation, not a long-running request service.

**Offline / online migration mode**

Alembic offline mode renders SQL without opening a database connection. Online
mode connects and applies operations. HookRelay configures both; CI exercises
online `upgrade head` against PostgreSQL.

**ORM (Object-Relational Mapper)**

A library mapping Python constructs to relational SQL concepts. HookRelay uses
SQLAlchemy, but its first domain models arrive in Stage 2.

**PostgreSQL**

The relational database used as HookRelay's durable source of truth. It is
chosen for constraints, transactions, concurrency, and future outbox behavior.

**`pool_pre_ping`**

A SQLAlchemy option that checks a pooled connection when it is checked out and
replaces it if stale. It cannot prevent PostgreSQL from failing after the check
or make an overloaded database healthy.

**Revision / head**

An Alembic revision is one versioned migration node. `head` means the newest
revision in the selected migration branch. Stage 1 has migration machinery but
no meaningless empty revision.

**Session / unit of work**

A SQLAlchemy object that groups related reads and writes into a transaction.
Future request and worker operations create short-lived sessions. A global
session is unsafe because unrelated concurrent work would share transaction
state.

**`SELECT 1`**

A tiny SQL query used by readiness to prove a real connection can execute work.
It proves basic database usability, not that every future table or query works.

**SQLAlchemy 2**

The Python SQL toolkit and ORM used for engine, pooling, statements, and later
unit-of-work sessions. Stage 1 uses its asynchronous engine API.

**Transaction**

A group of database operations committed or rolled back atomically. Future
event and outbox writes must share one PostgreSQL transaction.

**Transactional outbox (future, Stage 2)**

A table written in the same database transaction as domain state. A separate
publisher later transfers committed outbox records to a broker, avoiding the
loss window of an uncoordinated database-plus-queue dual write.

## Packaging, quality, and test terms

**API test**

A test that calls the HTTP application contract. Stage 1 API tests run
in-process, so they do not prove host networking or container behavior.

**CI (Continuous Integration)**

Automated checks on pushes and pull requests. GitHub Actions runs locked
installation, lint, format, types, tests, migration validation, and image build.

**GitHub Actions runner**

The temporary machine executing a CI workflow. Stage 1 uses a standard Ubuntu
runner, not a paid larger runner.

**Hatchling**

The PEP 517 build backend that turns HookRelay's `pyproject.toml` and
`src/hookrelay` package into installable artifacts such as a wheel. It is
build/packaging machinery, not an application server or runtime framework.

**Integration test**

A test crossing a real component boundary. PostgreSQL tests exercise the real
server, driver, SQL, and network rather than substituting SQLite.

**Lockfile (`uv.lock`)**

The resolved dependency graph, including exact package versions and hashes.
`uv sync --frozen` installs it without silently resolving a different graph.

**mypy**

A static type checker. Strict mypy catches incompatible interfaces without
executing code; it does not prove runtime behavior.

**pytest / pytest-asyncio**

The test runner and its async support. Strict asyncio mode makes async test and
fixture boundaries explicit.

**pytest-cov**

A pytest plugin backed by coverage.py that reports which Python lines executed
during a test run. It is useful for finding unexercised code but cannot prove
that assertions are meaningful or failures are realistic. Stage 1 makes it
available for local inspection and does not enforce an arbitrary CI percentage.

**Ruff**

A fast Python linter and formatter. Linting detects selected code-quality
problems; formatting gives the repository a deterministic style.

**Service container**

A dependency container started by CI beside the test process. The Stage 1
workflow uses PostgreSQL this way.

**`src/` layout**

Keeping importable package code under `src/hookrelay` rather than at the
repository root. This helps tests exercise the installed package instead of
accidentally importing a same-named working-directory folder.

**Unit test**

A focused test of code in isolation. It is fast and precise but cannot prove
PostgreSQL semantics, TCP networking, or container packaging.

**uv**

The Python dependency and environment tool used to resolve, lock, install, and
run this project reproducibly.

## Container terms

**Base image**

The starting filesystem and metadata named by Dockerfile `FROM`. Pinning an
explicit Python/PostgreSQL version policy makes upgrades intentional; a mutable
tag can still move, so digest pinning is the strictest reproducibility option.

**Build context**

The files Docker can see for a build. `.dockerignore` removes irrelevant or
sensitive local files before that context is sent to the builder.

**Build stage / runtime stage**

A build stage contains tools needed to create artifacts; the final runtime
stage contains only what the service needs to run. Multi-stage builds keep
compilers and build caches out of the shipped image.

**Container**

A running process isolated with Linux namespaces and resource controls, using
an image as its filesystem template. A container is not an individual full VM
and shares its host/VM's Linux kernel.

**Container port / host port**

The container port is where a process listens inside its network namespace. A
published host port forwards from the developer machine to it. Binding the
host side to `127.0.0.1` limits normal development exposure.

**`depends_on` with `service_healthy`**

A Compose startup relationship that can wait for a dependency's health check
before starting the dependent service. It does not continuously keep the
dependency healthy or stop/restart dependents after a later outage.

**DNS (Domain Name System)**

Name-to-address resolution. Compose provides internal DNS so hostname
`postgres` resolves to the PostgreSQL service on the project network.

**Docker Compose**

A declarative description of related containers, networks, volumes,
configuration, and health dependencies for local development.

**Docker Desktop**

The Windows/macOS application that supplies the Docker client, engine, and
supporting VM integration. On this Windows workflow, Linux containers normally
run through the WSL 2 backend.

**Dockerfile**

Build instructions that produce an image. It is source code, not the resulting
image and not a running container.

**Health check**

A command Docker runs periodically to classify a container as starting,
healthy, or unhealthy. A health check reports state; by itself it is not a
complete repair or restart policy.

**Graceful shutdown**

Stopping intake, allowing bounded in-flight cleanup, and releasing resources
after a termination signal. Compose grace periods give FastAPI lifespan time to
dispose the engine before a forced kill.

**Image**

An immutable, layered package containing a filesystem and runtime metadata.
Starting an image creates a container.

**Layer / layer cache**

Dockerfile instructions produce reusable build layers. Copying dependency
metadata before frequently changing source allows dependency-install layers to
remain cached during normal code edits.

**Named volume**

Storage managed by Docker independently from a container lifecycle. PostgreSQL
data survives container replacement until the volume is explicitly removed.

**Non-root container**

A container whose service process uses an unprivileged user. This reduces the
impact of a compromise but does not make the application or host automatically
secure.

**PID 1 / init process**

The first process in a Linux container, responsible for signal behavior and
reaping exited child processes. Compose `init: true` inserts a small init to
forward signals and reap orphans.

**Service-name DNS**

Compose's internal name resolution. The API connects to hostname `postgres`;
inside the API container, `localhost` means the API container itself.

**TCP (Transmission Control Protocol)**

The reliable byte-stream transport beneath PostgreSQL and ordinary HTTP/1.1.
In-process ASGI tests bypass TCP, which is why container/network checks cover a
different boundary.

**WSL 2 (Windows Subsystem for Linux 2)**

The lightweight Linux VM technology Docker Desktop commonly uses on Windows.
Linux containers share the WSL 2 VM's kernel; each container is not its own VM.

## Delivery-semantics terms

All entries in this section describe planned later stages unless stated
otherwise.

**Acknowledgment ambiguity**

The sender cannot tell whether a timed-out receiver did nothing or committed a
side effect and lost the response. This is why retries can duplicate delivery
and general exactly-once claims are invalid.

**At-least-once delivery (future)**

A logical event may be attempted more than once but is not intentionally
discarded after a transient ambiguous failure. Receiver idempotency is required
because duplicate delivery is possible.

**Circuit breaker (future, Stage 5)**

A destination-protection state machine that pauses ordinary attempts after a
failure threshold, waits through a cooldown, and permits a limited recovery
probe. It prevents repeated pressure but may delay recovery if tuned poorly.

**Dead letter (future, Stage 4)**

A terminally separated delivery that exhausted its automatic attempt policy.
It remains inspectable and may be replayed deliberately; it is not silently
discarded.

**Delivery attempt (future)**

One concrete outbound HTTP try for a logical delivery. At-least-once behavior
means one event/delivery can have multiple attempts.

**Exactly-once delivery**

A claim that each logical effect happens once. HookRelay cannot generally make
this guarantee across HTTP and an independent receiver database because an
acknowledgment can be lost after the side effect commits.

**Exponential backoff and jitter (future, Stage 4)**

Backoff increases the delay between repeated failures; jitter randomizes it so
many workers do not retry simultaneously. Both reduce retry storms but increase
eventual-delivery latency.

**HMAC signature (future, Stage 3)**

A keyed message authentication code attached to a webhook so a receiver can
verify payload integrity and possession of a shared secret. It is not
encryption and needs timestamp/replay handling and secret rotation.

**Idempotency (future)**

Making repeated processing of one logical event produce no extra side effect.
A receiver can record a stable event ID under a unique constraint in the same
transaction as its business change.

**NATS JetStream (future, Stage 3)**

A durable messaging layer planned between the outbox publisher and delivery
workers. It is not installed or running in Stage 1.

**Rate limit (future, Stage 5)**

A bound on attempts over time for a destination or tenant. It protects shared
capacity but needs a defined fairness and burst policy.

**Retry (future, Stage 4)**

Another attempt after a transient or ambiguous failure. Retrying reduces loss
but can duplicate effects, so it must be persistent, bounded, and paired with
idempotency.

**SSRF (Server-Side Request Forgery; future, Stage 5)**

A vulnerability where an attacker causes a server to request internal,
link-local, metadata, or otherwise prohibited destinations. A webhook product
must validate destinations while accounting for DNS changes and redirects.

## Later-stage operations tools

None of these tools is installed or running in Stage 1.

**Grafana (future, Stage 6)**

A dashboard and visualization system planned for exploring HookRelay metrics.
A dashboard displays collected evidence; it does not create correct metrics or
incident policy by itself.

**k6 (future, Stage 7)**

A load-generation tool planned for reproducible throughput and latency
scenarios. Results need recorded configuration, hardware, duration, and raw
outputs to be defensible.

**Kubernetes (out of the seven core stages)**

A container orchestration platform that can schedule replicas and act on
health probes. Stage 1 teaches probe semantics with Compose and does not add a
Kubernetes deployment.

**OpenTelemetry (future, Stage 6)**

A vendor-neutral set of APIs, SDKs, and protocols for traces, metrics, and
logs. HookRelay plans trace instrumentation later; JSON logs alone are not
distributed tracing.

**Prometheus (future, Stage 6)**

A time-series monitoring system that scrapes labeled metrics. Metric names and
label cardinality remain application design responsibilities.

**Terraform (optional deployment after the MVP)**

Infrastructure-as-code tooling for declaring and reviewing cloud resources.
It is intentionally absent from Stage 1 and does not replace application-level
reliability or security design.

**Toxiproxy (future, Stage 7)**

A controllable network proxy used to inject latency, disconnects, and related
failures between components. It supports repeatable experiments but cannot
represent every real production failure.
