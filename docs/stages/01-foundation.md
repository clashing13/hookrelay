# Stage 1: service foundation

Stage 1 builds the smallest deployable base on which HookRelay can safely add
durable event ingestion. The learning goal is not "make FastAPI return JSON";
it is to understand process lifecycle, dependency health, asynchronous I/O,
database ownership, reproducible environments, and the limits of each test.

This guide is written against the Stage 1 repository. It deliberately labels
future architecture: there is no event ingestion, transactional outbox, NATS,
delivery worker, retry loop, or webhook request in this stage.

## 1. The problem this stage solves

A reliable delivery system still needs an ordinary service foundation that can
answer five operational questions:

1. Can a clean checkout install the same dependency graph?
2. Can operators tell whether the process is alive separately from whether its
   database is usable?
3. Who owns database connections, and who closes them?
4. Can local development and CI run the same PostgreSQL-backed topology?
5. Can a schema evolve through explicit reviewed steps rather than startup side
   effects?

Stage 1 answers those questions with a FastAPI application factory, validated
settings, JSON-formatted HookRelay log records, a process-scoped SQLAlchemy async
engine, independent liveness/readiness routes, Alembic, locked dependencies,
Docker Compose, tests, and CI.

The key reliability invariant is:

> A PostgreSQL outage may make an instance unready, but it must not make the
> healthy Python process appear dead.

If liveness depended on PostgreSQL, an orchestrator could restart every API
process during a database outage. Those restarts cannot repair PostgreSQL and
may add a connection storm to the incident.

## 2. Deliberately out of scope

Stage 1 does **not** implement:

- tenants, endpoint registration, API keys, or authorization;
- an event submission API, idempotency keys, or domain tables;
- deliveries, delivery attempts, signing secrets, or HMAC signatures;
- a transactional outbox or publisher;
- NATS JetStream or any other queue;
- webhook HTTP workers, concurrency controls, retries, jitter, or dead letters;
- rate limiting, circuit breaking, SSRF defenses, or replay;
- tracing, Prometheus/Grafana, an operations UI, or load benchmarks;
- Kubernetes, Terraform, cloud deployment, or production secret management.

PostgreSQL is running but has no HookRelay domain schema yet. Alembic is ready
to manage a schema, but there is no empty baseline revision just to create the
appearance of progress. Stage 2 should introduce the first meaningful model
and revision.

## 3. Architecture before and after

### Before Stage 1

```text
Product idea
  -> no running API
  -> no validated configuration
  -> no durable dependency
  -> no health contract or test boundary
```

There was no operational unit on which later reliability behavior could be
built or tested.

### After Stage 1

```text
Developer / monitor
        |
        | HTTP
        v
+-----------------------------+
| Uvicorn + FastAPI process   |
|                             |
| /health/live -> local state |
|                             |
| /health/ready               |
+-------------+---------------+
              | asyncio.wait_for(...)
              | SQLAlchemy SELECT 1
              v
       async engine + pool
              |
              | asyncpg
              v
          PostgreSQL
              |
              v
       Docker named volume
```

The application is useful as an operational skeleton but is not yet a webhook
delivery platform. The future flow below is a roadmap, not a Stage 1 claim:

```text
Producer -> API -> PostgreSQL event + outbox       (Stage 2)
                    -> NATS -> delivery worker      (Stage 3)
                    -> retry/dead letter/replay     (Stage 4)
```

The cumulative [architecture document](../architecture.md) records current
boundaries and future stages in more detail.

## 4. Repository and file tour

### Packaging and entry points

| Path | Role |
| --- | --- |
| `pyproject.toml` | Python 3.12 requirement, runtime/dev dependencies, the `hookrelay` console command, package build, and Ruff/mypy/pytest policy |
| `uv.lock` | Exact resolved dependency graph used for reproducible installation |
| `src/hookrelay/__init__.py` | Package version (`0.1.0`) used in the public health contract |
| `src/hookrelay/__main__.py` | Supports `python -m hookrelay` |
| `src/hookrelay/main.py` | `create_app()` factory, application lifespan, dependency wiring, engine disposal, exported ASGI app, and Uvicorn runner |

The `src/` layout makes the importable package distinct from the repository
root. That reduces the chance that tests pass only because Python accidentally
imports source from the current directory instead of the installed package.

### Configuration, logging, and database

| Path | Role |
| --- | --- |
| `src/hookrelay/config.py` | Validates `HOOKRELAY_*` values, numeric bounds, allowed environments/log levels, and the async PostgreSQL URL scheme |
| `src/hookrelay/logging.py` | Formats HookRelay log records as compact one-line JSON |
| `src/hookrelay/database.py` | Defines the narrow `DatabaseHealth` protocol and `PostgresDatabase`, which owns one SQLAlchemy async engine and pool |
| `.env.example` | Safe local-development template; it is documentation, not a production secret strategy |
| `.gitignore` | Keeps real `.env` files, virtual environments, caches, and build output out of Git |

`Settings.database_url` is a Pydantic `SecretStr`. Its normal representation is
masked, rejected values are hidden from validation errors, and validation
requires `postgresql+asyncpg://`. These are useful guardrails, not encryption:
code can still extract the URL to create the engine, so logging it would still
leak credentials. Service name and version are literal-valued settings; an
environment override is rejected so a running instance cannot silently change
the exact health identity contract.

`PostgresDatabase` calls `create_async_engine()` once for an application
instance. `pool_size` controls persistent pooled connections and
`max_overflow` bounds temporary connections above that pool. `pool_pre_ping`
tests a checked-out pooled connection so stale connections can be replaced.
The engine is long-lived; future request/unit-of-work sessions must be
short-lived and never global.

### HTTP health API

| Path | Role |
| --- | --- |
| `src/hookrelay/api/health.py` | Typed liveness/readiness response models and both route handlers |

The liveness handler reads only process-local settings. The readiness handler
asks the `DatabaseHealth` dependency to run a query inside
`asyncio.wait_for()`. Its exception handler logs only the exception type and
returns a stable body; it never puts the exception message or database URL in
the HTTP response.

The protocol is structural typing: any object with compatible
`check_readiness()` and `dispose()` methods can satisfy it. API tests therefore
inject controlled database behavior without teaching the production route
about test classes.

### Database migrations

| Path | Role |
| --- | --- |
| `alembic.ini` | Alembic command and logging configuration |
| `migrations/env.py` | Builds the migration context and bridges Alembic's synchronous command model to the async database engine |
| `migrations/script.py.mako` | Template for future revision files |
| `migrations/versions/.gitkeep` | Keeps the intentionally empty revision directory in Git |

The API does not call Alembic or `metadata.create_all()` during startup. Run
migrations explicitly. When Stage 2 adds ORM metadata, an autogenerated
revision will still be a draft that must be reviewed; generation cannot infer
the intent of every rename, data backfill, or destructive change.

### Containers and CI

| Path | Role |
| --- | --- |
| `.dockerignore` | Excludes Git data, secrets, caches, tests, and local artifacts from the Docker build context |
| `Dockerfile` | Builds the locked package and runs the service as a non-root user |
| `compose.yaml` | Defines the `api` and `postgres` services, health checks, local-only ports, network settings, and named volume |
| `.github/workflows/ci.yml` | Runs reproducible installation, Ruff, mypy, tests, migration validation, and a Docker build on a standard Ubuntu runner |

### Tests and teaching material

| Path | Role |
| --- | --- |
| `tests/conftest.py` | Runs ASGI lifespan explicitly and supplies an async HTTPX2 client using in-process ASGI transport |
| `tests/api/test_liveness.py` | Asserts the exact liveness JSON/status and OpenAPI route |
| `tests/api/test_readiness.py` | Injects success, exceptions, and delay; checks `200`, sanitized `503`, liveness isolation, recovery, timeout, and shutdown disposal |
| `tests/unit/test_config.py` | Exercises prefixed environment parsing, invalid driver input hiding, credential masking, package-version agreement, and rejection of health-identity overrides |
| `tests/integration/test_postgres_readiness.py` | Runs readiness through SQLAlchemy/asyncpg against a real PostgreSQL server when the test URL is supplied |
| `tests/integration/test_alembic.py` | Starts an isolated Alembic subprocess and requires `upgrade head` to succeed against the supplied PostgreSQL test URL |
| `docs/architecture.md` | Cumulative current/future architecture and invariants |
| `docs/glossary.md` | Cumulative definitions, including explicit future-stage labels |
| `docs/interview-guide.md` | Honest project pitch and strong-answer ingredients |
| `docs/decisions/` | Short records of major decisions and rejected alternatives |

## 5. Request data-flow traces

### `GET /health/live`

1. A caller sends an HTTP request to Uvicorn.
2. Uvicorn invokes the FastAPI ASGI application.
3. FastAPI selects `liveness()` from `api/health.py`.
4. The handler reads service name and version from `app.state.settings`.
5. Pydantic constructs `LivenessResponse`.
6. FastAPI returns HTTP `200` and exactly:

   ```json
   {
     "status": "ok",
     "service": "hookrelay",
     "version": "0.1.0"
   }
   ```

There is no engine checkout, SQL, DNS lookup, or PostgreSQL call on this path.

### `GET /health/ready` when PostgreSQL is usable

1. FastAPI loads settings and the database component from application state.
2. The route starts `database.check_readiness()` under
   `asyncio.wait_for()` using `readiness_timeout_seconds`.
3. `PostgresDatabase` asks its long-lived engine for a connection.
4. SQLAlchemy uses the pool and `asyncpg` driver to reach PostgreSQL.
5. PostgreSQL evaluates `SELECT 1`.
6. The connection context returns the connection to the pool.
7. The route returns HTTP `200` with the same stable `ok` shape as liveness.

### `GET /health/ready` when PostgreSQL fails or hangs

1. Connection checkout/query raises, or `wait_for()` raises a timeout.
2. The handler catches the failure so a raw exception cannot become the public
   response.
3. Internal JSON logging records `database_readiness_failed` and the exception
   class, not its potentially secret message.
4. The route returns HTTP `503` and exactly:

   ```json
   {
     "status": "unavailable",
     "service": "hookrelay",
     "version": "0.1.0"
   }
   ```

5. A later request executes a new probe. The API does not cache failure, so it
   can report ready after PostgreSQL returns without an API restart.
6. The liveness path stays independent and continues returning `200`.

### Startup and shutdown

1. `create_app()` resolves validated settings and configures logging.
2. Entering FastAPI lifespan creates or accepts one database owner and stores
   dependencies on application state.
3. The process logs `application_started` and serves requests.
4. On graceful shutdown, the lifespan `finally` block awaits
   `database.dispose()` and then logs `application_stopped`.

The `finally` block matters: cleanup runs even if serving ends through an error.

## 6. Definitions of new technology and terms

The [cumulative glossary](../glossary.md) gives expanded definitions. These are
the Stage 1 concepts Tarun should be able to define without naming only a brand:

| Term | Working definition |
| --- | --- |
| FastAPI | An ASGI framework that routes HTTP and uses typed Pydantic contracts |
| ASGI | The async interface between Uvicorn and the Python application |
| Uvicorn | The server that binds a socket, owns the event loop, and invokes ASGI |
| Application factory | A function that constructs an app and wires explicit dependencies |
| Lifespan | The ASGI startup/shutdown context for resource ownership and cleanup |
| Pydantic Settings | Typed parsing and validation of process configuration |
| Structured JSON log | One machine-readable object with stable named fields per log event |
| Coroutine | Suspendable work created by `async def` and resumed by an event loop |
| Async I/O | Cooperative waiting that lets other tasks run; not CPU parallelism |
| Timeout | A deadline that requests cancellation/failure; not a hard OS kill of uncooperative work |
| Liveness | Whether this process can answer, independent of external dependencies |
| Readiness | Whether this instance can currently handle dependency-backed work |
| Sanitization | Removing internal/secret detail from a public failure response |
| Protocol | A structural typed capability that compatible objects can satisfy |
| SQLAlchemy async engine | Process-scoped database entry point and pool owner |
| `asyncpg` | Nonblocking PostgreSQL driver used beneath SQLAlchemy |
| Connection pool | Bounded reusable database connections owned by the engine |
| Session/unit of work | Short-lived future transaction boundary; unsafe as a global shared object |
| Alembic | Ordered, reviewable SQLAlchemy schema migration tooling |
| Revision/head | One migration node / the newest node in an Alembic history |
| `src/` layout | Package source separated from repository root to avoid accidental imports |
| Hatchling | PEP 517 build backend that packages `src/hookrelay` into installable artifacts |
| uv/lockfile | Dependency tool / exact resolved graph for reproducible sync |
| Ruff | Linter and deterministic formatter |
| mypy | Static type checker; it does not execute code |
| pytest/pytest-asyncio | Behavior test runner / explicit support for coroutine tests |
| pytest-cov | Coverage.py integration that reports executed lines; not proof of test quality |
| HTTPX2 | Maintained async client used for in-process ASGI API tests |
| ASGI transport | Calls the application directly without TCP networking |
| Integration test | Crosses a real boundary, here the PostgreSQL server and driver |
| CI | Clean-machine automated checks on pushes and pull requests |
| Dockerfile | Build recipe; not an image and not a running container |
| Image | Immutable layered build result used to start containers |
| Container | Isolated running process sharing a Linux host/VM kernel |
| Compose | Declarative local multi-container topology |
| Named volume | Docker-managed persistent data independent of one container |
| Health check | Periodic command that reports container state; not automatic repair |

Three misconceptions to correct immediately:

- `async def` does not make CPU work parallel and does not convert a blocking
  library into a nonblocking library.
- A Docker container is not a separate full virtual machine.
- An environment variable is not automatically a secure secret.

## 7. Why each technology was chosen

| Choice | Engineering reason |
| --- | --- |
| Python 3.12 | Familiar language lets the project focus on distributed-system failures; modern typing/async features are available |
| FastAPI + Pydantic | Typed HTTP contracts, generated OpenAPI, and an ecosystem Tarun can explain and extend |
| Pydantic Settings | Fails early on invalid environment configuration instead of discovering it during traffic |
| Standard-library JSON logging | Small foundation with no extra logging dependency; stable fields can later enter a log pipeline |
| SQLAlchemy 2 async | Explicit engine/pool/session model with reviewed SQL/ORM flexibility for later domain work |
| `asyncpg` | Maintained genuinely asynchronous PostgreSQL network driver |
| PostgreSQL | Strong transactions, constraints, concurrency, and future transactional-outbox fit |
| Alembic | Ordered reviewed schema history with explicit deployment execution |
| Hatchling | Lean standards-based build backend with direct `src/` package configuration |
| uv + `uv.lock` | Fast reproducible dependency resolution and frozen clean-checkout installs |
| HTTPX2 async client | Matches the async app, avoids the deprecated legacy test-client path in the resolved stack, and can later serve outbound HTTP work |
| pytest + pytest-asyncio | Focused fixtures and explicit async test execution |
| pytest-cov | Optional local line-coverage inspection for finding gaps without claiming a percentage proves correctness |
| Ruff + mypy | Deterministic style, selected correctness rules, and strict static contracts |
| Docker + Compose | Reproducible Linux packaging and a local topology with real PostgreSQL |
| GitHub Actions | Clean Ubuntu verification on pushes/PRs using a standard public-repository runner |

Python is a deliberate learning/productivity choice, not a claim that it always
outperforms Go. PostgreSQL is a correctness choice, not a claim that SQLite or
MongoDB are bad databases. The ADRs preserve this context.

## 8. Serious alternatives and why they were not selected

### Go instead of Python

Go offers cheap goroutines, static binaries, and attractive worker resource
usage. It is a credible post-MVP benchmark candidate. A mixed-language MVP
would add tooling and contract overhead before Python is measured as a
bottleneck. See [ADR 0001](../decisions/0001-python-over-go.md).

### Flask/Django instead of FastAPI

Flask is small and flexible; Django offers a comprehensive batteries-included
stack. FastAPI fits this service's async I/O and typed-contract needs with less
framework surface. Either alternative could work, but changing frameworks does
not itself solve delivery correctness.

### Synchronous SQLAlchemy/driver

A synchronous stack is simpler in some deployments and can scale with more
threads/processes. In an asyncio service, calling it directly would block the
event loop; offloading every call to threads adds another concurrency model.
The maintained async engine and driver provide a coherent I/O path.

### SQLite instead of PostgreSQL

SQLite is excellent for embedded use and fast unit tests. Its locking,
concurrency, SQL, and type behavior do not prove the multi-process PostgreSQL
transactions HookRelay will use. See
[ADR 0002](../decisions/0002-postgresql-over-sqlite-or-mongodb.md).

### MongoDB instead of PostgreSQL

MongoDB is credible for flexible document workloads and supported distributed
topologies. HookRelay's tenant/event/delivery/attempt/outbox relationships need
constraints, joins, and shared transactions; PostgreSQL is the more direct
model without a demonstrated document need.

### `metadata.create_all()` instead of Alembic

`create_all()` is useful for disposable prototypes but only creates missing
objects. It does not describe data backfills, rename intent, destructive
changes, or an ordered history. See
[ADR 0003](../decisions/0003-explicit-alembic-migrations.md).

### Automatic migration at API startup

It appears convenient but couples traffic startup to privileged DDL, lets
replicas race, and can cause restart loops. An explicit migration step is
observable and controllable.

### Setuptools or Flit instead of Hatchling

Setuptools is the most established general-purpose build backend and supports
complex extension/custom build needs; Flit is intentionally minimal for simple
packages. HookRelay needs only a straightforward pure-Python `src/` package, so
Hatchling gives concise standards-based configuration without legacy setup
files. The backend is replaceable if packaging needs materially change.

### A mandatory line-coverage percentage

A numeric threshold can expose completely untested code, but it can also reward
executing lines without meaningful assertions or realistic failures. Stage 1
keeps pytest-cov available for inspection and judges evidence by behavior and
boundary. A later threshold should follow an agreed risk policy, not an
arbitrary impressive number.

### Legacy synchronous test client

The resolved FastAPI/Starlette stack warns on its old HTTPX compatibility path.
An asynchronous HTTPX2 client with ASGI transport matches production coroutine
boundaries. Lifespan is entered explicitly because in-process transport alone
does not guarantee startup/shutdown events.

### Exactly-once delivery

This is a future semantic choice, but it affects how the system is described
now. HTTP acknowledgment can disappear after a receiver commits. HookRelay
cannot atomically control its database and an arbitrary receiver database, so
future delivery will be at least once with stable IDs and receiver-side
idempotency. See [ADR 0004](../decisions/0004-at-least-once-delivery.md).

## 9. Failure modes and design tradeoffs

| Failure or pressure | Stage 1 behavior | Tradeoff or remaining risk |
| --- | --- | --- |
| PostgreSQL is down | Readiness returns sanitized `503`; liveness stays `200` | A health probe cannot repair the DB; traffic routing must respect readiness |
| PostgreSQL accepts TCP but query hangs | `asyncio.wait_for` bounds readiness | Too-short timeouts cause false negatives; too-long timeouts delay removal |
| Pooled connection became stale | `pool_pre_ping` detects it on checkout | Adds a small check cost; cannot prevent every mid-query network failure |
| Many concurrent DB operations | Engine pool bounds reusable/base and overflow connections | Pool limits must match process count and PostgreSQL capacity; Stage 7 measures load |
| Invalid configuration | Pydantic fails process construction early | Strict validation reduces flexible typos but makes configuration errors explicit outages |
| Database exception contains URL | Public route omits message and logs only class | Exception type is useful but less diagnostic than a protected full trace; secrets must never be logged |
| API shuts down | Lifespan awaits engine disposal | Forced termination can bypass graceful cleanup; the OS/database still reclaim broken connections eventually |
| ORM model changes | Existing schema does not change | A reviewed Alembic revision is required; this is deliberate work, not automation failure |
| Real `.env` is present | Git and Docker ignore it | Ignore rules reduce accidental exposure but local malware/process access remains possible |
| PostgreSQL container replaced | Named volume retains database files | Explicit volume deletion loses data; volumes need backup outside this local workflow |
| Compose database becomes unhealthy after startup | API remains running and readiness changes | `depends_on` is only initial gating, not continuous health management |
| Unit/API tests pass | In-process behavior is supported | TCP, DNS, PostgreSQL semantics, migrations, and packaging still need separate checks |
| Full Stage 1 checks pass | Foundation behavior is supported in tested environments | It does not prove webhook correctness, security, or production scale |

### Database lifecycle tradeoff

One engine per process amortizes connection setup and centralizes pooling. It
also means total database connections multiply by API process count:

```text
possible connections ~= process count * (pool_size + max_overflow)
```

That is a capacity ceiling, not a recommended steady-state target. Later worker
processes also need a connection budget. A single global SQLAlchemy session is
not an optimization: sessions contain mutable transaction state and are unsafe
across unrelated concurrent work.

### Health-check tradeoff

`SELECT 1` is intentionally shallow. It detects network, credentials,
connection checkout, driver, and basic query execution. It does not prove that
future migrations are current or domain queries are fast. A very deep probe can
be expensive and can itself overload a failing dependency. Stage 1 chooses a
cheap continuously recoverable signal and validates migrations separately.

## Docker deep dive

This section connects every Docker instruction to an operational reason. Start
with four different objects:

- The **Dockerfile** is version-controlled build source: a recipe.
- `docker build` evaluates that recipe and produces an immutable layered
  **image**.
- `docker run` or Compose creates an isolated running **container** from the
  image.
- The **Docker Engine** is the daemon that builds images and manages containers,
  networks, and volumes.

A container is not a full virtual machine. On Windows, Docker Desktop normally
runs the Linux Docker Engine inside a WSL 2 Linux VM because Linux containers
need a Linux kernel. The API and PostgreSQL containers use separate Linux
namespaces and filesystems but share that WSL 2 VM's kernel. The Windows Docker
client sends requests to the engine in the VM; there is not one VM per
container.

### Dockerfile, instruction by instruction

Blank lines improve readability and do not create layers. Comments document
policy and likewise do not create runtime content.

| Lines | Instruction | Why it exists |
| --- | --- | --- |
| 1-3 | `ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm` | Gives both stages one reviewed base-image choice. Python patch and Debian suite are explicit. This is portable across supported architectures; a digest would be stricter immutability but requires a deliberate multi-architecture policy. Refresh the tag through a reviewed rebuild and security check. |
| 5 | `FROM ${PYTHON_IMAGE} AS builder` | Starts a build-time stage named `builder`. Tools and package installation can live here without all becoming runtime filesystem layers. |
| 7 | `ARG UV_VERSION=0.12.1` | Pins the build tool version rather than installing whatever `uv` happens to be newest. This argument is needed only in the builder stage. |
| 9-12 | `ENV PIP_DISABLE_PIP_VERSION_CHECK=1 ...` | Removes irrelevant pip update noise, asks uv to precompile bytecode, and makes uv copy dependencies instead of relying on links that may not survive the cross-stage copy. `UV_PYTHON_DOWNLOADS=never` requires uv to use the pinned base-image interpreter rather than silently downloading another Python. |
| 14 | `WORKDIR /app` | Makes `/app` the default location for later `COPY`, `RUN`, and startup operations. Docker creates it if needed. |
| 16 | `RUN ... pip install --no-cache-dir "uv==${UV_VERSION}"` | Installs the pinned resolver in the builder and discards pip's download cache from that layer. The runtime image does not repeat this tool installation. |
| 18-20 | `COPY pyproject.toml uv.lock README.md LICENSE ./` | Copies relatively stable dependency/build metadata before application source. Docker invalidates a layer when its inputs change, so normal source edits can preserve the expensive dependency layer. README and license are package-build inputs. |
| 21 | `RUN uv sync --frozen --no-dev --no-install-project` | Installs the exact locked runtime third-party graph but not dev tools or the HookRelay package yet. `--frozen` refuses to silently rewrite the lockfile. |
| 23 | `COPY src ./src` | Source changes enter only after third-party dependency installation, improving layer-cache reuse. |
| 24 | `RUN uv sync --frozen --no-dev --no-editable` | Installs HookRelay itself into `/app/.venv` as a regular package. `--no-editable` avoids a runtime package that depends on a mutable source-tree link. |
| 27 | `FROM ${PYTHON_IMAGE} AS runtime` | Begins a fresh minimal runtime stage. Builder-only uv/pip caches and intermediate layers are not copied automatically. This is the build-time/runtime boundary. |
| 29-31 | `ENV PATH=... PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1` | Puts the virtual environment's `hookrelay` command first, avoids runtime `.pyc` writes, and flushes logs promptly instead of buffering them. |
| 33 | `WORKDIR /app` | Establishes the runtime working directory independently; stages do not inherit one another's filesystem/configuration unless specified. |
| 35-37 | `RUN groupadd ... && useradd ...` | Creates a fixed unprivileged UID/GID 10001 with no dedicated home directory created and a non-login shell. `--no-log-init` avoids initializing login-account databases for a container-only identity. Fixed IDs make ownership predictable. This instruction runs as root at image-build time, when account creation is required. |
| 39 | `COPY --from=builder /app/.venv /app/.venv` | Copies only the completed runtime environment from the builder. This is the central multi-stage size and attack-surface reduction. |
| 40-43 | Root-owned `COPY` of `alembic.ini` and `migrations` | Includes migration tooling needed for an explicit deployment command. It stays readable but not writable by the service user, so a compromised API process cannot rewrite code used by a later operator command. No migration runs here or at API startup. |
| 45 | `USER 10001:10001` | Every subsequent runtime instruction and the default service process use the unprivileged account. Non-root reduces impact; it does not eliminate application vulnerabilities or excessive container permissions. |
| 47 | `EXPOSE 8000` | Documents the intended container port. It does **not** publish a host port or create firewall policy; Compose's `ports` mapping does that. |
| 49-50 | `HEALTHCHECK ... urllib.request.../health/live` | Asks the Python process itself whether it is alive every 10 seconds, with bounded timing and startup grace. Standard-library `urllib` avoids installing `curl`. Liveness is deliberate: a PostgreSQL outage must not label the API process dead. |
| 52 | `CMD ["hookrelay"]` | Sets the default startup command in exec form so the service receives Unix signals directly. The command comes from `pyproject.toml` and is installed in the copied virtual environment. |

#### Build-time versus runtime

Build time is when source and dependency metadata become an image. It may need
installers and network access. Runtime is when the already-built service starts
and handles traffic. A multi-stage build lets the runtime receive the finished
virtual environment without carrying all builder machinery. It is not a magic
security scanner: the team still reviews dependencies, rebuilds base images,
and tests the artifact.

#### Layer caching

Each filesystem-changing instruction produces a cacheable result. Docker
reuses it only when the instruction and all relevant inputs match. The order
above means:

```text
change only src/ -> reuse base + uv install + third-party dependency layers
change uv.lock   -> rebuild dependency layer and everything after it
change base tag  -> rebuild both stage ancestry chains
```

`--pull` during the reviewed CI/local build checks for a refreshed base matching
the pinned tag. A fully content-addressed digest is stronger against tag
movement, but portable multi-architecture digest maintenance is an explicit
tradeoff rather than hidden here.

### `.dockerignore`, line by line

Docker sends a **build context** to its builder. Ignoring a file prevents it
from entering that context; deleting it later in a Dockerfile layer would be too
late because the builder already received it.

| Lines | Pattern group | Reason |
| --- | --- | --- |
| 1-2 | `.git`, `.github` | Git history and CI configuration are not package runtime inputs. |
| 3-11 | `.venv`, uv/Python/test/type/lint/coverage caches | Host environments may be incompatible with Linux and make the context large or nondeterministic. The image rebuilds from the lockfile. |
| 13-17 | `.env`, `.env.*`, then `!.env.example` | Excludes potentially credential-bearing local files while allowing the safe documentation template. The Dockerfile currently does not copy the template, but the exception states policy and permits future explicit use. |
| 19-28 | tests, docs, build output, IDE and OS files | Keeps development-only or generated content out of the production image context. CI runs tests before image build rather than shipping them. |

`.dockerignore` complements `.gitignore`; neither is a secret manager. If a
secret was committed previously, ignoring it now does not erase Git history.

### Compose, stanza by stanza

Compose creates an implicit private network for this project. Service names are
DNS names on that network.

| Lines | Compose configuration | Meaning |
| --- | --- | --- |
| 1-8 | Project name and `api` build/image | Names the Compose project, builds from this directory's Dockerfile, and tags the local result `hookrelay-api:stage1`. |
| 9-17 | API environment | Uvicorn listens on `0.0.0.0` **inside** the container. The database URL uses hostname `postgres`; `localhost` here would mean the API container. `${NAME:-default}` allows `.env` development overrides. |
| 18-19 | `127.0.0.1:${API_HOST_PORT:-8000}:8000` | Publishes host loopback port 8000 (or override) to container port 8000. Binding to `127.0.0.1` avoids intentional exposure on LAN interfaces. |
| 20-22 | health-conditioned `depends_on` | Gates initial API startup until PostgreSQL first reports healthy. It does not continuously guarantee dependency health after startup. |
| 23-32 | API health check | Repeats the image's dependency-free liveness request with explicit interval, timeout, retries, and startup grace. It reports health; it is not a complete restart policy. |
| 33 | `init: true` | Adds a small PID 1 that forwards signals and reaps orphaned child processes in the container. |
| 34 | API grace period | Allows ten seconds for Uvicorn/FastAPI lifespan shutdown, including awaited engine disposal, before forced termination. |
| 36-43 | PostgreSQL image/environment | Uses explicit `postgres:17.7-bookworm` and local bootstrap values. Defaults are for development only and are not acceptable production credentials. |
| 44-45 | PostgreSQL port publishing | Maps host loopback to container 5432 for local integration tests/tools. The API itself uses the private network, not this host mapping. |
| 46-47 | `postgres_data:/var/lib/postgresql/data` | Mounts Docker-managed persistent storage at PostgreSQL's data directory so container replacement does not erase it. |
| 48-55 | `pg_isready` health check | Asks the server whether it is accepting connections for the configured user/database. `$$` escapes Compose substitution so the variables expand inside the container. It does not prove credentials can run every query or that migrations are current. |
| 56 | PostgreSQL grace period | Gives PostgreSQL thirty seconds to checkpoint and stop cleanly before force. |
| 58-59 | top-level named volume declaration | Tells Docker to manage `postgres_data` independently of a particular container. |

#### Container networking and ports

Two address spaces are involved:

```text
Windows caller -> 127.0.0.1:8000 -> API container:8000
API container  -> postgres:5432  -> PostgreSQL container:5432
```

The host port can change without changing the container port. Service-name DNS
works only on the Compose network. `EXPOSE 8000` documents a port but does not
create either arrow.

#### Named-volume persistence

Containers should be replaceable. PostgreSQL's data therefore lives in
`postgres_data`, whose lifecycle is separate:

- `docker compose down` removes the containers/network but preserves the named
  volume.
- `docker compose up` can attach new containers to the same data.
- `docker compose down --volumes` deliberately deletes it and is destructive.

A named volume is local persistence, not a backup. The application team still
owns backups, restore drills, schema compatibility, retention, and production
storage policy.

#### Health checks and `depends_on`

PostgreSQL's `pg_isready` gates **initial** API startup. The API check calls
liveness, so Docker does not confuse a later database outage with a dead Python
process. Application readiness separately reports `503` to a traffic router or
operator. Compose does not continuously restart or pause the API just because
PostgreSQL later becomes unhealthy; this is why the safe failure exercise is
meaningful.

### What Docker manages and what the team still manages

| Docker/Compose can manage | The application/operations team must still manage |
| --- | --- |
| Image build steps and layer cache | Correct code, dependency review, base refresh, vulnerability response |
| Container process isolation | Least privilege, patching, threat model, SSRF/auth controls |
| Private network and service DNS | Timeouts, retries, readiness semantics, TLS and production ingress |
| Host-to-container port forwarding | Exposure policy and firewalls outside local development |
| Named-volume lifecycle | Backups, restore testing, encryption, capacity, retention |
| Health status and initial dependency gating | Failure recovery, routing policy, incident response, SLOs |
| Passing environment values | Secret generation, storage, access, rotation, and audit |
| Signal delivery and grace periods | Correct graceful shutdown and bounded cleanup code |

Docker makes the environment reproducible; it does not make the application
reliable, secure, scalable, or correctly backed up by itself.

## 10. Exact commands for running and testing

Except for prerequisite installation and the clone itself, run commands from
the repository root. The examples below use Windows PowerShell because this
project is being developed on Windows. After bootstrap, they call the
project-local `.venv` executables directly and do not depend on a PATH-level
`python` or `uv`. On POSIX, create the environment with `python3.12 -m venv
.venv` and use `./.venv/bin/uv`.

### Inspect prerequisites

```powershell
git --version
winget --version
docker version
docker compose version
wsl --status
```

`docker version` must show both client and server. A client-only result usually
means Docker Desktop's engine is not running. If Docker or WSL is blocked by
device policy, do not bypass the policy; use GitHub Codespaces for the
interactive Linux/PostgreSQL workflow and GitHub Actions for integration/image
checks.

### Install or verify CPython 3.12

The Codex desktop app may have an internal bundled Python, but its private cache
path is not a portable developer prerequisite. The repository requires a
normal CPython 3.12 installation. First try:

```powershell
py -3.12 --version
```

If the Windows Python launcher is absent or cannot find 3.12, install the
per-user Python.org distribution through Windows Package Manager:

```powershell
winget install --exact --id Python.Python.3.12 --scope user
```

Close and reopen PowerShell so launcher/PATH changes take effect, then require
`py -3.12 --version` to succeed before continuing. If `winget` is unavailable,
install Python 3.12 from python.org with the Python launcher enabled; do not
substitute an older interpreter. If device policy blocks installation, do not
bypass it; use a GitHub Codespace with Python 3.12 instead.

### Clean-checkout Python setup

```powershell
git clone --branch codex/stage-01-foundation --single-branch https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

That clone command checks out the Stage 1 branch while its pull request is
open. After Stage 1 is merged, omit `--branch ... --single-branch` to clone
`main` instead.

`--frozen` makes a lockfile mismatch an error instead of modifying `uv.lock`.
`--all-groups` includes test, lint, formatting, and type-check tools. The
Docker runtime intentionally uses `--no-dev` instead.

If the repository is already present, do not clone over it. Inspect
`git status` and preserve existing work before syncing.

### Full Docker Compose workflow

Create an untracked local configuration and edit its development password:

```powershell
if (-not (Test-Path -LiteralPath '.env')) { Copy-Item .env.example .env }
notepad .env
```

For this local Compose template, choose a URL-safe password made from letters,
digits, `-`, `_`, or `.`. Compose inserts the same raw value into PostgreSQL's
bootstrap setting and the SQLAlchemy URL; characters such as `@`, `:`, `/`,
`%`, whitespace, or URL delimiters would require separate URL encoding and must
not be used in this shared development value. Production must construct an
encoded URL from an independently managed secret rather than reuse this
shortcut.

Then validate, build, and start:

```powershell
docker compose config --quiet
docker compose up --build --detach
docker compose ps
```

Wait until both services report healthy. Apply migrations explicitly from the
API image, even though Stage 1 has no domain revision:

```powershell
docker compose exec api alembic upgrade head
```

Inspect both contracts:

```powershell
curl.exe -sS -i http://127.0.0.1:8000/health/live
curl.exe -sS -i http://127.0.0.1:8000/health/ready
```

Follow logs and identify the HookRelay-owned JSON events:

```powershell
docker compose logs --follow api
```

Press Ctrl+C to stop following logs; it does not stop the service. Stop the
environment while retaining database data with:

```powershell
docker compose down
```

Do not add `--volumes` unless deleting the local database is intentional.

### Run Python on the host with Compose PostgreSQL

This workflow gives quick local Uvicorn restarts while still using real PostgreSQL.
Start only the database. The values below match an unchanged `.env.example`; if
you edited its password or port, use the same values in both URLs.

```powershell
if (-not (Test-Path -LiteralPath '.env')) { Copy-Item .env.example .env }
docker compose up --detach postgres
$env:HOOKRELAY_DATABASE_URL = 'postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay'
$env:HOOKRELAY_TEST_DATABASE_URL = $env:HOOKRELAY_DATABASE_URL
.\.venv\Scripts\uv.exe run alembic upgrade head
.\.venv\Scripts\uv.exe run hookrelay
```

In a second PowerShell window:

```powershell
curl.exe -sS -i http://127.0.0.1:8000/health/live
curl.exe -sS -i http://127.0.0.1:8000/health/ready
```

The host process uses `127.0.0.1`; only the API container uses Compose DNS name
`postgres`. Stop the host server with Ctrl+C and the database with:

```powershell
docker compose down
```

### Python-only checks when Docker is unavailable

The fast suite does not require a live dependency:

```powershell
.\.venv\Scripts\uv.exe run ruff check .
.\.venv\Scripts\uv.exe run ruff format --check .
.\.venv\Scripts\uv.exe run mypy
.\.venv\Scripts\uv.exe run pytest -m "not integration"
```

Optionally inspect executed lines without treating the percentage as a
correctness score:

```powershell
.\.venv\Scripts\uv.exe run pytest -m "not integration" --cov=hookrelay --cov-report=term-missing
```

Stage 1 CI does not enforce a coverage threshold; the behavioral and
integration boundaries in the test table matter more than a standalone number.

The liveness-only API can start without PostgreSQL because engine connections
are acquired lazily. Readiness should return `503` in that condition; that is
expected behavior, not a substitute for the integration checks.

### Complete local check sequence

With PostgreSQL available and both URL environment variables set:

```powershell
.\.venv\Scripts\uv.exe sync --frozen --all-groups
.\.venv\Scripts\uv.exe run ruff check .
.\.venv\Scripts\uv.exe run ruff format --check .
.\.venv\Scripts\uv.exe run mypy
.\.venv\Scripts\uv.exe run pytest -m "not integration"
.\.venv\Scripts\uv.exe run pytest -m integration
.\.venv\Scripts\uv.exe run alembic upgrade head
docker compose config --quiet
docker build --pull --tag hookrelay:stage1 .
```

This mirrors CI's sensible fail-fast order: cheap static failures occur before
slower integration and image work.

### Alembic commands for future schema work

Stage 1 should only validate `upgrade head`. After Stage 2 defines shared ORM
metadata and imports it in `migrations/env.py`, the workflow becomes:

```powershell
.\.venv\Scripts\uv.exe run alembic revision --autogenerate -m "describe the schema change"
.\.venv\Scripts\uv.exe run alembic upgrade head
```

Do not run the `revision` command in Stage 1. Never accept autogenerated output
without reviewing operations, constraints, data movement, and downgrade logic.

### CI order

`.github/workflows/ci.yml` runs on pushes and pull requests on a standard
`ubuntu-latest` runner with PostgreSQL `17.7-bookworm` as a service:

1. check out the repository;
2. install Python `3.12.13` and uv `0.12.1`;
3. run `uv sync --frozen --all-groups`;
4. run `ruff check`;
5. run `ruff format --check`;
6. run strict `mypy`;
7. run non-integration unit/API tests;
8. run real PostgreSQL integration tests;
9. run `alembic upgrade head`;
10. validate Compose and build the Docker image with `--pull`.

The job uses read-only repository permissions, a twenty-minute bound, and
concurrency cancellation for superseded runs. Standard GitHub-hosted runners
are available for public repositories; no paid larger runner is selected.

## 11. How each test works—and what it cannot prove

### Test harness

`tests/conftest.py` uses `LifespanManager` to enter application startup/shutdown
and HTTPX2 `ASGITransport` to call the ASGI app directly. This is faster and
more deterministic than binding a socket. It also means those tests do not
exercise host ports, Compose DNS, TCP connection setup, TLS, or the Docker
image.

| Test | Mechanism and evidence | What it does not prove |
| --- | --- | --- |
| `test_liveness_contract` | Calls `/health/live` through ASGI and asserts exact `200` JSON | Real networking, PostgreSQL, container packaging, or scale |
| `test_openapi_contains_liveness_route` | Reads generated `/openapi.json` and checks both health paths exist | That external clients use the schema correctly or routes work over a socket |
| `test_readiness_contract_when_database_is_usable` | Injects a successful `DatabaseHealth` stub and asserts exact ready contract | Actual SQL, credentials, driver, DNS, or PostgreSQL behavior |
| `test_readiness_failure_is_sanitized_and_liveness_stays_healthy` | Injects an exception containing a fake secret; asserts `503`, exact public body, absence of secret, and liveness `200` | That every future exception/log path is secret-free or a real outage behaves identically |
| `test_readiness_recovers_without_application_restart` | Makes one stub fail and the next call succeed on the same app | Real PostgreSQL restart timing or stale pooled-connection behavior |
| `test_readiness_query_has_a_bounded_timeout` | Injects a slow coroutine with a tiny configured timeout and expects `503` | OS-level TCP timeout behavior or the best production timeout value |
| `test_application_disposes_database_during_shutdown` | Observes the injected database before/after lifespan exit | That every forced-kill path runs graceful cleanup |
| `test_settings_read_prefixed_environment` | Sets prefixed process variables and verifies typed parsing | Every deployment platform's environment injection |
| `test_settings_reject_non_async_postgresql_url` | Supplies a synchronous PostgreSQL URL containing a fake secret; expects validation failure and proves the rejected input is hidden | Reachability or correctness of a syntactically valid async PostgreSQL URL |
| `test_settings_mask_database_credentials` | Ensures a known password is absent from settings `repr` | That application code never explicitly logs the extracted value |
| `test_settings_version_matches_package_version` | Guards the duplicated literal health version against package-version drift | Automated release/version bumping across future artifacts |
| `test_settings_reject_health_contract_identity_overrides` | Supplies environment overrides and proves startup rejects a changed service/version contract | That every future response field is immutable or correctly versioned |
| `test_readiness_executes_against_real_postgresql` | With `HOOKRELAY_TEST_DATABASE_URL`, builds the real app and gets ready `200` through SQLAlchemy and asyncpg | Failure recovery, migration/domain correctness, production latency, or load |
| `test_alembic_upgrade_head_against_postgresql` | Copies the test environment, maps the test URL to Alembic's runtime setting, launches `python -m alembic upgrade head` with a 30-second bound, and requires exit code zero | Any future revision's data safety/downgrade quality or production data volume; Stage 1 still has no domain revision |
| CI `alembic upgrade head` | Connects Alembic's async environment to the disposable PostgreSQL service and runs the current history | A Stage 2 schema revision, downgrade safety, or production data volume; no domain revision exists yet |
| CI `docker build` | Recreates the Linux runtime from locked inputs and reaches the final image | Container startup/network behavior unless Compose is also run; vulnerability absence |

The PostgreSQL/Alembic integration tests without
`HOOKRELAY_TEST_DATABASE_URL` are deliberately skipped. A green suite
containing those skips is not evidence that PostgreSQL or migrations were
tested; read pytest's skip summary and run CI or configure the URL.

Likewise, a successful demo proves one observed path. It does not prove
production throughput, tail latency, retry-storm behavior, or sustained
recovery. Stage 7 will define reproducible load and failure measurements.

### Component learning loops

Use the same five-step pattern rather than passively reading each component.

#### Health separation

1. **Problem:** A database outage must not trigger API restart amplification.
2. **Build:** Trace both handlers in `api/health.py`, the injected protocol, and
   the `wait_for` timeout.
3. **Inspect:** Get `200` from both probes in a healthy Compose environment.
4. **Break it safely:** Stop only PostgreSQL and compare the two responses.
5. **Explain it:** Describe why a router may use readiness while a process
   supervisor uses liveness.

#### Engine lifecycle

1. **Problem:** Opening a connection per request is wasteful, but shared
   transaction state is unsafe.
2. **Build:** Trace `PostgresDatabase.__init__`, connection context use, and the
   lifespan `finally` block.
3. **Inspect:** Run the real readiness test and shutdown-disposal API test.
4. **Break it safely:** Temporarily make the injected disposal method record or
   raise in a local experiment; do not commit the change.
5. **Explain it:** Distinguish engine, pool, connection, and session in one
   sentence each.

#### Configuration and logging

1. **Problem:** A typo or secret leak should not become a hidden runtime fault.
2. **Build:** Trace Pydantic constraints, `SecretStr`, JSON formatter, and the
   sanitized readiness log fields.
3. **Inspect:** Run settings tests and read one startup log as JSON.
4. **Break it safely:** Start once with an invalid port or SQLite URL, observe
   validation, then remove the temporary environment variable.
5. **Explain it:** State why environment variables and masking are guardrails,
   not production secret management.

#### Migrations

1. **Problem:** Existing schemas require ordered transitions, not recreation.
2. **Build:** Trace `alembic.ini` into `migrations/env.py`, including the async
   engine, synchronous facade, `NullPool`, and explicit disposal.
3. **Inspect:** Run `alembic upgrade head` against disposable PostgreSQL and
   observe that no domain tables/revision are invented.
4. **Break it safely:** Point a single shell at an unreachable test database,
   observe the migration command fail, then restore the variable; do not change
   the checked-in configuration.
5. **Explain it:** Describe why API startup does not own migrations and why
   autogenerated revisions require review.

#### Containers and CI

1. **Problem:** "Works on my machine" does not prove a clean Linux artifact or
   a real PostgreSQL integration.
2. **Build:** Trace Docker build stages, Compose network/volume/health settings,
   and CI's fail-fast steps.
3. **Inspect:** Compare the built image, running container, `docker compose ps`,
   and the GitHub Actions log.
4. **Break it safely:** Stop PostgreSQL without stopping the API; later restart
   it. Do not remove the named volume.
5. **Explain it:** Name one responsibility Docker handles and one reliability or
   security responsibility it leaves to the team.

## 12. Safe “break it intentionally” exercise

### Goal

Prove the central invariant through observation:

```text
PostgreSQL down -> readiness 503, liveness 200, API process stays up
PostgreSQL back -> readiness 200 without API restart
```

### Preconditions

Start the full Compose environment and confirm both services are healthy:

```powershell
docker compose up --build --detach
docker compose ps
curl.exe -sS -i http://127.0.0.1:8000/health/live
curl.exe -sS -i http://127.0.0.1:8000/health/ready
```

Both requests should initially return `HTTP/1.1 200 OK`.

### Introduce the safe failure

Stop only PostgreSQL. This does not delete its container or named volume:

```powershell
docker compose stop postgres
docker compose ps
curl.exe -sS -i http://127.0.0.1:8000/health/ready
curl.exe -sS -i http://127.0.0.1:8000/health/live
```

Expected observations:

- readiness returns `503` with `status: unavailable`; a refused connection or
  DNS failure may return before the configured deadline, while `wait_for`
  requests cancellation at the deadline for a probe that keeps waiting. The
  timeout is a bound on cooperative waiting, not a hard OS kill, so wall-clock
  time can exceed it while cancellation completes and through normal
  HTTP/scheduling overhead;
- the body contains no driver exception, password, hostname details, or URL;
- liveness still returns the exact `200` contract;
- `docker compose ps` shows the API process/container remains alive and its
  liveness-based container health check can remain healthy.

Inspect internal diagnostics:

```powershell
docker compose logs --since 2m api
```

Find `database_readiness_failed` and an exception class. Verify that the
database URL/password is not present.

### Recover

```powershell
docker compose start postgres
docker compose ps
curl.exe -sS -i http://127.0.0.1:8000/health/ready
```

PostgreSQL may need several seconds to report healthy. Repeat the final request
after `docker compose ps` reports it healthy. Readiness should return `200`
without restarting `api`. This works because every readiness request performs
a new engine checkout/query and `pool_pre_ping` can reject stale pooled
connections.

### Explain the result

Answer aloud:

1. Why did Compose's health-conditioned `depends_on` not stop the API later?
2. Why would a database-backed liveness check amplify this incident?
3. Which part proves public sanitization, and which part gives internal
   diagnostics?
4. What does this exercise still fail to prove about production recovery?

Finish with `docker compose down`; do not use `--volumes`.

## 13. Troubleshooting

### `docker version` cannot reach the server

Start Docker Desktop and wait for its engine to become ready. Check `wsl
--status` and Docker Desktop's WSL 2 backend settings. If installation or WSL is
blocked by managed-device policy, stop there and use Codespaces/Actions; do not
try to bypass policy.

### A host port is already in use

Change only the host side in the untracked `.env`, for example:

```dotenv
API_HOST_PORT=8001
POSTGRES_HOST_PORT=5433
```

Then browse `127.0.0.1:8001`. Container ports remain 8000 and 5432, and the API
container still connects to `postgres:5432`.

### Readiness is `503` in Compose

Run:

```powershell
docker compose ps
docker compose logs postgres
docker compose logs api
docker compose config --quiet
```

Confirm the API database hostname is `postgres`, not `localhost`, and that the
username/database/password agree. Do not paste logs containing real secrets
into an issue or chat. Avoid sharing plain `docker compose config` output too:
it renders interpolated environment values and can expose the database
password.

PostgreSQL bootstrap variables apply when a data directory is first initialized.
Changing `.env` later does not automatically change credentials stored in an
existing named volume. Restore the original local values or change the role
password interactively with PostgreSQL. Delete a disposable volume only after
confirming its exact scope and accepting data loss.

### Host-run API is `503` but Compose API is ready

The two processes use different network views:

- host Python connects to `127.0.0.1:<POSTGRES_HOST_PORT>`;
- Compose API connects to `postgres:5432`.

Set `HOOKRELAY_DATABASE_URL` appropriately in the host shell. Keep the required
`postgresql+asyncpg://` scheme. For an independently configured database,
URL-encode a reserved character in the URL's password component. For this
Compose template, do **not** percent-encode the shared `.env` password: choose a
URL-safe value because the same raw text bootstraps PostgreSQL and is
interpolated into the URL.

### Settings rejects the database URL

The application intentionally rejects `sqlite://`, synchronous
`postgresql://`, and other schemes. Use `postgresql+asyncpg://`. This prevents a
blocking or substitute driver from silently entering the async design.

### The integration tests are skipped

Both files under `tests/integration/` require
`HOOKRELAY_TEST_DATABASE_URL`. Set it to a disposable test database and rerun:

```powershell
$env:HOOKRELAY_TEST_DATABASE_URL = $env:HOOKRELAY_DATABASE_URL
.\.venv\Scripts\uv.exe run pytest -m integration -rs
```

Check that the final output says both tests passed rather than skipped.

### `uv sync --frozen` reports a stale lockfile

Do not delete the lockfile or remove `--frozen` to hide the mismatch. If the
manifest change is intentional, run `uv lock`, inspect the dependency diff, run
all checks, and commit `pyproject.toml` and `uv.lock` together. Otherwise revert
the unintended manifest edit.

### Alembic cannot connect

Alembic reads `HOOKRELAY_DATABASE_URL` through the same validated settings; it
does not use a committed URL from `alembic.ini`. Check the current shell or the
Compose service environment. `target_metadata = None` and an empty versions
directory are correct in Stage 1.

### API container is unhealthy

Inspect only the health field, then read application logs:

```powershell
$apiContainerId = docker compose ps --quiet api
docker inspect --format '{{json .State.Health}}' $apiContainerId
docker compose logs api
```

Avoid sharing unfiltered `docker inspect` output because container environment
configuration can include the database URL/password. The health command uses
Python's standard library against `127.0.0.1:8000` inside the container. A
database outage alone should not fail that route. A wrong bind address, process
crash, startup error, or invalid configuration can.

### Logs are not pretty text

Each HookRelay-owned event is intentionally one compact JSON object. Uvicorn's
server and access records remain plain text in Stage 1, and Compose may add a
service prefix to displayed lines. Parse the JSON portion in a JSON-aware viewer
or log system; its stable `event`/`level` fields are more useful to machines
than aligned prose. Unifying server logs, correlation IDs, and traces is Stage
6 work.

### Tests pass locally but CI fails

Compare Python/uv versions and use `uv sync --frozen`. CI also has boundaries a
fast local run may omit: clean Ubuntu, real PostgreSQL, Alembic, Compose config,
and Docker build. Fix the actual failing boundary rather than skipping it.

## 14. Recruiter and interviewer questions

The cumulative [interview guide](../interview-guide.md) has longer practice
answers. Use these ingredients, not memorized wording.

### “Why split liveness and readiness?”

- Liveness is process-local; readiness represents dependency-backed service.
- Database-backed liveness causes useless restart amplification during an
  outage.
- Evidence: injected failure test plus the safe Compose outage/recovery.
- Limit: probes report state; they do not repair PostgreSQL.

### “How does database lifecycle work?”

- One async engine/pool per app process.
- Connections are borrowed briefly; future sessions are per unit of work.
- Lifespan `finally` awaits disposal.
- A global session would mix concurrent transaction state.

### “Why PostgreSQL rather than SQLite or MongoDB?”

- Future relational constraints, concurrency, unique idempotency rules, and a
  transactional outbox need real transaction behavior.
- SQLite is a serious embedded/test option but a misleading substitute for
  these integration semantics.
- MongoDB is serious for document needs, but no current requirement offsets the
  relational mismatch.

### “Why not migrate on API startup?”

- Replica races, privileged DDL, slow startup, and restart loops.
- An explicit Alembic deployment step is reviewable and observable.
- `create_all()` is not schema history or data migration.
- No empty Stage 1 revision; first domain revision belongs to Stage 2.

### “What does async mean here?”

- Coroutines yield during nonblocking asyncpg/network waits.
- `async def` is not CPU parallelism and cannot redeem a blocking library.
- The readiness wait is bounded; later worker concurrency will also be bounded.

### “What does Docker add?”

- Locked repeatable Linux packaging and a real two-service topology.
- Compose DNS, loopback-only published ports, health checks, and durable local
  storage.
- Containers share the WSL 2 VM kernel on Windows; they are not separate VMs.
- Docker does not own app correctness, secret rotation, backups, or production
  recovery.

### “How do you know Stage 1 works?”

- Exact async HTTP contract tests.
- Injected failure, timeout, recovery, and disposal tests.
- Real PostgreSQL integration test.
- Explicit Alembic validation and Docker build in clean CI.
- Honest limit: none proves webhook delivery or production scale.

### “Can the final system deliver exactly once?”

- Not generally across HTTP and an independent receiver transaction.
- An acknowledgment may be lost after the receiver commits.
- Future behavior is at least once with stable event IDs and receiver-side
  idempotency.
- No delivery path exists in Stage 1.

## 15. Hands-on modification for Tarun

Implement this after completing the quiz, without copying a supplied solution:

> Add a validated `HOOKRELAY_DATABASE_POOL_TIMEOUT_SECONDS` setting and wire it
> into SQLAlchemy's engine so waiting for a pooled connection is bounded
> independently from the whole readiness timeout.

Acceptance criteria:

1. Choose and justify a safe local default and strict positive upper bound.
2. Add the field to `Settings` and pass it to engine construction.
3. Add one test that parses the prefixed environment value and one that rejects
   an invalid value.
4. Expose the development override in `.env.example` and `compose.yaml`.
5. Preserve both exact public health contracts.
6. Run Ruff, formatting, mypy, non-integration tests, the real integration test,
   and Docker build.
7. Explain the distinction between waiting for a pool slot, executing
   `SELECT 1`, and bounding the complete readiness coroutine.

Do not solve pool pressure by making the pool arbitrarily large. Total possible
connections multiply across every API/worker process and must fit PostgreSQL's
budget.

## 16. Comprehension quiz and teach-back checklist

### Quiz

Answer without looking at the guide, then verify against the code.

1. What precise question does liveness answer, and which dependencies may it
   call?
2. Why can a database-backed liveness probe make an outage worse?
3. Trace readiness from HTTP request through `asyncio`, SQLAlchemy, asyncpg, and
   PostgreSQL.
4. Why is the engine process-scoped while a future session is short-lived?
5. What does `pool_pre_ping` help with, and what can it not prevent?
6. Why does `async def` not make a blocking driver safe?
7. What is the difference among a Dockerfile, image, container, and WSL 2 VM?
8. Why does the API use `postgres` inside Compose but host Python uses
   `127.0.0.1`?
9. What does `EXPOSE 8000` do, and what actually publishes the host port?
10. What survives `docker compose down`, and which common option would delete
    it?
11. What does health-conditioned `depends_on` guarantee, and what does it not?
12. Why is there no Alembic revision in Stage 1?
13. Why can `create_all()` not replace reviewed migrations?
14. Which test proves real PostgreSQL connectivity, and what environment value
    prevents it from skipping?
15. Name one fact each that a unit test, integration test, and Docker build fail
    to prove.
16. Why are `.env` ignore rules and `SecretStr` not a production secret manager?
17. What future failure makes exactly-once webhook delivery an invalid general
    claim?

<details>
<summary>Self-check answer ingredients</summary>

1. Process/event-loop response only; no external dependency.
2. Restarts cannot repair PostgreSQL and can create churn/connection storms.
3. Route -> `wait_for` -> health protocol -> engine connection -> asyncpg ->
   `SELECT 1`; success `200`, bounded sanitized failure `503`.
4. Engine owns reusable infrastructure/pool; session owns mutable transaction
   state for one unit of work.
5. Detects stale connections on checkout; cannot prevent mid-query outages or
   capacity exhaustion.
6. The event loop yields only when the called library performs nonblocking I/O.
7. Recipe, layered artifact, running isolated process, shared Linux-kernel VM.
8. Compose service DNS versus host loopback/network namespace.
9. It documents a container port; Compose `ports` publishes it.
10. The named volume survives; `down --volumes` removes it.
11. Initial health gating only; no lifetime health guarantee or repair.
12. There is no meaningful domain schema; Stage 2 creates the first revision.
13. It lacks ordered transformations, backfills, rename intent, and reviewed
    history.
14. `test_readiness_executes_against_real_postgresql` and
    `HOOKRELAY_TEST_DATABASE_URL`.
15. Unit: real dependency; integration: production scale; image build: running
    network behavior (other valid limits exist).
16. They reduce accidental display/commit but provide no access control,
    rotation, audit, or protection from deliberate extraction.
17. The receiver can commit a side effect and HookRelay can lose its HTTP
    acknowledgment, making retry outcome ambiguous.

</details>

### Three-to-five-minute teach-back

Use only a blank architecture sketch and cover:

- **0:00-0:30:** HookRelay's product goal, Stage 1 outcome, and explicit scope
  boundary.
- **0:30-1:20:** One liveness trace and one failed/readiness-recovery trace.
- **1:20-2:10:** Application factory/lifespan, engine/pool/connection, and why a
  global session is unsafe.
- **2:10-3:10:** Dockerfile -> image -> container -> WSL 2 VM; host port,
  service DNS, named volume, health and initial `depends_on` gating.
- **3:10-4:10:** Alembic's explicit role and the evidence/limit from each test
  layer.
- **4:10-5:00:** Defend Python, PostgreSQL, and future at-least-once semantics
  against one serious alternative each.

### Teach-back checklist

- [ ] I can draw only the components that exist in Stage 1 and label future
      components separately.
- [ ] I can reproduce the exact healthy and unavailable response contracts.
- [ ] I can explain the timeout and sanitized log/response paths.
- [ ] I can distinguish engine, pool, connection, and session.
- [ ] I can explain why model edits do not migrate a database.
- [ ] I can explain every non-comment Dockerfile instruction.
- [ ] I can trace both Compose network address paths.
- [ ] I can state what `depends_on`, health checks, non-root, and ignore files do
      **not** guarantee.
- [ ] I can name the exact test that crosses the real PostgreSQL boundary and
      notice when it skips.
- [ ] I can state honestly that Stage 1 has no events, outbox, NATS, workers, or
      webhook delivery.
- [ ] I can describe at-least-once duplicate ambiguity without claiming exactly
      once.
- [ ] I can complete the pool-timeout modification and explain it without
      copying code.

## Stage 1 completion evidence checklist

This guide does not treat an unobserved external check as green. Before Stage 1
is declared done, confirm:

- [ ] clean-checkout frozen installation succeeds;
- [ ] local Python and Compose startup instructions work;
- [ ] liveness remains `200` while readiness becomes sanitized `503` without
      PostgreSQL and recovers after it returns;
- [ ] Ruff lint/format, strict mypy, fast tests, real PostgreSQL tests, Alembic,
      Compose validation, and Docker build all pass;
- [ ] the integration test did not skip in CI;
- [ ] no real `.env`, credential, or database URL was committed/logged;
- [ ] the Stage 1 commits are meaningful and the public pull request is open;
- [ ] CI is actually green on that commit;
- [ ] Tarun reviewed this guide and attempted the teach-back before Stage 2.

The current source establishes the intended readiness and lifecycle behavior;
CI/PR status remains external evidence and must be checked rather than inferred
from documentation.
