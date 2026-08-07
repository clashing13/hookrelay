# HookRelay

HookRelay is a fault-tolerant webhook delivery platform built as a seven-stage
distributed-systems learning project. The finished platform will durably store
events and make HMAC-signed webhook deliveries with at-least-once semantics,
retry recovery, traffic controls, dead-letter replay, and observability.

Stage 1 is the service foundation. It does **not** accept or deliver webhooks
yet. Start with the [Stage 1 learning guide](docs/stages/01-foundation.md) for a
code tour, design rationale, safe failure exercise, troubleshooting, interview
questions, hands-on task, and teach-back checklist.

## Stage 1 capabilities

- Python 3.12 `src/` package with a FastAPI application factory
- validated `HOOKRELAY_*` settings and structured JSON application logs
- dependency-free `GET /health/live`
- bounded, real-PostgreSQL `GET /health/ready`
- one SQLAlchemy async engine per process, disposed during graceful shutdown
- explicit asynchronous Alembic environment with no meaningless empty revision
- locked dependencies through `uv.lock`
- non-root, multi-stage container image
- Docker Compose API/PostgreSQL environment with localhost-only ports and a
  named database volume
- async unit/API tests and real PostgreSQL/Alembic integration tests
- GitHub Actions checks for linting, formatting, typing, tests, migrations, and
  the image build

NATS, event ingestion, tenants, authentication, delivery workers, retries,
rate limits, circuit breakers, dead letters, the operations UI, and cloud
deployment belong to later stages.

## Health contracts

```text
GET /health/live   process/event-loop liveness; never contacts PostgreSQL
GET /health/ready  runs SELECT 1 through the real async SQLAlchemy engine
```

Both return this contract when healthy:

```json
{"status":"ok","service":"hookrelay","version":"0.1.0"}
```

When PostgreSQL is unavailable, readiness returns `503` with a sanitized body:

```json
{"status":"unavailable","service":"hookrelay","version":"0.1.0"}
```

Liveness remains `200` during that dependency outage. Readiness retries on the
next request and can recover without restarting the API.

## Prerequisites

- Git
- CPython 3.12 or newer
- Docker Desktop with Docker Compose for the complete local workflow
- approximately 2 GB of free disk space for images and the named volume

The repository pins `uv` 0.12.1, Python image 3.12.13 on Debian Bookworm, and
PostgreSQL 17.7 on Debian Bookworm. `uv.lock` pins the complete Python dependency
graph. Image tags are updated only through a reviewed rebuild and test change.

## Fastest start: Docker Compose

Clone the Stage 1 branch while its pull request is open:

```powershell
git clone --branch codex/stage-01-foundation https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
Copy-Item .env.example .env
docker compose up --detach --build --wait
docker compose run --rm api alembic upgrade head
```

The values in `.env.example` are local-development defaults, not production
secrets. The copied `.env` is ignored by Git. Environment variables are a
configuration transport; they are not automatically a secure secrets manager.

Check the service from PowerShell:

```powershell
curl.exe --fail http://127.0.0.1:8000/health/live
curl.exe --fail http://127.0.0.1:8000/health/ready
docker compose ps
```

On macOS or Linux, use `cp .env.example .env` and `curl` instead. If host port
`5432` is already in use, set `POSTGRES_HOST_PORT=55432` in `.env` before
starting Compose. The API still reaches PostgreSQL at `postgres:5432` on the
internal Compose network.

Stop the containers while preserving database data:

```powershell
docker compose down
```

`docker compose down --volumes` also deletes the named PostgreSQL volume and
its data. Use it only when you intentionally want a clean database.

## Host Python workflow

Create a local environment and install exactly what `uv.lock` records.

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

macOS or Linux:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install uv==0.12.1
.venv/bin/uv sync --frozen --all-groups
```

Run PostgreSQL with Compose, then point the host process at its published port.
The commands below use the default port and local-only example password:

```powershell
if (-not (Test-Path -LiteralPath '.env')) { Copy-Item .env.example .env }
docker compose up --detach --wait postgres
$env:HOOKRELAY_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\hookrelay.exe
```

Open a second terminal for the two health requests. Use the database URL and
port from your `.env` if you changed either value.

## Checks

Fast checks do not need PostgreSQL:

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\mypy.exe
.\.venv\Scripts\pytest.exe -m "not integration"
```

With Compose PostgreSQL available on the default host port:

```powershell
$env:HOOKRELAY_TEST_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay"
$env:HOOKRELAY_DATABASE_URL = $env:HOOKRELAY_TEST_DATABASE_URL
.\.venv\Scripts\pytest.exe -m integration
.\.venv\Scripts\alembic.exe upgrade head
docker compose config --quiet
docker build --pull --tag hookrelay:stage1 .
```

Passing unit tests alone does not prove PostgreSQL behavior, container
networking, migration configuration, or packaging. The integration and image
checks cover those separate boundaries; none of them prove production scale.

## Migration policy

Run migrations explicitly with `alembic upgrade head`. The API never calls
`metadata.create_all()` and does not migrate automatically at startup. Stage 1
has no domain tables, so `migrations/versions/` intentionally contains no empty
baseline revision. Stage 2 will add the first meaningful revision with the
first domain model.

## Documentation

- [Architecture](docs/architecture.md)
- [Glossary](docs/glossary.md)
- [Interview guide](docs/interview-guide.md)
- [Stage 1: service foundation](docs/stages/01-foundation.md)
- [Architecture decision records](docs/decisions/README.md)

## Delivery guarantee

The future delivery pipeline will provide **at least once**, not general-purpose
exactly once. A destination can complete a side effect while its acknowledgment
is lost, so HookRelay may retry. Stable event IDs plus receiver-side idempotency
are the planned duplicate-safety strategy. Stage 1 makes no delivery claim
because it does not deliver events yet.

## License

[MIT](LICENSE) (c) 2026 Tarun Athreya
