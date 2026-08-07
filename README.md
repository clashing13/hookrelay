# HookRelay

HookRelay is a fault-tolerant webhook-delivery platform built as a seven-stage
distributed-systems learning project. Version `0.4.0` implements durable event
acceptance, signed delivery, and bounded failure recovery:

```text
Producer
  -> FastAPI
  -> PostgreSQL event + transactional outbox
  -> outbox publisher
  -> NATS JetStream
  -> bounded delivery worker
       -> success
       -> persistent retry schedule + delayed NAK
       -> expired worker-claim recovery
       -> dead-lettered terminal state
  -> authenticated manual replay as a fresh dispatch generation
```

Start with the [Stage 4 failure-recovery guide](docs/stages/04-failure-recovery.md)
for the complete state machine, exact backoff formula, crash exercise, tests,
and teach-back checklist. The [Stage 3](docs/stages/03-delivery-pipeline.md),
[Stage 2](docs/stages/02-event-ingestion.md), and
[Stage 1](docs/stages/01-foundation.md) guides preserve the earlier boundaries.

## Stage 4 capabilities

- authenticated, tenant-scoped, idempotent event ingestion;
- one PostgreSQL transaction for an event, delivery snapshots, and one fresh
  versioned outbox row per destination/generation;
- expiring outbox publisher claims and PubAck-before-`published_at` ordering;
- NATS JetStream file-backed work queue and shared durable pull consumer;
- bounded async worker/HTTP concurrency;
- exact-byte timestamped HMAC-SHA256 webhook signing;
- PostgreSQL-authoritative `retry_scheduled` state and `next_attempt_at`;
- capped exponential backoff with configurable bounded downward jitter;
- explicit success, transient, and permanent HTTP failure classification;
- maximum attempts per dispatch generation;
- delivery/attempt claim leases, abandonment, and stale-finalizer fencing;
- terminal dead-letter timestamp/reason before broker ACK;
- authenticated `202` manual replay with a new generation and outbox UUID;
- unchanged strict schema-v1 ID-only broker envelopes, with dispatch generation
  derived from the reconciled PostgreSQL outbox row;
- a configurable local receiver retaining bounded exact-byte/header evidence.

Default recovery policy:

| Setting | Default |
| --- | --- |
| HTTP timeout | 10 seconds |
| Delivery claim TTL | 20 seconds |
| Finalization margin | 5 seconds |
| Attempts per dispatch generation | 5 |
| Retry base/cap | 1 / 60 seconds |
| Downward jitter ratio | 0.25 |
| Policy-block handling | terminal `target_blocked`; 30-second delayed-NAK fallback |

For generation attempt `n`, HookRelay caps `base * 2^(n-1)` at the configured
maximum, then samples uniformly from 75%-100% of that ceiling with the default
jitter. The exact database due time is authoritative; delayed NAK is only the
broker wake-up mechanism.

## Current guarantee and limits

HookRelay is **at least once**, never general-purpose exactly once. A receiver
may commit its side effect before HookRelay loses the response or a worker is
killed. Claim fencing prevents stale database finalization, not a remote side
effect. Receivers must atomically deduplicate the stable event ID with their
business change.

JetStream `MaxDeliver` remains unlimited because broker delivery count is not
HTTP attempt count. PostgreSQL counts actual and ambiguous abandoned attempts
within the current dispatch generation. Permanent failures dead-letter
immediately; transient failures dead-letter after the configured maximum.
Manual replay increments the generation and creates a fresh outbox UUID while
preserving lifetime attempt history.

Broker schema v1 remains unchanged for payload compatibility. Manual replay
should wait until Stage 4 worker cutover completes: an old Stage 3 worker can
parse the message but lacks generation fencing and may send an extra stale
request.

Outbound execution is still restricted to controlled `local`/`test` targets.
Workers use an explicit hostname allowlist, disable redirects, and ignore
environment proxies. A blocked target is persisted as dead-lettered with reason
`target_blocked` and ACKed; replay can recover it after a reviewed configuration
change. This is not complete SSRF protection. Stage 5 owns DNS/IP/rebinding and
egress defenses, rate limiting, circuit breaking, size policy, and secret
rotation.

Stage 6 owns full history/attempt APIs, observability, dashboards, and the
operations UI. Stage 7 owns fault/load evidence, capacity measurements, and
release claims. Local Compose has one NATS server/replica and named volume; it
is reproducible persistence, not HA, backup, or disaster recovery.

## HTTP contract

| Method and route | Authentication | Success contract |
| --- | --- | --- |
| `GET /health/live` | none | `200`; dependency-free process health |
| `GET /health/ready` | none | `200`; bounded PostgreSQL probe succeeds |
| `POST /v1/bootstrap/tenants` | bootstrap bearer token | `201`; tenant and one-time initial API key |
| `GET /v1/tenant` | tenant API key | `200`; authenticated tenant metadata |
| `POST /v1/endpoints` | tenant API key | `201`; endpoint and one-time signing secret |
| `GET /v1/endpoints/{endpoint_id}` | tenant API key | `200`; secret-free tenant metadata |
| `POST /v1/events` | tenant key + `Idempotency-Key` | `201`; event/deliveries/outbox committed |
| `GET /v1/events/{event_id}` | tenant API key | `200`; current state, generation, due time, and terminal reason |
| `POST /v1/deliveries/{delivery_id}/replay` | tenant API key + JSON expected generation | `202`; dead-lettered delivery reset to a fresh pending generation |

Replay requires
`{"expected_dispatch_generation": <observed positive generation>}` and returns
`Location: /v1/events/{event_id}`. Missing and cross-tenant IDs share opaque
`404`; changed generation returns `409 delivery_generation_conflict`; a
delivery not currently dead-lettered returns `409 delivery_not_replayable`.
`202` proves PostgreSQL committed the new generation and outbox intent, not
broker publication or receiver success.

The receiver is a local inspection tool, not a product API:

| Route | Purpose |
| --- | --- |
| `GET /health/live` | Receiver process health |
| `POST /webhooks` | Configurable delivery target |
| `GET /requests` | Bounded captured-request list |
| `GET /requests/{delivery_id}` | Captures for one delivery |
| `DELETE /requests` | Clear in-memory captures |

## Prerequisites

- Git
- CPython 3.12 or newer
- Docker Desktop with Docker Compose
- approximately 2 GB of free disk space

The repository pins `uv` 0.12.1, Python 3.12.13 on Debian Bookworm,
PostgreSQL 17.7, and NATS 2.14.3 on Alpine 3.22. `uv.lock` pins the complete
Python dependency graph.

## Fastest start: Docker Compose

Windows PowerShell:

```powershell
git clone --branch codex/stage-04-failure-recovery --single-branch https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
Copy-Item .env.example .env
```

Before starting Compose, edit ignored `.env`: replace the example database
password, encryption key, and bootstrap token. Checked-in examples are not
deployable secrets.

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose build
docker compose up --detach --wait postgres nats receiver
docker compose run --rm api alembic upgrade head
docker compose up --detach --wait api
docker compose up --detach outbox-publisher worker
docker compose ps
```

Check every local boundary:

```powershell
curl.exe --fail http://127.0.0.1:8000/health/live
curl.exe --fail http://127.0.0.1:8000/health/ready
curl.exe --fail http://127.0.0.1:9000/health/live
Invoke-RestMethod 'http://127.0.0.1:8222/healthz?js-enabled-only=true'
```

Bootstrap a tenant and retain the one-time API key:

```powershell
$bootstrapHeaders = @{
  Authorization = "Bearer replace-this-local-bootstrap-token-before-use"
}
$bootstrapBody = @{
  name = "Local demo"
  initial_api_key_name = "developer"
} | ConvertTo-Json
$bootstrap = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/bootstrap/tenants `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body $bootstrapBody
$apiKey = $bootstrap.api_key.key
```

Create the local receiver endpoint. The worker uses Compose service-name DNS:

```powershell
$authHeaders = @{ Authorization = "Bearer $apiKey" }
$endpointBody = @{
  name = "Local receiver"
  url = "http://receiver:9000/webhooks"
} | ConvertTo-Json
$endpoint = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/endpoints `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $endpointBody
```

Submit an event and read asynchronous current state:

```powershell
$eventHeaders = @{
  Authorization = "Bearer $apiKey"
  "Idempotency-Key" = "demo-event-0001"
}
$eventBody = @{
  type = "order.created"
  payload = @{ order_id = "ord_123"; total = 42 }
  endpoint_ids = @($endpoint.id)
} | ConvertTo-Json -Depth 5
$event = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $eventHeaders `
  -ContentType "application/json" `
  -Body $eventBody

Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/v1/events/$($event.id)" `
  -Headers $authHeaders
```

The Stage 4 guide contains exact demonstrations for transient recovery,
permanent dead letter, authenticated replay, and a safe local worker kill:
[run and test Stage 4](docs/stages/04-failure-recovery.md#10-exact-commands-for-running-and-testing).

Stop containers while preserving PostgreSQL and NATS data:

```powershell
docker compose down
```

`docker compose down --volumes` deletes both named volumes and is only for an
intentional fresh start.

## Host Python workflow

Create a locked environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

Start dependencies and use host addresses:

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose up --detach --wait postgres nats receiver
$env:HOOKRELAY_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:55432/hookrelay"
$env:HOOKRELAY_NATS_URL = "nats://127.0.0.1:4222"
$env:HOOKRELAY_ENVIRONMENT = "local"
$env:HOOKRELAY_DELIVERY_ALLOWED_HOSTS = '["127.0.0.1","localhost"]'
.\.venv\Scripts\alembic.exe upgrade head
```

Run these in separate terminals with the same environment:

```powershell
.\.venv\Scripts\hookrelay.exe
.\.venv\Scripts\hookrelay-outbox.exe
.\.venv\Scripts\hookrelay-worker.exe
```

For a host worker, use `http://127.0.0.1:9000/webhooks` as the endpoint.

## Checks

Fast checks:

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\mypy.exe
.\.venv\Scripts\pytest.exe -m "not integration"
```

Real-service checks:

```powershell
$postgresHostPort = "55432"
$env:POSTGRES_HOST_PORT = $postgresHostPort
docker compose stop api outbox-publisher worker receiver
docker compose up --detach --wait postgres nats

# Enter the values from the current untracked .env used by this PostgreSQL
# container. The password prompt is masked.
$postgresUser = Read-Host "POSTGRES_USER from .env"
$securePostgresPassword = Read-Host "POSTGRES_PASSWORD from .env" -AsSecureString
$postgresCredential = [System.Net.NetworkCredential]::new(
  $postgresUser,
  $securePostgresPassword
)
if ([string]::IsNullOrWhiteSpace($postgresCredential.UserName) -or
    [string]::IsNullOrEmpty($postgresCredential.Password)) {
  throw "PostgreSQL user and password are required."
}

$testDatabase = "hookrelay_test"
$databaseExists = docker compose exec -T postgres psql `
  --username $postgresCredential.UserName `
  --dbname postgres `
  --tuples-only `
  --no-align `
  --command "SELECT 1 FROM pg_database WHERE datname = '$testDatabase';"
if ($LASTEXITCODE -ne 0) {
  throw "Could not inspect the PostgreSQL databases."
}
if (($databaseExists -join "").Trim() -eq "1") {
  $disposableConfirmation = Read-Host `
    "$testDatabase already exists; type its exact name to confirm it is disposable"
  if ($disposableConfirmation -cne $testDatabase) {
    throw "Refusing to run integration tests against an unconfirmed database."
  }
} else {
  docker compose exec -T postgres createdb `
    --username $postgresCredential.UserName `
    --owner $postgresCredential.UserName `
    $testDatabase
  if ($LASTEXITCODE -ne 0) {
    throw "Could not create the disposable $testDatabase database."
  }
}

$encodedPostgresUser = [Uri]::EscapeDataString($postgresCredential.UserName)
$encodedPostgresPassword = [Uri]::EscapeDataString($postgresCredential.Password)
$env:HOOKRELAY_TEST_DATABASE_URL = `
  "postgresql+asyncpg://${encodedPostgresUser}:${encodedPostgresPassword}@127.0.0.1:$postgresHostPort/$testDatabase"
$env:HOOKRELAY_DATABASE_URL = $env:HOOKRELAY_TEST_DATABASE_URL
$env:HOOKRELAY_NATS_URL = "nats://127.0.0.1:4222"
$env:HOOKRELAY_TEST_NATS_URL = "nats://127.0.0.1:4222"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\pytest.exe -m "integration and not concurrency"
.\.venv\Scripts\pytest.exe -m concurrency
.\.venv\Scripts\alembic.exe check
docker compose config --quiet
docker build --pull --tag hookrelay:stage4 .
```

Stopping long-lived application processes prevents them from claiming rows
created by deterministic integration harnesses. The commands create or reuse
only the explicitly named `hookrelay_test`; they never point tests at the normal
`POSTGRES_DB` or drop a database. Reserve `hookrelay_test` for disposable test
data and stop if it contains anything valuable.
At the Stage 4 checkpoint, the complete suite passed all 143 tests. Its twelve
Stage 4 real-service scenarios include fresh database-clock behavior after a row
lock wait and a worker subprocess killed after receiver capture with successful
replacement recovery. That proves the encoded schedules, not every process-kill
timing, exactly once, Stage 5 hostile-network safety, HA, or production capacity.

## Migration policy

Run `alembic upgrade head` explicitly; application processes never migrate at
startup.

- `20260802_0001`: domain schema and transactional outbox.
- `20260803_0002`: recoverable outbox publisher claims.
- `20260804_0003`: retry schedule, delivery claim lease, generation, dead-letter
  state/reason, attempt fencing, and per-generation outbox uniqueness.

The Stage 4 upgrade converts any pre-existing unfinished Stage 3 attempts to
`abandoned` and schedules their deliveries immediately. Downgrade intentionally
refuses data Stage 3 cannot represent: active/scheduled/dead-lettered Stage 4
state, any non-1 generation, or a `BIGINT` attempt duration outside the former
32-bit range. Changing ORM models still does not migrate an existing database;
review both artifacts and run `alembic check`.

## Documentation

- [Architecture](docs/architecture.md)
- [Glossary](docs/glossary.md)
- [Interview guide](docs/interview-guide.md)
- [Stage 1: service foundation](docs/stages/01-foundation.md)
- [Stage 2: durable event ingestion](docs/stages/02-event-ingestion.md)
- [Stage 3: durable delivery pipeline](docs/stages/03-delivery-pipeline.md)
- [Stage 4: failure recovery](docs/stages/04-failure-recovery.md)
- [Architecture decision records](docs/decisions/README.md)

## License

[MIT](LICENSE) (c) 2026 Tarun Athreya
