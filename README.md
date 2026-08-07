# HookRelay

HookRelay is a fault-tolerant webhook-delivery platform built as a seven-stage
distributed-systems learning project. Version `0.3.0` implements the first
complete local happy path:

```text
Producer
  -> FastAPI
  -> PostgreSQL event + transactional outbox
  -> outbox publisher
  -> NATS JetStream
  -> bounded delivery worker
  -> HMAC-signed HTTP request
  -> configurable local test receiver
```

Start with the [Stage 3 delivery-pipeline guide](docs/stages/03-delivery-pipeline.md)
for the full event trace, exact wire contract, failure exercises, tests, and
teach-back checklist. The [Stage 2](docs/stages/02-event-ingestion.md) and
[Stage 1](docs/stages/01-foundation.md) guides remain the detailed foundation.

## Stage 3 capabilities

- authenticated, tenant-scoped, idempotent event ingestion;
- one PostgreSQL transaction for an event, delivery snapshots, and one
  versioned outbox row per destination;
- expiring PostgreSQL claim leases so publishers do not hold row locks during
  broker I/O;
- NATS 2.14.3 with a file-backed work-queue stream,
  `HOOKRELAY_DELIVERIES_V1`, on `hookrelay.delivery.requested.v1`;
- a shared durable pull consumer, `HOOKRELAY_DELIVERY_WORKERS_V1`;
- ID-only broker payloads and `Nats-Msg-Id` set to the outbox UUID;
- bounded async worker concurrency and matching HTTP connection limits;
- attempt-before-HTTP state and success-commit-before-ACK ordering;
- deterministic version-1 JSON signed with HMAC-SHA256 over
  `timestamp + "." + exact_body_bytes`;
- explicit HTTP timeout, no redirects, and no environment proxy inheritance;
- a configurable local receiver that retains bounded exact body/header evidence;
- a real PostgreSQL/JetStream/HTTP happy-path test.

The default 60-second claim TTL must remain greater than the aggregate broker
publish-timeout budget (`25` rows x `2` seconds each), with margin for database
finalization and loop overhead. Cooperative publisher shutdown releases the
unprocessed claim remainder, and NATS drain is bounded to five seconds.

The API, outbox publisher, worker, and receiver are separate processes. The API
can continue durable PostgreSQL acceptance while NATS is temporarily
unavailable; background backlog is observable in `outbox_messages`.

## Current guarantee and limits

HookRelay is **at least once**, never general-purpose exactly once. JetStream
may store a publication whose PubAck is lost, or a receiver may commit a side
effect before HookRelay loses the HTTP response or its own success update.
Stable event IDs and receiver-side idempotency are required.

Stage 3 is intentionally a local/test delivery boundary:

- workers refuse to run in staging or production;
- target hostnames must be explicitly allowlisted;
- redirects and environment proxies are disabled;
- complete DNS/IP/rebinding/egress SSRF protection arrives in Stage 5.

Stage 4 owns designed retry scheduling, exponential backoff, jitter, maximum
attempts, classification, stale-attempt/worker-crash recovery, dead letters,
and replay. Today, a timeout, transport error, or non-2xx result is recorded as
a transient failure and left unacknowledged for JetStream `AckWait` redelivery.
That behavior is not a finished retry policy. Policy-blocked destinations also
remain unacknowledged/recoverable; only malformed or authoritative-state-
mismatched poison messages are terminated.

Local Compose uses one file-backed NATS replica and named volume. It does not
claim high availability, disaster recovery, production security, or benchmark
scale.

## HTTP contract

| Method and route | Authentication | Success contract |
| --- | --- | --- |
| `GET /health/live` | none | `200`; process-local health |
| `GET /health/ready` | none | `200` after a bounded PostgreSQL probe |
| `POST /v1/bootstrap/tenants` | bootstrap bearer token | `201`; tenant and raw initial API key returned once |
| `GET /v1/tenant` | tenant API key | `200`; authenticated tenant only |
| `POST /v1/endpoints` | tenant API key | `201`; endpoint and raw signing secret returned once |
| `GET /v1/endpoints/{endpoint_id}` | tenant API key | `200`; secret-free tenant-owned metadata |
| `POST /v1/events` | tenant API key plus `Idempotency-Key` | `201`; durable event and initial pending deliveries |
| `GET /v1/events/{event_id}` | tenant API key | `200`; event and current delivery states |

The receiver is a local inspection tool, not a product API:

| Route | Purpose |
| --- | --- |
| `GET /health/live` | receiver process health |
| `POST /webhooks` | configurable delivery target |
| `GET /requests` | bounded captured-request list |
| `GET /requests/{delivery_id}` | captures for one delivery |
| `DELETE /requests` | clear in-memory captures |

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
git clone --branch codex/stage-03-delivery-pipeline --single-branch https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
Copy-Item .env.example .env
```

Before the first Compose startup, edit the ignored `.env`: choose the local
database password, encryption key, and bootstrap token you will actually use.
The checked-in examples are not deployable secrets. Commands below that show
example credentials must be updated to those chosen values.

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose build
docker compose up --detach --wait postgres nats receiver
docker compose run --rm api alembic upgrade head
docker compose up --detach --wait api
docker compose up --detach outbox-publisher worker
docker compose ps
```

Check each local boundary:

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

Create a receiver endpoint. Because the worker runs inside Compose, use the
service name `receiver`, not host loopback:

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
$signingSecret = $endpoint.signing_secret
```

Submit an event and poll the receiver/current-state route:

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
$deliveryId = $event.deliveries[0].id

do {
  Start-Sleep -Milliseconds 250
  $captures = @(
    Invoke-RestMethod "http://127.0.0.1:9000/requests/$deliveryId"
  )
} while ($captures.Count -eq 0)

Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/v1/events/$($event.id)" `
  -Headers $authHeaders
```

See the [Stage 3 guide](docs/stages/03-delivery-pipeline.md#verify-exact-body-bytes-and-hmac-independently)
for independent HMAC verification and durable-state inspection.

Stop containers while preserving PostgreSQL and NATS data:

```powershell
docker compose down
```

`docker compose down --volumes` deletes both named volumes. Use it only for an
intentional fresh start.

## Host Python workflow

Create a locked environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

Start PostgreSQL, NATS, and the receiver, then use host addresses:

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose up --detach --wait postgres nats receiver
$env:HOOKRELAY_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:55432/hookrelay"
$env:HOOKRELAY_NATS_URL = "nats://127.0.0.1:4222"
$env:HOOKRELAY_ENVIRONMENT = "local"
$env:HOOKRELAY_DELIVERY_ALLOWED_HOSTS = '["127.0.0.1","localhost"]'
.\.venv\Scripts\alembic.exe upgrade head
```

Run these long-lived commands in separate terminals with the same environment:

```powershell
.\.venv\Scripts\hookrelay.exe
.\.venv\Scripts\hookrelay-outbox.exe
.\.venv\Scripts\hookrelay-worker.exe
```

For a host-run worker, create endpoints using
`http://127.0.0.1:9000/webhooks`.

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
$env:POSTGRES_HOST_PORT = "55432"
docker compose stop api outbox-publisher worker receiver
docker compose up --detach --wait postgres nats
$env:HOOKRELAY_TEST_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:55432/hookrelay"
$env:HOOKRELAY_DATABASE_URL = $env:HOOKRELAY_TEST_DATABASE_URL
$env:HOOKRELAY_NATS_URL = "nats://127.0.0.1:4222"
$env:HOOKRELAY_TEST_NATS_URL = "nats://127.0.0.1:4222"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\pytest.exe -m "integration and not concurrency"
.\.venv\Scripts\pytest.exe -m concurrency
.\.venv\Scripts\alembic.exe check
docker compose config --quiet
docker build --pull --tag hookrelay:stage3 .
```

Stopping the long-lived application processes prevents them from claiming rows
created by the deterministic integration harness. Substitute the password you
chose in `.env`, and use only a disposable local/test database.

Passing every current check still does not prove exactly once, failure
recovery, HA, hostile network safety, or production capacity.

## Migration policy

Run migrations explicitly with `alembic upgrade head`. The API and background
processes never migrate at startup. Stage 2 revision `20260802_0001` creates the
domain schema. Stage 3 revision `20260803_0002` adds recoverable outbox claim
tokens/expiries, consistency constraints, and a partial claim-scan index.

ORM models and migrations are separate artifacts. Changing one does not update
the other or an existing database; review both and run `alembic check`.

## Documentation

- [Architecture](docs/architecture.md)
- [Glossary](docs/glossary.md)
- [Interview guide](docs/interview-guide.md)
- [Stage 1: service foundation](docs/stages/01-foundation.md)
- [Stage 2: durable event ingestion](docs/stages/02-event-ingestion.md)
- [Stage 3: durable delivery pipeline](docs/stages/03-delivery-pipeline.md)
- [Architecture decision records](docs/decisions/README.md)

## License

[MIT](LICENSE) (c) 2026 Tarun Athreya
