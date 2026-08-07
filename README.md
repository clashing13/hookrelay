# HookRelay

HookRelay is a fault-tolerant webhook delivery platform built as a seven-stage
distributed-systems learning project. The finished platform will durably store
events and make HMAC-signed webhook deliveries with at-least-once semantics,
retry recovery, traffic controls, dead-letter replay, and observability.

Stage 2 implements **authenticated, idempotent, durable event ingestion**. An
accepted request is committed to PostgreSQL together with one pending delivery
and one unpublished transactional-outbox row per selected endpoint. It is not
sent over HTTP yet. Start with the [Stage 2 learning guide](docs/stages/02-event-ingestion.md)
for the full request trace, schema rationale, tests, safe failure exercise, and
teach-back checklist. The [Stage 1 guide](docs/stages/01-foundation.md) remains
the foundation reference.

## Stage 2 capabilities

- protected tenant bootstrap with a one-time initial API-key response;
- tenant authentication through `Authorization: Bearer <api-key>`;
- tenant-scoped webhook endpoint creation and inspection;
- generated endpoint signing secrets encrypted at rest and returned only when
  the endpoint is created;
- `POST /v1/events` with a required, tenant-scoped `Idempotency-Key`;
- deterministic replay: the same key and logical request returns the original
  response with HTTP `201` and `Idempotency-Replayed: true`;
- conflict detection: reusing the key for a different logical request returns
  HTTP `409` without creating more work;
- one PostgreSQL transaction for the event, delivery snapshots, and
  `delivery.requested` outbox rows;
- strict request models, stable `application/problem+json` errors, and opaque
  cross-tenant `404` responses;
- the first explicit Alembic revision, with relational constraints that enforce
  tenant ownership and close concurrent idempotency races;
- async unit/API tests plus real PostgreSQL migration, integration, security,
  rollback, and concurrency tests.

Stage 2 does **not** contain NATS, an outbox publisher, outbound webhook HTTP,
HMAC request signing, retries, or delivery-attempt creation. The
`delivery_attempts` table reserves the later audit model, but a successful
Stage 2 submission creates zero attempt rows.

## HTTP contract at a glance

| Method and route | Authentication | Success contract |
| --- | --- | --- |
| `GET /health/live` | none | `200`; process-local health |
| `GET /health/ready` | none | `200` after a bounded PostgreSQL probe |
| `POST /v1/bootstrap/tenants` | deployment bootstrap bearer token | `201`; tenant and raw initial API key returned once |
| `GET /v1/tenant` | tenant API key | `200`; authenticated tenant only |
| `POST /v1/endpoints` | tenant API key | `201`; endpoint and raw signing secret returned once |
| `GET /v1/endpoints/{endpoint_id}` | tenant API key | `200`; tenant-owned endpoint metadata, never the secret |
| `POST /v1/events` | tenant API key plus `Idempotency-Key` | `201`; durable event and pending deliveries |
| `GET /v1/events/{event_id}` | tenant API key | `200`; tenant-owned event and current delivery states |

JSON mutation routes require `Content-Type: application/json`. Bootstrap and
endpoint creation add `Cache-Control: no-store` and `Pragma: no-cache` because
their successful responses contain one-time credentials. Event creation adds a
`Location` header on both first acceptance and replay. A replay additionally
adds `Idempotency-Replayed: true`; a first acceptance omits that header.

## Prerequisites

- Git
- CPython 3.12 or newer
- Docker Desktop with Docker Compose for the complete local workflow
- approximately 2 GB of free disk space for images and the named volume

The repository pins `uv` 0.12.1, Python image 3.12.13 on Debian Bookworm, and
PostgreSQL 17.7 on Debian Bookworm. `uv.lock` pins the complete Python dependency
graph. Image tags are updated only through a reviewed rebuild and test change.

## Fastest start: Docker Compose

```powershell
git clone --branch codex/stage-02-event-ingestion --single-branch https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
Copy-Item .env.example .env
```

While the stacked Stage 2 pull request is open, the branch flags select its
implementation. After Stage 2 merges into the default branch, omit
`--branch codex/stage-02-event-ingestion --single-branch`.

Edit the ignored `.env` now. Replace `POSTGRES_PASSWORD`,
`HOOKRELAY_SECRET_ENCRYPTION_KEY`, and `HOOKRELAY_BOOTSTRAP_TOKEN` before any
container initializes or stores data. Then start and migrate:

```powershell
docker compose build api
docker compose up --detach --wait postgres
docker compose run --rm api alembic upgrade head
docker compose up --detach --wait api
```

The checked-in values are local-development examples. For a quick
loopback-only walkthrough they do run as written, but do not reuse them outside
local development.

Check the service:

```powershell
curl.exe --fail http://127.0.0.1:8000/health/live
curl.exe --fail http://127.0.0.1:8000/health/ready
docker compose ps
```

Bootstrap a local tenant. The token below matches `.env.example`; substitute
your replacement if you changed it:

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

Treat `$apiKey` as a password. HookRelay stores only its SHA-256 digest and
cannot show the raw key again.

Create an endpoint and retain its one-time signing secret:

```powershell
$authHeaders = @{ Authorization = "Bearer $apiKey" }
$endpointBody = @{
  name = "Local receiver"
  url = "http://127.0.0.1:9000/hooks"
} | ConvertTo-Json
$endpoint = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/endpoints `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $endpointBody
$signingSecret = $endpoint.signing_secret
```

The URL may use HTTP only in the `local` and `test` environments. No request is
sent to it in Stage 2.

Submit an event and then replay the exact logical request:

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
$first = Invoke-WebRequest `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $eventHeaders `
  -ContentType "application/json" `
  -Body $eventBody
$replay = Invoke-WebRequest `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers $eventHeaders `
  -ContentType "application/json" `
  -Body $eventBody
$first.StatusCode
$first.Headers.Location
$replay.StatusCode
$replay.Headers["Idempotency-Replayed"]
$replay.Content
```

Both status codes are `201`; the replay header is `true`, and the replay body
contains the original event and delivery IDs. Change `payload`, `type`, or the
set of `endpoint_ids` while keeping the same key to observe the documented
`409 idempotency_key_reused` response.

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

Run PostgreSQL with Compose, then point the host process at its published port:

```powershell
if (-not (Test-Path -LiteralPath '.env')) { Copy-Item .env.example .env }
docker compose up --detach --wait postgres
$env:HOOKRELAY_DATABASE_URL = "postgresql+asyncpg://hookrelay:change-me-for-local-development@127.0.0.1:5432/hookrelay"
$env:HOOKRELAY_BOOTSTRAP_ENABLED = "true"
$env:HOOKRELAY_BOOTSTRAP_TOKEN = "replace-this-local-bootstrap-token-before-use"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\hookrelay.exe
```

The default local encryption key is loaded from `.env`. Set a unique
`HOOKRELAY_SECRET_ENCRYPTION_KEY` before storing anything you care about.

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
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\pytest.exe -m integration
.\.venv\Scripts\alembic.exe check
docker compose config --quiet
docker build --pull --tag hookrelay:stage2 .
```

The integration suite owns its test rows but not an arbitrary personal
database. Point it only at a disposable local/CI database. Passing unit tests
alone does not prove PostgreSQL constraints, transactional rollback, concurrent
idempotency, migration alignment, container networking, or packaging. Passing
every current check still does not prove production capacity.

## Migration policy

Run migrations explicitly with `alembic upgrade head`. The API never calls
`metadata.create_all()` and does not migrate at startup. Stage 2 adds the first
meaningful revision, `20260802_0001`, for tenants, credentials, endpoints,
events, deliveries, delivery attempts, and outbox messages. ORM models and
migrations are separate artifacts; update and review both when the schema
changes.

## Security boundary and current limitations

- Raw API keys are returned once and stored only as SHA-256 digests. Endpoint
  signing secrets must be recoverable for future signing, so they are stored as
  AES-256-GCM ciphertext with versioned key metadata and bound associated data.
- The development encryption key and bootstrap token are intentionally unsafe
  examples. Production requires an external secret manager, rotation, audit,
  least privilege, and a disabled bootstrap surface after provisioning.
- Stage 2 validates URLs and requires HTTPS destinations in staging/production,
  but it does not resolve hosts or block loopback, private, link-local, metadata,
  or rebinding targets. That is not complete SSRF protection. There is no
  outbound request in this stage; a future worker must add defenses before it
  fetches untrusted URLs.
- The application serves plain HTTP. Compose binds it to `127.0.0.1`; deployed
  environments need TLS termination and a trusted proxy/network boundary.
  Destination-URL HTTPS validation does not encrypt producer-to-HookRelay
  traffic.
- Field lengths, event-type grammar, unique endpoint IDs, and a 100-endpoint
  maximum are enforced, but there is no explicit whole-request byte limit or
  payload-size quota yet. A reverse proxy limit and application-level quota are
  required before exposing ingestion to untrusted traffic.
- Rate limiting, key-management APIs, secret rotation workflows, NATS,
  publishing, outbound delivery, attempts, retries, dead letters, and production
  observability remain later-stage work.

## Documentation

- [Architecture](docs/architecture.md)
- [Glossary](docs/glossary.md)
- [Interview guide](docs/interview-guide.md)
- [Stage 1: service foundation](docs/stages/01-foundation.md)
- [Stage 2: durable event ingestion](docs/stages/02-event-ingestion.md)
- [Architecture decision records](docs/decisions/README.md)

## Delivery guarantee

Stage 2 guarantees durable acceptance only after PostgreSQL commits the event,
its delivery snapshots, and their outbox messages. It makes no claim that a
webhook has been attempted or delivered.

The future delivery pipeline will provide **at least once**, not
general-purpose exactly once. A destination can complete a side effect while
its acknowledgment is lost, so HookRelay may retry. Stable event IDs plus
receiver-side idempotency are the duplicate-safety strategy.

## License

[MIT](LICENSE) (c) 2026 Tarun Athreya
