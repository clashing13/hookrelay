# HookRelay

HookRelay is a fault-tolerant webhook-delivery platform built as a seven-stage
distributed-systems learning project. Version `0.6.0` adds correlated
observability, tenant-safe delivery history, and a same-origin operations
console to the secured Stage 5 delivery loop:

```text
Producer
  -> FastAPI request-byte and tenant-authorization boundary
       -> correlation ID + structured log/span/metric boundary
       -> endpoint URL preflight / signing-secret rotation
  -> PostgreSQL event + transactional outbox
       -> persisted correlation and optional trace context
  -> instrumented outbox publisher
  -> NATS JetStream + observability headers
  -> bounded delivery worker
       -> shared PostgreSQL rate/circuit admission
       -> DNS/IP validation + IP-pinned connection
       -> success
       -> persistent retry schedule + delayed NAK
       -> expired worker-claim recovery
       -> dead-lettered terminal state
  -> tenant delivery/attempt history + authenticated replay
  -> same-origin operations console

API / publisher / worker
  -> bounded Prometheus metrics + OpenTelemetry traces
  -> Collector / Prometheus / Tempo / Grafana (optional Compose profile)
```

Start with the
[Stage 6 observability and operations-console guide](docs/stages/06-observability-operations-console.md)
for the telemetry model, operations workflow, exact commands, safe exercises,
tests, and teach-back checklist. The
[Stage 5](docs/stages/05-security-traffic-control.md),
[Stage 4](docs/stages/04-failure-recovery.md),
[Stage 3](docs/stages/03-delivery-pipeline.md),
[Stage 2](docs/stages/02-event-ingestion.md), and
[Stage 1](docs/stages/01-foundation.md) guides preserve the earlier boundaries.

## Stage 6 capabilities (cumulative)

- structured JSON logs for every process with service, version, environment,
  process role, correlation ID, trace ID, and span ID where available;
- request correlation IDs returned as `X-Correlation-ID`, plus W3C trace
  context carried through PostgreSQL outbox rows and NATS headers without
  changing the strict schema-v1 broker body;
- explicit OpenTelemetry spans exported over OTLP/HTTP when telemetry is
  enabled, with product work isolated from collector/export failures;
- bounded-label Prometheus metrics from the API at `/metrics`, the publisher on
  internal port `9101`, and the worker on internal port `9102`;
- an optional Compose observability profile with an OpenTelemetry Collector,
  Prometheus, Tempo, and provisioned Grafana data sources and dashboards;
- tenant-scoped delivery listing and delivery/attempt detail APIs with
  descending keyset pagination, opaque filter-bound cursors, and secret-free
  response models;
- current delivery state plus immutable HTTP-attempt history without claiming
  event sourcing or a complete state-transition audit;
- a same-origin React operations console at `/console/` whose tenant API key is
  held only in memory and whose replay action uses the observed generation;
- authenticated, tenant-scoped, idempotent event ingestion;
- opaque missing/cross-tenant resource behavior plus composite tenant foreign
  keys at the database boundary;
- a one-MiB ASGI request-body cap that counts actual streamed bytes before
  routing/parsing, and a 256-KiB canonical UTF-8 event-payload cap;
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
- endpoint creation that rejects unsafe URL/DNS answers before persistence;
- connection-time resolution of every A/AAAA answer, non-global/metadata and
  special-address rejection, numeric-IP connection pinning, and peer-IP
  verification while retaining the original HTTP Host and TLS SNI name;
- PostgreSQL-authoritative per-endpoint fixed-window limiting and persistent
  closed/open/half-open circuit state with one leased recovery probe;
- rate/circuit deferrals that persist `retry_scheduled` due time and delayed
  NAK without creating an HTTP attempt or spending attempt budget;
- tenant-scoped, optimistic signing-secret rotation that retains older
  versions for already accepted delivery snapshots;
- unchanged strict schema-v1 ID-only broker envelopes, with dispatch generation
  derived from the reconciled PostgreSQL outbox row;
- non-root application containers with a read-only root filesystem, all Linux
  capabilities dropped, no-new-privileges, a bounded PID count, and a small
  hardened `/tmp` tmpfs;
- a configurable local receiver retaining bounded exact-byte/header evidence.

Default Stage 6 operations settings:

| Setting | Default |
| --- | --- |
| OpenTelemetry export | disabled; enabling requires an OTLP/HTTP endpoint |
| API metrics | `http://127.0.0.1:8000/metrics` |
| Publisher metrics | internal `:9101/metrics` |
| Worker metrics | internal `:9102/metrics` |
| Delivery page size | `50`; maximum `100` |
| Console | `http://127.0.0.1:8000/console/` |
| Grafana / Prometheus | `http://127.0.0.1:3000` / `http://127.0.0.1:9090` |

Default Stage 5 policy additions:

| Setting | Default |
| --- | --- |
| Maximum HTTP request body | 1,048,576 bytes |
| Maximum canonical event payload | 262,144 bytes |
| DNS timeout | 2 seconds |
| Per-endpoint fixed-window allowance | 10 requests / 1 second |
| Circuit transient-failure threshold | 5 |
| Circuit cooldown | 30 seconds |
| Local/test private-host exemptions | `receiver`, `127.0.0.1`, `localhost` |

The Stage 4 recovery defaults remain:

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

Broker schema v1 remains unchanged for payload compatibility. Correlation and
optional canonical `traceparent` values travel in persisted outbox columns and
NATS headers, not in the JSON body. As with the earlier replay rollout, mixed
old/new worker fleets require deliberate cutover because an old Stage 3 worker
can parse the body but lacks generation fencing.

Stage 5 validates public destinations both when an endpoint is created and when
the worker opens each connection. It validates every DNS answer, connects to a
selected validated numeric IP, verifies the peer, disables redirects and
environment proxies, and never opens Unix-domain destination sockets. Exact
private-host exemptions exist only for controlled `local`/`test` demos;
staging/production reject any non-empty exemption set and require HTTPS.

This is application-layer SSRF protection, not a complete production network
boundary. A deployment still needs deny-by-default egress/firewall rules,
metadata-service controls, trusted DNS, ingress TLS, secret management,
monitoring, and incident procedures. The fixed-window limiter also permits a
boundary burst and is not a tenant billing quota or capacity proof.

Endpoint signing-secret rotation is snapshot-safe: old encrypted rows remain
available to deliveries accepted before rotation, while new events select the
new active version. The process currently loads only one master AES key/version;
changing that key without a separate re-encryption/key-ring procedure makes
retained ciphertext undecryptable. Endpoint rotation is not master-key
rotation.

Stage 6 history means current delivery state plus immutable HTTP-attempt rows.
It is not event sourcing and does not preserve every former state, traffic
deferral, stale broker observation, or replay actor. Its keyset cursors are
opaque navigation tokens bound to the active filter; the tenant predicate and
API key remain the authorization boundary. Response models omit tenant IDs,
payloads, target URLs, signing-secret references/ciphertext, claim tokens,
broker payloads, and free-form exception text.

Telemetry is deliberately non-authoritative. Collector, Prometheus, Tempo, or
Grafana failure does not roll back product work or change readiness, which
continues to probe PostgreSQL only. Metrics use bounded labels and process-local
registries: delivery IDs, endpoint IDs, URLs, event types, and free-form errors
are never labels. The console keeps the tenant key only in browser memory, so a
refresh signs out; it is a local operator aid, not production identity, RBAC,
or an audit trail.

Stage 7 owns broader fault/load evidence, percentiles, capacity measurements,
and release claims. Local Compose remains a single-node teaching stack without
TLS, HA, backup/restore, SLOs, or production authentication. Its PostgreSQL,
NATS, Prometheus, Tempo, and Grafana named volumes provide local persistence,
not disaster recovery.

## HTTP contract

| Method and route | Authentication | Success contract |
| --- | --- | --- |
| `GET /health/live` | none | `200`; dependency-free process health |
| `GET /health/ready` | none | `200`; bounded PostgreSQL probe succeeds |
| `GET /metrics` | none | `200`; API process Prometheus exposition |
| `GET /console/` | none | `200`; same-origin static operations console |
| `POST /v1/bootstrap/tenants` | bootstrap bearer token | `201`; tenant and one-time initial API key |
| `GET /v1/tenant` | tenant API key | `200`; authenticated tenant metadata |
| `POST /v1/endpoints` | tenant API key | `201`; endpoint and one-time signing secret |
| `GET /v1/endpoints/{endpoint_id}` | tenant API key | `200`; secret-free tenant metadata |
| `POST /v1/endpoints/{endpoint_id}/signing-secret/rotate` | tenant API key + JSON expected version | `200`; new version and one-time signing secret |
| `POST /v1/events` | tenant key + `Idempotency-Key` | `201`; event/deliveries/outbox committed |
| `GET /v1/events/{event_id}` | tenant API key | `200`; current state, generation, due time, and terminal reason |
| `GET /v1/deliveries` | tenant API key | `200`; filtered reverse-keyset delivery page |
| `GET /v1/deliveries/{delivery_id}` | tenant API key | `200`; safe current delivery detail |
| `GET /v1/deliveries/{delivery_id}/attempts` | tenant API key | `200`; reverse-keyset lifetime attempt page |
| `GET /v1/deliveries/{delivery_id}/attempts/{attempt_id}` | tenant API key | `200`; safe immutable attempt detail |
| `POST /v1/deliveries/{delivery_id}/replay` | tenant API key + JSON expected generation | `202`; dead-lettered delivery reset to a fresh pending generation |

`GET /v1/deliveries` accepts optional `status`, `endpoint_id`, and `event_id`
filters plus `limit` and `cursor`; attempts accept `limit` and `cursor`.
Delivery ordering is `(created_at DESC, id DESC)`, while attempts use lifetime
`attempt_number DESC` with ID as a deterministic tie-breaker. Cursors are
versioned base64url tokens and a malformed cursor, or reuse under a different
filter, returns `422`. All inspection responses set `Cache-Control: no-store`.

Replay requires
`{"expected_dispatch_generation": <observed positive generation>}` and returns
`Location: /v1/events/{event_id}`. Missing and cross-tenant IDs share opaque
`404`; changed generation returns `409 delivery_generation_conflict`; a
delivery not currently dead-lettered returns `409 delivery_not_replayable`.
`202` proves PostgreSQL committed the new generation and outbox intent, not
broker publication or receiver success.

Secret rotation requires `{"expected_active_version": <observed positive
version>}`. A stale value returns `409 signing_secret_version_conflict` and the
safe `HookRelay-Active-Secret-Version` response header; missing/cross-tenant
endpoint IDs remain opaque `404`. Successful plaintext is returned once with
`Cache-Control: no-store` and `Pragma: no-cache`.

Any HTTP body larger than `HOOKRELAY_MAX_REQUEST_BODY_BYTES` receives
`413 request_body_too_large` before routing, authentication, or JSON parsing.
An otherwise valid event whose compact sorted-key UTF-8 `payload` exceeds
`HOOKRELAY_MAX_EVENT_PAYLOAD_BYTES` receives `413 event_payload_too_large`
before ingestion.

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
- approximately 4 GB of free disk space for the full observability profile
- Node.js 24 or newer and pnpm 11 only for host-side console development

The repository pins `uv` 0.12.1, Python 3.12.13 on Debian Bookworm,
PostgreSQL 17.7, NATS 2.14.3 on Alpine 3.22, and pnpm 11.16.0. The optional
profile pins OpenTelemetry Collector 0.158.0, Tempo 2.10.5, Prometheus 3.12.0,
and Grafana 13.1.0. `uv.lock` and `web/pnpm-lock.yaml` pin the complete Python
and browser dependency graphs.

## Fastest start: Docker Compose

Windows PowerShell:

```powershell
git clone --branch codex/stage-06-observability-console --single-branch https://github.com/clashing13/hookrelay.git
Set-Location hookrelay
Copy-Item .env.example .env
```

Before starting Compose, edit ignored `.env`: replace the example database
password, encryption key, and bootstrap token. Checked-in examples are not
deployable secrets.

```powershell
$env:POSTGRES_HOST_PORT = "55432"
$env:HOOKRELAY_TELEMETRY_ENABLED = "true"
docker compose build
docker compose up --detach --wait postgres nats receiver
docker compose run --rm api alembic upgrade head
docker compose --profile observability up --detach --wait
docker compose ps
```

Check every local boundary:

```powershell
curl.exe --fail http://127.0.0.1:8000/health/live
curl.exe --fail http://127.0.0.1:8000/health/ready
curl.exe --fail http://127.0.0.1:8000/metrics
curl.exe --fail http://127.0.0.1:8000/console/
curl.exe --fail http://127.0.0.1:9000/health/live
Invoke-RestMethod 'http://127.0.0.1:8222/healthz?js-enabled-only=true'
curl.exe --fail http://127.0.0.1:9090/-/ready
curl.exe --fail http://127.0.0.1:3000/api/health
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

$history = Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/v1/deliveries?event_id=$($event.id)" `
  -Headers $authHeaders
$deliveryId = $history.items[0].id
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/v1/deliveries/$deliveryId" `
  -Headers $authHeaders
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/v1/deliveries/$deliveryId/attempts" `
  -Headers $authHeaders
```

Open the [console](http://127.0.0.1:8000/console/) and paste the one-time tenant
key to inspect, filter, paginate, and replay dead-lettered deliveries. A browser
refresh intentionally signs out. Open [Grafana](http://127.0.0.1:3000/) with
the local-only default `admin` / `hookrelay-local-only`, or inspect
[Prometheus](http://127.0.0.1:9090/) and the [API docs](http://127.0.0.1:8000/docs).

The Stage 6 guide contains the exact correlation, metrics, trace, history,
console, and failure-isolation demonstrations:
[run and test Stage 6](docs/stages/06-observability-operations-console.md#10-exact-commands-for-running-and-testing).
The earlier [Stage 5 exercises](docs/stages/05-security-traffic-control.md#10-exact-commands-for-running-and-testing)
remain useful for destination blocking, request limits, endpoint throttling,
circuit recovery, secret rotation, and container permissions.

Stop the complete profile while preserving all named data:

```powershell
docker compose --profile observability down
```

Adding `--volumes` deletes the PostgreSQL, NATS, Prometheus, Tempo, and Grafana
volumes and is only for an intentional fresh start.

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
Publisher and worker Prometheus listeners default to `127.0.0.1:9101/metrics`
and `127.0.0.1:9102/metrics` in this workflow. Telemetry remains disabled unless
you provide a reachable OTLP/HTTP endpoint.

For live console development, install the locked browser dependencies and run
Vite; its `/v1` proxy targets the host API:

```powershell
pnpm --dir web install --frozen-lockfile
pnpm --dir web run dev
```

## Checks

Fast checks:

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\mypy.exe
.\.venv\Scripts\pytest.exe -m "not integration"
pnpm --dir web run typecheck
pnpm --dir web run test
pnpm --dir web run build
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
docker compose --profile observability config --quiet
docker build --pull --tag hookrelay:stage6 .
```

After installing Playwright's pinned Chromium and starting a migrated API on
port 8000, the browser workflow can provision a disposable tenant with the
local bootstrap token:

```powershell
$env:HOOKRELAY_E2E_BOOTSTRAP_TOKEN = "replace-this-local-bootstrap-token-before-use"
pnpm --dir web exec playwright install chromium
pnpm --dir web run test:e2e
```

Stopping long-lived application processes prevents them from claiming rows
created by deterministic integration harnesses. The commands create or reuse
only the explicitly named `hookrelay_test`; they never point tests at the normal
`POSTGRES_DB` or drop a database. Reserve `hookrelay_test` for disposable test
data and stop if it contains anything valuable.

Stage 6-focused evidence lives in
`tests/unit/test_stage6_observability.py`,
`tests/unit/test_stage6_console.py`,
`tests/api/test_stage6_operations.py`,
`tests/integration/test_stage6_operations.py`,
`web/src/App.test.tsx`, and
`web/e2e/console.spec.ts`. It covers log/correlation contracts, persisted W3C
context propagation, bounded metric labels/listeners, tenant-safe history and
cursors, replay retention, static-console security headers, credential
lifecycle, browser inspection, and the real-API console path. Migration
`20260806_0005`, `alembic check`, the multi-stage image build, Compose
validation, and provisioned observability configuration cover their respective
schema and packaging boundaries. These checks do not turn telemetry into a
correctness proof, establish production SLOs/capacity, make the local dashboard
HA, or make the console a production identity/audit system.

Stage 5-focused evidence lives in
`tests/unit/test_stage5_security.py`,
`tests/unit/test_stage5_traffic_control.py`,
`tests/api/test_stage5_security.py`,
`tests/integration/test_stage5_security.py`, and
`tests/integration/test_stage5_traffic_control.py`. It covers address classes,
mixed DNS answers, IP-pinned peer/Host/SNI behavior, byte boundaries,
tenant-safe rotation, shared concurrent fixed-window admission, and one leased
recovery probe against real PostgreSQL. Those tests prove their encoded cases,
not arbitrary DNS/resolver compromise, external egress policy, every race,
exactly once, HA, or production capacity.

## Migration policy

Run `alembic upgrade head` explicitly; application processes never migrate at
startup.

- `20260802_0001`: domain schema and transactional outbox.
- `20260803_0002`: recoverable outbox publisher claims.
- `20260804_0003`: retry schedule, delivery claim lease, generation, dead-letter
  state/reason, attempt fencing, and per-generation outbox uniqueness.
- `20260805_0004`: one persistent, composite tenant/endpoint traffic-control
  row, fixed-window counters, circuit state, and recovery-probe lease fields;
  upgrade backfills existing endpoints and adds `is_circuit_probe` attempt
  evidence.
- `20260806_0005`: outbox correlation/trace-context columns plus tenant-safe
  reverse-keyset delivery and lifetime-attempt history indexes.

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
- [Stage 5: security and traffic control](docs/stages/05-security-traffic-control.md)
- [Stage 6: observability and operations console](docs/stages/06-observability-operations-console.md)
- [Architecture decision records](docs/decisions/README.md)
- Stage 6 decisions:
  [outbox-preserved observability context](docs/decisions/0019-outbox-preserved-observability-context.md),
  [bounded process-local metrics](docs/decisions/0020-bounded-process-local-metrics.md),
  [tenant keyset delivery history](docs/decisions/0021-tenant-keyset-delivery-history.md), and
  [same-origin memory-only operations console](docs/decisions/0022-same-origin-memory-only-operations-console.md)
- Stage 5 decisions:
  [resolved-address SSRF](docs/decisions/0014-resolved-address-ssrf-policy.md),
  [database-authoritative traffic controls](docs/decisions/0015-database-authoritative-endpoint-traffic-controls.md),
  [pre-parse request limits](docs/decisions/0016-request-byte-limits-before-parsing.md),
  [versioned secret rotation](docs/decisions/0017-versioned-signing-secret-rotation.md), and
  [least-privilege app containers](docs/decisions/0018-least-privilege-app-containers.md)

## License

[MIT](LICENSE) (c) 2026 Tarun Athreya
