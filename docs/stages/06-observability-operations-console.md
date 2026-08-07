# Stage 6: observability and the operations console

Stage 6 makes HookRelay explainable while it is running. It adds correlated JSON logs,
distributed traces, bounded-cardinality Prometheus metrics, a provisioned Grafana dashboard,
tenant-scoped delivery-history APIs, and a deliberately small React operations console.

The central rule is that observability is evidence about product behavior, not part of the
product's correctness path. PostgreSQL remains authoritative, the version-1 broker payload and
signed webhook contract remain unchanged, and a failed telemetry backend cannot make ingestion,
delivery, liveness, or readiness fail.

## 1. The problem this stage solves

Before this stage, HookRelay durably accepted events and recovered deliveries, but an operator
needed database access and several log searches to answer ordinary questions:

- Was an event accepted but not yet published?
- Is a delivery waiting, actively leased, scheduled for retry, successful, or dead-lettered?
- Which HTTP attempts happened, in what generation, and with what bounded outcome?
- Did the worker ACK, delay-NACK, or terminate the broker observation?
- Is the outbox moving, are attempts slowing down, and are circuits avoiding failed calls?
- Can a dead letter be replayed without racing another operator?

Stage 6 creates three complementary views. Logs explain discrete decisions. Traces connect work
across API, outbox, NATS, worker, and outbound HTTP boundaries. Metrics summarize rates and
latencies without unbounded identifiers. The operations API and console expose PostgreSQL's
authoritative current state plus immutable attempt history.

This is not “full event sourcing.” HookRelay still does not retain every former status,
rate-limit deferral, stale broker observation, or replaying human identity. The UI and API use the
more precise phrase “current delivery state plus attempt history.”

## 2. What is deliberately out of scope

Stage 6 does not add:

- a customer-facing dashboard, design system, or general administration product;
- user accounts, browser sessions, OAuth, or storage of bearer keys in the browser;
- bulk replay, editing endpoints, viewing payloads, or viewing destination URLs;
- tenant IDs, secret IDs, ciphertext, claim tokens, trace baggage, or exception text in responses;
- a log backend such as Loki or Elasticsearch;
- a complete transition-audit table;
- performance claims, SLOs, alert paging, k6, or Toxiproxy evidence;
- a new broker schema or new signed-webhook fields;
- production authentication or TLS for the local Grafana demonstration.

The console holds the API key only in React memory. Refreshing the page intentionally signs the
operator out. That small inconvenience prevents a long-lived credential from being written to a
URL, local storage, session storage, build variable, or static asset.

## 3. Architecture before and after the stage

Before Stage 6, the runtime data path was observable only through process logs and direct state
inspection:

```mermaid
flowchart LR
    Client --> API
    API --> PostgreSQL[(PostgreSQL)]
    PostgreSQL --> Publisher[Outbox publisher]
    Publisher --> NATS[(NATS JetStream)]
    NATS --> Worker
    Worker --> Receiver[Webhook receiver]
```

After Stage 6, the correctness path is unchanged and a failure-isolated observability plane sits
beside it:

```mermaid
flowchart LR
    Operator[Operations console] -->|Bearer API key| History[History and replay API]
    Client --> API
    API --> PostgreSQL[(PostgreSQL)]
    PostgreSQL --> Publisher[Outbox publisher]
    Publisher --> NATS[(NATS JetStream)]
    NATS --> Worker
    Worker --> Receiver[Webhook receiver]

    API -. OTLP spans .-> Collector[OpenTelemetry Collector]
    Publisher -. OTLP spans .-> Collector
    Worker -. OTLP spans .-> Collector
    Collector --> Tempo[(Tempo)]

    Prometheus[(Prometheus)] -. scrape .-> API
    Prometheus -. scrape .-> Publisher
    Prometheus -. scrape .-> Worker
    Grafana --> Prometheus
    Grafana --> Tempo
```

The API creates a correlation UUID and server span. Event creation stores the correlation UUID
and canonical W3C `traceparent` beside each outbox row. The publisher restores that parent,
creates a producer span, and injects trace context into optional NATS headers. The worker extracts
the headers and creates a consumer span. The strict JSON payload remains byte-compatible schema
version 1.

Prometheus scrapes a custom process-local registry. The API exposes `/metrics` on port 8000; the
publisher and worker expose internal listeners on ports 9101 and 9102. These listeners do not
carry tenant, event, delivery, endpoint, URL, event-type, or arbitrary error labels.

## 4. A repository and file tour

Key backend files:

- `src/hookrelay/observability.py` owns correlation context, W3C propagation, explicit spans,
  exporter lifecycle, and the pure-ASGI request middleware.
- `src/hookrelay/metrics.py` owns one custom Prometheus registry per process and the internal
  scrape listeners.
- `src/hookrelay/logging.py` emits JSON with service, version, environment, process role,
  correlation ID, and active trace/span IDs.
- `src/hookrelay/api/deliveries.py` implements list, detail, attempt, and replay operations.
- `src/hookrelay/schemas.py` defines the safe public history representation.
- `src/hookrelay/console.py` serves built assets at `/console` with restrictive browser headers.
- `migrations/versions/20260806_0005_operations_history.py` adds deterministic history indexes
  and persisted outbox trace context.

Frontend files live in `web/`. `web/src/api.ts` is the fixed same-origin API client,
`web/src/App.tsx` owns the small workflow, and the test files cover components and a real browser
path. Vite builds with `/console/` as its base path.

Local observability configuration lives under `observability/`:

- `otel-collector.yaml` receives OTLP/HTTP and batches spans to Tempo;
- `tempo.yaml` uses a local single-binary trace store;
- `prometheus.yaml` scrapes all three HookRelay process roles;
- Grafana provisioning creates immutable Prometheus and Tempo data sources;
- `delivery-operations.json` provisions the delivery dashboard.

`Dockerfile` builds the React assets in a pinned Node stage, copies them into the root-owned
Python package, and still runs the final image as UID/GID 10001 on a read-only root filesystem.

## 5. A request and delivery data-flow trace

### Event acceptance

1. The pure-ASGI middleware validates a canonical inbound correlation UUID or creates a new one.
2. It extracts a valid W3C parent if present and starts an API server span.
3. Authentication selects the tenant from the bearer credential; requests never supply a tenant
   ID to history queries.
4. Event, delivery, and outbox rows commit atomically as before.
5. Each outbox row stores the correlation UUID and canonical trace parent outside the strict
   broker JSON payload.
6. The response returns `X-Correlation-ID`. Logs inside the context contain the same UUID.

### Publication and delivery

1. The publisher claims a bounded outbox batch in PostgreSQL.
2. It restores the persisted context and starts a producer span.
3. JetStream receives the unchanged ID-only JSON plus optional W3C and correlation headers.
4. Only after a PubAck does the publisher finalize `published_at` and increment publication
   metrics.
5. A worker extracts the optional headers. Old messages with no telemetry headers still work.
6. It starts a consumer span and runs the existing lease, rate, circuit, signing, HTTP, and
   finalization flow.
7. Attempt metrics are incremented only after PostgreSQL durably stores the outcome.
8. ACK/NAK/TERM observations are counted separately from database delivery state.

### History and replay

1. The console sends the memory-only bearer key to a fixed relative `/v1` path.
2. `GET /v1/deliveries` filters on the authenticated tenant and orders by
   `(created_at DESC, id DESC)`.
3. The opaque cursor contains the last timestamp/UUID and a digest of active filters. It is a
   pagination aid, not authorization.
4. Detail and attempt queries repeat the tenant predicate, including joined resources.
5. Replay sends the generation the operator actually observed.
6. The existing transaction locks the row and returns `409` if status or generation changed.
7. A successful replay creates a new outbox dispatch generation without deleting old attempts.

## 6. Definitions of every new technology and term

**Correlation ID:** an opaque UUID used to find related logs. It is not an authorization token.

**Trace:** a tree of timed operations describing one distributed flow.

**Span:** one timed operation within a trace, such as API handling, broker publication, message
consumption, or outbound HTTP delivery.

**Trace context:** standardized identifiers and flags that let the next process attach its span
to the existing trace.

**W3C Trace Context:** the interoperable `traceparent` header format. HookRelay propagates only
this bounded context and does not propagate baggage.

**OpenTelemetry SDK:** the in-process API that creates spans and batches export work.

**OTLP:** OpenTelemetry Protocol, used here over HTTP to send spans to the collector.

**Collector:** a failure-isolation and routing process between instrumented applications and a
trace backend.

**Tempo:** Grafana's trace store and query backend.

**Prometheus:** a time-series database that periodically scrapes metric endpoints.

**Counter:** a metric that only increases, such as completed attempts.

**Gauge:** a metric that can rise and fall, such as in-flight worker messages.

**Histogram:** observations placed into fixed buckets so quantiles can be estimated over a time
window.

**Cardinality:** the number of distinct label combinations. IDs and URLs create unbounded
cardinality and can exhaust a metrics system.

**Grafana provisioning:** version-controlled YAML and JSON that create data sources and dashboards
at startup rather than through unrecorded clicks.

**Keyset pagination:** requesting rows after a stable ordering key rather than skipping an offset.

**Opaque cursor:** a client token whose internal fields are not part of the public contract.

**Same-origin:** the console and API use the same scheme, host, and port, avoiding CORS and
preventing the UI from forwarding credentials to a configurable destination.

**Content Security Policy (CSP):** a browser policy restricting scripts, connections, frames,
objects, and other resource types.

## 7. Why each technology and design was chosen

Manual OpenTelemetry spans were chosen over broad automatic instrumentation because HookRelay
uses explicit transaction and durability boundaries. A span named “publish” should reflect a
real JetStream PubAck; an attempt outcome metric should reflect a committed PostgreSQL record.
Manual instrumentation also avoids changing the specialized `httpx2` and NATS behavior.

OTLP export uses a batch processor. Request and worker tasks enqueue spans; they do not wait for
Tempo during normal work. Export is disabled by default unless an endpoint is configured, and
shutdown is bounded by the configured timeout.

Custom Prometheus registries avoid global collectors accumulating when tests call `create_app()`
many times. Closed label vocabularies protect memory. Separate process endpoints are required
because the API, publisher, and worker do not share Python memory.

Keyset pagination remains stable as the table grows and avoids the shifting pages produced by
offsets during concurrent inserts. The UUID tie-breaker handles equal timestamps deterministically.

The same-origin console is intentionally a static client of the same tenant API. It receives no
database, NATS, bootstrap, or Grafana privileges. Grafana owns charts; the console owns history
and replay.

## 8. Serious alternatives and why they were not chosen

**Put trace fields in broker JSON.** Rejected because the schema is strict, durable, and already
versioned. Optional NATS headers preserve payload compatibility and allow old messages to work.

**Use tenant or delivery IDs as metric labels.** Rejected because each delivery would create a
new time series. Identifiers belong in logs, traces, and tenant-authorized detail APIs.

**Use offset pagination.** Rejected because concurrent inserts move rows between offsets and large
offset scans become expensive.

**Expose direct SQL or an admin database console.** Rejected because it bypasses tenant isolation,
safe schemas, replay preconditions, and secret minimization.

**Store the API key in local storage.** Rejected because browser extensions, shared profiles, and
later JavaScript can retrieve a durable credential after the operator leaves.

**Add a frontend backend-for-frontend.** Rejected because the existing API already provides the
correct tenant security boundary. Another privileged service would increase deployment and audit
surface for no Stage 6 benefit.

**Make telemetry part of readiness.** Rejected because losing a dashboard must not cause healthy
API instances to be removed or webhook work to stop.

## 9. Failure modes and design tradeoffs

- If the collector or Tempo is down, the bounded span queue may drop telemetry after exporter
  timeouts. Product work continues and readiness remains PostgreSQL-only.
- If Prometheus is down, counters continue only in process memory and are lost on restart. The
  database remains authoritative for delivery state.
- Process-local metrics reset on deployment. Queries should use rate functions and tolerate
  resets.
- A correlation ID connects logs but does not prove causality or authorization.
- Trace sampling or queue pressure can make a valid flow absent from Tempo.
- A cursor is versioned and carries a filter digest for consistency, but it is not signed and is
  never trusted as tenant identity.
- Current state can change immediately after it is read. Replay therefore uses an optimistic
  generation precondition and can safely return `409`.
- Attempt history contains bounded error codes, not exception messages or response bodies.
- The local Grafana password, loopback HTTP, single-node Tempo, and short retention are demo
  choices, not a production observability deployment.
- A successful response from the receiver can still be observed twice under at-least-once
  delivery if the worker loses the response or crashes before durable completion.

## 10. Exact commands for running and testing

Run these commands from the repository root in PowerShell.

### Install locked dependencies

```powershell
& '.\.venv\Scripts\uv.exe' sync --frozen --all-groups
& 'C:\Users\tarun\.cache\codex-runtimes\codex-primary-runtime\dependencies\bin\fallback\pnpm.cmd' --dir web install --frozen-lockfile
```

### Run backend and console checks

```powershell
& '.\.venv\Scripts\ruff.exe' check .
& '.\.venv\Scripts\ruff.exe' format --check .
& '.\.venv\Scripts\mypy.exe'
& '.\.venv\Scripts\pytest.exe' -m "not integration"
& 'C:\Users\tarun\.cache\codex-runtimes\codex-primary-runtime\dependencies\bin\fallback\pnpm.cmd' --dir web run typecheck
& 'C:\Users\tarun\.cache\codex-runtimes\codex-primary-runtime\dependencies\bin\fallback\pnpm.cmd' --dir web run test
& 'C:\Users\tarun\.cache\codex-runtimes\codex-primary-runtime\dependencies\bin\fallback\pnpm.cmd' --dir web run build
```

### Apply and verify the database revision

```powershell
$env:POSTGRES_HOST_PORT='55432'
docker compose up --detach --wait postgres nats
$env:HOOKRELAY_DATABASE_URL='postgresql+asyncpg://hookrelay:hookrelay-dev-only@127.0.0.1:55432/hookrelay'
& '.\.venv\Scripts\alembic.exe' upgrade head
& '.\.venv\Scripts\alembic.exe' check
```

### Run the application and observability stack

```powershell
$env:POSTGRES_HOST_PORT='55432'
$env:HOOKRELAY_TELEMETRY_ENABLED='true'
docker compose --profile observability up --detach --build --wait
```

Open:

- console: `http://127.0.0.1:8000/console/`
- API docs: `http://127.0.0.1:8000/docs`
- API metrics: `http://127.0.0.1:8000/metrics`
- Grafana: `http://127.0.0.1:3000` (`admin` / `hookrelay-local-only` by default)
- Prometheus: `http://127.0.0.1:9090`

Use the bootstrap and endpoint/event commands in the README to obtain a tenant API key and create
a delivery. Paste only the tenant API key into the console. Do not paste the bootstrap token.

### Run real integration and browser tests

```powershell
& '.\.venv\Scripts\pytest.exe' -m integration
& 'C:\Users\tarun\.cache\codex-runtimes\codex-primary-runtime\dependencies\bin\fallback\pnpm.cmd' --dir web exec playwright install chromium
& 'C:\Users\tarun\.cache\codex-runtimes\codex-primary-runtime\dependencies\bin\fallback\pnpm.cmd' --dir web run e2e
```

### Stop the local stack

```powershell
docker compose --profile observability down
Remove-Item Env:HOOKRELAY_TELEMETRY_ENABLED -ErrorAction SilentlyContinue
```

Named volumes retain PostgreSQL, NATS, Prometheus, Tempo, and Grafana state. Do not add `--volumes`
unless you intentionally want to erase this local data.

## 11. How each test works and what it fails to prove

Unit tests verify UUID correlation normalization, context isolation, JSON fields, W3C injection,
no-op telemetry, bounded metric labels, and console security headers. They are fast and
deterministic, but a fake or in-memory exporter does not prove collector interoperability.

API tests use the real FastAPI middleware stack and controlled sessions. They prove correlation
headers appear on normal and error responses, public schemas omit internal columns, cross-tenant
lookups are opaque, malformed cursors fail safely, and equal timestamps paginate without gaps.
They do not prove PostgreSQL query plans at production scale.

Migration tests upgrade, downgrade, and re-upgrade an isolated real PostgreSQL database. They
prove backfill and constraints work on representative data; they do not substitute for a backup,
restore, and timed production migration rehearsal.

Pipeline integration tests use real PostgreSQL, JetStream, and receiver sockets. They preserve
the frozen broker and webhook contracts, but they are bounded scenarios rather than a load test.

Component tests exercise credential, history, detail, failure, and replay UI states in jsdom.
They do not reproduce every browser networking or accessibility behavior.

Playwright drives the real console and API against PostgreSQL. It proves the packaged workflow
connects end to end, but one browser path does not prove compatibility with every browser,
assistive technology, or concurrent operator race. Stage 7 supplies load and fault evidence.

## 12. A safe “break it intentionally” exercise

This exercise proves that trace storage is not a correctness dependency.

1. Start the complete stack and submit one event. Confirm the delivery appears in the console.
2. Stop only the collector:

   ```powershell
   docker compose --profile observability stop otel-collector
   ```

3. Submit another event and inspect both health endpoints:

   ```powershell
   curl.exe -i http://127.0.0.1:8000/health/live
   curl.exe -i http://127.0.0.1:8000/health/ready
   ```

4. Confirm both remain `200`, the delivery completes, history is readable, and metrics continue.
5. Observe bounded exporter warnings; no credential or destination URL should appear.
6. Restart the collector and submit a third event:

   ```powershell
   docker compose --profile observability start otel-collector
   ```

7. Confirm new traces appear. Spans dropped during the outage need not reappear.

Do not stop PostgreSQL or delete volumes during this exercise. That tests a different boundary.

## 13. Troubleshooting guidance

**The console is 404 in local Python development.** Build `web/` or run the Vite development
server. Production assets are copied into the Python package during the Docker build.

**The console returns 401.** Use the tenant API key returned by bootstrap, including its full
one-time secret. The bootstrap token is not a tenant credential. A browser refresh clears the key.

**Replay returns 409.** Refresh detail. Another operator or worker changed the status or dispatch
generation; the guard prevented a duplicate action.

**A cursor returns 422 after filters change.** Cursors are bound to the filter set that created
them. Clear the cursor by using Refresh, then paginate the new query.

**Grafana has no data.** Ensure the `observability` profile is running, telemetry was enabled
before the app processes started, and Prometheus targets are up. Metrics can appear even when
traces are disabled.

**Tempo has no trace.** Check collector logs and the OTLP endpoint. A disabled exporter, dropped
batch, or an operation outside an active span can explain absence. Do not restart product services
merely to repair a dashboard.

**Prometheus target is down for publisher or worker.** Compose must set `METRICS_HOST=0.0.0.0`;
the safe code default is loopback. Confirm ports 9101 and 9102 inside their containers.

**Metrics explode in series count.** Stop and inspect labels. No identifier, URL, event type, raw
path, or unrestricted error string belongs in a label. Fix instrumentation before increasing
capacity.

**Docker build cannot find frontend assets.** Confirm `web/pnpm-lock.yaml` is committed and
`pnpm run build` creates `web/dist/index.html`. The Docker copy occurs before the Python wheel is
installed.

## 14. Recruiter questions with strong-answer ingredients

### Why use logs, metrics, and traces together?

A strong answer distinguishes them: metrics reveal aggregate change, traces show causal timing
across processes, and logs explain discrete decisions. PostgreSQL remains the authoritative
product state.

### How did you control Prometheus cardinality?

Mention custom registries, closed enum labels, route templates, and the deliberate ban on tenant,
event, delivery, endpoint, URL, event type, and free-form error labels.

### How did you add tracing without breaking the broker contract?

Explain that correlation and canonical trace context are persisted beside the outbox row,
restored by the publisher, and carried in optional NATS headers. The strict schema-v1 JSON bytes
and signed webhook remain unchanged; older headerless messages still work.

### Why is telemetry absent from readiness?

Readiness answers whether the API can serve dependency-backed product traffic. A collector outage
reduces diagnostic evidence but should not cause restarts or stop delivery.

### How do you prevent cross-tenant leakage in history?

The authenticated bearer credential supplies tenant identity. Every delivery and attempt query
includes that tenant predicate, cross-tenant UUIDs return the same opaque 404 as missing UUIDs,
and safe schemas omit secrets and internal leases.

### Why use keyset pagination?

It is deterministic under concurrent inserts when ordered by timestamp and UUID, avoids large
offset scans, and binds the cursor to active filters. The tenant predicate remains authoritative.

### What does the UI deliberately not do?

It stores credentials only in memory, uses fixed same-origin paths, exposes no payloads or URLs,
does not edit system configuration, and performs only generation-checked single replay.

## 15. A hands-on modification Tarun completes himself

Add an “attempts with transient failures” visual filter to the detail timeline.

Constraints:

1. The filter is local UI state; do not add a new backend query or metric label.
2. Default behavior still shows every attempt across every dispatch generation.
3. Use a labeled checkbox or segmented control that is keyboard accessible.
4. Empty filtered results explain that attempts exist but none match.
5. Add a component test that toggles the filter and preserves attempt order.
6. Run `pnpm --dir web run typecheck`, `test`, and `build`.

This exercise reinforces the boundary between bounded presentation state and authoritative
delivery history without changing production semantics.

## 16. A comprehension quiz and teach-back checklist

### Quiz

1. Why is a correlation ID not an authorization mechanism?
2. Where is trace context stored before an outbox row is published?
3. Why are optional NATS headers safer than adding trace fields to schema-v1 JSON?
4. What happens to delivery when Tempo is unavailable?
5. Which process exposes metrics on each of ports 8000, 9101, and 9102?
6. Why can a delivery ID appear in a log but not a Prometheus label?
7. What two fields make history pagination deterministic?
8. Why does changing filters invalidate an existing cursor?
9. Which fields must never appear in delivery-history responses?
10. Why does replay include `expected_dispatch_generation`?
11. What history is missing from the current model?
12. Why does refreshing the browser sign the operator out?
13. What does a Playwright pass prove that a component test does not?
14. What performance conclusion can Stage 6 evidence support?

### Teach-back checklist

Without notes, Tarun should be able to:

- draw the product data plane and separate observability plane;
- trace one correlation UUID from API through outbox headers to worker logs;
- explain server, producer, consumer, and outbound spans;
- name three forbidden high-cardinality labels;
- distinguish attempt completion, database terminal state, and broker disposition;
- explain why PostgreSQL—not Tempo or Prometheus—owns delivery truth;
- describe the tenant predicate and opaque 404 rule;
- explain keyset pagination with an equal-timestamp example;
- demonstrate a dead-letter replay conflict and safe refresh;
- stop the collector and predict health and delivery behavior;
- state the console credential-storage rule;
- name the measurements and fault scenarios deferred to Stage 7.

Stage 6 is complete only when backend, migration, console, browser, Compose, and image-hardening
checks are green and this explanation matches the behavior observed in the running system.
