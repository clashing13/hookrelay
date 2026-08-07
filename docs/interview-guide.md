# HookRelay interview guide

This is a cumulative speaking guide, not a script to memorize. Strong answers
state the problem, name the invariant, trace the mechanism, discuss a serious
alternative, and acknowledge what has not been proved.

## Honest project pitch

### One sentence

HookRelay is a staged fault-tolerant webhook delivery platform I am building to
practice durable state, explicit delivery semantics, failure recovery,
concurrency, security, observability, and measurement.

### Thirty-second Stage 1 version

Stage 1 establishes a deployable Python/FastAPI foundation. It validates
environment configuration, emits HookRelay-owned log records as JSON, owns one
asynchronous SQLAlchemy engine per process, and separates dependency-free
liveness from a bounded real PostgreSQL readiness query. It disposes resources
during shutdown, uses explicit Alembic migrations, and reproduces the topology
with non-root Docker and Compose. Async API tests, real-PostgreSQL integration
tests, migration checks, and CI cover different boundaries. Event ingestion and
webhook delivery are not implemented yet.

### Future product version

When the later stages are complete, a producer submits an event, PostgreSQL
stores it with a transactional outbox, a publisher moves committed work to
NATS JetStream, and bounded workers send HMAC-signed HTTP requests with retry
and dead-letter behavior. The intended guarantee is at least once, so stable
event IDs and receiver-side idempotency address duplicate effects.

Do not use the future version as a claim about the Stage 1 code.

## A strong-answer shape

Use this sequence for a two-minute technical answer:

1. **Problem:** Name the failure or constraint.
2. **Invariant:** State what must remain true.
3. **Mechanism:** Trace the actual code/configuration path.
4. **Evidence:** Cite the test or observation that exercises it.
5. **Tradeoff:** Compare one credible alternative.
6. **Boundary:** Say what the evidence does not prove.

Example: "A database outage should remove an instance from dependency-backed
traffic without causing an API restart storm. Therefore liveness never touches
PostgreSQL, while readiness runs a bounded `SELECT 1`. A controlled failure
test makes readiness return a sanitized `503` while liveness remains `200`; a
real integration test proves the success path, and the Compose exercise
observes recovery after PostgreSQL returns. This proves the probe behavior, not
production capacity or every domain query."

## Stage 1 questions and strong-answer ingredients

### What problem does HookRelay solve?

- A producer should not silently lose a webhook because the destination or
  network is temporarily unavailable.
- HookRelay will durably record work and retry delivery in later stages.
- It does not repair the receiver and cannot remove acknowledgment ambiguity.
- Stage 1 only supplies the service and dependency foundation.

### Why Python rather than Go?

- The core learning target is distributed-systems reasoning within seven
  focused stages.
- Existing Python/FastAPI fluency preserves time for correctness and failure
  work.
- The workload is largely I/O-bound and has maintained async libraries.
- Go is a serious worker alternative because of goroutines, static deployment,
  and resource efficiency.
- Benchmark first; a Go worker is a possible post-MVP experiment, not an
  intuition-driven rewrite.

### What does `async def` buy you?

- A coroutine can yield to the event loop while genuine async I/O waits.
- This permits concurrency without one thread per request.
- It does not make CPU-heavy work parallel.
- Calling a blocking driver inside `async def` still blocks the loop.
- Concurrency must be bounded later to protect memory, connection pools, and
  destinations.

### Why an application factory?

- Construction is explicit: settings, database ownership, routes, and lifespan
  are wired in one place.
- Tests can build isolated applications with controlled dependencies.
- It avoids relying solely on import-time global state.
- A module-level ASGI app can still be exported for Uvicorn; the factory remains
  the construction mechanism.

### Why separate liveness and readiness?

- Liveness asks whether the process can answer; readiness asks whether it can
  serve dependency-backed traffic.
- If liveness queried PostgreSQL, an outage could make an orchestrator restart
  healthy API processes, add connection pressure, and fail to repair the DB.
- Readiness executes a real, timeout-bounded `SELECT 1`, returns `503` on
  dependency failure, sanitizes its public response, and probes again on the
  next request so it can recover.

### Does `SELECT 1` prove the application works?

- It proves basic connectivity, authentication, connection checkout, and query
  execution through the real stack.
- It does not prove domain tables exist, migrations are current, complex queries
  work, or there is enough capacity under load.
- Later stages may add deeper startup or operational checks without making
  liveness dependency-bound.

### Why one SQLAlchemy engine but not one global session?

- The engine is process-scoped infrastructure and owns a reusable connection
  pool.
- A session carries mutable transaction/unit-of-work state.
- Sharing one session across unrelated concurrent requests can interleave
  transactions and corrupt isolation assumptions.
- Later operations create short-lived sessions and return connections promptly.

### Why PostgreSQL instead of SQLite?

- Later stages need concurrent multi-process transactions, relational
  constraints, unique idempotency rules, and a transactional outbox.
- SQLite is excellent for embedded use and fast tests, but its locking,
  transaction behavior, SQL, and types differ.
- PostgreSQL integration tests prevent a false sense of confidence from a
  substitute database.

### Why not MongoDB?

- HookRelay's tenant, endpoint, event, delivery, attempt, and outbox data has
  strong relational constraints and transaction boundaries.
- MongoDB is credible for document-oriented requirements and offers
  transactions in supported topologies.
- No current requirement offsets the cost of a different consistency and query
  model.

### Why Alembic rather than `metadata.create_all()`?

- Models describe the current desired shape; a live database has historical
  versions and data.
- `create_all()` creates missing objects but is not an ordered, reviewable data
  transition.
- Alembic records revisions and supports deliberate DDL/data movement.
- Migrations run as an explicit release step, avoiding replica startup races.
- Stage 1 correctly has no empty revision; Stage 2 will add the first meaningful
  schema.

### What does dependency locking prove?

- `uv.lock` records an exact resolved graph so CI and clean checkouts install
  the same dependency set with `uv sync --frozen`.
- The project still needs an intentional update and vulnerability-review
  process.
- A lockfile does not make upstream software correct or automatically secure.

### What is the difference between a Dockerfile, image, and container?

- A Dockerfile is a recipe.
- An image is the immutable layered build result.
- A container is a running process created from the image.
- On Windows, Linux containers share Docker Desktop's WSL 2 Linux VM kernel;
  each container is not its own full VM.

### What does running as non-root accomplish?

- It limits what a compromised application process can modify inside the
  container and reduces some host-impact paths.
- It is defense in depth, not a security boundary that excuses vulnerable code,
  broad mounts, excessive capabilities, or poor secret handling.

### Does Compose `depends_on` keep the database healthy?

- A health-conditioned dependency can gate initial API startup until PostgreSQL
  first becomes healthy.
- It does not guarantee PostgreSQL remains healthy for the API lifetime.
- The application must tolerate later failures; readiness reports them and must
  recover when the dependency returns.

### Why JSON logs?

- Stable fields are easier for log systems to filter and aggregate than parsed
  prose.
- Stage 1 includes process events and internal readiness diagnostics.
- Secrets and raw database URLs must remain absent.
- Correlation IDs, tracing, metrics, and dashboards arrive in Stage 6.

### Why HTTPX2 and an explicit lifespan manager in API tests?

- An async client matches the async application and avoids a deprecated legacy
  client path in the resolved FastAPI/Starlette stack.
- ASGI transport tests HTTP behavior without a real socket.
- In-process transports do not necessarily trigger ASGI lifespan, so the
  manager explicitly runs startup and shutdown.
- These tests still do not prove DNS, host ports, TLS, or container networking.

### What does CI cover, and what can it miss?

- Ruff checks lint and formatting; mypy checks static interfaces; pytest checks
  behavior; real PostgreSQL tests exercise integration; Alembic validates the
  migration path; Docker builds the Linux artifact.
- A standard Ubuntu runner makes clean-environment errors visible.
- CI does not prove production load, long-lived reliability, cloud policy,
  receiver compatibility, or absence of every vulnerability.

### Can HookRelay guarantee exactly once?

- Not across HTTP and an independent receiver database.
- The receiver may commit and its acknowledgment may be lost.
- Retrying yields at-least-once delivery; not retrying risks loss.
- A stable event ID plus a receiver-side unique record in the same transaction
  as its side effect can provide effectively-once business behavior.
- Delivery is future-stage work, not a Stage 1 feature.

## Recruiter-oriented questions

### "What was the most important design decision?"

Choose one real invariant rather than listing tools. Good options at Stage 1:

- separating liveness from readiness to prevent restart amplification;
- using the production database in integration tests;
- keeping migrations explicit and outside process startup;
- selecting Python to optimize learning velocity while reserving benchmarks for
  performance claims.

Explain the alternative and the failure that the choice prevents.

### "Tell me about a failure you tested."

Use the safe PostgreSQL outage exercise:

- Establish both probes at `200`.
- Stop PostgreSQL without stopping the API.
- Observe readiness become sanitized `503` while liveness remains `200`.
- Restart PostgreSQL and observe readiness recover without API restart.
- Connect the observation to orchestrator routing and incident amplification.

Do not claim that this one exercise proves worker crash recovery; workers do not
exist yet.

### "How did you know it worked?"

Name evidence by layer, then its limitation:

- exact in-process HTTP contract tests;
- controlled injected failure for sanitization and isolation;
- a real PostgreSQL integration test and controlled Compose outage/recovery;
- Alembic connection/upgrade validation;
- clean Docker build and Compose observation;
- CI on a standard Ubuntu runner.

Avoid saying "all tests passed, therefore it scales." Scale claims require the
Stage 7 methodology and recorded measurements.

### "What would you improve next?"

At the end of Stage 1, the correct answer is Stage 2's durable ingestion model:
tenants, endpoints, authenticated submission, idempotency, event/delivery state,
and a transactional outbox in one PostgreSQL transaction. NATS and delivery
workers wait for Stage 3 so the durability boundary is understood first.

## Claims to avoid

- "Async code is parallel."
- "Containers are lightweight VMs, one VM per container."
- "The `.env` file makes secrets secure."
- "Compose handles database outages for us."
- "SQLAlchemy automatically migrates production schemas."
- "Unit tests prove the PostgreSQL integration."
- "At least once means the receiver's side effect happens exactly once."
- "The system is production scale" before Stage 7 measurements exist.
- "HookRelay delivers webhooks" while reviewing only Stage 1.

## Three-to-five-minute Stage 1 teach-back

Aim for this timing:

1. **0:00-0:30 — Goal and boundary:** Describe the product, then state exactly
   what Stage 1 does and does not implement.
2. **0:30-1:20 — Request trace:** Contrast liveness's process-local path with
   readiness's bounded SQLAlchemy/asyncpg/PostgreSQL path.
3. **1:20-2:10 — Lifecycle:** Explain application factory, lifespan, one
   engine/pool, short future sessions, and shutdown disposal.
4. **2:10-3:00 — Deployment:** Distinguish Dockerfile/image/container; trace
   `127.0.0.1` host publishing, Compose DNS `postgres`, health checks, and the
   named volume.
5. **3:00-4:00 — Evidence:** Name each test layer and one thing it cannot prove.
6. **4:00-5:00 — Decisions:** Defend Python, PostgreSQL, explicit migrations,
   and the future at-least-once guarantee against their serious alternatives.

If any part cannot be explained without reading the code aloud, return to the
relevant file and trace one concrete request by hand.
