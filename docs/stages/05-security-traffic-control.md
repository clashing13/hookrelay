# Stage 5: security and traffic control

Stage 5 turns the Stage 4 delivery engine into a deliberately bounded network
participant. It does not claim that HookRelay is "secure now." It adds concrete,
testable controls at five boundaries: tenant data access, outbound destinations,
per-endpoint traffic, inbound bytes, secret lifecycle, and application-container
privilege.

This guide describes version `0.5.0` as implemented on 2026-08-05. When a prose
example disagrees with code, the code and tests are authoritative; update both the
decision record and this guide when the design changes.

Implemented defaults at a glance:

| Boundary | Default and behavior |
| --- | --- |
| Raw HTTP request | 1,048,576 bytes (1 MiB), counted before routing or JSON parsing |
| Canonical event `payload` | 262,144 bytes (256 KiB) after deterministic UTF-8 encoding |
| DNS resolution | all A and AAAA answers, two-second deadline, at most 32 answers |
| Outbound scheme | HTTPS in staging/production; HTTP allowed only in local/test |
| Redirects and environment proxies | disabled |
| Connection reuse | disabled so every new attempt revalidates and reconnects |
| Endpoint rate limit | fixed window, 10 admitted HTTP requests per one second |
| Circuit breaker | opens after five consecutive transient failures |
| Circuit recovery | 30-second cooldown, then one probe leased for the claim TTL |
| Delivery claim/probe lease | 20 seconds |
| Local private-host exemptions | exact `receiver`, `127.0.0.1`, and `localhost` only |
| Application container identity | UID/GID `10001:10001` |
| Explicit application temp mount | 16 MiB hardened `/tmp` tmpfs; image root is read-only |
| Application PID limit | 256 |

## 1. The problem this stage solves

Stage 4 can reliably retry a webhook. Reliability makes an unsafe destination more
dangerous: a malicious tenant could ask the worker to retry requests against an
internal administrator service or cloud metadata endpoint. A fleet of reliable
workers could also overwhelm a struggling customer faster than a single process.

Stage 5 addresses six concrete failure classes.

### Tenant-boundary mistakes

The caller must never choose a tenant ID. Authentication verifies a bearer API key
and derives one `tenant_id`; endpoint, event, delivery, and rotation queries must use
that scope. A cross-tenant object ID must not become an existence oracle. The public
API therefore returns the same opaque 404 for a missing object and an object owned by
another tenant.

This is application-layer isolation. PostgreSQL row-level security (RLS) is **not**
enabled. A forgotten tenant predicate in privileged application code could still
cross the boundary, which is a documented residual risk rather than a hidden claim.

### Server-side request forgery and DNS rebinding

Endpoint URLs are untrusted network instructions. Blocking the text `127.0.0.1` is
not sufficient: a hostname can resolve to loopback, an A/AAAA set can mix public and
private addresses, IPv4 can be represented inside IPv6, and DNS can change between an
API check and a worker connection.

HookRelay validates the complete current answer set immediately before TCP connect,
connects to the selected numeric IP, and keeps the original hostname for HTTP Host,
TLS SNI, and certificate verification. Redirects and proxy environment variables are
off so the client cannot silently choose an unvalidated next hop.

### Fleet-wide traffic control

Eight default worker tasks are not one rate limiter. If each process held its own
counter, scaling the worker fleet would scale the traffic sent to the customer.
HookRelay stores one shared control row per tenant endpoint and serializes decisions
with a PostgreSQL row lock.

The fixed-window limiter bounds admitted requests. The circuit breaker stops routine
traffic after repeated transient failures. One leased half-open probe tests recovery
without releasing a thundering herd. A deferral is scheduling, not an HTTP attempt,
so it does not create attempt evidence or consume retry budget.

### Unbounded inbound memory and parsing work

Pydantic cannot reject a body until the server has received and parsed it. A claimed
`Content-Length` can be absent or false. Pure ASGI middleware counts actual streamed
bytes before routing, authentication, and JSON parsing. Event payloads receive a
second semantic bound based on deterministic canonical UTF-8 bytes.

### Signing-secret lifecycle

A receiver-facing HMAC secret eventually needs replacement. Updating ciphertext in
place would change the meaning of deliveries already accepted. Stage 5 rotates by
immutable version and snapshots the selected secret ID into each delivery. The old
version remains available for old delivery work while new events select the new one.

### Container blast radius

Running a process as container root on a writable root filesystem gives a compromise
unnecessary options. The four application services run as UID/GID 10001, with a
read-only root filesystem, no Linux capabilities, no privilege gain, a PID limit, and
an explicit bounded hardened `/tmp` tmpfs for application temporary files.

The outcome is a smaller, explicit blast radius. It is not a replacement for an
egress firewall, a secrets manager, database RLS, host hardening, monitoring, or an
external security review.

## 2. What is deliberately out of scope

Stage 5 deliberately does **not** implement:

- PostgreSQL RLS policies or a separate restricted database role per tenant;
- a production egress firewall, Kubernetes NetworkPolicy, service-mesh authorization,
  or cloud security-group policy;
- an organization-wide public-host allowlist or customer-owned DNS verification;
- redirect following, explicit outbound proxies, HTTP/2, or connection keepalive;
- automatic failover across safe DNS answers or health-aware address selection;
- DNSSEC validation, a pinned recursive resolver, or detection of every future IANA
  special-purpose range independent of the Python runtime;
- IPv6 transition tunnels and the deprecated IPv4 `192.88.99.0/24` 6to4 relay range
  for webhook delivery, even when an embedded destination might be public;
- per-tenant API request rate limiting, quotas, billing, or abuse scoring;
- per-endpoint custom rate/circuit settings in the public API;
- token-bucket or sliding-window rate limiting;
- a traffic-control inspection/reset endpoint or operations UI;
- distributed traffic control outside PostgreSQL;
- compression-ratio limits, HTTP header-size limits, connection limits, or reverse
  proxy configuration;
- streaming JSON parsing; accepted request bodies are buffered up to the configured
  limit and then parsed downstream;
- scheduled signing-secret rotation, receiver acknowledgement, a secret-revocation
  workflow, or plaintext recovery after the one-time response;
- AES-GCM master encryption-key rotation, a decryption key ring, or bulk
  re-encryption. Changing the configured master key/version alone is unsafe because
  old ciphertext becomes unreadable;
- a managed secrets service. Local Compose still receives secrets through environment
  variables and the documented development defaults are not production credentials;
- rootless Docker, a custom seccomp/AppArmor/SELinux profile, image signing/SBOM
  enforcement, or stateful PostgreSQL/NATS container hardening;
- exactly-once delivery. Every Stage 4 at-least-once caveat still applies;
- proof of production scale, penetration resistance, or denial-of-service immunity.

These exclusions matter in interviews and designs. Say "Stage 5 constrains these
boundaries and records the remaining layers," not "SSRF is solved" or "the containers
are secure."

## 3. Architecture before and after the stage

### Before Stage 5

```text
untrusted client
  -> FastAPI/Pydantic body parsing
  -> bearer-key authentication
  -> tenant-filtered application query
  -> PostgreSQL event + delivery + outbox

JetStream message
  -> worker delivery lease
  -> local/test hostname gate
  -> ordinary HTTP client
  -> destination

container process
  -> non-root Dockerfile user
  -> otherwise mostly default Compose runtime permissions
```

The Stage 4 pipeline was durable, but the outbound gate was intentionally temporary.
Traffic state was not shared across workers, request size was not bounded before
parsing, endpoint signing secrets had no public rotation workflow, and the Compose
runtime did not assert the full application hardening set.

### After Stage 5

```text
untrusted client
  -> 1 MiB raw ASGI byte counter
  -> route/media/schema checks
  -> bearer API key derives tenant_id
  -> every resource query includes tenant_id
  -> optional 256 KiB canonical event-payload check
  -> PostgreSQL transaction

endpoint creation
  -> normalize URL
  -> reject unsafe literal
  -> resolve all current A + AAAA answers
  -> reject the whole set if any answer is unsafe
  -> store endpoint + version-1 secret + traffic-control row atomically

JetStream delivery
  -> lock delivery row
  -> synchronously reject unsafe literal URL
  -> lock endpoint traffic-control row
  -> defer OR reserve fixed-window slot and optional probe lease
  -> only admitted work creates DeliveryAttempt
  -> commit short claim transaction
  -> resolve all A + AAAA at connection time
  -> validate complete answer set
  -> TCP connect to selected numeric IP
  -> verify connected peer == selected IP
  -> HTTP Host + TLS SNI/certificate use original hostname
  -> redirects off; environment proxies off; keepalive off
  -> finalize attempt and shared circuit state under locks

secret rotation
  -> tenant-scoped endpoint + active-secret row locks
  -> expected version check
  -> retire old row + insert encrypted new version atomically
  -> return replacement plaintext once
  -> existing delivery snapshots retain old secret ID

api / publisher / worker / receiver containers
  -> UID:GID 10001:10001
  -> read-only root + no capabilities + no-new-privileges
  -> 256 PID limit + 16 MiB non-executable /tmp tmpfs
```

The important trust boundaries are layered:

1. The API authenticates a tenant and constrains accepted input.
2. PostgreSQL serializes durable state shared by processes.
3. The outbound transport validates the destination at socket time.
4. The container runtime limits what a compromised application process can do.
5. Production infrastructure must still restrict where the container can send
   packets. Stage 5 does not provide layer 5.

## 4. A repository/file tour

| Path | Stage 5 responsibility |
| --- | --- |
| `src/hookrelay/config.py` | Validates limits, traffic defaults, DNS timeout, environment-only exemptions, and cross-setting invariants. Reports version `0.5.0`. |
| `src/hookrelay/request_limits.py` | Pure ASGI raw-body counter, early `Content-Length` optimization, bounded message replay, and sanitized 413 response. |
| `src/hookrelay/main.py` | Installs body middleware before routes and constructs the shared API destination policy during lifespan. |
| `src/hookrelay/destination_policy.py` | IP classification (including explicit metadata/transition registry exceptions), bounded all-answer resolver, exact local/test exemptions, peer-pinned HTTP Core backend, and HTTPX 2 async transport. |
| `src/hookrelay/traffic_control.py` | Pure fixed-window and closed/open/half-open state transitions. |
| `src/hookrelay/models.py` | `EndpointTrafficControl`, circuit consistency constraints, secret versions, delivery secret snapshots, and `is_circuit_probe`. |
| `migrations/versions/20260805_0004_security_traffic_control.py` | Creates/backfills one traffic row per endpoint and adds probe evidence to attempts. |
| `src/hookrelay/api/dependencies.py` | Derives tenant identity from a verified API key and exposes the destination policy to routes. |
| `src/hookrelay/api/endpoints.py` | Validates outbound URLs at creation and implements tenant-scoped, version-checked signing-secret rotation. |
| `src/hookrelay/api/events.py` | Enforces the 256 KiB canonical payload limit before ingestion. |
| `src/hookrelay/ingestion.py` | Loads only tenant-owned active endpoints/secrets and snapshots `target_url` plus `signing_secret_id` into deliveries. |
| `src/hookrelay/delivery.py` | Locks traffic state, distinguishes deferral from attempt, builds the safe client, maps policy/transport outcomes, and finalizes the circuit. |
| `src/hookrelay/schemas.py` | Strict rotation request/response and existing tenant-safe public contracts. |
| `src/hookrelay/security.py` | Generates endpoint HMAC secrets and encrypts recoverable versions with AES-256-GCM. It does not implement a master-key ring. |
| `Dockerfile` | Multi-stage image, explicit UID/GID 10001, root-owned runtime artifacts, and permanent non-root `USER`. |
| `compose.yaml` | Reusable least-privilege anchor for all four application services and loopback-only host ports. |
| `.env.example` | Documents exact Stage 5 defaults and marks private-host exemptions as local/test only. |
| `pyproject.toml`, `uv.lock` | Add explicit public-interface dependencies `httpx2>=2.9.1,<3` and `httpcore2>=2.9.1,<3`. |
| `tests/unit/test_stage5_security.py` | Address classification, all-answer DNS policy, pinning, Host/SNI, peer verification, no reuse, and error mapping. |
| `tests/unit/test_stage5_traffic_control.py` | Deterministic rate-window, circuit, probe lease, expiry, and outcome transitions. |
| `tests/api/test_stage5_security.py` | Raw/canonical size boundaries, secret rotation contracts, cross-tenant opacity, and blocked literal endpoint creation. |
| `tests/integration/test_stage5_security.py` | Real-PostgreSQL control-row creation, secret snapshots, concurrent rotation, and cross-tenant non-mutation. |
| `tests/integration/test_stage5_traffic_control.py` | Real-PostgreSQL concurrent-worker evidence for shared limits and one recovery probe. |
| `docs/decisions/0014-...` through `0018-...` | Focused records for the five major decisions and their consequences. |

Read the code in this order for the shortest learning path:

1. `config.py` to learn the contract and invariants.
2. `request_limits.py` and `traffic_control.py` because both isolate small state
   machines from framework plumbing.
3. `destination_policy.py` from classifier to resolver to network backend to
   transport.
4. `delivery.py` around `_claim_attempt`, `execute`, and `_finish_attempt` to see the
   controls composed with Stage 4 leases.
5. `api/endpoints.py` and `ingestion.py` to follow secret versions across time.
6. Tests, then migration and Compose, to compare claims with executable evidence.

## 5. A request or event data-flow trace

### A. Every inbound HTTP body meets the raw byte boundary

1. Uvicorn emits ASGI `http.request` messages.
2. `RequestBodyLimitMiddleware` examines one valid decimal `Content-Length` only as
   an early rejection hint. It strips leading zeroes and compares decimal digit count
   and equal-length bytes to the configured maximum; it never parses an unbounded
   attacker string as a Python integer.
3. It counts each actual body chunk. At 1,048,576 bytes the request remains eligible;
   byte 1,048,577 returns sanitized HTTP 413.
4. Accepted messages are replayed downstream unchanged.
5. Only then do routing, authentication, Pydantic validation, and the handler run.

This order means an oversized request cannot cause tenant lookup or JSON parsing. It
does not mean one connection cannot hold a worker while slowly streaming; server and
ingress timeouts remain necessary.

### B. Authentication creates, rather than accepts, tenant context

1. The caller presents a versioned `hrk_...` bearer key.
2. HookRelay parses the public lookup ID and constant-time verifies the secret hash.
3. Authentication returns `AuthenticatedTenant(tenant_id, api_key_id)`.
4. Routes do not accept a tenant ID from path, query, header, or JSON.
5. Resource selects include both object identity and the authenticated `tenant_id`.
6. Composite foreign keys retain tenant association across endpoint, secret,
   delivery, and outbox relationships.

An API test showing cross-tenant 404 is evidence for these routes, not a proof that
every future query will remember the predicate. RLS is not present as a database
backstop.

### C. Endpoint creation validates the destination and creates security state

1. The global body limit runs, then `EndpointCreate` accepts a bounded `HttpUrl` with
   no userinfo or fragment.
2. `DestinationPolicy.validate_url()` normalizes the hostname, requires HTTPS outside
   local/test, and synchronously rejects a non-public literal IP unless it is an exact
   local/test exemption.
3. For a non-exempt hostname, the API resolves every current A/AAAA answer under the
   two-second deadline and rejects the URL if any answer is unsafe.
4. The API generates endpoint ID, secret ID, and high-entropy version-1 signing
   secret.
5. AES-256-GCM encrypts the secret with associated data containing envelope/key/
   secret versions plus tenant, endpoint, and secret identity.
6. One transaction inserts the endpoint, encrypted secret, and closed/empty traffic
   control row.
7. A 201 response returns the signing plaintext once with `no-store` headers.

The DNS result is not stored or trusted later. Creation-time resolution gives early
feedback; connection-time resolution enforces the boundary.

### D. Event ingestion applies the semantic limit and snapshots the active secret

1. Pydantic has already constrained the request to a JSON object and rejected
   non-finite numbers.
2. The handler serializes only `payload` with sorted keys, compact separators,
   UTF-8, and `ensure_ascii=False`.
3. More than 262,144 bytes returns 413 before ingestion writes anything.
4. Ingestion loads the requested endpoints by authenticated tenant, active flag, and
   active secret (`retired_at IS NULL`). Missing and cross-tenant IDs collapse into
   the same 404 behavior.
5. Each delivery copies the endpoint URL and active signing-secret ID.
6. Event, delivery snapshots, and outbox intent commit atomically as before.

Rotation after this commit does not rewrite the snapshot.

### E. A worker admission is serialized before an attempt exists

1. The worker locks the tenant delivery row and reconciles the broker message,
   outbox row, generation, status, due time, and any expired claim.
2. `validate_url()` rejects obvious unsafe literals while the delivery lock is held,
   before reserving an attempt.
3. The worker checks the generation attempt budget.
4. It locks the shared `(tenant_id, endpoint_id)` traffic-control row.
5. After acquiring the possibly contended traffic row, the worker refreshes PostgreSQL
   `clock_timestamp()`. Rate windows, cooldowns, and probe expiry use this post-lock
   value rather than a timestamp made stale while waiting.
6. Open/half-open circuit state is evaluated before the fixed-window counter.
7. A denial persists `retry_scheduled` and exact `next_attempt_at`, commits, and later
   becomes a delayed NAK. It creates no `DeliveryAttempt` and consumes no budget.
8. An admission increments the window count. If recovery is due, the claim token also
   becomes the sole half-open probe token with a claim-TTL expiry.
9. Only admitted work loads/decrypts its snapshotted secret, creates an attempt, and
   commits the delivery lease.

No PostgreSQL lock is held across remote HTTP.

### F. Connection-time validation closes the DNS rebinding gap

1. The worker enters the overall 10-second HTTP timeout and asks the shared HTTPX 2
   client to stream a POST.
2. The custom transport revalidates scheme/components and overwrites any caller Host
   or SNI override with values derived from the delivery URL.
3. HTTP Core asks `PolicyNetworkBackend` to connect to the original hostname.
4. The backend resolves all A/AAAA answers under the smaller of the DNS default and
   remaining connect timeout.
5. Any invalid/unsafe member blocks the whole destination. Exact local/test
   exemptions are the only exception.
6. After all answers pass, the first answer is selected and passed as a numeric IP to
   the underlying AnyIO TCP backend.
7. The backend reads `server_addr` and requires it to normalize to the selected IP.
8. HTTP Core starts TLS on that stream using the original hostname. Certificate
   verification therefore checks the customer name, not the numeric IP.
9. HTTP/1.1 sends the original URL-derived Host header. Redirects, environment
   proxies, internal retries, HTTP/2, and keepalive reuse are disabled.

DNS failure or network failure is a transient transport outcome. A policy block is a
terminal `target_blocked` outcome.

### G. Finalization updates delivery and circuit evidence together

1. The worker classifies 2xx as success; 408, 425, 429, and 5xx as transient; other
   normal HTTP statuses as permanent.
2. It locks the delivery and endpoint control row and verifies claim/probe fencing.
3. A success marks the delivery succeeded and clears the circuit.
4. A permanent result dead-letters the delivery and clears the circuit because it is
   not a transient outage signal.
5. A target policy block dead-letters as `target_blocked` and clears transient circuit
   state.
6. A transient result increments the consecutive count; the fifth opens the circuit
   at database time.
7. A transient half-open probe reopens immediately and begins a new 30-second
   cooldown.
8. A retry due time is never earlier than an open circuit's cooldown boundary.

### H. Secret rotation is concurrent-safe and preserves accepted work

1. The authenticated caller sends `expected_active_version` to the tenant-scoped
   rotate route.
2. PostgreSQL locks the endpoint and current active secret.
3. A concurrent winner changes the version first; the loser wakes, observes the new
   version, and receives 409 rather than rotating again.
4. The winner retires the old row and inserts a freshly encrypted next version in one
   transaction.
5. The plaintext is returned once with no-cache headers.
6. New events snapshot the replacement secret ID. Existing deliveries and replays of
   those deliveries retain the old ID and remain verifiable with the old receiver
   secret.

This gives a safe storage and concurrency model. Receiver rollout still requires an
operational overlap plan because HookRelay cannot install the new verifier for the
customer.

## 6. Definitions of every new technology and term

| Term | Meaning in this stage |
| --- | --- |
| Server-side request forgery (SSRF) | Abuse of a server's outbound client to make requests chosen by an attacker, often reaching resources the attacker cannot contact directly. |
| DNS rebinding | Changing or mixing hostname answers so validation observes a safe IP but a later connection reaches an unsafe one. |
| Time of check/time of use (TOCTOU) | A security gap where the fact checked can change before the operation uses it. |
| A record | DNS record that maps a hostname to IPv4. |
| AAAA record | DNS record that maps a hostname to IPv6. |
| `getaddrinfo` | Platform resolver interface that returns address-family/socket candidates; HookRelay asks for all TCP IPv4/IPv6 candidates. |
| Global unicast | An address intended to be globally reachable as one destination, rather than private, local, multicast, reserved, or unspecified. |
| Metadata address | A special link-local/provider endpoint that can expose workload credentials or instance data; it must not be reachable through tenant webhook configuration. |
| IPv4-mapped IPv6 | IPv6 syntax in `::ffff:0:0/96` carrying an IPv4 address. HookRelay rejects the mapped form even when the embedded IPv4 looks public. |
| Transition address | Forms such as IPv6 NAT64, Teredo, ORCHIDv2, 6to4, or deprecated IPv4 `192.88.99.0/24` relay anycast whose routing/embedded-address behavior is inappropriate for this webhook policy. |
| Fail closed | Reject when validation is incomplete, ambiguous, mixed, timed out, malformed, or unsafe. |
| IP pinning | Connect the socket to the exact numeric address that just passed policy instead of asking the HTTP stack to resolve the hostname again. |
| Peer verification | Read the connected socket's remote address and require it to equal the selected validated IP. |
| HTTP Host | HTTP/1.1 request field identifying the logical URL authority. It remains the original destination hostname even when TCP is pinned to an IP. |
| TLS SNI | Server Name Indication sent during TLS so a multi-host server selects the right certificate/site. HookRelay uses the original URL hostname. |
| Certificate hostname verification | Checking that the server certificate authenticates the requested hostname. It must not silently switch to the pinned IP. |
| Transport | HTTPX layer that converts a high-level request into lower-level HTTP Core operations. |
| Network backend | HTTP Core interface that opens TCP/Unix streams and starts network I/O. HookRelay customizes TCP connect through this public seam. |
| Redirect | A 3xx response suggesting another URL. Following is disabled because the new URL did not pass the original decision. |
| Proxy | An intermediary that makes the actual connection. Environment proxies are ignored because they would move the network boundary. |
| Fixed window | Count admissions in one interval, reset at its exact end, and defer excess work to the next window. |
| Rate limit | Maximum admitted requests for a resource and interval: 10 per endpoint per one second by default. |
| Circuit breaker | State machine that stops normal calls after repeated transient failures and cautiously tests recovery. |
| Closed circuit | Normal state; requests may proceed subject to rate limiting. |
| Open circuit | Requests are deferred until cooldown because the endpoint recently crossed the transient-failure threshold. |
| Half-open circuit | Recovery state in which exactly one leased probe is allowed and competitors wait. |
| Cooldown | Minimum wait from circuit opening to probe eligibility; 30 seconds by default. |
| Recovery probe | The sole real HTTP attempt allowed to test an endpoint after cooldown. |
| Probe lease | Claim token plus expiry that lets another worker recover if the probing worker dies. |
| Deferral | Durable scheduling decision with no HTTP request, attempt row, or attempt-budget charge. |
| Row lock | PostgreSQL lock from `SELECT ... FOR UPDATE` that serializes writers/lockers for one row until transaction end. |
| Database-authoritative | The persisted value under transaction/locking wins; broker timers and worker memory are hints or caches. |
| Tenant authorization boundary | Rule that authenticated tenant context, not caller-supplied tenant identity, constrains every resource operation. |
| Opaque 404 | Same not-found result for absent and cross-tenant IDs so an attacker cannot enumerate another tenant's objects. |
| Row-level security (RLS) | PostgreSQL policies that can enforce row visibility/modification in the database. Stage 5 does not enable them. |
| ASGI middleware | Low-level async callable around the application that sees `scope`, `receive`, and `send`; used here to count bytes before FastAPI parses. |
| `Content-Length` hint | Claimed body length used for early rejection only. A single all-digit value is compared as bounded decimal text, while actual streamed bytes remain authoritative. |
| 413 Content Too Large | HTTP status returned when raw request or canonical event payload crosses its configured limit. |
| Canonical UTF-8 payload | Deterministic compact JSON bytes with sorted keys and stable Unicode handling, used so semantically equal payloads get the same size decision. |
| Secret rotation | Replace a credential with a fresh version while controlling transition from old to new. |
| Optimistic precondition | Caller supplies the version it observed; a mismatch returns conflict instead of repeating a stale operation. |
| Secret snapshot | Delivery stores the exact signing-secret row ID active at event acceptance, preserving future retry behavior. |
| Retired secret | No longer selected for new deliveries but retained for already accepted snapshots. |
| Master encryption key | AES-GCM key that protects endpoint secret ciphertext at rest. Its rotation is not implemented here. |
| Least privilege | Give a process only the identity, capabilities, writable paths, and resources needed for its work. |
| UID/GID | Numeric Linux user/group identity. Application services use `10001:10001`. |
| Linux capability | Individually separable traditional root privilege. App containers drop all capabilities. |
| `no-new-privileges` | Kernel/runtime rule preventing the process and descendants from gaining additional privilege through execution. |
| Read-only root filesystem | Container image filesystem cannot be modified at runtime; explicitly mounted writable areas remain writable. |
| `tmpfs` | Ephemeral memory-backed filesystem. Compose explicitly gives app temporary files a 16 MiB `/tmp` mount while the image root remains read-only. |
| `noexec`, `nosuid`, `nodev` | Mount flags that disallow direct execution, set-ID semantics, and device files on `/tmp`. |
| PID limit | Bound on processes/threads represented as tasks in the container cgroup; 256 for each app service. |
| Egress firewall | Infrastructure rule restricting outbound network destinations independently of application code. It remains required. |

Primary material for these definitions:

- [OWASP SSRF Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)
- [Python `ipaddress`](https://docs.python.org/3/library/ipaddress.html)
- [IANA IPv4](https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml)
  and [IPv6](https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml)
  special-purpose registries
- [Python asynchronous DNS](https://docs.python.org/3/library/asyncio-eventloop.html#dns)
- [HTTPX transports](https://www.python-httpx.org/advanced/transports/) and
  [environment variables](https://www.python-httpx.org/environment_variables/)
- [HTTP Core network backends](https://www.encode.io/httpcore/network-backends/) and
  [connection pools](https://www.encode.io/httpcore/connection-pools/)
- [PostgreSQL row locks](https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-ROWS)
- [PostgreSQL RLS](https://www.postgresql.org/docs/17/ddl-rowsecurity.html)
- [Starlette pure ASGI middleware](https://www.starlette.io/middleware/#pure-asgi-middleware)
- [RFC 9110 413](https://www.rfc-editor.org/rfc/rfc9110.html#name-413-content-too-large)
- [OWASP Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [RFC 2104 HMAC](https://www.rfc-editor.org/rfc/rfc2104.html)
- [Docker build best practices](https://docs.docker.com/build/building/best-practices/)
- [Docker Compose services](https://docs.docker.com/reference/compose-file/services/)

## 7. Why each technology and design was chosen

### Early URL validation plus authoritative connection-time validation

The API should reject a metadata literal immediately rather than store work that can
never run. That improves feedback and avoids a pointless delivery row. But DNS is
time-varying, so the worker repeats the security decision at the only point that
closes the TOCTOU gap: immediately before opening the socket.

The URL-only check is deliberately synchronous. A delivery containing an obviously
unsafe literal can be dead-lettered under its database lock before an attempt or
traffic slot exists. A hostname needs network I/O and is deferred to the transport
after the short claim transaction.

### Complete A/AAAA validation and fail-closed selection

Validating every answer prevents a resolver from hiding one internal address behind a
public first result. Only after the complete, bounded set passes does the policy select
an address. The first-address selection is simple and deterministic enough for Stage
5; it is not advertised as load balancing.

The standard library `ipaddress` classification expresses the broad rule: only global
unicast is eligible. Explicit cloud metadata and transition networks make security
intent reviewable and protect against registry/runtime differences. In particular,
`192.88.99.0/24` is denied because Python 3.12 can report a member as global while the
IANA special-purpose registry marks the deprecated relay range. IPv4-mapped IPv6 is
always denied so alternate syntax cannot bypass an IPv4 rule.

### A public HTTP Core network backend instead of hostname rewriting

Replacing the URL hostname with a numeric IP at the HTTPX layer would also replace
Host and often TLS certificate identity. The public HTTP Core network-backend seam is
the correct layer: TCP receives the selected IP, while the HTTP/TLS origin remains the
customer hostname. Tests assert the numeric connect target, Host bytes, TLS SNI, and
peer address without patching private attributes.

The explicit `httpcore2>=2.9.1,<3` dependency acknowledges that HookRelay uses that
public interface directly rather than receiving it only as a transitive dependency.

### No redirect, proxy, retry, HTTP/2, or keepalive ambiguity

Each feature can be safe with a more complex policy, but each adds another path by
which the eventual socket can differ from the validated origin. Stage 5 chooses one
request, one freshly resolved connection, and one origin. `trust_env=False` prevents
ambient proxy variables from changing behavior between deployments.

Disabling reuse is a security/simplicity tradeoff, not a universal performance
recommendation. DNS and TLS now occur for every outbound attempt. Later optimization
would require a reviewed connection-to-validation lifetime contract.

### Exact private-host exemptions only in local/test

The Compose receiver is named `receiver` and resolves to a private Docker address.
Without an exception the secure production rule would make the learning environment
unusable. Exact normalized equality makes the exception narrow: allowing `receiver`
does not allow `receiver.attacker.example`, and wildcards are rejected.

Staging and production fail during settings validation if the set is non-empty. This
turns "remember to remove the development bypass" into an executable invariant.

### PostgreSQL for shared endpoint traffic state

The delivery transaction already requires PostgreSQL, and the database already owns
retry due times and leases. One row and `FOR UPDATE` make a decision atomic across all
worker tasks and processes without introducing another state system. Locks are held
only around local database work; the network call occurs after commit.

This choice is deliberately correctness-first. A very hot endpoint may contend on one
row. Metrics in Stage 6 should inform whether a different distributed limiter is
warranted rather than adding Redis preemptively.

### A fixed window before a more elaborate limiter

The state is two fields: window start and admitted count. Its exact boundary is easy
to test with fresh post-lock database time, denied work does not alter the count, and
the next eligible time is deterministic. Refreshing `clock_timestamp()` after a row-
lock wait prevents an old timestamp from extending a window or cooldown. The known
cost is a boundary burst: 10 calls at the end of one window and 10 at the start of the
next can cluster closely.

### A three-state circuit with one leased probe

Five consecutive transient failures indicate that normal retry traffic is unlikely to
help. A 30-second open period gives the receiver breathing room. One half-open probe
answers "has it recovered?" without releasing all queued deliveries.

The probe uses the existing delivery claim token and TTL. If a worker dies, another
worker can replace the probe at expiry. Success, permanent response, and policy block
clear the transient counter because only transient availability failures should keep
the circuit open.

### Deferrals are schedules, not attempts

An attempt row means HookRelay crossed the admission boundary and intended a real HTTP
call. An open circuit or full rate window does neither. Separating those concepts
keeps `delivery_max_attempts` meaningful and preserves audit evidence: five attempts
mean five admitted executions, not five scheduler denials.

### Raw and semantic size limits

The 1 MiB limit protects the HTTP/parser boundary for every route. The 256 KiB event
payload limit protects the durable business object and leaves room for the rest of the
request. They answer different questions, so neither replaces the other.

Actual stream counting defeats false headers. Decimal text comparison also makes a
pathological 5,000-digit numeric length an immediate 413 without invoking Python's
bounded integer parser. Canonical JSON makes payload size depend on the stored meaning
rather than whitespace/key ordering. The settings invariant
`max_event_payload_bytes <= max_request_body_bytes` prevents an impossible policy.

### Pure ASGI middleware

The ASGI `receive` channel is the earliest framework-level place to count actual body
chunks. Doing this before FastAPI dependencies means oversized unauthenticated data
does not perform authentication or Pydantic parsing. Buffer-and-replay keeps existing
route code unchanged and makes the byte contract testable with constructed messages.

### Immutable signing-secret versions and delivery snapshots

Retries should use the credential selected when HookRelay accepted the event. An
immutable secret row ID gives that property naturally. Retiring a row stops new
selection without destroying old work. `expected_active_version` plus row locks makes
the operator action concurrency-safe and understandable after a lost race.

AES-GCM supplies confidentiality and integrity for recoverable secrets; associated
data binds ciphertext to tenant, endpoint, row, secret version, envelope version, and
configured encryption-key version. The plaintext replacement is returned once and is
not placed in ordinary representations or logs.

### Application-container least privilege in both image and Compose

The Dockerfile's `USER` establishes a safe image default. Compose repeats the numeric
identity and adds runtime controls so local orchestration demonstrates the full
baseline. Root-owned application/migration files plus a read-only root prevent the
service account from preparing code that a later operator might execute.

Dropping every capability is appropriate because the app binds unprivileged ports and
does not administer the host. A small `/tmp` grants the specific write need. The PID
limit bounds one form of resource abuse. These controls are layered because no single
setting describes least privilege.

## 8. Serious alternatives and why they were not chosen

### Maintain a production hostname allowlist

An allowlist is strongest when all destinations are known, but HookRelay's product
purpose is to deliver to arbitrary tenant-owned public webhook domains. Maintaining
every customer hostname centrally would become a provisioning system of its own. The
current policy permits global unicast and relies on production egress filtering for a
separate infrastructure layer.

### Block string prefixes such as `10.` or `127.`

String rules miss IPv6, unusual canonical forms, mapped representations, DNS answers,
and special-purpose registries. Parsing to address objects and testing properties is
less ambiguous.

### Resolve once at endpoint registration and store the IP

Permanent pinning breaks ordinary DNS changes, certificate/CDN operations, and address
rotation. More importantly, it treats an old answer as permanently trusted. HookRelay
resolves every connection and pins only for that connection.

### Allow a safe answer from a mixed DNS set

Choosing a public member while ignoring a private member gives attackers answer-order
and rebinding opportunities. Mixed means blocked. Availability loses to safety when
the resolver state is ambiguous.

### Monkey-patch HTTPX internals

Private connection-pool attributes can change without compatibility guarantees and
are difficult to audit. The implementation uses the documented custom transport and
network-backend interfaces and pins compatible major versions.

### Permit redirects with a maximum hop count

A hop count controls loops, not destination safety. Every Location target would need
fresh scheme, hostname, DNS, address, and connection validation. Since webhooks do not
need redirects for the MVP, the safer behavior is to surface the 3xx as a permanent
HTTP result.

### Use environment proxy variables for enterprise compatibility

An ambient proxy becomes the actual peer and can vary by process environment. That
invalidates the direct-peer policy. A future enterprise proxy must be an explicit,
authenticated, separately trusted deployment decision.

### Keep alive safe connections indefinitely

A connection validated earlier may outlive DNS and endpoint-policy changes. Reuse can
be designed safely with bounded lifetimes and explicit invalidation, but Stage 5
chooses revalidation and a new connection per attempt.

### Store rate/circuit state in memory

It is neither fleet-wide nor durable. Process restarts reset protection, and scaling
changes the effective limit.

### Introduce Redis now

Redis scripts can implement richer high-throughput limits, but the system would gain
a new dependency, availability policy, persistence question, and transaction boundary.
The current PostgreSQL row is correct at present scale and exposes contention for
measurement.

### Use a sliding window or token bucket now

They reduce boundary burst and can represent smooth refill, but they require more
fields, arithmetic, and tests. The fixed window is deliberately a first auditable
policy. Section 15 leaves a richer algorithm as learner work.

### Let the circuit breaker own the retry budget

The circuit describes endpoint health shared by deliveries; retry budget describes
one delivery generation. Conflating them would let another delivery's failures
consume or reset this delivery's evidence.

### Create an attempt before traffic admission

That would record work that never crossed the network boundary and let scheduler
pressure dead-letter an otherwise healthy event. Admission must precede attempt
creation.

### Count only `Content-Length`

It is absent for some streams and is attacker-controlled. The header remains useful
for early rejection, but actual chunks decide acceptance.

### Rely on Nginx, a load balancer, or an API gateway for body size

Ingress rejection saves more resources and should be configured, but alternate paths
and drift exist. Application enforcement provides a tested final boundary.

### Measure the whole event request at 256 KiB

That would make endpoint list/string overhead subtract unpredictably from the business
payload contract. HookRelay has a 1 MiB envelope bound and a separate canonical
payload bound.

### Update the active signing secret in place

An old delivery would silently begin using a new receiver credential. Versioned rows
and a foreign-key snapshot keep accepted work stable.

### Automatically delete a retired secret

Outstanding/replayed deliveries may still reference it. Deletion needs a lifecycle
that proves no executable snapshot remains and accounts for backup/audit policy.

### Accept two active secrets in HookRelay

Receivers may temporarily accept both, but HookRelay needs one deterministic secret
for each new delivery. One active database row plus immutable old snapshots makes
selection unambiguous.

### Treat `HOOKRELAY_SECRET_ENCRYPTION_KEY_VERSION` as master-key rotation

The current cipher holds exactly one key and rejects ciphertext with another key
version. Incrementing the number is metadata, not a migration. Safe master-key
rotation requires a key ring, re-encryption, staged deployment, recovery, and tests;
claiming otherwise risks permanent data loss.

### Run the app as root and depend on container isolation

Container root has more power inside its namespace and interacts dangerously with
misconfigured mounts/capabilities. The application has no root requirement.

### Use only a read-only root filesystem

That reduces persistence but does not remove capabilities, privilege escalation, or
process exhaustion. Least privilege is a set of independent controls.

### Apply app settings to PostgreSQL and NATS without testing

Both services need persistent writable data and have upstream-specific runtime
requirements. Their security review belongs to deployment engineering; blindly
adding app flags can corrupt availability without producing a correct policy.

## 9. Failure modes and design tradeoffs

### Outbound policy and transport

| Failure or edge | Implemented behavior | Tradeoff or remaining work |
| --- | --- | --- |
| Unsafe literal URL at endpoint creation | 422 `destination_not_allowed`; no endpoint transaction starts | Safe, sanitized response does not tell an attacker which internal category matched. |
| Unsafe literal in an old delivery | Dead-letter `target_blocked` before attempt admission | Prevents contact and attempt charge; operator must correct/recreate endpoint and replay policy deliberately. |
| DNS returns public + private | Entire destination blocked | Loses availability for a misconfigured multi-answer host rather than guessing. |
| DNS returns no usable A/AAAA, errors, or exceeds deadline | Sanitized resolution/transport failure; admitted attempt is transient | A resolver outage can consume attempts and open the circuit. Production needs reliable controlled DNS. |
| More than 32 distinct answers | Resolution failure | Bounds work; unusually large legitimate answer sets are unsupported by default. |
| Address changes after resolution | TCP uses numeric selected IP and verifies peer | Constrains rebinding for that socket; network routing compromise remains outside the process. |
| IPv4-mapped IPv6, transition address, or deprecated `192.88.99.0/24` relay | Blocked even if an embedded address appears public or Python 3.12 reports global | Conservative policy may reject a legitimate specialized network; registry vectors must be reviewed on runtime upgrades. |
| Receiver returns 3xx | Not followed; status classification makes ordinary 3xx permanent | No SSRF redirect hop; receiver must expose the final webhook URL. |
| `HTTP_PROXY`/`HTTPS_PROXY` is set | Ignored by worker client | Enterprise proxy deployment needs an explicit future design. |
| Public endpoint has multiple safe addresses | First validated answer selected | No connection failover to the next safe answer in the same attempt. |
| TLS hostname/certificate mismatch | Transient HTTP transport error | TCP pinning does not weaken PKI; repeated misconfiguration may exhaust attempts/open circuit. |
| Every attempt performs DNS + TCP + TLS | Expected | Reduced throughput and higher latency buy a simple validation lifetime. Measure before optimizing. |
| Application classifier has a bug | Potential SSRF path | Production egress firewall/network policy must independently block internal and metadata networks. |

The policy protects HTTP calls made through `build_http_client()`. New code that opens
raw sockets or constructs another client can bypass it. Code review and egress policy
must treat the safe client as a mandatory boundary.

### Rate limiter and circuit breaker

| Failure or edge | Implemented behavior | Tradeoff or remaining work |
| --- | --- | --- |
| More than 10 admissions in one endpoint window | Excess deliveries persist retry at exact window end; no attempt row | Fixed window allows a boundary burst of up to roughly 20 calls close together. |
| Many workers race on one endpoint | PostgreSQL row lock serializes decisions | Correct fleet-wide count, but a hot row can become a latency bottleneck. |
| Different endpoints race | Different rows proceed independently | Database pool/CPU can still be shared bottlenecks. |
| Five consecutive transient finalizations | Circuit opens at database time | Concurrent in-flight requests admitted before opening can still complete afterward. |
| Circuit is open | Due work defers to cooldown; no HTTP/attempt/rate charge | A message may wake more than once due to broker timing, but PostgreSQL due state wins. |
| Several workers reach cooldown | One gets probe token; others defer to lease expiry | Row lock and token stop a herd. |
| Probe worker dies | Lease expires; another probe can replace it | Detection waits until claim/probe expiry, 20 seconds by default. |
| Probe returns transient | Circuit reopens for a fresh 30 seconds | Slow recovery favors destination protection over rapid throughput. |
| Success, permanent response, or target block | Circuit closes and transient count resets | Permanent failure dead-letters that delivery but is evidence of reachability, not availability failure. |
| PostgreSQL unavailable | No authoritative admission/finalization | Worker cannot safely substitute local counters. Normal retry/recovery policy applies outside this state machine. |
| Control row missing/inconsistent | Message rejected/runtime error rather than guessed repair | Migration and endpoint transaction must maintain invariant; operator investigation is required. |

Circuit failure count is ordered by finalization transactions, not request start time.
That is the only fleet-wide order available without holding the endpoint lock over
HTTP. It means overlapping results can interleave; correctness is defined by committed
state transitions. Finalization refreshes database time after it acquires the traffic
row, so a lock wait cannot backdate the resulting open/cooldown timestamp.

### Request and payload limits

| Failure or edge | Implemented behavior | Tradeoff or remaining work |
| --- | --- | --- |
| Valid declared length over 1 MiB | Immediate 413 before reading body | The ASGI server/proxy still accepted the connection and headers. |
| One 5,000-digit all-numeric declared length | Leading-zero normalization plus digit/lexical comparison yields immediate sanitized 413 without reading body | HookRelay still relies on the ASGI server/ingress for a total header-size limit. |
| Missing, duplicate, malformed, or false-small length | Stream is counted; byte 1 MiB + 1 gets 413 | Accepted bytes are buffered in application memory. |
| ASGI server emits one already-large body chunk | Middleware rejects it before parsing but only after receiving that allocated message | The application limit cannot prevent server/ingress allocation; configure an upstream limit too. |
| Exactly 1 MiB raw body | Allowed to continue | Schema/media/auth may still reject it later. |
| Canonical event payload >256 KiB | 413 before database ingestion | Raw request was already parsed under the 1 MiB cap. |
| Pretty and compact JSON express same payload | Same canonical payload decision | Raw body decisions can differ only if one crosses the independent 1 MiB bound. |
| Many simultaneous near-limit bodies | Each has its own bound | Aggregate memory is concurrency times body size plus parser copies; ingress concurrency controls remain needed. |
| Slow streaming client | Byte count remains correct | This middleware is not a slowloris timeout. Configure server/ingress timeouts. |
| Compressed input handled upstream | Depends on upstream behavior | Coordinate compressed/decompressed limits; Stage 5 does not implement decompression-ratio policy. |

### Tenant boundaries and secret rotation

| Failure or edge | Implemented behavior | Tradeoff or remaining work |
| --- | --- | --- |
| Caller supplies another tenant's endpoint ID | Tenant-filtered query returns opaque 404 | Application query discipline is the boundary; PostgreSQL RLS is absent. |
| Two rotations expect version 1 | Row locking gives one 200 and one 409 | Loser must read operational state and retry with deliberate intent. |
| Rotation success response is lost | Plaintext cannot be fetched again | Perform another controlled rotation; do not log/store the response casually. |
| Receiver installs only the new secret immediately | Old snapshotted deliveries may fail verification | Receiver should overlap old/new verification until old work is no longer executable. |
| Old secret row is retired | New ingestion ignores it; old snapshots still load it | Retention grows until a safe cleanup policy exists. |
| Master key/version changes without row migration | Old ciphertext decryption fails | Master-key rotation is explicitly not implemented. Keep the existing key available and design a key ring first. |
| Database operator or app-role compromise | Encrypted signing values reduce plaintext exposure | The runtime master key plus ciphertext can recover secrets; use a secrets manager and least privilege. |

### Container boundary

| Failure or edge | Implemented behavior | Tradeoff or remaining work |
| --- | --- | --- |
| App writes into the image root outside an explicit writable mount | Fails on read-only root | Libraries must be configured for bounded `/tmp` or changed deliberately. |
| App needs privileged port/capability | Fails under UID 10001/cap-drop | HookRelay uses ports 8000/9000 and needs no capabilities. |
| Fork/thread/process explosion | Bounded at 256 PIDs/tasks | CPU and memory need separate deployment limits. |
| Write executable to `/tmp` then execute directly | `noexec` mount blocks direct execution | Interpreters can still read data; noexec is one layer, not malware prevention. |
| Process reads environment secrets | Still possible | Move production values to a managed secret delivery mechanism; env vars are not automatically safe. |
| Process reaches Docker-network services | Still possible | Compose app hardening is not egress segmentation. Add firewall/network policy and distinct credentials. |
| Host/Docker daemon compromised | Outside container control | Rootless/runtime/host hardening and patching remain infrastructure responsibilities. |

### Misconceptions to correct

- `async def` does not make DNS or parsing CPU work parallel. The platform
  `getaddrinfo` coroutine uses a thread-pool-backed synchronous resolver.
- A successful address check at endpoint creation does not secure a later socket.
- TLS certificate verification does not stop SSRF by itself; an internal HTTPS service
  may have a valid certificate, and HTTP is intentionally available in local/test.
- A database row lock does not remain held after commit and does not protect the
  remote call; claim tokens fence the later finalization.
- A rate-limit deferral is not a failed delivery attempt.
- An open circuit protects a destination; it does not prove the destination is down.
- A 413 raw-body limit is not a total-process memory limit.
- Encrypted secrets are not harmless. Whoever has ciphertext and the master key can
  decrypt them.
- Incrementing a key-version integer is not cryptographic key rotation.
- Non-root and read-only containers are not virtual machines or egress firewalls.
- Passing unit tests does not prove real DNS, TLS, PostgreSQL concurrency, container
  runtime behavior, or production scale.

## 10. Exact commands for running and testing

These commands use Windows PowerShell from the repository root and publish the local
PostgreSQL port as `55432`. Containers still use service DNS `postgres:5432`.

### Prepare the locked Python environment

```powershell
Set-Location "C:\Users\tarun\OneDrive\Documents\personal project"
Copy-Item .env.example .env

py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --disable-pip-version-check uv==0.12.1
.\.venv\Scripts\uv.exe sync --frozen --all-groups
```

If `.env` already exists, do not overwrite it. It is intentionally ignored because it
can contain credentials. Before starting, replace at least these local examples:

```text
POSTGRES_PASSWORD
HOOKRELAY_SECRET_ENCRYPTION_KEY
HOOKRELAY_BOOTSTRAP_TOKEN
```

Generate a 32-byte URL-safe base64 AES key without printing unrelated environment
state:

```powershell
.\.venv\Scripts\python.exe -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Paste that one value into the ignored `.env`. The committed development key is
rejected in staging/production.

### Build, migrate, and start the hardened local stack

```powershell
$env:POSTGRES_HOST_PORT = "55432"
docker compose config --quiet
docker compose build
docker compose up --detach --wait postgres nats receiver
docker compose run --rm api alembic upgrade head
docker compose up --detach --wait api
docker compose up --detach outbox-publisher worker
docker compose ps
```

Migration `20260805_0004` creates and backfills one traffic-control row for every
existing endpoint and adds `delivery_attempts.is_circuit_probe`. Migrations remain an
explicit release step; `depends_on` does not continuously migrate or repair a running
database.

### Inspect the application-container boundary

```powershell
docker compose exec --no-TTY api id
docker compose exec --no-TTY worker id
docker compose exec --no-TTY api sh -c "test ! -w /app && test -w /tmp"

$applicationContainers = @(
  "hookrelay-api-1",
  "hookrelay-outbox-publisher-1",
  "hookrelay-worker-1",
  "hookrelay-receiver-1"
)
foreach ($container in $applicationContainers) {
  docker inspect --format `
    '{{.Name}} user={{.Config.User}} readonly={{.HostConfig.ReadonlyRootfs}} pids={{.HostConfig.PidsLimit}} caps={{json .HostConfig.CapDrop}} security={{json .HostConfig.SecurityOpt}}' `
    $container
}
```

Expect UID/GID `10001`, `readonly=true`, `pids=256`, `ALL` dropped, and
`no-new-privileges`. Compose-generated container names can differ when the project
name is overridden; use `docker compose ps --format json` to resolve them instead of
guessing.

### Bootstrap a tenant and create the local receiver endpoint

Use the bootstrap token from the ignored `.env`:

```powershell
$bootstrapToken = Read-Host "HOOKRELAY_BOOTSTRAP_TOKEN from .env"
$bootstrapHeaders = @{ Authorization = "Bearer $bootstrapToken" }
$bootstrapBody = @{
  name = "Stage 5 demo"
  initial_api_key_name = "developer"
} | ConvertTo-Json
$bootstrap = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/bootstrap/tenants `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body $bootstrapBody
$apiKey = $bootstrap.api_key.key
$authHeaders = @{ Authorization = "Bearer $apiKey" }

$endpointBody = @{
  name = "Local Stage 5 receiver"
  url = "http://receiver:9000/webhooks"
} | ConvertTo-Json
$endpointResponse = Invoke-WebRequest `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/endpoints `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $endpointBody
$endpoint = $endpointResponse.Content | ConvertFrom-Json
$endpointId = $endpoint.id
$versionOneSecret = $endpoint.signing_secret
$endpointResponse.Headers["Cache-Control"]
```

`$versionOneSecret` is shown for the learning exercise only. In real operations,
transfer it to the receiver's secret store and do not place it in shell history,
source control, tickets, or logs.

### Prove an obvious SSRF target is rejected before storage

```powershell
$blockedBody = @{
  name = "Must not exist"
  url = "http://169.254.169.254/latest/meta-data/"
} | ConvertTo-Json
curl.exe -i `
  -X POST `
  -H "Authorization: Bearer $apiKey" `
  -H "Content-Type: application/json" `
  --data $blockedBody `
  http://127.0.0.1:8000/v1/endpoints
```

Expect 422 with `destination_not_allowed`. Do not add the metadata address to the
local exemption set. The exercise does not send a request to that address: literal
classification rejects it synchronously.

### Submit an event and inspect its delivery

```powershell
$idempotencyKey = "stage5-demo-$([Guid]::NewGuid().ToString('N'))"
$eventBody = @{
  type = "security.demo"
  payload = @{ message = "bounded and signed" }
  endpoint_ids = @($endpointId)
} | ConvertTo-Json -Depth 5
$event = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers ($authHeaders + @{ "Idempotency-Key" = $idempotencyKey }) `
  -ContentType "application/json" `
  -Body $eventBody
$deliveryId = $event.deliveries[0].id

Start-Sleep -Seconds 2
$detail = Invoke-RestMethod `
  -Method Get `
  -Uri "http://127.0.0.1:8000/v1/events/$($event.id)" `
  -Headers $authHeaders
$detail | ConvertTo-Json -Depth 5
Invoke-RestMethod `
  -Method Get `
  -Uri "http://127.0.0.1:9000/requests/$deliveryId"
```

Expect one signed request and eventually `succeeded`. At-least-once semantics still
allow duplicates after ambiguous failures.

### Rotate the endpoint signing secret and prove snapshot behavior

```powershell
$rotateBody = @{ expected_active_version = 1 } | ConvertTo-Json
$rotationResponse = Invoke-WebRequest `
  -Method Post `
  -Uri "http://127.0.0.1:8000/v1/endpoints/$endpointId/signing-secret/rotate" `
  -Headers $authHeaders `
  -ContentType "application/json" `
  -Body $rotateBody
$rotation = $rotationResponse.Content | ConvertFrom-Json
$versionTwoSecret = $rotation.signing_secret
$rotation.version
$rotationResponse.Headers["Cache-Control"]
```

Expect version `2` and `no-store`. A second request still expecting version 1 returns
409 and reveals only current version 2:

```powershell
curl.exe -i `
  -X POST `
  -H "Authorization: Bearer $apiKey" `
  -H "Content-Type: application/json" `
  --data $rotateBody `
  "http://127.0.0.1:8000/v1/endpoints/$endpointId/signing-secret/rotate"
```

Do not confuse this with rotation of `HOOKRELAY_SECRET_ENCRYPTION_KEY`. The master-key
workflow does not exist in Stage 5.

### Inspect shared traffic state without changing it

Enter the values from the ignored `.env`; the password prompt is masked:

```powershell
$postgresUser = Read-Host "POSTGRES_USER from .env"
$securePostgresPassword = Read-Host "POSTGRES_PASSWORD from .env" -AsSecureString
$postgresCredential = [System.Net.NetworkCredential]::new(
  $postgresUser,
  $securePostgresPassword
)
docker compose exec -e PGPASSWORD=$($postgresCredential.Password) --no-TTY postgres `
  psql `
  --username $postgresCredential.UserName `
  --dbname hookrelay `
  --command "SELECT endpoint_id, rate_window_count, circuit_state, circuit_consecutive_failures, probe_token, probe_expires_at FROM endpoint_traffic_controls WHERE endpoint_id = '$endpointId';"
```

The command is read-only. Avoid placing database passwords directly in a command
literal or committed script.

### Run fast quality and contract gates

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
.\.venv\Scripts\mypy.exe
.\.venv\Scripts\pytest.exe -m "not integration"
```

Run only Stage 5 fast tests while learning:

```powershell
.\.venv\Scripts\pytest.exe -q `
  tests/unit/test_stage5_security.py `
  tests/unit/test_stage5_traffic_control.py `
  tests/api/test_stage5_security.py
```

### Run the real-PostgreSQL Stage 5 tests safely

The integration suite requires a **disposable** database. This creates or confirms
only the explicit name `hookrelay_test`; it does not drop it or point tests at the
normal `hookrelay` database.

```powershell
$postgresHostPort = "55432"
$env:POSTGRES_HOST_PORT = $postgresHostPort
docker compose up --detach --wait postgres

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
$databaseExists = docker compose exec -e PGPASSWORD=$($postgresCredential.Password) `
  --no-TTY postgres psql `
  --username $postgresCredential.UserName `
  --dbname postgres `
  --tuples-only `
  --no-align `
  --command "SELECT 1 FROM pg_database WHERE datname = '$testDatabase';"
if ($LASTEXITCODE -ne 0) {
  throw "Could not inspect PostgreSQL databases."
}
if (($databaseExists -join "").Trim() -eq "1") {
  $confirmation = Read-Host `
    "$testDatabase exists; type its exact name only if it contains disposable test data"
  if ($confirmation -cne $testDatabase) {
    throw "Refusing to use an unconfirmed database."
  }
} else {
  docker compose exec -e PGPASSWORD=$($postgresCredential.Password) `
    --no-TTY postgres createdb `
    --username $postgresCredential.UserName `
    --owner $postgresCredential.UserName `
    $testDatabase
  if ($LASTEXITCODE -ne 0) {
    throw "Could not create $testDatabase."
  }
}

$encodedUser = [Uri]::EscapeDataString($postgresCredential.UserName)
$encodedPassword = [Uri]::EscapeDataString($postgresCredential.Password)
$env:HOOKRELAY_TEST_DATABASE_URL = `
  "postgresql+asyncpg://${encodedUser}:${encodedPassword}@127.0.0.1:$postgresHostPort/$testDatabase"
$env:HOOKRELAY_DATABASE_URL = $env:HOOKRELAY_TEST_DATABASE_URL
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\pytest.exe -q `
  tests/integration/test_stage5_security.py `
  tests/integration/test_stage5_traffic_control.py
.\.venv\Scripts\alembic.exe check
```

The tests never need your normal database. Stop if the named test database contains
anything valuable.

### Validate packaging and the runtime image

```powershell
.\.venv\Scripts\uv.exe sync --frozen --all-groups
docker compose config --quiet
docker build --pull --tag hookrelay:stage5 .
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges `
  --pids-limit 256 --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777 `
  --entrypoint python hookrelay:stage5 -c "import os,hookrelay; print(os.getuid(), hookrelay.__version__)"
```

Expect `10001 0.5.0`.

### Stop without deleting durable local data

```powershell
docker compose down
```

Do not add `--volumes` unless you explicitly intend to destroy local PostgreSQL and
JetStream data and have verified the exact Compose project.

## 11. How each test works and what it fails to prove

### Outbound policy and transport unit tests

`tests/unit/test_stage5_security.py` proves, with deterministic fakes:

- ordinary public IPv4/IPv6 are accepted;
- loopback, RFC 1918, shared, link-local, metadata, multicast, unspecified, ULA,
  IPv4-mapped IPv6, transition, deprecated `192.88.99.0/24` relay, and invalid forms
  are rejected;
- the system resolver asks for AF_UNSPEC TCP results, gathers A and AAAA, deduplicates,
  and times out;
- a mixed public/private result fails closed;
- a local/test exception is exact and cannot exist in staging/production;
- private/metadata/mapped literal URLs fail synchronously;
- the network backend connects to the validated numeric IP and verifies its peer;
- Host and TLS SNI come from the original URL, not caller overrides;
- two requests produce two DNS resolutions/connections because keepalive is off;
- DNS errors and HTTP Core network errors become distinct public exceptions with the
  originating HTTP request attached.

These tests do **not** open a real socket, query public DNS, negotiate a real TLS
certificate, prove OS routing, test a corporate proxy, or exercise a cloud metadata
firewall. The fake stream verifies interface composition, not Internet behavior.

### Pure traffic-control unit tests

`tests/unit/test_stage5_traffic_control.py` constructs ORM state objects in memory and
calls pure transition functions at exact timestamps. It proves:

- the fixed window admits its limit, does not charge denial, and resets exactly at the
  boundary;
- one endpoint object's transitions do not mutate another;
- the transient threshold opens the circuit;
- cooldown boundary and active-probe expiry decisions are exact;
- only one half-open probe token is active;
- expired probes can be replaced;
- success/permanent/block outcomes close and clear state;
- transient probe failure reopens with a new cooldown;
- impossible `TrafficAdmission` combinations are rejected.

These tests do not prove two database transactions serialize, a worker creates no
attempt on deferral, or a process crash releases behavior correctly. That needs the
PostgreSQL tests and Stage 4 recovery evidence.

### ASGI/API security tests

`tests/api/test_stage5_security.py` uses the application boundary with controlled
dependencies. It proves:

- exactly 1 MiB is accepted by middleware and one byte more gets the sanitized 413;
- missing or false `Content-Length` cannot bypass streamed-byte counting;
- a declared oversize request is rejected before the body channel is consumed;
- a pathological 5,000-digit numeric declaration is rejected without unbounded
  integer conversion or body consumption;
- exactly 256 KiB canonical payload is eligible and one byte over never calls
  ingestion;
- rotation returns fresh plaintext once, persists ciphertext, retires the predecessor,
  and sends no-store headers;
- a stale expected version returns 409 with only the current version;
- cross-tenant rotation returns opaque 404 and does not mutate state;
- OpenAPI advertises event 413;
- a metadata literal endpoint is rejected before database mutation.

ASGI transport tests do not prove Uvicorn/reverse-proxy buffering, TCP slow clients,
PostgreSQL constraints, or concurrent row-lock behavior.

### Real-PostgreSQL secret tests

`tests/integration/test_stage5_security.py` migrates a disposable database and uses
the public API plus independent database sessions. It proves:

- endpoint creation creates one closed traffic-control row;
- deliveries before and after rotation snapshot different secret versions;
- two concurrent rotations with the same expected version yield exactly one success
  and one conflict;
- a cross-tenant rotation is opaque and leaves secret rows unchanged.

It does not prove a real receiver can deploy overlapping verification safely, that
plaintext never reaches external telemetry, or that an AES master-key rotation works.
The latter is not implemented.

### Real-PostgreSQL traffic tests

`tests/integration/test_stage5_traffic_control.py` creates real tenant/endpoints/events,
runs several executors concurrently, and gates mock HTTP completions. It proves:

- four competing deliveries to a two-slot endpoint produce exactly two HTTP calls and
  two durable deferrals with zero attempt rows;
- a different endpoint remains independent;
- two transient results open shared circuit state;
- after cooldown, four workers create one half-open probe and three deferrals;
- probe success closes and clears the shared breaker;
- a claim blocked past a fixed-window boundary refreshes database time after acquiring
  the control row and admits from the new window;
- a transient finalization blocked on that row records `circuit_opened_at` no earlier
  than lock release, rather than backdating cooldown.

The HTTP transport is mocked so these tests isolate database concurrency. They do not
combine real DNS/TLS, JetStream scheduling, multiple machines, or load-level row-lock
contention.

### Migration, configuration, and regression tests

The existing Alembic suite checks upgrade head and schema behavior. Configuration
tests reject staging/production exemptions, impossible body-limit ordering, and a
circuit cooldown shorter than the claim TTL. Stage 2-4 API, ingestion, delivery,
recovery, and broker tests guard against security work changing durable semantics.

Passing them proves compatibility with tested scenarios. It cannot prove every old
database shape, deployment secret, or downgrade is safe. Always test migrations
against a restored production-like backup.

### Container inspection

Build/config commands prove the image's default user and Compose's declared
read-only/capability/privilege/PID/tmpfs settings. Running `id` and writeability checks
proves the local engine applied the common cases.

They do not prove absence of a kernel/container-runtime vulnerability, correct cloud
network policy, image provenance, vulnerability patch status, or PostgreSQL/NATS
hardening.

### Evidence map

| Claim | Best evidence | Still not proved |
| --- | --- | --- |
| Mixed DNS fails closed | resolver/policy unit test | behavior of every production resolver |
| Socket uses validated IP and original name | fake network-backend Host/SNI/peer test | real Internet routing and PKI deployment |
| Limit is fleet-wide | real PostgreSQL concurrency test | high-scale latency/throughput |
| Deferrals consume no attempts | integration attempt-row assertions | all future admission branches |
| One probe exists | concurrent PostgreSQL gated test | cross-region database topology |
| Raw oversize stops before parsing | ASGI body-channel tests | ingress/server memory outside app |
| Secret snapshots survive rotation | real PostgreSQL before/after rows | customer's overlap procedure |
| Cross-tenant rotation is opaque | API and integration tests | forgotten predicates in future routes |
| App runs least privilege | image/config/runtime inspection | host isolation and egress policy |

## 12. A safe "break it intentionally" exercise

### Goal

Open the circuit against the disposable local receiver, watch later deliveries defer
without attempts, then recover through exactly one probe. This intentionally breaks
only the local demo endpoint. Do not run it against a customer URL or valuable
database.

### Prepare short, observable local settings

In the ignored `.env`, set:

```text
HOOKRELAY_DELIVERY_RATE_LIMIT_REQUESTS=100
HOOKRELAY_DELIVERY_RATE_LIMIT_WINDOW_SECONDS=1
HOOKRELAY_DELIVERY_CIRCUIT_FAILURE_THRESHOLD=2
HOOKRELAY_DELIVERY_CIRCUIT_COOLDOWN_SECONDS=30
```

Keep cooldown at least the 20-second claim TTL. Recreate only processes that consume
these settings:

```powershell
$env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE = "503"
docker compose up --detach --wait --force-recreate receiver
docker compose up --detach --force-recreate worker
```

Use the tenant/endpoint and PostgreSQL credential variables from section 10. Submit
one event and let its first two 503 attempts open the circuit:

```powershell
$openingBody = @{
  type = "circuit.break"
  payload = @{ role = "opens-circuit" }
  endpoint_ids = @($endpointId)
} | ConvertTo-Json -Depth 5
$openingEvent = Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/events `
  -Headers ($authHeaders + @{
    "Idempotency-Key" = "stage5-open-$([Guid]::NewGuid().ToString('N'))"
  }) `
  -ContentType "application/json" `
  -Body $openingBody

$circuitState = ""
for ($poll = 1; $poll -le 30; $poll++) {
  $result = docker compose exec `
    -e PGPASSWORD=$($postgresCredential.Password) `
    --no-TTY postgres psql `
    --username $postgresCredential.UserName `
    --dbname hookrelay `
    --tuples-only `
    --no-align `
    --command "SELECT circuit_state FROM endpoint_traffic_controls WHERE endpoint_id = '$endpointId';"
  $circuitState = (($result -join "").Trim())
  if ($circuitState -eq "open") { break }
  Start-Sleep -Seconds 1
}
if ($circuitState -ne "open") {
  throw "The circuit did not open; inspect worker/receiver logs before continuing."
}
```

Only after observing `open`, submit three new events. They cannot race ahead of the
failure finalizations that opened the breaker:

```powershell
$deferredEvents = 1..3 | ForEach-Object {
  $body = @{
    type = "circuit.defer"
    payload = @{ sequence = $_ }
    endpoint_ids = @($endpointId)
  } | ConvertTo-Json -Depth 5
  Invoke-RestMethod `
    -Method Post `
    -Uri http://127.0.0.1:8000/v1/events `
    -Headers ($authHeaders + @{
      "Idempotency-Key" = "stage5-defer-$([Guid]::NewGuid().ToString('N'))"
    }) `
    -ContentType "application/json" `
    -Body $body
}
$deferredIds = @($deferredEvents | ForEach-Object { $_.deliveries[0].id })
$deferredIdList = ($deferredIds | ForEach-Object { "'$_'" }) -join ","
for ($poll = 1; $poll -le 20; $poll++) {
  $result = docker compose exec `
    -e PGPASSWORD=$($postgresCredential.Password) `
    --no-TTY postgres psql `
    --username $postgresCredential.UserName `
    --dbname hookrelay `
    --tuples-only `
    --no-align `
    --command "SELECT count(*) FROM deliveries WHERE id IN ($deferredIdList) AND status = 'retry_scheduled';"
  if ((($result -join "").Trim()) -eq "3") { break }
  Start-Sleep -Seconds 1
}
```

### Inspect the break

Query the endpoint row and associated deliveries/attempts:

```powershell
docker compose exec -e PGPASSWORD=$($postgresCredential.Password) --no-TTY postgres `
  psql `
  --username $postgresCredential.UserName `
  --dbname hookrelay `
  --command "SELECT circuit_state, circuit_consecutive_failures, circuit_opened_at, probe_token, probe_expires_at FROM endpoint_traffic_controls WHERE endpoint_id = '$endpointId';"

docker compose exec -e PGPASSWORD=$($postgresCredential.Password) --no-TTY postgres `
  psql `
  --username $postgresCredential.UserName `
  --dbname hookrelay `
  --command "SELECT d.id, d.status, d.next_attempt_at, count(a.id) AS attempts FROM deliveries d LEFT JOIN delivery_attempts a ON a.delivery_id = d.id WHERE d.id IN ('$($openingEvent.deliveries[0].id)', '$($deferredEvents[0].deliveries[0].id)', '$($deferredEvents[1].deliveries[0].id)', '$($deferredEvents[2].deliveries[0].id)') GROUP BY d.id, d.status, d.next_attempt_at ORDER BY d.id;"
```

Expect `open` with two consecutive failures. The opening delivery has two attempts;
the three events submitted after the observed open state are `retry_scheduled` with
zero attempts. This sequencing avoids confusing already-admitted in-flight work with
post-open deferrals.

### Restore the receiver and observe one probe

```powershell
$env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE = "204"
docker compose up --detach --wait --force-recreate receiver
```

Wait until after the 30-second cooldown. While a probe is in progress the row may be
`half_open` with one `probe_token`; after its 204 finalizes it becomes `closed`, the
failure count returns to zero, and deferred deliveries can proceed subject to normal
scheduling.

```powershell
Start-Sleep -Seconds 35
docker compose logs --no-color --since 2m worker receiver
docker compose exec -e PGPASSWORD=$($postgresCredential.Password) --no-TTY postgres `
  psql `
  --username $postgresCredential.UserName `
  --dbname hookrelay `
  --command "SELECT circuit_state, circuit_consecutive_failures, probe_token, probe_expires_at FROM endpoint_traffic_controls WHERE endpoint_id = '$endpointId';"
```

### Restore normal local configuration

Remove the temporary four lines from `.env` or restore their documented defaults,
clear the temporary receiver environment variable in this shell, and recreate the
worker/receiver:

```powershell
Remove-Item Env:HOOKRELAY_RECEIVER_RESPONSE_STATUS_CODE -ErrorAction SilentlyContinue
docker compose up --detach --wait --force-recreate receiver
docker compose up --detach --force-recreate worker
```

This does not delete data. Keep the event/attempt evidence for inspection or use a
separate disposable database for a clean rerun.

### Explain what broke

Be able to state all five observations:

1. 503 is transient, so completed requests increment the shared consecutive count.
2. The threshold opens one database row shared by the worker fleet.
3. Circuit denials persist a due time but create no attempt and spend no retry slot.
4. Cooldown permits one token-fenced probe, not all waiting deliveries.
5. Probe success clears endpoint health state; it does not alter Stage 4 at-least-once
   semantics or erase historical attempts.

## 13. Troubleshooting guidance

### Settings fail with `staging and production forbid private delivery host exemptions`

The local defaults include `receiver`, `127.0.0.1`, and `localhost`. Set
`HOOKRELAY_DELIVERY_ALLOWED_HOSTS=[]` for staging/production. Do not replace the list
with `*` or a suffix pattern.

### Settings fail because circuit cooldown is shorter than claim TTL

`HOOKRELAY_DELIVERY_CIRCUIT_COOLDOWN_SECONDS` must be at least
`HOOKRELAY_DELIVERY_CLAIM_TTL_SECONDS`. A probe uses the claim lease; a shorter
cooldown would invite competitors before ownership expires.

### A public endpoint gets `destination_not_allowed`

Check all current A and AAAA answers, not only the first:

```powershell
Resolve-DnsName example.com -Type A
Resolve-DnsName example.com -Type AAAA
```

If any answer is private, loopback, link-local, metadata, mapped, special transition,
or otherwise non-global, the rejection is intentional. Fix DNS. Do not add a
production exemption. Also verify staging/production uses HTTPS and the URL has no
userinfo or fragment.

### Endpoint creation succeeds but a later delivery becomes `target_blocked`

Creation-time DNS is advisory feedback; connection-time DNS is authoritative. The
answer set may have changed or become mixed. Inspect authoritative DNS and deployment
resolver behavior. This is the security design working, not a retry bug.

### DNS failures appear as transient transport errors

The two-second DNS deadline or platform resolver failed. Check resolver reachability,
thread-pool saturation, and whether the host has usable A/AAAA records. Do not turn a
resolution error into a policy exemption.

### TLS fails although the numeric IP is reachable

HookRelay intentionally verifies the original hostname. Ensure the certificate covers
that hostname, SNI routing is configured, and the certificate chain is trusted. Do not
change SNI to the selected IP.

### An endpoint redirect no longer works

Redirect following is disabled. Configure the endpoint to the final HTTPS URL. Adding
`follow_redirects=True` without per-hop validation reopens SSRF.

### A configured proxy is ignored

The worker uses `trust_env=False` and a direct custom transport. This is intentional.
An enterprise proxy needs an explicit design that validates/authenticates the proxy
and defines whether peer pinning happens before or after it.

### Outbound throughput is lower after Stage 5

Every attempt resolves and creates a fresh TCP/TLS connection; keepalive and HTTP/2
are off. Measure DNS, connect, TLS, database lock, and receiver latency separately.
Do not enable reuse until the validation lifetime and policy invalidation design are
documented and tested.

### Deliveries are `retry_scheduled` with no attempt row

That is expected for rate/circuit deferral. Inspect `endpoint_traffic_controls`,
`next_attempt_at`, and worker logs for `rate_limited`, `circuit_open`, or
`circuit_probe_active`. An attempt exists only after admission.

### Rate count seems to reset unexpectedly

The limiter is a fixed window. At `rate_window_started_at + window <= database_now`, a
new window starts with count zero and the admitted call becomes count one. Use
PostgreSQL time rather than workstation time when reasoning about the boundary. The
implementation refreshes that time after acquiring the traffic row so a lock wait
does not make the admission decision use a stale instant.

### More than 10 calls appear close together

A fixed window permits boundary burst: up to 10 late in one window and 10 early in
the next. It is not a sliding one-second guarantee. Consider the unsolved exercise in
section 15 only after understanding the current test contract.

### The circuit opens later than expected

Only committed transient outcomes count. Permanent statuses, successes, and policy
blocks reset transient health. Concurrent requests are ordered by finalization, not
start. Confirm status classification and the committed row.

### The circuit stays half-open

Check the probe delivery claim and `probe_expires_at`. A live probe owns it; competitors
wait. If the worker died, another due delivery can replace the probe only at expiry.
An inconsistent/missing expiry is a database invariant failure, not something to fix
with an in-memory reset.

### Deferrals consume the delivery's maximum attempts

They should not. Query `delivery_attempts` for that delivery. If a row was created for
a denied admission, the implementation violated ADR 0015 and needs a regression test;
do not increase `delivery_max_attempts` to hide it.

### Raw body one byte over does not return 413

Confirm `RequestBodyLimitMiddleware` is installed by `create_app()`, the process loaded
the intended `HOOKRELAY_MAX_REQUEST_BODY_BYTES`, and the request reached HookRelay
rather than a different port. A proxy may return its own 413 shape earlier.

### `Content-Length` says small but request is rejected

Actual streamed bytes are authoritative. A false-small header cannot bypass the
counter. Fix the client framing rather than trusting its declared value.

### A huge all-digit `Content-Length` produces 413 instead of a parser error

That is intentional. HookRelay strips leading zeroes and compares bounded decimal
text by digit count/value; it never calls `int()` on the attacker-sized value. A
single 5,000-digit numeric declaration is obviously above 1 MiB and is rejected
without consuming the body. Ingress should still enforce a total HTTP-header limit.

### A small-looking event payload gets 413

The payload limit counts canonical UTF-8 bytes, not characters or pretty-printed
display. Non-ASCII characters can occupy multiple bytes. Reproduce with the exact
`json.dumps(..., ensure_ascii=False, separators=(",", ":"), sort_keys=True)` contract.

### Rotation returns `409 signing_secret_version_conflict`

Another rotation already changed the active version or the caller used stale state.
Read the `HookRelay-Active-Secret-Version` header, investigate who rotated, and decide
whether a new rotation is intended. Do not blindly loop because every success creates
new secret material.

### Rotation returned 200 but the plaintext was lost

It cannot be fetched again. Rotate once more with the now-current expected version and
handle the new response through an approved secret channel. Database ciphertext is
not a plaintext recovery API.

### Old deliveries fail after endpoint-secret rotation

HookRelay deliberately uses their old snapshotted secret ID. Configure the receiver
to accept the retired version during the overlap. Do not rewrite delivery snapshots or
delete the retired row.

### Existing ciphertext fails after changing the master encryption key

Restore the exact previous key/version from the approved secret store. Stage 5 does
not implement a master-key ring or re-encryption migration. Do not keep trying random
keys or mutate ciphertext rows.

### A cross-tenant resource returns 404 instead of 403

That is intentional opacity. The API must not reveal that another tenant's UUID
exists. Authenticate with the owning tenant when access is legitimate.

### "Tenant isolation" appears in docs but no RLS policies exist

The boundary is enforced by authentication-derived tenant context, tenant-filtered
queries, composite constraints, and tests. PostgreSQL RLS is explicitly future defense
in depth. Do not claim it is enabled.

### An app container cannot write a cache or certificate file

The root filesystem is read-only. Configure the dependency to use bounded `/tmp` if
the data is ephemeral and safe, or design an explicit least-privilege mount. Do not
make the whole root writable as a quick fix.

### `docker compose exec api id` shows a different user

Run `docker compose config`, rebuild, and force-recreate the app service. Confirm both
Dockerfile `USER 10001:10001` and Compose `user`. An old image/container may predate
the Stage 5 settings.

### The app still reaches PostgreSQL/NATS despite `cap_drop: ALL`

Capabilities do not block ordinary outbound sockets, and the Compose network remains
connected. This is expected. Production egress/network segmentation and distinct
service credentials are still required.

## 14. Recruiter questions with strong-answer ingredients

### "What security problem did Stage 5 solve?"

A strong answer includes:

- It reduced risk at several trust boundaries rather than claiming total security.
- Untrusted endpoint URLs now receive connection-time SSRF enforcement.
- Per-endpoint traffic state is shared by the fleet in PostgreSQL.
- Raw requests are bounded before parsing, signing secrets rotate by version, and app
  containers run least privilege.
- Remaining gaps are named: RLS, egress firewall, managed secrets, and master-key
  rotation.

### "How do you prevent DNS rebinding?"

A strong answer includes:

- Resolve every A and AAAA result immediately before connect under a deadline.
- Reject the complete destination if any result is unsafe or invalid.
- Connect the socket to a selected validated numeric IP and verify the peer.
- Preserve original Host, TLS SNI, and certificate hostname.
- Disable redirects, proxy environment variables, retries, and connection reuse that
  could create an unvalidated next hop.
- Say "constrain" rather than "eliminate" and mention egress defense in depth.

### "Why isn't checking the URL during endpoint creation sufficient?"

A strong answer includes:

- DNS is mutable; creation and delivery can be separated by minutes or months.
- A normal client may resolve again after validation.
- Creation-time checking improves feedback; socket-time checking is authoritative.
- Literal URLs are also checked synchronously before attempt reservation.

### "How can TCP connect to an IP while TLS verifies a hostname?"

A strong answer includes:

- TCP routing target and application/TLS origin identity are different layers.
- A custom public HTTP Core backend opens TCP to the validated IP.
- HTTP Core starts TLS on that stream with the original hostname as SNI.
- HTTP Host and certificate validation also use the original URL authority.
- Tests record all three values to prevent accidental coupling.

### "Why reject a hostname if only one of its DNS answers is private?"

A strong answer includes:

- Selecting only a safe member permits answer-order/rebinding ambiguity.
- The application cannot prove which answer a second resolver or alternate connection
  would use unless it owns the connect.
- Fail-closed whole-set validation is a deliberate availability-for-safety tradeoff.

### "Why disable redirects, proxies, and keepalive?"

A strong answer includes:

- Redirects introduce a second unvalidated URL.
- Ambient proxies become a different real peer.
- Indefinite reuse lets a connection outlive the decision that admitted it.
- The MVP favors one validated fresh connection per attempt; throughput cost is
  measured and documented.

### "How does the rate limiter work across several workers?"

A strong answer includes:

- One `(tenant_id, endpoint_id)` control row is locked with `FOR UPDATE`.
- PostgreSQL time defines the fixed one-second window.
- The first 10 admissions increment the shared count; later work defers to window end.
- Different endpoints use different rows.
- No lock is held during HTTP.
- Fixed-window boundary burst and hot-row contention are known tradeoffs.

### "What's the difference between a deferral and an attempt?"

A strong answer includes:

- A deferral is a durable scheduler decision caused by rate/circuit state.
- It writes `retry_scheduled` and `next_attempt_at`, then uses delayed NAK as wake-up.
- It creates no attempt row, sends no HTTP, and spends no max-attempt budget.
- An attempt starts only after traffic admission and represents real execution intent.

### "Describe your circuit breaker's state transitions."

A strong answer includes:

- Closed counts consecutive transient outcomes.
- The fifth transient opens it for 30 seconds.
- Before cooldown, work defers without attempts.
- At cooldown, one claim-token-fenced probe enters half-open; competitors wait.
- Probe success closes/clears; transient probe failure reopens/new cooldown.
- Probe expiry permits crash recovery.
- Permanent/block results do not represent transient endpoint health and clear state.

### "Why put traffic control in PostgreSQL rather than Redis?"

A strong answer includes:

- PostgreSQL already participates in the claim transaction and is the scheduling
  source of truth.
- Row locks give atomic fleet-wide behavior with no new dependency.
- This is a measured-stage choice, not a claim that PostgreSQL is the best limiter at
  all scales.
- Hot-row contention is the signal that could justify Redis/token bucket later.

### "How do request limits work if Content-Length lies?"

A strong answer includes:

- One valid length over 1 MiB enables early rejection.
- The comparison never converts an unbounded decimal string to an integer; even a
  5,000-digit numeric value gets immediate sanitized 413.
- Actual ASGI body chunks are always counted for requests that continue.
- Missing, malformed, duplicate, or false-small length cannot bypass the counter.
- Exact limit passes; first byte over returns sanitized 413 before parsing.
- Accepted bodies are buffered, so aggregate memory and slow clients still require
  ingress/runtime controls.

### "Why is the event payload limit different from the request limit?"

A strong answer includes:

- The 1 MiB raw limit protects transport/parser work for every route.
- The 256 KiB canonical limit protects the durable payload business object.
- Canonical compact sorted-key UTF-8 makes semantic size independent of whitespace and
  key order.
- The payload limit is configured not to exceed the request limit.

### "How does signing-secret rotation avoid breaking retries?"

A strong answer includes:

- Secrets are immutable encrypted version rows with one active row.
- Event ingestion snapshots the active secret ID in each delivery.
- Rotation retires old and inserts new atomically under endpoint/secret row locks.
- Old deliveries keep old; new deliveries select new.
- Receiver must overlap verification, and plaintext new secret is returned once.

### "How do concurrent rotations behave?"

A strong answer includes:

- Caller sends `expected_active_version`.
- `SELECT FOR UPDATE` serializes endpoint and active-secret rows.
- One request retires/inserts/commits; the other observes changed version and gets 409.
- The conflict exposes current version only, not plaintext or cross-tenant existence.

### "Did you implement encryption-key rotation?"

A strong answer is an unambiguous **no**:

- Stage 5 rotates receiver-facing endpoint HMAC secrets.
- Ciphertext records carry an encryption-key version, but the process has one AES key.
- Changing it without re-encryption makes old rows unreadable.
- Safe master-key rotation needs a key ring, staged re-encryption, rollback, and
  availability tests.

### "How is tenant isolation enforced, and what is missing?"

A strong answer includes:

- Bearer-key verification derives tenant context; callers do not submit tenant ID.
- Queries include tenant predicates and cross-tenant IDs become opaque 404.
- Composite database relationships preserve tenant association.
- Tests prove named routes and non-mutation.
- PostgreSQL RLS is not enabled, so application/DB-role compromise or a future missing
  predicate remains a risk.

### "What container hardening did you add?"

A strong answer includes:

- App services run UID/GID 10001 with root-owned code.
- Read-only root, all capabilities dropped, no-new-privileges, PID 256.
- Only 16 MiB `/tmp` tmpfs, with noexec/nosuid/nodev.
- No host/Docker socket mounts and local ports bind loopback.
- PostgreSQL/NATS need separate stateful hardening.
- These controls do not restrict egress or replace host/runtime patching.

### "What would you do next before production?"

A strong answer prioritizes:

1. infrastructure egress deny-by-default including metadata/private ranges;
2. controlled DNS and ingress body/header/connection/time limits;
3. managed secrets and a designed AES master-key rotation/key ring;
4. least-privilege database roles and evaluated PostgreSQL RLS;
5. metrics/traces/alerts for policy blocks, deferrals, circuit state, DNS/TLS, and lock
   latency;
6. load, chaos, penetration, image scanning/signing, and deployment-specific hardening.

## 15. A hands-on modification Tarun completes himself

### Unsolved task: replace the fixed window with a durable token bucket

Do this yourself after you can explain every current transition. This guide
intentionally provides **no implementation or completed patch**.

Replace only the per-endpoint fixed-window limiter with a database-authoritative token
bucket while preserving the circuit breaker and all Stage 4 delivery semantics.

Required behavior:

- default capacity is 10 tokens;
- default refill is 10 tokens per second;
- PostgreSQL `clock_timestamp()` and the locked endpoint row are authoritative;
- admission consumes exactly one token, including a half-open probe;
- a denial computes the earliest exact database time at which one token is available;
- denied work creates no attempt, sends no HTTP, and spends no retry budget;
- different endpoints remain independent;
- capacity bounds idle accumulation;
- concurrency cannot create more successful reservations than available tokens;
- arithmetic must avoid nondeterministic floating-point drift in persisted state;
- circuit-open and active-probe deferrals still run before rate admission;
- worker crash/restart does not reset tokens.

Your deliverables:

1. Write a new ADR that supersedes the rate-limit portion of ADR 0015 and explains
   numeric representation, refill math, clock behavior, and alternatives.
2. Add an Alembic migration that converts existing window state without silently
   granting an unbounded burst. Define and test downgrade behavior.
3. Update settings and `.env.example` with capacity/refill names and invariants.
4. Refactor the pure transition function before integrating it into the worker.
5. Add unit tests for empty/full bucket, partial refill, exact one-token boundary,
   capacity cap, backward time defense, and denial retry time.
6. Add a real-PostgreSQL concurrency test in which 11 workers compete for 10 tokens
   and exactly 10 create attempts.
7. Prove circuit and probe tests still pass unchanged or explain reviewed contract
   changes.
8. Update this stage guide's defaults, diagrams, commands, failure modes, quiz, and
   evidence limits.

Constraints:

- Do not add Redis or an in-memory authoritative counter.
- Do not hold a database lock across HTTP.
- Do not count a denial as an attempt.
- Do not edit an applied migration; add a new revision.
- Do not delete historical attempts or traffic evidence to make a test pass.

Before coding, write down answers to these questions: What unit is stored? How is one
token represented exactly? Which timestamp advances when the bucket is already full?
What happens if database time appears earlier than the stored timestamp? How does a
denied worker calculate `retry_at` without consuming a fraction? Those choices are the
heart of the exercise.

## 16. A comprehension quiz and teach-back checklist

### Quiz: 40 questions with answers

1. **What is the Stage 5 security claim?**

   **Answer:** It adds tested controls at specific tenant, input, outbound, traffic,
   secret, and container boundaries. It does not claim complete production security.

2. **Where does authenticated tenant identity come from?**

   **Answer:** From a verified bearer API key. Public routes do not accept a caller-
   supplied tenant ID.

3. **Does PostgreSQL enforce tenant isolation with RLS in Stage 5?**

   **Answer:** No. Application query predicates, derived tenant context, composite
   relationships, and tests enforce current routes; RLS remains future defense in
   depth.

4. **Why do cross-tenant endpoint operations return 404 rather than 403?**

   **Answer:** To avoid revealing that another tenant's UUID exists.

5. **What is the default raw request-body limit?**

   **Answer:** 1,048,576 bytes, exactly 1 MiB.

6. **When does raw-body counting occur?**

   **Answer:** In pure ASGI middleware before routing, authentication, Pydantic JSON
   parsing, and handler execution.

7. **Why is `Content-Length` not authoritative, and how is a huge numeric value safe?**

   **Answer:** It can be missing, duplicated, malformed, or false, so actual streamed
   ASGI body bytes decide acceptance. One numeric value is compared after leading-zero
   stripping by digit count and equal-length bytes, never unbounded `int()` parsing.

8. **Are exactly 1 MiB and exactly 256 KiB accepted?**

   **Answer:** Yes. Each policy rejects only the first byte over its boundary.

9. **What is the default event payload limit and how is it measured?**

   **Answer:** 262,144 bytes (256 KiB), measured from compact sorted-key UTF-8 JSON of
   the `payload` only, with non-finite numbers disallowed.

10. **Why have both a raw limit and a canonical payload limit?**

    **Answer:** The raw limit protects transport/parser work for all requests; the
    canonical limit bounds the durable semantic event payload.

11. **What makes endpoint URLs an SSRF risk?**

    **Answer:** The tenant chooses where a server inside HookRelay's network connects,
    potentially reaching loopback, private, link-local, metadata, or rebound DNS
    destinations.

12. **Which DNS record families are validated?**

    **Answer:** Every TCP-capable A (IPv4) and AAAA (IPv6) answer returned by
    `getaddrinfo`.

13. **What happens to a mixed public/private DNS answer set?**

    **Answer:** The whole destination fails closed; HookRelay does not choose only the
    public member.

14. **Why are IPv4-mapped IPv6 and explicit deprecated transition ranges rejected?**

    **Answer:** Alternate syntax/routing could bypass simple address reasoning. The
    policy rejects mapped forms regardless of the embedded address and explicitly
    blocks registry exceptions such as `192.88.99.0/24` that Python 3.12 may report as
    global.

15. **What is the default DNS deadline and answer cap?**

    **Answer:** Two seconds and 32 distinct addresses.

16. **Why validate both at endpoint creation and connection time?**

    **Answer:** Creation gives early feedback; connection time is authoritative because
    DNS can change and a later client resolution would create TOCTOU.

17. **What exactly is IP pinning here?**

    **Answer:** After all answers pass, the TCP backend connects to the selected
    numeric IP and verifies the socket peer matches it.

18. **What hostname is used for Host, TLS SNI, and certificate verification?**

    **Answer:** The original normalized URL hostname, never a caller override and not
    the numeric TCP target.

19. **Why are redirects disabled?**

    **Answer:** A redirect target is a new destination that did not pass the original
    validation.

20. **Why is `trust_env=False` used?**

    **Answer:** To stop `HTTP_PROXY`, `HTTPS_PROXY`, and related environment state from
    replacing the validated direct peer with an unreviewed proxy.

21. **Where can private-host exemptions exist?**

    **Answer:** Only local/test, as an immutable exact normalized hostname/IP set.
    Staging/production reject every non-empty set.

22. **Why is connection keepalive disabled?**

    **Answer:** So every new delivery attempt receives a fresh DNS/policy/connect
    boundary instead of reusing a connection that outlived its validation.

23. **How is an unsafe destination different from DNS failure?**

    **Answer:** Unsafe policy is terminal `target_blocked`; DNS/network failure is a
    transient transport outcome eligible for the normal bounded retry policy.

24. **What is the default endpoint rate algorithm and limit?**

    **Answer:** A PostgreSQL-authoritative fixed window with 10 admitted requests per
    one second for each tenant endpoint.

25. **Why is traffic state stored in PostgreSQL?**

    **Answer:** All worker processes need one atomic durable decision, and PostgreSQL
    already owns the delivery claim/schedule transaction. Database time is refreshed
    after the traffic-row lock so waiting cannot stale the decision.

26. **What is the fixed-window boundary-burst tradeoff?**

    **Answer:** Up to 10 calls late in one window and 10 early in the next can occur
    close together even though each individual window is valid.

27. **What does a traffic deferral persist and consume?**

    **Answer:** It persists `retry_scheduled` plus `next_attempt_at`; it consumes no
    HTTP call, attempt row, or retry-budget slot.

28. **When does the default circuit open?**

    **Answer:** On the fifth consecutive committed transient failure for the endpoint.

29. **What is the default cooldown?**

    **Answer:** 30 seconds from the database opening timestamp.

30. **What does half-open mean?**

    **Answer:** One claim-token-leased real request is probing recovery; competing
    deliveries defer until its lease expires or it finalizes.

31. **What happens if the probe worker crashes?**

    **Answer:** The delivery/probe lease expires and a later worker can record/recover
    the abandoned work and lease a replacement probe.

32. **Which outcomes close/reset the circuit?**

    **Answer:** Success, permanent failure, and target block. Only transient failures
    describe availability and increment/open it.

33. **Is a PostgreSQL row lock held while sending HTTP?**

    **Answer:** No. HookRelay commits the short claim/admission transaction, sends
    outside it, then locks/fences again for finalization.

34. **What request rotates an endpoint signing secret?**

    **Answer:** Authenticated `POST /v1/endpoints/{endpoint_id}/signing-secret/rotate`
    with `{"expected_active_version": N}`.

35. **What happens when two rotations expect the same version?**

    **Answer:** Row locks serialize them; one creates the next version and the other
    receives 409 after observing the changed active version.

36. **Why do old deliveries keep an old signing secret after rotation?**

    **Answer:** Ingestion snapshots the exact signing-secret row ID, keeping accepted
    retries deterministic and allowing receiver overlap.

37. **Can the new signing plaintext be fetched again?**

    **Answer:** No. It is returned once with no-store headers. Loss requires another
    deliberate rotation.

38. **Is AES master-key rotation implemented?**

    **Answer:** No. The process has one configured key; changing key/version without
    re-encrypting existing rows makes them unreadable.

39. **List the application-container hardening baseline.**

    **Answer:** UID/GID 10001, read-only root, all capabilities dropped,
    no-new-privileges, PID limit 256, 16 MiB `/tmp` tmpfs with
    noexec/nosuid/nodev, root-owned code, and no host/Docker socket mount.

40. **What two major infrastructure controls remain explicitly required?**

    **Answer:** A production egress firewall/network policy and stronger database
    defense such as least-privilege roles/evaluated RLS; managed secrets and other
    deployment hardening are also pending.

### Three-to-five-minute teach-back

Set a timer and explain Stage 5 without reading. Use this sequence, aiming for about
30-45 seconds per paragraph:

1. **Threat and scope:** Explain why a reliable webhook worker amplifies SSRF and
   destination overload, then name raw input, tenant data, secrets, and containers as
   additional boundaries. State that RLS, master-key rotation, and egress firewalling
   are not implemented.
2. **Outbound path:** Walk from URL normalization through all-answer A/AAAA validation,
   fail-closed classification, numeric TCP connect/peer verification, and original
   Host/SNI/certificate identity. Explain no redirects, proxies, or reuse.
3. **Traffic path:** Describe one PostgreSQL row per endpoint, 10/one-second fixed
   window, five-transient threshold, 30-second cooldown, one leased half-open probe,
   and why deferrals create no attempts.
4. **Input and tenant path:** Describe 1 MiB stream counting before parsing, 256 KiB
   canonical payload after parsing, bearer-derived tenant identity, opaque 404, and
   the absence of RLS.
5. **Secret and runtime path:** Explain expected-version rotation, immutable encrypted
   secret versions/delivery snapshots, the master-key limitation, then UID 10001,
   read-only/cap-drop/no-new-privileges/tmpfs/PID controls and why egress still matters.
6. **Evidence:** Name one unit, API, PostgreSQL concurrency, and container inspection
   test, then say what each cannot prove.

If the explanation runs under three minutes, add the fixed-window boundary burst, DNS
thread-pool deadline limitation, and receiver secret-overlap procedure. If it exceeds
five, remove implementation class names but keep invariants, defaults, and limitations.

### Teach-back checklist

You are ready to move on when you can do all of the following without looking:

- [ ] State the six boundaries Stage 5 changes and at least four things it does not.
- [ ] Explain why creation-time URL validation is helpful but not authoritative.
- [ ] Draw the DNS-to-peer path and distinguish TCP IP from Host/SNI identity.
- [ ] Explain why one unsafe A/AAAA answer blocks the whole hostname.
- [ ] Name the two-second DNS deadline, 32-answer cap, and environment exemption rule.
- [ ] Explain why redirects, proxies, HTTP/2, retries, and keepalive are off.
- [ ] State 10 requests/one second, threshold five, cooldown 30, and probe lease 20.
- [ ] Show why a deferral produces no attempt or retry-budget charge.
- [ ] Trace closed -> open -> half-open -> closed and the transient-probe failure path.
- [ ] Explain why database time and endpoint row locks matter across worker processes.
- [ ] State both byte limits and why actual chunks/canonical bytes are different.
- [ ] Explain how rotation concurrency yields one winner and how snapshots protect old
  deliveries.
- [ ] Say clearly that endpoint-secret rotation is implemented and AES master-key
  rotation is not.
- [ ] Describe API tenant isolation and say clearly that PostgreSQL RLS is not enabled.
- [ ] List every application-container hardening flag and its remaining gaps.
- [ ] Name one thing unit tests, API tests, PostgreSQL tests, and container checks each
  fail to prove.
- [ ] Explain why an egress firewall remains required even with IP pinning.
- [ ] Complete the safe circuit-break exercise and interpret database evidence.
- [ ] Outline your token-bucket design before writing code for section 15.
- [ ] Give the complete three-to-five-minute teach-back to another person and answer
  one follow-up without using "secure" as an unqualified absolute.
