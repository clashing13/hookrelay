# ADR 0016: Enforce request byte limits before routing and parsing

- Status: Accepted
- Date: 2026-08-05

## Context

Schema validation happens after an HTTP body has crossed the server boundary and is
parsed. Without an earlier bound, an unauthenticated client can make the process read
and parse an arbitrarily large JSON body. `Content-Length` alone is not authoritative:
it can be absent, duplicated, malformed, false, or replaced by streamed/chunked body
messages. A reverse proxy limit is useful but can be missing in local development or
misconfigured independently from the application.

HookRelay also needs a semantic limit on the event `payload`, separate from the whole
request envelope. Equivalent JSON should receive the same payload-size decision
regardless of whitespace or object key order.

## Decision

Install pure ASGI `RequestBodyLimitMiddleware` around the FastAPI application before
routing, authentication dependencies, Pydantic parsing, and handler execution.

- The default whole-request limit is 1,048,576 bytes (1 MiB).
- One well-formed decimal `Content-Length` greater than the limit is rejected
  immediately without reading the body. The comparison strips leading zeroes, then
  compares decimal digit count and equal-length bytes lexicographically against the
  configured maximum. It never converts an attacker-sized decimal string to a Python
  integer.
- The header is only a hint. The middleware always counts actual `http.request` body
  bytes for accepted streams.
- Missing, malformed, duplicate, or falsely small lengths cannot bypass the streamed
  count.
- A pathological all-digit value (the regression test uses 5,000 digits) is bounded
  by the same comparison and receives sanitized 413 without body consumption rather
  than triggering Python's integer-conversion digit limit.
- The exact limit is accepted; the first byte over it returns HTTP 413 with HookRelay's
  sanitized Problem Details media type and `request_body_too_large` code.
- Bounded ASGI messages are buffered and replayed to the downstream application so
  FastAPI sees the original request stream.

After schema parsing, `POST /v1/events` independently canonicalizes only `payload`
using sorted keys, compact separators, UTF-8, no ASCII escaping, and no non-finite
numbers. Its default limit is 262,144 bytes (256 KiB). The payload's exact canonical
limit is accepted; one byte over returns 413 `event_payload_too_large` before
ingestion mutates PostgreSQL.

Configuration requires the semantic payload limit not to exceed the whole-request
limit. Deployment ingress should enforce an equal or smaller transport limit as a
first layer, while the application remains the authoritative backstop.

## Serious alternatives

### Trust `Content-Length`

It enables early rejection but does not describe the bytes actually received in all
valid or adversarial requests. HookRelay treats it as an optimization only.

### Parse JSON and then measure it

That allows a large attacker-controlled body to consume parser memory and CPU before
the decision. The global middleware bounds raw bytes first; only the smaller semantic
payload decision occurs after parsing.

### Depend only on a reverse proxy

Production ingress should have a limit, but tests, local runs, alternate ingress
paths, and configuration drift can bypass it. Defense in depth keeps a bound in the
application.

### Stream accepted JSON directly into the parser

This could reduce buffering, but FastAPI/Pydantic expect the body contract and a
streaming JSON parser would add complexity. Stage 5 buffers at most one configured
request limit and documents that concurrency multiplies the memory budget.

### Measure the original JSON spelling of `payload`

Whitespace and key order would change the result without changing the stored JSONB
value. Canonical UTF-8 bytes provide one deterministic semantic measure.

## Consequences

- Oversized unauthenticated bodies are rejected before route logic and JSON parsing.
- A false or absent size header does not bypass the actual byte counter.
- Each in-flight accepted request can consume up to the configured raw-body limit,
  plus downstream parsing copies; the limit is not a total-process memory quota.
- The 256 KiB payload budget leaves room inside the 1 MiB request budget for type,
  endpoint IDs, headers, JSON syntax, and encoding overhead.
- The semantic limit applies only to event payloads; other JSON routes receive the
  global 1 MiB bound.
- Application limits do not constrain HTTP headers, compressed-body expansion at an
  upstream proxy, connection counts, or request rates. Those need server and ingress
  controls.

## Primary references

- [ASGI HTTP and WebSocket message specification](https://asgi.readthedocs.io/en/latest/specs/www.html)
- [Starlette pure ASGI middleware](https://www.starlette.io/middleware/#pure-asgi-middleware)
- [RFC 9110: 413 Content Too Large](https://www.rfc-editor.org/rfc/rfc9110.html#name-413-content-too-large)
