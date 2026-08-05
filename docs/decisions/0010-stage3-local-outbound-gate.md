# ADR 0010: Restrict Stage 3 outbound delivery to local/test targets

- Status: Accepted; temporary until Stage 5
- Date: 2026-08-03

## Context

Stage 3 is the first stage that performs server-side HTTP to a tenant-configured
URL. That activates SSRF risk. URL syntax, an HTTPS scheme, or a hostname string
check cannot prevent requests to loopback, private, link-local, metadata, or
rebinding targets after DNS resolution.

The roadmap assigns complete SSRF, egress, rate, and circuit controls to Stage
5, but the happy-path pipeline needs a safe, reproducible receiver now.

## Decision

Fail closed outside the controlled Stage 3 environment:

- the worker and delivery executor run only when
  `HOOKRELAY_ENVIRONMENT` is `local` or `test`;
- every target URL hostname must match an explicit non-wildcard
  `HOOKRELAY_DELIVERY_ALLOWED_HOSTS` entry after lowercase/trailing-dot
  normalization;
- local defaults permit only `receiver`, `127.0.0.1`, and `localhost`;
- the async HTTP client does not follow redirects;
- environment proxy variables are ignored;
- the local receiver is a separate configurable process with bounded capture;
- staging/production worker startup is rejected until the Stage 5 design is
  implemented and reviewed.

Distinguish data corruption from policy:

- malformed broker envelopes or identities that contradict PostgreSQL are
  poison and may be terminated;
- a valid durable delivery whose destination is blocked by current policy is
  not poison. Leave it unacknowledged/recoverable so a later reviewed policy or
  configuration change can process it.

## Stage 4 evolution

Stage 4 replaces repeated unacknowledged policy redelivery with an explicit
recoverable terminal decision. The executor checks the allowlist while holding
the delivery row lock, records `dead_lettered_at` and reason `target_blocked`,
and commits before the worker ACKs. It creates no HTTP attempt. After a reviewed
allowlist/policy correction, the tenant-scoped manual replay operation starts a
fresh dispatch generation and outbox identity.

The worker keeps a fixed delayed-NAK fallback only if a target-block exception
escapes before the executor can persist terminal state. That fallback preserves
work without returning to raw `AckWait` churn.

## Serious alternatives

### Enable arbitrary URLs and document the risk

Documentation does not contain an outbound capability. A local developer could
still reach privileged services or accidentally contact a real customer URL.

### Treat `HttpUrl` or HTTPS as complete SSRF protection

Syntax/scheme validation says nothing about resolved addresses, rebinding,
redirect targets, proxy routes, or network egress. It would create a false
security claim.

### Implement full Stage 5 security now

A correct resolver/connection binding strategy, special-range policy, redirect
policy, egress rules, rate limits, and circuit breaker deserve their own stage
and adversarial tests. Pulling them into Stage 3 would obscure the delivery
state-machine learning goal and invite a partial implementation to be called
complete.

### ACK policy-blocked messages without durable terminal state

That would discard valid accepted work because of a temporary execution policy.
Stage 4 ACKs only after storing an inspectable `target_blocked` state that can
be manually replayed, keeping policy separate from message corruption.

## Consequences

- The API/ingestion model may store wider endpoint URLs, but Stage 3 workers
  execute only controlled local/test targets.
- The Compose service name `receiver` exercises real DNS and HTTP without
  contacting a third party.
- Redirect and environment-proxy paths do not bypass the simple hostname gate.
- Stage 3 policy-blocked work remained pending/unacknowledged. Stage 4 records
  `target_blocked`, ACKs after that commit, and recovers only through deliberate
  replay after a policy fix.
- The allowlist is not a production SSRF solution. It does not resolve or pin
  addresses, classify IPv4/IPv6 ranges, detect rebinding, or enforce egress.
- Stage 5 must replace/supersede this ADR before staging or production workers
  are enabled.
