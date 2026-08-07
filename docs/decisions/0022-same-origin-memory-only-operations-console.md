# ADR 0022: Serve a same-origin console with memory-only credentials

Status: Accepted

Date: 2026-08-06

## Context

Stage 6 needs a visible operations workflow, but adding browser sessions or another privileged
backend would distract from the delivery system and increase security surface. A bearer API key is
already the tenant boundary and must not become a durable browser artifact.

## Decision

Build a small React/TypeScript console at `/console/` and serve it from the API image. Its client
uses only fixed relative `/v1` paths. The tenant API key exists only in React memory and is cleared
on page reload or sign-out. The console lists/filter deliveries, inspects attempts, and performs a
confirmed generation-checked single replay.

Built assets receive a restrictive CSP, `nosniff`, `no-referrer`, frame denial, and permissions
policy. The static mount never masks `/v1` errors.

## Serious alternatives

- Store the key in local or session storage: rejected because the credential outlives the current
  in-memory operator session and is available to later JavaScript.
- Add a backend-for-frontend: rejected because it duplicates the existing tenant boundary and
  requires another privileged deployment.
- Host the console on another origin: rejected because it requires CORS and a configurable target
  that could receive bearer credentials.

## Consequences

The console remains small and deploys as immutable root-owned files in the non-root application
image. Operators must re-enter the key after refresh. It is an educational/local operations tool,
not a production identity system.
