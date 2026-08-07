# ADR 0021: Paginate tenant delivery history by a deterministic keyset

Status: Accepted

Date: 2026-08-06

## Context

Operators need recent delivery state and attempt history without direct database access. The table
changes concurrently, timestamps can tie, and a cursor supplied by a browser must never become an
authorization mechanism.

## Decision

Every history query derives tenant identity from the authenticated API key and repeats the tenant
predicate. Deliveries are ordered by `(created_at DESC, id DESC)` with matching indexes. Opaque,
versioned base64url cursors contain the last key and a digest of active filters. Attempts retain
lifetime `attempt_number` ordering across replay generations.

Cross-tenant and missing resources return the same sanitized 404. Schemas omit tenant IDs,
payloads, URLs, secret identities, ciphertext, claims, credentials, broker payloads, and exception
text.

## Serious alternatives

- Offset pagination: rejected because concurrent inserts shift pages and deep offsets require
  progressively more scanning.
- Timestamp-only cursors: rejected because equal timestamps create gaps or duplicates.
- Trust a tenant embedded in the cursor: rejected because the authenticated principal is the only
  authorization source.

## Consequences

Pagination is stable for the encoded ordering and filters. Cursors intentionally fail when filters
change. The API exposes current delivery state plus attempt history, not every prior transition.
