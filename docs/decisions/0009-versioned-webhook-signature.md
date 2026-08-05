# ADR 0009: Versioned exact-byte HMAC webhook contract

- Status: Accepted
- Date: 2026-08-03

## Context

A webhook receiver needs to verify that a request came from a party holding the
endpoint secret and that the body was not modified. The protocol must be
unambiguous across languages and serializers. A timestamp should let a receiver
apply a freshness window, while stable event identity supports receiver-side
idempotency.

Signing an abstract parsed JSON object is ambiguous because whitespace, object
member order, Unicode representation, and number formatting can change while
the logical value appears equivalent.

## Decision

Define webhook wire contract version 1.

Serialize one compact, sorted-key UTF-8 JSON object containing:

```json
{
  "created_at": "<original event timestamp in UTC with microseconds>",
  "delivery_id": "<delivery UUID>",
  "id": "<stable event UUID>",
  "payload": {},
  "schema_version": 1,
  "type": "<event type>"
}
```

Reject non-finite numbers. The byte array used for the HMAC must be the byte
array sent as the HTTP body.

At attempt time, calculate integer Unix seconds and sign:

```text
signed_content = ASCII(decimal timestamp) + b"." + exact_body_bytes
signature = "v1=" + lowercase_hex(HMAC-SHA256(endpoint_secret, signed_content))
```

Send:

- `Content-Type: application/json`;
- `User-Agent: HookRelay/<service version>`;
- `HookRelay-Delivery-Id`;
- `HookRelay-Event-Id`;
- `HookRelay-Signature`;
- `HookRelay-Timestamp`;
- `HookRelay-Webhook-Version: 1`.

Use the signing-secret row/version snapshotted on the delivery. Decrypt it only
inside the worker with the existing AES-GCM associated-data contract. Do not
put plaintext secrets into the broker, logs, attempts, or receiver capture.

A receiver should verify against raw request bytes, compare the presented
signature in constant time, reject timestamps outside its documented freshness
window, and atomically deduplicate the stable event ID with its business side
effect.

## Serious alternatives

### Sign only the body

This protects body integrity but does not bind a receiver-checkable attempt
time. It makes straightforward freshness policy harder.

### Sign timestamp and parsed fields separately

This creates cross-language canonicalization rules for every value. Signing one
explicit byte grammar is smaller and has fixed test vectors.

### Place a signature inside the JSON body

The signature would need to exclude itself or require a two-pass special
canonicalization rule. A header keeps authentication metadata outside the
signed representation while the timestamp remains explicitly bound.

### JWT/JWS

Standardized signed envelopes can be appropriate for broader identity and key
distribution requirements, but add encoding, claim, algorithm-negotiation, and
library complexity not required for one shared endpoint secret.

### Asymmetric signatures

They avoid sharing a verification secret with receivers and support public-key
distribution, but require key generation, rotation, discovery, and algorithm
policy. The Stage 2 data model already provisions per-endpoint shared secrets,
so HMAC is the narrower Stage 3 choice.

## Consequences

- Exact deterministic vectors can be tested in any receiver language.
- A one-byte body or timestamp change invalidates the signature.
- HMAC proves integrity and possession of the endpoint secret; it does not
  encrypt payloads or identify a human/operator.
- Timestamp signing enables freshness checks but does not alone prevent replay.
- Stable event ID, not delivery/attempt ID, is the receiver's business
  idempotency key.
- Changing the body grammar or signed-content grammar requires an explicit new
  version rather than silently changing version 1.
- Diagnostic headers added later are not authenticated unless the signed
  grammar explicitly includes them. They must not be trusted for authorization.
- Stage 4 changes retry/dispatch state but preserves webhook body and signature
  version 1. Dispatch generation remains internal PostgreSQL metadata and is not
  silently added to the signed body.
- Secret rotation and overlapping verification windows remain Stage 5 work.
