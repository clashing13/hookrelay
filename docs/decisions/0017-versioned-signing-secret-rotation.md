# ADR 0017: Rotate endpoint signing secrets by version and snapshot

- Status: Accepted
- Date: 2026-08-05

## Context

Receivers use an endpoint signing secret to verify HookRelay's HMAC delivery
signatures. Replacing a secret in place can invalidate already accepted deliveries,
make retry behavior depend on rotation timing, and erase which credential signed an
historical request. Two concurrent operator requests can also rotate twice unless the
operation has an explicit precondition.

The secret must be recoverable by workers but must not be stored in plaintext or
returned by ordinary endpoint reads. A receiver needs the replacement plaintext once
so it can update its verifier.

This decision concerns endpoint HMAC signing secrets. HookRelay's AES-GCM master
encryption-key rotation is a different lifecycle and is not implemented in Stage 5.

## Decision

Expose authenticated, tenant-scoped:

```text
POST /v1/endpoints/{endpoint_id}/signing-secret/rotate
```

Require:

```json
{"expected_active_version": 1}
```

The route derives the tenant from the verified bearer API key, locks the matching
tenant endpoint and its active secret with `SELECT ... FOR UPDATE`, and compares the
expected version. A missing or cross-tenant endpoint returns the same opaque 404. A
stale version returns `409 signing_secret_version_conflict` and reveals only the
current numeric version in `HookRelay-Active-Secret-Version`.

On success, one transaction:

1. obtains PostgreSQL wall-clock time;
2. marks the old secret's `retired_at`;
3. generates a fresh `whsec_` secret from 32 random bytes;
4. inserts the next integer version encrypted with AES-256-GCM and identity-bound
   associated data;
5. commits before returning the plaintext replacement once.

The response uses `Cache-Control: no-store` and `Pragma: no-cache`. Ordinary endpoint
responses never include a secret. Database constraints permit one active secret per
endpoint and one row per `(endpoint_id, version)`.

Event ingestion snapshots the active `signing_secret_id` into each new delivery.
Deliveries accepted before rotation continue to decrypt and sign with their old
version; deliveries accepted afterward use the replacement. Retired rows therefore
remain available for historical delivery and replay rather than being deleted.

## Serious alternatives

### Update ciphertext in the existing row

This changes the meaning of an identifier already referenced by deliveries and makes
historical retries use a different credential. Immutable version rows preserve the
accepted-delivery contract.

### Make every outstanding delivery switch immediately

That may strand receivers that have not deployed the replacement and makes retries
nondeterministic. Snapshotting preserves which secret an accepted delivery uses.

### Keep two active rows without an explicit version precondition

It complicates ingestion selection and allows concurrent rotations to create
surprising extra versions. One active row plus an expected-version precondition gives
one winner under row locking.

### Delete the retired secret

Old delivery snapshots would become undecryptable. Retention is required while those
deliveries can still run or replay; a later retention policy must account for that
reference graph.

### Rotate the endpoint secret and AES master key together

They solve different problems and have different rollout requirements. Stage 5
rotates receiver-facing HMAC secrets only. Master-key rotation needs a key ring,
reencryption procedure, rollback plan, and availability testing before it is safe.

## Consequences

- Concurrent rotations with the same expected version serialize; one succeeds and
  one receives 409.
- Cross-tenant rotation is opaque and non-mutating at the API boundary.
- New plaintext appears only in the success response and process memory; PostgreSQL
  stores ciphertext, a four-character hint, version metadata, and retirement time.
- Receivers need an operational overlap plan: keep accepting the old secret for
  previously accepted deliveries while installing the new secret for new ones.
- HookRelay does not expose retired plaintext again. Losing the one-time response
  requires another controlled rotation.
- Retired secrets remain sensitive ciphertext and need retention, backup, and access
  controls.
- Stage 5 does not provide scheduled rotation, receiver acknowledgement, revocation
  of an old version while referenced, audit-event export, or master-key rotation.
- Changing `HOOKRELAY_SECRET_ENCRYPTION_KEY` or its version without re-encrypting rows
  makes existing ciphertext unavailable; that is not a supported rotation procedure.

## Primary references

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [OWASP Cryptographic Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cryptographic_Storage_Cheat_Sheet.html)
- [RFC 2104: HMAC](https://www.rfc-editor.org/rfc/rfc2104.html)
- [`AESGCM` authenticated encryption](https://cryptography.io/en/latest/hazmat/primitives/aead/#cryptography.hazmat.primitives.ciphers.aead.AESGCM)
- [PostgreSQL row-level locking](https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-ROWS)
