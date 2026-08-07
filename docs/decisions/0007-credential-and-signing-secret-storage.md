# ADR 0007: Hash API-key secrets and encrypt endpoint signing secrets

- Status: Accepted
- Date: 2026-08-02

## Context

Stage 2 introduces three secret roles with different lifecycles:

1. a deployment bootstrap token authorizes creation of an initial tenant;
2. tenant API keys authenticate inbound producer requests;
3. endpoint signing secrets will let a future worker authenticate outbound
   webhook requests to receivers.

Persisting every raw value in plaintext would turn any database read or backup
leak into immediate credential compromise. Irreversibly hashing every value is
also wrong because HookRelay must later recover an endpoint signing secret to
calculate an HMAC.

The public API must make the different recoverability properties visible rather
than implying that “secret” is one storage category.

## Decision

### Bootstrap token

Load the deployment bootstrap token from validated secret process
configuration, compare it in constant time, and keep bootstrap explicitly
disabled unless provisioning requires it. Do not store it in the HookRelay
domain tables. Production deployment must supply it through an external secret
manager and disable the route after use.

### Tenant API keys

Generate tokens as `hrk_<public-id>.<secret>`, where the secret contains 256
random bits. Return the complete token once during bootstrap. Store only:

- the public lookup ID;
- a SHA-256 digest of the random secret;
- a four-character display hint;
- ownership, creation, optional expiry, and revocation metadata.

Authenticate by parsing the bounded token grammar, selecting the public ID,
hashing the presented secret, and comparing digests with
`hmac.compare_digest`. Missing, malformed, unknown, revoked, expired, and
wrong-secret cases share the same public `401` response.

### Endpoint signing secrets

Generate values as `whsec_` plus 256 random bits. Return the raw value once when
the endpoint is created, with `Cache-Control: no-store` and `Pragma: no-cache`.
Do not return it from later endpoint reads.

Encrypt the value using AES-256-GCM under a validated 32-byte process key. Store
an envelope version, unique 96-bit nonce, ciphertext/authentication tag,
encryption-key version, secret version, and four-character hint. Authenticate
tenant ID, endpoint ID, secret row ID, secret version, envelope version, and
key version as AAD so ciphertext substitution across rows fails.

Keep signing-secret versions separate from endpoints. Permit one active
non-retired version per endpoint, and make accepted deliveries reference the
specific version they snapshot.

Reject the documented development encryption key in staging and production.
This configuration safeguard does not replace managed key storage.

## Serious alternatives

### Store raw API keys and signing secrets in plaintext

This is simple and makes recovery easy, but a database query, replica, log, or
backup compromise reveals immediately usable credentials. It is not justified
for the current design.

### Hash every secret

This is correct for inbound API-key verification but would make a future HMAC
signing secret unrecoverable. The worker could not compute the value the
receiver expects.

### Encrypt every secret, including API keys

This permits API-key recovery even though the product does not need it. It
unnecessarily exposes raw inbound credentials to anyone or any code path with
decrypt authority. One-way verification is the smaller privilege.

### Use a password hash for the API-key secret

Argon2id or another slow password KDF is appropriate for human-chosen,
low-entropy passwords. HookRelay generates 256-bit random secrets, so offline
guessing is already infeasible and a fast SHA-256 digest avoids unnecessary
authentication cost. This decision must not be copied to user passwords.

### Store one signing-secret column directly on the endpoint

This simplifies the schema but loses version identity and makes rotation or
historical delivery snapshots ambiguous. Separate secret rows allow a delivery
to retain the exact accepted version.

### Call a remote KMS for every cryptographic operation

Managed cryptographic services can improve key isolation and audit. They also
add network availability, latency, quotas, cost, and provider coupling. The
current MVP uses a process-supplied data key with version metadata; production
may evolve to envelope encryption/KMS without changing the one-time API
contract.

## Consequences

- A database-only leak does not directly reveal raw API-key or signing-secret
  values.
- API keys cannot be recovered or shown again; clients must store the creation
  response securely and require a new key if it is lost.
- The application can decrypt signing secrets and therefore remains in their
  trusted computing base. Encryption is not protection from a fully compromised
  application process.
- Losing an encryption key makes corresponding signing-secret ciphertext
  unrecoverable; backups and disaster recovery must preserve keys separately
  and securely.
- Key version fields make rotation representable, but Stage 2 does not provide
  re-encryption, endpoint secret rotation, API-key creation/revocation, or
  bootstrap lifecycle APIs.
- No-store headers reduce caching but cannot control screenshots, client logs,
  shell history, or downstream storage.
- Bearer credentials still require TLS in transit. Local loopback HTTP is not a
  production transport boundary.
- Error responses and logs must continue omitting raw tokens, signing secrets,
  ciphertext, encryption keys, database URLs, and submitted secret values.
