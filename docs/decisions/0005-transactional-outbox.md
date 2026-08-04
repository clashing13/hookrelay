# ADR 0005: Transactional outbox for durable dispatch intent

- Status: Accepted
- Date: 2026-08-02

## Context

Accepting an event eventually requires state in two independent systems:
PostgreSQL must retain the event/delivery state, and a message transport must
make each delivery visible to workers. A normal database commit and a broker
publish cannot participate in one local PostgreSQL transaction.

If HookRelay commits the event first and crashes before publishing, it has
acknowledged work that no worker can discover. If it publishes first, a worker
can observe a delivery whose database transaction later rolls back. Retrying a
partially failed pair can also create duplicate messages.

Stage 2 needs to establish a durable handoff even though the broker and worker
do not arrive until Stage 3.

## Decision

Write dispatch intent to an `outbox_messages` table in the **same PostgreSQL
transaction** as the accepted event and its delivery rows.

For each selected endpoint, event ingestion creates:

1. one immutable event shared by all targets;
2. one pending delivery that snapshots the endpoint URL and signing-secret
   version;
3. one unpublished outbox message with topic `delivery.requested`.

The outbox payload is versioned and ID-only. It contains message, tenant,
event, endpoint, and delivery identifiers, but no endpoint signing secret or
event payload. A unique `(delivery_id, topic)` constraint prevents a second
dispatch fact for the same current topic. A partial index ordered by creation
time and ID supports future scans of rows where `published_at IS NULL`.

The API returns `201` only after this transaction commits. A failure in any row
creation rolls back all of it.

Stage 2 deliberately stops there. It does not publish the outbox, connect to
NATS, send an HTTP request, update `published_at`, or create a delivery attempt.

## Stage 3 implementation

Migration `20260803_0002` adds an expiring claim token and expiry to each
outbox row. Publishers select a bounded ordered batch of eligible rows with
`FOR UPDATE SKIP LOCKED`, validate the persisted ID-only contract, write the
claim, and commit before NATS I/O. This avoids holding a database lock and
connection while an independent broker responds.

The publisher sends the outbox UUID as `Nats-Msg-Id`, waits for a JetStream
PubAck from the expected stream, and conditionally sets `published_at` only
while its claim token still matches. A handled failure releases remaining
owned claims; a process crash leaves them to expire.

This implementation closes no cross-system atomicity gap: if JetStream stores
the message but PubAck/finalization is lost, the same row can publish again.
Finite-window broker deduplication reduces quick duplicates but does not change
the at-least-once guarantee.

The claim TTL must be strictly greater than
`outbox_batch_size * nats_publish_timeout_seconds`, the maximum aggregate
configured broker-publish wait budget. Database finalization and loop overhead
are additional, so deployments should preserve operating margin. Cooperative
shutdown checks between items and releases the unprocessed remainder; a hard
crash still relies on TTL expiry.

## Serious alternatives

### Publish after committing the event

This is simple during normal operation but has an unrecoverable crash window
between commit and publish unless another durable scanner exists. Adding such a
scanner is the transactional-outbox design under another name.

### Publish before committing the event

This prevents the event-without-message ordering but exposes uncommitted or
rolled-back work to consumers. Holding a database transaction open while
waiting for a broker also increases lock/connection pressure without making the
two systems atomic.

### Distributed two-phase commit

Coordinated transactions add availability, operational, and protocol costs,
and the planned broker/HTTP receiver boundaries do not form one practical XA
transaction. They would not solve the later ambiguity of a receiver committing
a side effect before its HTTP acknowledgment is lost.

### Poll pending deliveries directly

A worker could treat the delivery table as a queue. This removes one table but
couples scheduling concerns to mutable domain state, makes publication/audit
intent less explicit, and complicates future fan-out to a broker. PostgreSQL
can be a queue in some systems; HookRelay chooses a separate append-oriented
handoff record so domain state remains the source of truth and transport work
has its own identity.

### One outbox message per event

An event can target many endpoints whose execution, retry, and terminal state
are independent. Per-event messages would require another fan-out step and make
per-destination scheduling less direct. One message per delivery matches the
  delivery worker's unit of work.

## Consequences

- Accepted domain work and durable publish intent are atomic inside
  PostgreSQL.
- Rollback and concurrent-idempotency tests can inspect exact cardinality: one
  event, N deliveries, N outbox rows, and zero attempts.
- PostgreSQL becomes both source of truth and the initial durable handoff, so
  outbox retention, cleanup, indexing, and publisher lag need operational
  monitoring later.
- Publication will still be at least once. A publisher can succeed at NATS and
  crash before recording `published_at`, causing a duplicate publish on retry.
- Claim leases keep NATS latency outside the database transaction but introduce
  TTL-delayed recovery after a hard publisher crash.
- Consumers must be idempotent by stable message/delivery identity.
- The outbox does not guarantee global ordering across tenants or destinations;
  any ordering contract must be designed and tested explicitly.
- ID-only messages reduce broker data exposure but require the worker to load
  authoritative state from PostgreSQL.
- No event-creation response may imply that an unpublished row was attempted or
  delivered; current state is observed separately.
