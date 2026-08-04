# ADR 0008: NATS JetStream for durable delivery dispatch

- Status: Accepted
- Date: 2026-08-03

## Context

Stage 2 commits an event, per-endpoint delivery snapshots, and outbox messages
to PostgreSQL. Stage 3 needs a transport that can retain dispatch work across
worker disconnects, confirm server-side publication, expose one shared durable
cursor to multiple workers, and support explicit acknowledgments and bounded
pull-based flow control.

PostgreSQL remains the source of truth. The transport carries only an internal
versioned command identifying the authoritative rows; it is not the event or
secret database.

## Decision

Use NATS JetStream through `nats-py`.

Create or validate one versioned, file-backed work-queue stream:

- name `HOOKRELAY_DELIVERIES_V1`;
- subject `hookrelay.delivery.requested.v1`;
- `WorkQueuePolicy` retention;
- `DiscardNew` when the configured byte limit is full;
- 16 KiB maximum message size;
- one local replica and a persistent Compose volume;
- a finite duplicate window, currently 600 seconds.

Create or validate one shared durable pull consumer:

- name `HOOKRELAY_DELIVERY_WORKERS_V1`;
- exact subject filter;
- deliver-all and instant replay;
- explicit acknowledgments;
- bounded `MaxAckPending`;
- unlimited Stage 3 `MaxDeliver`.

Publish the existing strict ID-only outbox envelope. Set `Nats-Msg-Id` to the
outbox UUID and wait for a PubAck from the expected stream before conditionally
setting PostgreSQL `published_at`.

Workers fetch no more than one local concurrency window and acknowledge with
`ack_sync` only after PostgreSQL commits successful attempt and delivery state.
Publisher and worker connections drain on cooperative shutdown with a bounded
default drain timeout of five seconds.

## Serious alternatives

### Kafka

Kafka is a strong fit for high-throughput retained partitioned logs, replayable
data platforms, and large consumer ecosystems. HookRelay currently needs a
small durable work queue, not an indefinitely retained event log. Kafka would
introduce partition assignment, consumer rebalancing, broker/controller
operations, and a larger local footprint before measurement justifies them.

### Redis Streams

Redis Streams support consumer groups and pending entries, but would require a
new Redis durability/replication/trimming/claim operating model solely for this
transport. JetStream directly supplies the selected publish-ack, durable pull,
and explicit-ack semantics.

### Celery

Celery provides mature task routing, scheduling, and retry abstractions. Those
features would hide the broker acknowledgment, attempt state machine, and retry
boundaries this learning project needs to implement and explain explicitly. It
would also add a framework-specific execution model around the existing async
Python service.

### PostgreSQL delivery-table polling only

Workers could skip a broker and poll pending delivery rows. PostgreSQL queues
can be valid designs, but HookRelay already separates immutable dispatch intent
through an outbox and wants independent worker flow control. Direct polling
would couple queue mechanics to mutable delivery state and remove the intended
broker-learning boundary.

### Core NATS

Core NATS is intentionally transient and does not provide the required durable
stream, consumer cursor, or PubAck semantics.

## Consequences

- Local setup remains compact while exercising real durable messaging.
- Pull delivery and explicit acknowledgments expose backpressure and ACK
  ordering directly.
- ID-only messages avoid copying event bodies, URLs, or secrets into NATS, at
  the cost of a PostgreSQL load for every attempt.
- Durable-asset drift fails startup instead of being silently mutated.
- `Nats-Msg-Id` can reduce duplicate publication only inside its finite window.
- A PubAck and PostgreSQL finalization are not one transaction; duplicate
  publication remains possible.
- `AckWait` redelivery is not the Stage 4 retry policy.
- Local single-server, single-replica file storage is reproducible persistence,
  not HA, quorum durability, backup, or a production deployment recommendation.
- Stage 7 measurements may justify revisiting broker/topology decisions.

## References

- [NATS JetStream streams](https://docs.nats.io/nats-concepts/jetstream/streams)
- [NATS JetStream consumers](https://docs.nats.io/nats-concepts/jetstream/consumers)
- [nats.py](https://github.com/nats-io/nats.py)
