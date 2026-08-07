"""NATS JetStream topology, publication, and durable pull-consumer helpers."""

import json
from dataclasses import dataclass
from typing import Literal, Protocol, Self
from uuid import UUID

import nats
from nats.aio.client import Client as NatsClient
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    DiscardPolicy,
    PubAck,
    ReplayPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.client import JetStreamContext
from nats.js.errors import APIError, NotFoundError
from pydantic import BaseModel, ConfigDict

from hookrelay.config import Settings

OUTBOX_SCHEMA_VERSION = 1
OUTBOX_TOPIC = "delivery.requested"
NATS_MESSAGE_ID_HEADER = "Nats-Msg-Id"


class BrokerTopologyError(RuntimeError):
    """The durable broker asset exists with an incompatible contract."""


class DeliveryRequestedMessage(BaseModel):
    """Strict versioned ID-only command persisted in the outbox and JetStream."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["delivery.requested"]
    schema_version: Literal[1]
    message_id: UUID
    tenant_id: UUID
    event_id: UUID
    endpoint_id: UUID
    delivery_id: UUID


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    """The server-confirmed stream position for one outbox publication."""

    stream: str
    sequence: int
    duplicate: bool


class OutboxPublisher(Protocol):
    """Small publication seam used by the database publisher and its tests."""

    async def publish(self, message: DeliveryRequestedMessage) -> PublishReceipt:
        """Wait until JetStream confirms that the message is stored."""


def encode_delivery_message(message: DeliveryRequestedMessage) -> bytes:
    """Create deterministic bytes without adding event bodies or credentials."""

    return json.dumps(
        message.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def decode_delivery_message(data: bytes) -> DeliveryRequestedMessage:
    """Validate the complete broker contract before using any referenced IDs."""

    return DeliveryRequestedMessage.model_validate_json(data)


class JetStreamBroker:
    """Own one NATS connection and reconcile HookRelay's durable broker assets."""

    def __init__(self, settings: Settings, client_name: str) -> None:
        self._settings = settings
        self._client_name = client_name
        self._client: NatsClient | None = None
        self._jetstream: JetStreamContext | None = None

    @property
    def jetstream(self) -> JetStreamContext:
        """Return the connected context without exposing connection credentials."""

        if self._jetstream is None:
            msg = "JetStream broker is not connected"
            raise RuntimeError(msg)
        return self._jetstream

    async def connect(self) -> Self:
        """Connect and fail if the durable stream or consumer has configuration drift."""

        if self._client is not None:
            return self
        self._client = await nats.connect(
            servers=[self._settings.nats_server_url()],
            name=self._client_name,
            connect_timeout=self._settings.nats_connect_timeout_seconds,
            drain_timeout=self._settings.nats_drain_timeout_seconds,
            reconnect_time_wait=1,
            max_reconnect_attempts=-1,
        )
        self._jetstream = self._client.jetstream(
            timeout=self._settings.nats_publish_timeout_seconds
        )
        await self.ensure_topology()
        return self

    def desired_stream_config(self) -> StreamConfig:
        """Define one file-backed work queue that refuses new data when its limit is full."""

        return StreamConfig(
            name=self._settings.nats_stream_name,
            description="HookRelay durable delivery requests",
            subjects=[self._settings.nats_subject],
            retention=RetentionPolicy.WORK_QUEUE,
            max_consumers=1,
            max_msgs=-1,
            max_bytes=self._settings.nats_stream_max_bytes,
            max_age=0,
            discard=DiscardPolicy.NEW,
            discard_new_per_subject=False,
            max_msgs_per_subject=-1,
            max_msg_size=16_384,
            storage=StorageType.FILE,
            num_replicas=1,
            no_ack=False,
            duplicate_window=self._settings.nats_duplicate_window_seconds,
        )

    def desired_consumer_config(self) -> ConsumerConfig:
        """Define one shared durable pull cursor with explicit acknowledgements."""

        return ConsumerConfig(
            name=self._settings.nats_consumer_name,
            durable_name=self._settings.nats_consumer_name,
            description="HookRelay bounded delivery workers",
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=self._settings.nats_ack_wait_seconds,
            max_deliver=-1,
            filter_subject=self._settings.nats_subject,
            replay_policy=ReplayPolicy.INSTANT,
            max_waiting=512,
            max_ack_pending=self._settings.nats_max_ack_pending,
            num_replicas=1,
        )

    async def ensure_topology(self) -> None:
        """Create assets once and reject silent drift on every later process start."""

        desired_stream = self.desired_stream_config()
        try:
            stream_info = await self.jetstream.stream_info(self._settings.nats_stream_name)
        except NotFoundError:
            try:
                stream_info = await self.jetstream.add_stream(config=desired_stream)
            except APIError:
                stream_info = await self.jetstream.stream_info(self._settings.nats_stream_name)
        self._validate_stream_config(stream_info.config, desired_stream)

        desired_consumer = self.desired_consumer_config()
        try:
            consumer_info = await self.jetstream.consumer_info(
                self._settings.nats_stream_name,
                self._settings.nats_consumer_name,
            )
        except NotFoundError:
            try:
                consumer_info = await self.jetstream.add_consumer(
                    self._settings.nats_stream_name,
                    config=desired_consumer,
                )
            except APIError:
                consumer_info = await self.jetstream.consumer_info(
                    self._settings.nats_stream_name,
                    self._settings.nats_consumer_name,
                )
        self._validate_consumer_config(consumer_info.config, desired_consumer)

    def _validate_stream_config(self, actual: StreamConfig, desired: StreamConfig) -> None:
        checks = {
            "subjects": actual.subjects == desired.subjects,
            "retention": actual.retention == desired.retention,
            "max_consumers": actual.max_consumers == desired.max_consumers,
            "max_msgs": actual.max_msgs == desired.max_msgs,
            "max_bytes": actual.max_bytes == desired.max_bytes,
            "max_age": actual.max_age == desired.max_age,
            "discard": actual.discard == desired.discard,
            "discard_new_per_subject": (
                actual.discard_new_per_subject == desired.discard_new_per_subject
            ),
            "max_msgs_per_subject": (actual.max_msgs_per_subject == desired.max_msgs_per_subject),
            "max_msg_size": actual.max_msg_size == desired.max_msg_size,
            "storage": actual.storage == desired.storage,
            "num_replicas": actual.num_replicas == desired.num_replicas,
            "no_ack": actual.no_ack == desired.no_ack,
            "duplicate_window": actual.duplicate_window == desired.duplicate_window,
        }
        drift = sorted(name for name, matches in checks.items() if not matches)
        if drift:
            msg = f"NATS stream configuration drift: {', '.join(drift)}"
            raise BrokerTopologyError(msg)

    def _validate_consumer_config(self, actual: ConsumerConfig, desired: ConsumerConfig) -> None:
        checks = {
            "durable_name": actual.durable_name == desired.durable_name,
            "deliver_policy": actual.deliver_policy == desired.deliver_policy,
            "ack_policy": actual.ack_policy == desired.ack_policy,
            "ack_wait": actual.ack_wait == desired.ack_wait,
            "max_deliver": actual.max_deliver == desired.max_deliver,
            "filter_subject": actual.filter_subject == desired.filter_subject,
            "replay_policy": actual.replay_policy == desired.replay_policy,
            "max_waiting": actual.max_waiting == desired.max_waiting,
            "max_ack_pending": actual.max_ack_pending == desired.max_ack_pending,
            "num_replicas": actual.num_replicas == desired.num_replicas,
        }
        drift = sorted(name for name, matches in checks.items() if not matches)
        if drift:
            msg = f"NATS consumer configuration drift: {', '.join(drift)}"
            raise BrokerTopologyError(msg)

    async def publish(self, message: DeliveryRequestedMessage) -> PublishReceipt:
        """Publish ID-only bytes and wait for JetStream's persistence acknowledgement."""

        acknowledgement: PubAck = await self.jetstream.publish(
            self._settings.nats_subject,
            encode_delivery_message(message),
            timeout=self._settings.nats_publish_timeout_seconds,
            stream=self._settings.nats_stream_name,
            headers={
                NATS_MESSAGE_ID_HEADER: str(message.message_id),
                "Content-Type": "application/json",
                "HookRelay-Schema-Version": str(message.schema_version),
            },
        )
        if acknowledgement.stream != self._settings.nats_stream_name:
            msg = "JetStream acknowledged an unexpected stream"
            raise RuntimeError(msg)
        return PublishReceipt(
            stream=acknowledgement.stream,
            sequence=acknowledgement.seq,
            duplicate=bool(acknowledgement.duplicate),
        )

    async def pull_subscription(self) -> JetStreamContext.PullSubscription:
        """Bind to the existing shared durable consumer without creating an ephemeral cursor."""

        return await self.jetstream.pull_subscribe_bind(
            durable=self._settings.nats_consumer_name,
            stream=self._settings.nats_stream_name,
            pending_msgs_limit=self._settings.nats_max_ack_pending,
            pending_bytes_limit=self._settings.nats_max_ack_pending * 16_384,
        )

    async def close(self) -> None:
        """Drain the connection so buffered acknowledgements are not discarded on shutdown."""

        if self._client is None:
            return
        client = self._client
        try:
            if not client.is_closed:
                await client.drain()
        finally:
            if not client.is_closed:
                await client.close()
            self._client = None
            self._jetstream = None
