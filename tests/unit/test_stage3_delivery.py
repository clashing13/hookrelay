"""Pure Stage 3 signing, broker-envelope, and bounded-worker contracts."""

import asyncio
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from hookrelay.broker import (
    DeliveryRequestedMessage,
    JetStreamBroker,
    decode_delivery_message,
    encode_delivery_message,
)
from hookrelay.config import Settings
from hookrelay.delivery import (
    DeliveryExecutionResult,
    DeliveryTargetBlocked,
    DeliveryWork,
    build_signed_request,
    encode_webhook_body,
    sign_delivery,
    verify_delivery_signature,
)
from hookrelay.worker import DeliveryBrokerMessage, DeliveryWorker


def _delivery_work() -> DeliveryWork:
    return DeliveryWork(
        attempt_id=UUID(int=5),
        attempt_number=1,
        generation_attempt_number=1,
        dispatch_generation=1,
        claim_token=UUID(int=6),
        tenant_id=UUID(int=1),
        delivery_id=UUID(int=2),
        event_id=UUID(int=3),
        endpoint_id=UUID(int=4),
        target_url="http://receiver:9000/webhooks",
        event_type="order.created",
        event_created_at=datetime(2026, 8, 3, 12, 34, 56, 123456, tzinfo=UTC),
        payload={"z": 2, "a": {"b": True}},
        signing_secret="whsec_test",
    )


def _delivery_message() -> DeliveryRequestedMessage:
    return DeliveryRequestedMessage(
        type="delivery.requested",
        schema_version=1,
        message_id=UUID(int=10),
        tenant_id=UUID(int=1),
        event_id=UUID(int=3),
        endpoint_id=UUID(int=4),
        delivery_id=UUID(int=2),
    )


def test_webhook_body_and_signature_match_a_fixed_wire_vector() -> None:
    work = _delivery_work()
    body = encode_webhook_body(work)

    assert body == (
        b'{"created_at":"2026-08-03T12:34:56.123456Z",'
        b'"delivery_id":"00000000-0000-0000-0000-000000000002",'
        b'"id":"00000000-0000-0000-0000-000000000003",'
        b'"payload":{"a":{"b":true},"z":2},"schema_version":1,'
        b'"type":"order.created"}'
    )
    assert sign_delivery("whsec_test", 1_700_000_000, body) == (
        "v1=c1f0b2a72200007b0a7541c81a3af1b1fd7dd5cdbdc64ac48b12f1a504adc33a"
    )


def test_signature_binds_timestamp_and_exact_body_bytes() -> None:
    body = encode_webhook_body(_delivery_work())
    signature = sign_delivery("whsec_test", 1_700_000_000, body)

    assert verify_delivery_signature("whsec_test", 1_700_000_000, body, signature)
    assert not verify_delivery_signature("whsec_test", 1_700_000_001, body, signature)
    assert not verify_delivery_signature("whsec_test", 1_700_000_000, body + b" ", signature)
    assert not verify_delivery_signature("different", 1_700_000_000, body, signature)
    assert not verify_delivery_signature("whsec_test", 1_700_000_000, body, "v1=é")


def test_signed_request_has_stable_identity_headers_and_hides_secret_in_repr() -> None:
    work = _delivery_work()
    request = build_signed_request(work, 1_700_000_000, "0.4.0")

    assert request.headers == {
        "Content-Type": "application/json",
        "User-Agent": "HookRelay/0.4.0",
        "HookRelay-Delivery-Id": str(work.delivery_id),
        "HookRelay-Event-Id": str(work.event_id),
        "HookRelay-Signature": sign_delivery("whsec_test", 1_700_000_000, request.body),
        "HookRelay-Timestamp": "1700000000",
        "HookRelay-Webhook-Version": "1",
    }
    assert "whsec_test" not in repr(work)


def test_broker_envelope_is_deterministic_id_only_and_strict() -> None:
    message = _delivery_message()
    encoded = encode_delivery_message(message)

    assert decode_delivery_message(encoded) == message
    assert b"payload" not in encoded
    assert b"secret" not in encoded
    assert encoded == encode_delivery_message(message)
    with pytest.raises(ValidationError):
        DeliveryRequestedMessage.model_validate({**message.model_dump(), "schema_version": 2})
    with pytest.raises(ValidationError):
        DeliveryRequestedMessage.model_validate(
            {**message.model_dump(), "target_url": "http://metadata.invalid"}
        )


def test_desired_jetstream_topology_is_file_backed_durable_and_bounded() -> None:
    settings = Settings(environment="test", _env_file=None)
    broker = JetStreamBroker(settings, client_name="unit-test")
    stream = broker.desired_stream_config()
    consumer = broker.desired_consumer_config()

    assert stream.name == "HOOKRELAY_DELIVERIES_V1"
    assert stream.subjects == ["hookrelay.delivery.requested.v1"]
    assert stream.retention is not None and stream.retention.value == "workqueue"
    assert stream.storage is not None and stream.storage.value == "file"
    assert stream.discard is not None and stream.discard.value == "new"
    assert stream.max_msg_size == 16_384
    assert consumer.durable_name == "HOOKRELAY_DELIVERY_WORKERS_V1"
    assert consumer.ack_policy is not None and consumer.ack_policy.value == "explicit"
    assert consumer.max_deliver == -1
    assert consumer.max_ack_pending == settings.nats_max_ack_pending
    assert consumer.ack_wait is not None
    assert consumer.ack_wait > settings.delivery_http_timeout_seconds


class _FakeBrokerMessage:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.acknowledgements = 0
        self.progress_updates = 0
        self.negative_ack_delays: list[float | None] = []
        self.terminations = 0

    async def ack_sync(self, _seconds: float = 1.0, /) -> object:
        self.acknowledgements += 1
        return object()

    async def in_progress(self) -> None:
        self.progress_updates += 1

    async def nak(self, delay: float | None = None) -> None:
        self.negative_ack_delays.append(delay)

    async def term(self) -> None:
        self.terminations += 1


class _GatedExecutor:
    def __init__(self, expected_peak: int) -> None:
        self.expected_peak = expected_peak
        self.active = 0
        self.peak = 0
        self.at_peak = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, _message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        self.active += 1
        self.peak = max(self.peak, self.active)
        if self.active == self.expected_peak:
            self.at_peak.set()
        try:
            await self.release.wait()
            return DeliveryExecutionResult("succeeded")
        finally:
            self.active -= 1


class _BlockedExecutor:
    async def execute(self, _message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        raise DeliveryTargetBlocked("test policy gate")


class _FailedExecutor:
    async def execute(self, _message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        return DeliveryExecutionResult("retry_scheduled", retry_after_seconds=3.0)


@pytest.mark.asyncio
async def test_worker_never_exceeds_configured_local_concurrency() -> None:
    settings = Settings(
        environment="test",
        delivery_worker_concurrency=3,
        nats_max_ack_pending=3,
        _env_file=None,
    )
    executor = _GatedExecutor(expected_peak=3)
    worker = DeliveryWorker(settings, executor)
    messages = [_FakeBrokerMessage(encode_delivery_message(_delivery_message())) for _ in range(9)]

    batch_task = asyncio.create_task(
        worker.process_batch(cast("list[DeliveryBrokerMessage]", messages))
    )
    await asyncio.wait_for(executor.at_peak.wait(), timeout=1)
    assert executor.peak == settings.delivery_worker_concurrency
    executor.release.set()
    await asyncio.wait_for(batch_task, timeout=1)

    assert executor.peak == settings.delivery_worker_concurrency
    assert all(message.acknowledgements == 1 for message in messages)
    assert all(message.terminations == 0 for message in messages)


@pytest.mark.asyncio
async def test_worker_terminates_invalid_internal_messages() -> None:
    settings = Settings(environment="test", _env_file=None)
    executor = _GatedExecutor(expected_peak=1)
    worker = DeliveryWorker(settings, executor)
    message = _FakeBrokerMessage(b'{"schema_version":999}')

    await worker.process_message(message)

    assert message.terminations == 1
    assert message.acknowledgements == 0
    assert executor.peak == 0


@pytest.mark.asyncio
async def test_worker_keeps_policy_blocked_delivery_recoverable() -> None:
    settings = Settings(environment="test", _env_file=None)
    worker = DeliveryWorker(settings, _BlockedExecutor())
    message = _FakeBrokerMessage(encode_delivery_message(_delivery_message()))

    await worker.process_message(message)

    assert message.terminations == 0
    assert message.acknowledgements == 0
    assert message.progress_updates == 0
    assert message.negative_ack_delays == [settings.delivery_policy_block_delay_seconds]


@pytest.mark.asyncio
async def test_worker_leaves_failed_attempt_unacknowledged() -> None:
    settings = Settings(environment="test", _env_file=None)
    worker = DeliveryWorker(settings, _FailedExecutor())
    message = _FakeBrokerMessage(encode_delivery_message(_delivery_message()))

    await worker.process_message(message)

    assert message.acknowledgements == 0
    assert message.terminations == 0
    assert message.progress_updates == 0
    assert message.negative_ack_delays == [3.0]
