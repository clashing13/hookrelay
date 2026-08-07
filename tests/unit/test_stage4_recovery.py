"""Pure Stage 4 retry policy and broker-disposition contracts."""

from dataclasses import dataclass
from uuid import UUID

import pytest
from pydantic import ValidationError

from hookrelay.broker import DeliveryRequestedMessage, encode_delivery_message
from hookrelay.config import Settings
from hookrelay.delivery import (
    DeliveryExecutionResult,
    classify_http_status,
    retry_delay_seconds,
)
from hookrelay.worker import DeliveryWorker


def _message() -> DeliveryRequestedMessage:
    data: dict[str, object] = {
        "type": "delivery.requested",
        "schema_version": 1,
        "message_id": UUID(int=10),
        "tenant_id": UUID(int=1),
        "event_id": UUID(int=3),
        "endpoint_id": UUID(int=4),
        "delivery_id": UUID(int=2),
    }
    return DeliveryRequestedMessage.model_validate(data)


def test_retry_delay_uses_capped_exponential_backoff_and_bounded_jitter() -> None:
    assert (
        retry_delay_seconds(
            1,
            base_seconds=4,
            maximum_seconds=10,
            jitter_ratio=0.25,
            random_value=0,
        )
        == 3
    )
    assert (
        retry_delay_seconds(
            1,
            base_seconds=4,
            maximum_seconds=10,
            jitter_ratio=0.25,
            random_value=1,
        )
        == 4
    )
    assert (
        retry_delay_seconds(
            2,
            base_seconds=4,
            maximum_seconds=10,
            jitter_ratio=0.25,
            random_value=1,
        )
        == 8
    )
    assert (
        retry_delay_seconds(
            10_000,
            base_seconds=4,
            maximum_seconds=10,
            jitter_ratio=0.25,
            random_value=1,
        )
        == 10
    )


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (200, "succeeded"),
        (204, "succeeded"),
        (299, "succeeded"),
        (301, "permanent_failure"),
        (400, "permanent_failure"),
        (404, "permanent_failure"),
        (408, "transient_failure"),
        (425, "transient_failure"),
        (429, "transient_failure"),
        (500, "transient_failure"),
        (503, "transient_failure"),
        (599, "transient_failure"),
    ],
)
def test_http_status_classification_is_explicit(status_code: int, expected: str) -> None:
    assert classify_http_status(status_code) == expected


def test_broker_envelope_stays_v1_and_keeps_generation_in_postgresql() -> None:
    message = _message()

    assert b"dispatch_generation" not in encode_delivery_message(message)
    with pytest.raises(ValidationError):
        DeliveryRequestedMessage.model_validate({**message.model_dump(), "schema_version": 2})
    with pytest.raises(ValidationError):
        DeliveryRequestedMessage.model_validate({**message.model_dump(), "dispatch_generation": 1})


def test_retry_result_requires_exactly_one_positive_delay_for_retry_states() -> None:
    assert DeliveryExecutionResult("retry_scheduled", 1.25).retry_after_seconds == 1.25
    with pytest.raises(ValueError, match="require"):
        DeliveryExecutionResult("retry_scheduled")
    with pytest.raises(ValueError, match="only retryable"):
        DeliveryExecutionResult("succeeded", 1)
    with pytest.raises(ValueError, match="positive"):
        DeliveryExecutionResult("in_progress", 0)


def test_recovery_settings_validate_lease_and_backoff_relationships() -> None:
    with pytest.raises(ValidationError, match="delivery_claim_ttl_seconds"):
        Settings(
            delivery_http_timeout_seconds=10,
            delivery_claim_ttl_seconds=10.5,
            delivery_finalization_margin_seconds=1,
            _env_file=None,
        )
    with pytest.raises(ValidationError, match="delivery_retry_max_seconds"):
        Settings(
            delivery_retry_base_seconds=10,
            delivery_retry_max_seconds=9,
            _env_file=None,
        )


@dataclass
class _DispositionExecutor:
    result: DeliveryExecutionResult

    async def execute(self, _message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        return self.result


class _BrokerMessage:
    def __init__(self) -> None:
        self.data = encode_delivery_message(_message())
        self.acknowledgements = 0
        self.negative_ack_delays: list[float | None] = []
        self.terminations = 0

    async def ack_sync(self, _seconds: float = 1.0, /) -> object:
        self.acknowledgements += 1
        return object()

    async def in_progress(self) -> None:
        return None

    async def nak(self, delay: float | None = None) -> None:
        self.negative_ack_delays.append(delay)

    async def term(self) -> None:
        self.terminations += 1


class _FailingAckMessage(_BrokerMessage):
    async def ack_sync(self, _seconds: float = 1.0, /) -> object:
        raise RuntimeError("injected ACK failure")


class _FailingNakMessage(_BrokerMessage):
    async def nak(self, delay: float | None = None) -> None:
        raise RuntimeError("injected NAK failure")


class _FailingTermMessage(_BrokerMessage):
    def __init__(self) -> None:
        super().__init__()
        self.data = b'{"schema_version":999}'

    async def term(self) -> None:
        raise RuntimeError("injected TERM failure")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal",
    [
        DeliveryExecutionResult("succeeded"),
        DeliveryExecutionResult("already_succeeded"),
        DeliveryExecutionResult("dead_lettered"),
        DeliveryExecutionResult("stale"),
    ],
)
async def test_terminal_worker_dispositions_ack(terminal: DeliveryExecutionResult) -> None:
    settings = Settings(environment="test", _env_file=None)
    message = _BrokerMessage()
    worker = DeliveryWorker(settings, _DispositionExecutor(terminal))

    await worker.process_message(message)

    assert message.acknowledgements == 1
    assert message.negative_ack_delays == []
    assert message.terminations == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "retryable",
    [
        DeliveryExecutionResult("retry_scheduled", 7.25),
        DeliveryExecutionResult("in_progress", 7.25),
    ],
)
async def test_retryable_worker_dispositions_use_exact_delayed_nak(
    retryable: DeliveryExecutionResult,
) -> None:
    settings = Settings(environment="test", _env_file=None)
    message = _BrokerMessage()
    worker = DeliveryWorker(settings, _DispositionExecutor(retryable))

    await worker.process_message(message)

    assert message.acknowledgements == 0
    assert message.negative_ack_delays == [7.25]
    assert message.terminations == 0


@pytest.mark.asyncio
async def test_broker_disposition_failures_do_not_escape_the_worker_task() -> None:
    settings = Settings(environment="test", _env_file=None)

    await DeliveryWorker(
        settings,
        _DispositionExecutor(DeliveryExecutionResult("succeeded")),
    ).process_message(_FailingAckMessage())
    await DeliveryWorker(
        settings,
        _DispositionExecutor(DeliveryExecutionResult("retry_scheduled", 1)),
    ).process_message(_FailingNakMessage())
    await DeliveryWorker(
        settings,
        _DispositionExecutor(DeliveryExecutionResult("succeeded")),
    ).process_message(_FailingTermMessage())
