"""Bounded durable-consumer loop for signed webhook delivery."""

import asyncio
import logging
from collections.abc import Mapping
from typing import Protocol, cast

from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from pydantic import ValidationError

from hookrelay.broker import DeliveryRequestedMessage, JetStreamBroker, decode_delivery_message
from hookrelay.config import Settings, get_settings
from hookrelay.database import PostgresDatabase
from hookrelay.delivery import (
    DeliveryClaimLost,
    DeliveryExecutionResult,
    DeliveryExecutor,
    DeliveryMessageRejected,
    DeliveryTargetBlocked,
    build_http_client,
)
from hookrelay.logging import configure_logging
from hookrelay.metrics import HookRelayMetrics, PrometheusServer, start_metrics_server
from hookrelay.observability import (
    NOOP_TELEMETRY,
    Telemetry,
    correlation_scope,
    extract_trace_context,
    message_correlation_id,
)
from hookrelay.runtime import install_stop_handlers
from hookrelay.security import SecretCipher


class DeliveryBrokerMessage(Protocol):
    """Acknowledgement operations needed from one JetStream message."""

    data: bytes

    async def ack_sync(self, seconds: float = 1.0, /) -> object:
        """Confirm that the server durably processed a success acknowledgement."""

    async def in_progress(self) -> None:
        """Extend the acknowledgment deadline for an already-active attempt."""

    async def nak(self, delay: float | None = None) -> None:
        """Request redelivery after an optional server-side delay."""

    async def term(self) -> None:
        """Stop redelivery of an internally malformed poison message."""


class PullSubscription(Protocol):
    """Bounded fetch behavior shared by the real subscription and unit fakes."""

    async def fetch(
        self,
        batch: int = 1,
        wait_seconds: float | None = 5,
        heartbeat_seconds: float | None = None,
        /,
    ) -> list[Msg]:
        """Fetch no more than ``batch`` durable messages."""


class DeliveryExecutorProtocol(Protocol):
    """Execute one validated durable delivery command."""

    async def execute(self, message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        """Return the acknowledgement decision for the broker message."""


class DeliveryWorker:
    """Process messages with a hard per-process concurrency ceiling."""

    def __init__(
        self,
        settings: Settings,
        executor: DeliveryExecutorProtocol,
        telemetry: Telemetry | None = None,
        metrics: HookRelayMetrics | None = None,
    ) -> None:
        self._settings = settings
        self._executor = executor
        self._semaphore = asyncio.Semaphore(settings.delivery_worker_concurrency)
        self._telemetry = telemetry or NOOP_TELEMETRY
        self._metrics = metrics
        self._logger = logging.getLogger("hookrelay.worker")

    async def _ack(self, broker_message: DeliveryBrokerMessage, delivery_id: str) -> None:
        try:
            await broker_message.ack_sync(self._settings.nats_ack_timeout_seconds)
            if self._metrics is not None:
                self._metrics.observe_broker_disposition("ack", succeeded=True)
        except Exception as exc:
            if self._metrics is not None:
                self._metrics.observe_broker_disposition("ack", succeeded=False)
            self._logger.warning(
                "delivery_ack_failed",
                extra={"delivery_id": delivery_id, "error_type": type(exc).__name__},
            )

    async def _nak(
        self,
        broker_message: DeliveryBrokerMessage,
        delay_seconds: float,
        delivery_id: str,
    ) -> None:
        try:
            await broker_message.nak(delay_seconds)
            if self._metrics is not None:
                self._metrics.observe_broker_disposition("nak", succeeded=True)
        except Exception as exc:
            if self._metrics is not None:
                self._metrics.observe_broker_disposition("nak", succeeded=False)
            self._logger.warning(
                "delivery_nak_failed",
                extra={"delivery_id": delivery_id, "error_type": type(exc).__name__},
            )

    async def _term(self, broker_message: DeliveryBrokerMessage, delivery_id: str | None) -> None:
        try:
            await broker_message.term()
            if self._metrics is not None:
                self._metrics.observe_broker_disposition("term", succeeded=True)
        except Exception as exc:
            if self._metrics is not None:
                self._metrics.observe_broker_disposition("term", succeeded=False)
            self._logger.warning(
                "delivery_term_failed",
                extra={"delivery_id": delivery_id, "error_type": type(exc).__name__},
            )

    async def process_message(self, broker_message: DeliveryBrokerMessage) -> None:
        """Validate, execute, and ACK only after PostgreSQL records success."""

        async with self._semaphore:
            if self._metrics is not None:
                self._metrics.worker_message_started()
            headers = cast(
                "Mapping[str, object] | None",
                getattr(broker_message, "headers", None),
            )
            try:
                with correlation_scope(message_correlation_id(headers)):
                    with self._telemetry.start_as_current_span(
                        "delivery consume",
                        kind=SpanKind.CONSUMER,
                        parent_context=extract_trace_context(headers),
                        attributes={
                            "messaging.system": "nats",
                            "messaging.operation.name": "process",
                            "messaging.destination.name": self._settings.nats_subject,
                        },
                    ) as span:
                        await self._process_message_in_context(broker_message, span)
            finally:
                if self._metrics is not None:
                    self._metrics.worker_message_finished()

    async def _process_message_in_context(
        self,
        broker_message: DeliveryBrokerMessage,
        span: Span,
    ) -> None:
        try:
            message = decode_delivery_message(broker_message.data)
        except ValidationError:
            span.set_attribute("error.type", "invalid_message")
            span.set_status(Status(StatusCode.ERROR))
            if self._metrics is not None:
                self._metrics.observe_worker_message("invalid")
            self._logger.error("delivery_message_invalid")
            await self._term(broker_message, None)
            return

        span.set_attribute("messaging.message.id", str(message.message_id))
        span.set_attribute("hookrelay.delivery.id", str(message.delivery_id))
        span.set_attribute("hookrelay.event.id", str(message.event_id))
        try:
            result = await self._executor.execute(message)
            span.set_attribute("hookrelay.delivery.state", result.state)
            if self._metrics is not None:
                self._metrics.observe_worker_message(result.state)
            if result.state in {
                "succeeded",
                "already_succeeded",
                "dead_lettered",
                "stale",
            }:
                await self._ack(broker_message, str(message.delivery_id))
            else:
                if result.retry_after_seconds is None:
                    raise RuntimeError("retry disposition is missing its delay")
                await self._nak(
                    broker_message,
                    result.retry_after_seconds,
                    str(message.delivery_id),
                )
                self._logger.warning(
                    "delivery_redelivery_scheduled",
                    extra={
                        "delivery_id": str(message.delivery_id),
                        "retry_after_seconds": result.retry_after_seconds,
                        "state": result.state,
                    },
                )
        except DeliveryTargetBlocked as exc:
            # This is a recoverable policy gate, not a malformed command. Delay it so
            # a configuration mistake cannot create an AckWait redelivery storm.
            if self._metrics is not None:
                self._metrics.observe_worker_message("target_blocked")
            self._logger.warning(
                "delivery_target_blocked",
                extra={
                    "delivery_id": str(message.delivery_id),
                    "error_type": type(exc).__name__,
                },
            )
            await self._nak(
                broker_message,
                self._settings.delivery_policy_block_delay_seconds,
                str(message.delivery_id),
            )
        except DeliveryClaimLost as exc:
            # A newer worker fenced this attempt. Do not let the stale broker handle
            # ACK, TERM, or reschedule work now owned by that newer execution.
            if self._metrics is not None:
                self._metrics.observe_worker_message("claim_lost")
            self._logger.warning(
                "delivery_claim_lost",
                extra={
                    "delivery_id": str(message.delivery_id),
                    "error_type": type(exc).__name__,
                },
            )
        except DeliveryMessageRejected as exc:
            span.set_attribute("error.type", type(exc).__name__)
            span.set_status(Status(StatusCode.ERROR))
            if self._metrics is not None:
                self._metrics.observe_worker_message("rejected")
            self._logger.error(
                "delivery_message_rejected",
                extra={
                    "delivery_id": str(message.delivery_id),
                    "error_type": type(exc).__name__,
                },
            )
            await self._term(broker_message, str(message.delivery_id))
        except Exception as exc:
            span.set_attribute("error.type", type(exc).__name__)
            span.set_status(Status(StatusCode.ERROR))
            if self._metrics is not None:
                self._metrics.observe_worker_message("interrupted")
            self._logger.warning(
                "delivery_processing_interrupted",
                extra={
                    "delivery_id": str(message.delivery_id),
                    "error_type": type(exc).__name__,
                },
            )

    async def process_batch(self, messages: list[DeliveryBrokerMessage]) -> None:
        """Await every bounded task so the loop never creates an unbounded backlog."""

        await asyncio.gather(*(self.process_message(message) for message in messages))

    async def run(
        self,
        subscription: PullSubscription,
        stop_event: asyncio.Event,
    ) -> None:
        """Pull at most one local concurrency window and finish it before fetching more."""

        while not stop_event.is_set():
            try:
                messages = await subscription.fetch(
                    self._settings.delivery_worker_concurrency,
                    self._settings.delivery_fetch_timeout_seconds,
                )
            except TimeoutError:
                continue
            await self.process_batch(list(messages))


async def _run() -> None:
    settings = get_settings()
    settings.require_delivery_runtime()
    logger = configure_logging(settings, service_role="worker")
    telemetry = Telemetry.from_settings(settings, service_role="worker")
    metrics = HookRelayMetrics()
    metrics_server: PrometheusServer | None = None
    database = PostgresDatabase(settings)
    broker = JetStreamBroker(settings, client_name="hookrelay-worker")
    http_client = build_http_client(settings)
    stop_event = asyncio.Event()
    subscription: JetStreamContext.PullSubscription | None = None
    install_stop_handlers(stop_event)
    try:
        try:
            metrics_server = await start_metrics_server(
                settings,
                metrics,
                service_role="worker",
            )
        except OSError as exc:
            logger.warning(
                "metrics_listener_failed",
                extra={"error_type": type(exc).__name__, "service_role": "worker"},
            )
        await broker.connect()
        subscription = await broker.pull_subscription()
        executor = DeliveryExecutor(
            settings,
            database.session_factory,
            SecretCipher(
                settings.secret_encryption_key_bytes(),
                settings.secret_encryption_key_version,
            ),
            http_client,
            telemetry=telemetry,
            metrics=metrics,
        )
        worker = DeliveryWorker(settings, executor, telemetry, metrics)
        await worker.run(subscription, stop_event)
    finally:
        try:
            if metrics_server is not None:
                await metrics_server.close()
        finally:
            try:
                if subscription is not None:
                    await subscription.unsubscribe()
            finally:
                try:
                    await http_client.aclose()
                finally:
                    try:
                        await broker.close()
                    finally:
                        try:
                            await database.dispose()
                        finally:
                            await telemetry.shutdown()


def run() -> None:
    """Start one concurrent delivery-worker process."""

    asyncio.run(_run())
