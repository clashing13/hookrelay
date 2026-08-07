"""Bounded durable-consumer loop for signed webhook delivery."""

import asyncio
import logging
from typing import Protocol

from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
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

    def __init__(self, settings: Settings, executor: DeliveryExecutorProtocol) -> None:
        self._settings = settings
        self._executor = executor
        self._semaphore = asyncio.Semaphore(settings.delivery_worker_concurrency)
        self._logger = logging.getLogger("hookrelay.worker")

    async def _ack(self, broker_message: DeliveryBrokerMessage, delivery_id: str) -> None:
        try:
            await broker_message.ack_sync(self._settings.nats_ack_timeout_seconds)
        except Exception as exc:
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
        except Exception as exc:
            self._logger.warning(
                "delivery_nak_failed",
                extra={"delivery_id": delivery_id, "error_type": type(exc).__name__},
            )

    async def _term(self, broker_message: DeliveryBrokerMessage, delivery_id: str | None) -> None:
        try:
            await broker_message.term()
        except Exception as exc:
            self._logger.warning(
                "delivery_term_failed",
                extra={"delivery_id": delivery_id, "error_type": type(exc).__name__},
            )

    async def process_message(self, broker_message: DeliveryBrokerMessage) -> None:
        """Validate, execute, and ACK only after PostgreSQL records success."""

        async with self._semaphore:
            try:
                message = decode_delivery_message(broker_message.data)
            except ValidationError:
                self._logger.error("delivery_message_invalid")
                await self._term(broker_message, None)
                return

            try:
                result = await self._executor.execute(message)
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
                self._logger.warning(
                    "delivery_claim_lost",
                    extra={
                        "delivery_id": str(message.delivery_id),
                        "error_type": type(exc).__name__,
                    },
                )
            except DeliveryMessageRejected as exc:
                self._logger.error(
                    "delivery_message_rejected",
                    extra={
                        "delivery_id": str(message.delivery_id),
                        "error_type": type(exc).__name__,
                    },
                )
                await self._term(broker_message, str(message.delivery_id))
            except Exception as exc:
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
    configure_logging(settings)
    database = PostgresDatabase(settings)
    broker = JetStreamBroker(settings, client_name="hookrelay-worker")
    http_client = build_http_client(settings)
    stop_event = asyncio.Event()
    subscription: JetStreamContext.PullSubscription | None = None
    install_stop_handlers(stop_event)
    try:
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
        )
        worker = DeliveryWorker(settings, executor)
        await worker.run(subscription, stop_event)
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
                    await database.dispose()


def run() -> None:
    """Start one concurrent delivery-worker process."""

    asyncio.run(_run())
