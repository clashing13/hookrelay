"""Recoverable PostgreSQL-to-JetStream transactional-outbox publisher."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import CursorResult, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hookrelay.broker import (
    OUTBOX_SCHEMA_VERSION,
    OUTBOX_TOPIC,
    DeliveryRequestedMessage,
    JetStreamBroker,
    OutboxPublisher,
)
from hookrelay.config import Settings, get_settings
from hookrelay.database import PostgresDatabase
from hookrelay.logging import configure_logging
from hookrelay.models import OutboxMessage
from hookrelay.runtime import install_stop_handlers


class OutboxContractError(RuntimeError):
    """A persisted internal message does not match its strict versioned envelope."""


class OutboxClaimLost(RuntimeError):
    """A publisher tried to finalize a row no longer owned by its lease token."""


@dataclass(frozen=True, slots=True)
class ClaimedOutboxMessage:
    """Broker-ready data detached from the short PostgreSQL claim transaction."""

    outbox_id: UUID
    claim_token: UUID
    message: DeliveryRequestedMessage


@dataclass(frozen=True, slots=True)
class PublishBatchResult:
    """Observable progress from one bounded scan."""

    claimed: int
    published: int
    duplicates: int


def _validated_message(row: OutboxMessage) -> DeliveryRequestedMessage:
    if row.topic != OUTBOX_TOPIC or row.schema_version != OUTBOX_SCHEMA_VERSION:
        msg = "outbox row uses an unsupported topic or schema version"
        raise OutboxContractError(msg)
    message = DeliveryRequestedMessage.model_validate(row.payload)
    if (
        message.message_id != row.id
        or message.tenant_id != row.tenant_id
        or message.delivery_id != row.delivery_id
    ):
        msg = "outbox payload identity does not match its authoritative row"
        raise OutboxContractError(msg)
    return message


class TransactionalOutboxPublisher:
    """Claim short batches, publish ID-only messages, then finalize acknowledged rows."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: OutboxPublisher,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._publisher = publisher
        self._logger = logging.getLogger("hookrelay.outbox")

    async def _claim_batch(self) -> list[ClaimedOutboxMessage]:
        claim_token = uuid4()
        async with self._session_factory() as session:
            database_now = await session.scalar(select(func.now()))
            if database_now is None:
                msg = "PostgreSQL did not return its current time"
                raise RuntimeError(msg)
            claim_expires_at = database_now + timedelta(
                seconds=self._settings.outbox_claim_ttl_seconds
            )
            rows = list(
                await session.scalars(
                    select(OutboxMessage)
                    .where(
                        OutboxMessage.published_at.is_(None),
                        or_(
                            OutboxMessage.claim_expires_at.is_(None),
                            OutboxMessage.claim_expires_at <= database_now,
                        ),
                    )
                    .order_by(OutboxMessage.created_at, OutboxMessage.id)
                    .limit(self._settings.outbox_batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            claimed = [
                ClaimedOutboxMessage(
                    outbox_id=row.id,
                    claim_token=claim_token,
                    message=_validated_message(row),
                )
                for row in rows
            ]
            for row in rows:
                row.claim_token = claim_token
                row.claim_expires_at = claim_expires_at
            await session.commit()
        return claimed

    async def _mark_published(self, item: ClaimedOutboxMessage) -> None:
        async with self._session_factory() as session:
            result = cast(
                "CursorResult[Any]",
                await session.execute(
                    update(OutboxMessage)
                    .where(
                        OutboxMessage.id == item.outbox_id,
                        OutboxMessage.claim_token == item.claim_token,
                        OutboxMessage.published_at.is_(None),
                    )
                    .values(
                        published_at=func.now(),
                        claim_token=None,
                        claim_expires_at=None,
                    )
                ),
            )
            if result.rowcount != 1:
                await session.rollback()
                raise OutboxClaimLost
            await session.commit()

    async def _release_claims(self, items: list[ClaimedOutboxMessage]) -> None:
        if not items:
            return
        outbox_ids = [item.outbox_id for item in items]
        claim_token = items[0].claim_token
        async with self._session_factory() as session:
            await session.execute(
                update(OutboxMessage)
                .where(
                    OutboxMessage.id.in_(outbox_ids),
                    OutboxMessage.claim_token == claim_token,
                    OutboxMessage.published_at.is_(None),
                )
                .values(claim_token=None, claim_expires_at=None)
            )
            await session.commit()

    async def publish_available_once(
        self,
        stop_event: asyncio.Event | None = None,
    ) -> PublishBatchResult:
        """Publish at most one bounded claim batch and surface any ambiguous failure."""

        claimed = await self._claim_batch()
        published = 0
        duplicates = 0
        for index, item in enumerate(claimed):
            if stop_event is not None and stop_event.is_set():
                await self._release_claims(claimed[index:])
                break
            try:
                receipt = await self._publisher.publish(item.message)
            except Exception:
                await self._release_claims(claimed[index:])
                raise
            duplicates += int(receipt.duplicate)
            await self._mark_published(item)
            published += 1
        return PublishBatchResult(
            claimed=len(claimed),
            published=published,
            duplicates=duplicates,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll forever with a fixed Stage 3 cadence; backoff policy arrives in Stage 4."""

        while not stop_event.is_set():
            try:
                result = await self.publish_available_once(stop_event)
                if result.published:
                    self._logger.info(
                        "outbox_batch_published",
                        extra={
                            "published_count": result.published,
                            "duplicate_count": result.duplicates,
                        },
                    )
                    continue
            except Exception as exc:
                self._logger.warning(
                    "outbox_publish_failed",
                    extra={"error_type": type(exc).__name__},
                )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self._settings.outbox_poll_interval_seconds,
                )
            except TimeoutError:
                pass


async def _run() -> None:
    settings = get_settings()
    configure_logging(settings)
    database = PostgresDatabase(settings)
    broker = JetStreamBroker(settings, client_name="hookrelay-outbox")
    stop_event = asyncio.Event()
    install_stop_handlers(stop_event)
    try:
        await broker.connect()
        publisher = TransactionalOutboxPublisher(
            settings,
            database.session_factory,
            broker,
        )
        await publisher.run(stop_event)
    finally:
        try:
            await broker.close()
        finally:
            await database.dispose()


def run() -> None:
    """Start the dedicated outbox-publisher process."""

    asyncio.run(_run())
