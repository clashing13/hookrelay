"""Authoritative delivery loading, exact-byte HMAC signing, and HTTP attempt persistence."""

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal, cast
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx2 as httpx
from pydantic import JsonValue, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hookrelay.broker import OUTBOX_SCHEMA_VERSION, OUTBOX_TOPIC, DeliveryRequestedMessage
from hookrelay.config import Settings
from hookrelay.models import (
    Delivery,
    DeliveryAttempt,
    EndpointSigningSecret,
    Event,
    OutboxMessage,
)
from hookrelay.security import SecretCipher

WEBHOOK_SCHEMA_VERSION = 1
SIGNATURE_VERSION = "v1"


class DeliveryMessageRejected(RuntimeError):
    """Broker data cannot be reconciled with authoritative PostgreSQL state."""


class DeliveryTargetBlocked(RuntimeError):
    """A valid delivery is temporarily outside the Stage 3 destination allowlist."""


DeliveryExecutionResult = Literal[
    "succeeded",
    "already_succeeded",
    "failed",
    "in_progress",
]


@dataclass(frozen=True, slots=True)
class DeliveryWork:
    """Non-ORM data needed after the short attempt-claim transaction commits."""

    attempt_id: UUID
    tenant_id: UUID
    delivery_id: UUID
    event_id: UUID
    endpoint_id: UUID
    target_url: str
    event_type: str
    event_created_at: datetime
    payload: dict[str, JsonValue]
    signing_secret: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class SignedWebhookRequest:
    """The exact bytes and headers sent to a receiver."""

    body: bytes
    headers: dict[str, str]


def encode_webhook_body(work: DeliveryWork) -> bytes:
    """Build a versioned deterministic envelope with signed stable identities."""

    created_at = work.event_created_at.astimezone(UTC).isoformat(timespec="microseconds")
    if created_at.endswith("+00:00"):
        created_at = f"{created_at[:-6]}Z"
    envelope = {
        "created_at": created_at,
        "delivery_id": str(work.delivery_id),
        "id": str(work.event_id),
        "payload": work.payload,
        "schema_version": WEBHOOK_SCHEMA_VERSION,
        "type": work.event_type,
    }
    return json.dumps(
        envelope,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sign_delivery(secret: str, timestamp: int, body: bytes) -> str:
    """Sign ``timestamp.body`` with HMAC-SHA-256 and an explicit version prefix."""

    signed_content = str(timestamp).encode("ascii") + b"." + body
    digest = hmac.new(secret.encode("utf-8"), signed_content, hashlib.sha256).hexdigest()
    return f"{SIGNATURE_VERSION}={digest}"


def verify_delivery_signature(
    secret: str,
    timestamp: int,
    body: bytes,
    presented_signature: str,
) -> bool:
    """Constant-time verification helper for the test receiver and user examples."""

    expected = sign_delivery(secret, timestamp, body)
    try:
        presented = presented_signature.encode("ascii")
    except UnicodeEncodeError:
        return False
    return hmac.compare_digest(expected.encode("ascii"), presented)


def build_signed_request(work: DeliveryWork, timestamp: int, version: str) -> SignedWebhookRequest:
    """Bind the timestamp and exact serialized body to the endpoint secret."""

    body = encode_webhook_body(work)
    signature = sign_delivery(work.signing_secret, timestamp, body)
    return SignedWebhookRequest(
        body=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": f"HookRelay/{version}",
            "HookRelay-Delivery-Id": str(work.delivery_id),
            "HookRelay-Event-Id": str(work.event_id),
            "HookRelay-Signature": signature,
            "HookRelay-Timestamp": str(timestamp),
            "HookRelay-Webhook-Version": str(WEBHOOK_SCHEMA_VERSION),
        },
    )


class DeliveryExecutor:
    """Execute one database-serialized, timeout-bounded delivery attempt."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        secret_cipher: SecretCipher,
        http_client: httpx.AsyncClient,
        *,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        settings.require_stage3_delivery_runtime()
        self._settings = settings
        self._session_factory = session_factory
        self._secret_cipher = secret_cipher
        self._http_client = http_client
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic

    def _require_allowed_target(self, target_url: str) -> None:
        hostname = urlsplit(target_url).hostname
        normalized = hostname.lower().rstrip(".") if hostname is not None else ""
        if normalized not in self._settings.delivery_allowed_hosts:
            msg = "delivery target is outside the Stage 3 local/test host allowlist"
            raise DeliveryTargetBlocked(msg)

    async def _claim_attempt(
        self,
        message: DeliveryRequestedMessage,
    ) -> DeliveryWork | DeliveryExecutionResult:
        async with self._session_factory() as session:
            delivery = await session.scalar(
                select(Delivery)
                .where(
                    Delivery.id == message.delivery_id,
                    Delivery.tenant_id == message.tenant_id,
                )
                .with_for_update()
            )
            if delivery is None:
                raise DeliveryMessageRejected("delivery does not exist in the claimed tenant")

            outbox = await session.get(OutboxMessage, message.message_id)
            try:
                persisted_message = (
                    DeliveryRequestedMessage.model_validate(outbox.payload)
                    if outbox is not None
                    else None
                )
            except ValidationError as exc:
                raise DeliveryMessageRejected("persisted outbox payload is invalid") from exc
            if (
                outbox is None
                or persisted_message != message
                or outbox.topic != OUTBOX_TOPIC
                or outbox.schema_version != OUTBOX_SCHEMA_VERSION
                or outbox.delivery_id != delivery.id
                or outbox.tenant_id != delivery.tenant_id
                or delivery.event_id != message.event_id
                or delivery.endpoint_id != message.endpoint_id
            ):
                raise DeliveryMessageRejected("broker identity does not match PostgreSQL state")

            if delivery.status == "succeeded":
                await session.commit()
                return "already_succeeded"

            unfinished_attempt = await session.scalar(
                select(DeliveryAttempt.id).where(
                    DeliveryAttempt.delivery_id == delivery.id,
                    DeliveryAttempt.finished_at.is_(None),
                )
            )
            if delivery.status == "delivering" and unfinished_attempt is not None:
                await session.commit()
                return "in_progress"
            if delivery.status != "pending" or unfinished_attempt is not None:
                raise DeliveryMessageRejected("delivery state is not executable in Stage 3")

            event = await session.scalar(
                select(Event).where(
                    Event.id == delivery.event_id,
                    Event.tenant_id == delivery.tenant_id,
                )
            )
            signing_secret = await session.scalar(
                select(EndpointSigningSecret).where(
                    EndpointSigningSecret.id == delivery.signing_secret_id,
                    EndpointSigningSecret.tenant_id == delivery.tenant_id,
                    EndpointSigningSecret.endpoint_id == delivery.endpoint_id,
                )
            )
            if event is None or signing_secret is None:
                raise DeliveryMessageRejected("delivery snapshot dependencies are missing")
            self._require_allowed_target(delivery.target_url)

            raw_secret = self._secret_cipher.decrypt_endpoint_secret(
                signing_secret.tenant_id,
                signing_secret.endpoint_id,
                signing_secret.id,
                signing_secret.version,
                signing_secret.encryption_key_version,
                signing_secret.ciphertext,
            )
            last_attempt_number = await session.scalar(
                select(func.max(DeliveryAttempt.attempt_number)).where(
                    DeliveryAttempt.delivery_id == delivery.id
                )
            )
            attempt = DeliveryAttempt(
                id=uuid4(),
                tenant_id=delivery.tenant_id,
                delivery_id=delivery.id,
                attempt_number=int(last_attempt_number or 0) + 1,
            )
            session.add(attempt)
            delivery.status = "delivering"
            await session.flush()
            work = DeliveryWork(
                attempt_id=attempt.id,
                tenant_id=delivery.tenant_id,
                delivery_id=delivery.id,
                event_id=event.id,
                endpoint_id=delivery.endpoint_id,
                target_url=delivery.target_url,
                event_type=event.event_type,
                event_created_at=event.created_at,
                payload=cast("dict[str, JsonValue]", event.payload),
                signing_secret=raw_secret,
            )
            await session.commit()
            return work

    async def _finish_attempt(
        self,
        work: DeliveryWork,
        *,
        outcome: Literal["succeeded", "transient_failure"],
        response_status_code: int | None,
        error_code: str | None,
        duration_ms: int,
    ) -> None:
        async with self._session_factory() as session:
            delivery = await session.scalar(
                select(Delivery)
                .where(
                    Delivery.id == work.delivery_id,
                    Delivery.tenant_id == work.tenant_id,
                )
                .with_for_update()
            )
            attempt = await session.get(DeliveryAttempt, work.attempt_id)
            if (
                delivery is None
                or attempt is None
                or attempt.delivery_id != work.delivery_id
                or attempt.finished_at is not None
                or delivery.status != "delivering"
            ):
                raise DeliveryMessageRejected("attempt ownership changed before finalization")
            database_now = await session.scalar(select(func.now()))
            if database_now is None:
                raise RuntimeError("PostgreSQL did not return its current time")
            attempt.finished_at = database_now
            attempt.outcome = outcome
            attempt.response_status_code = response_status_code
            attempt.error_code = error_code
            attempt.duration_ms = duration_ms
            delivery.status = "succeeded" if outcome == "succeeded" else "pending"
            await session.commit()

    async def execute(self, message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        """Claim, send, and durably finalize one message without holding DB locks over HTTP."""

        claimed = await self._claim_attempt(message)
        if isinstance(claimed, str):
            return claimed

        timestamp = int(self._clock().astimezone(UTC).timestamp())
        request = build_signed_request(claimed, timestamp, self._settings.version)
        started = self._monotonic()
        response_status_code: int | None = None
        error_code: str | None = None
        succeeded = False
        try:
            async with asyncio.timeout(self._settings.delivery_http_timeout_seconds):
                async with self._http_client.stream(
                    "POST",
                    claimed.target_url,
                    content=request.body,
                    headers=request.headers,
                ) as response:
                    if 100 <= response.status_code <= 599:
                        response_status_code = response.status_code
                        succeeded = 200 <= response.status_code < 300
                    else:
                        error_code = "invalid_http_status"
                    if not succeeded and error_code is None:
                        error_code = "http_status"
        except TimeoutError:
            error_code = "request_timeout"
        except httpx.HTTPError:
            error_code = "transport_error"

        duration_ms = max(0, round((self._monotonic() - started) * 1000))
        await self._finish_attempt(
            claimed,
            outcome="succeeded" if succeeded else "transient_failure",
            response_status_code=response_status_code,
            error_code=error_code,
            duration_ms=duration_ms,
        )
        return "succeeded" if succeeded else "failed"


def build_http_client(settings: Settings) -> httpx.AsyncClient:
    """Create one bounded, proxy-free client for the lifetime of a worker process."""

    timeout = httpx.Timeout(
        connect=settings.delivery_http_timeout_seconds,
        read=settings.delivery_http_timeout_seconds,
        write=settings.delivery_http_timeout_seconds,
        pool=settings.delivery_http_timeout_seconds,
    )
    limits = httpx.Limits(
        max_connections=settings.delivery_worker_concurrency,
        max_keepalive_connections=settings.delivery_worker_concurrency,
    )
    return httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        follow_redirects=False,
        trust_env=False,
    )
