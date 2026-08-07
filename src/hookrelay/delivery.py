"""Database-authoritative retry recovery, exact-byte signing, and HTTP delivery."""

import asyncio
import hashlib
import hmac
import json
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx2 as httpx
from pydantic import JsonValue, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hookrelay.broker import (
    OUTBOX_SCHEMA_VERSION,
    OUTBOX_TOPIC,
    DeliveryRequestedMessage,
)
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
MIN_REDELIVERY_DELAY_SECONDS = 0.1


class DeliveryMessageRejected(RuntimeError):
    """Broker data cannot be reconciled with authoritative PostgreSQL state."""


class DeliveryClaimLost(RuntimeError):
    """A stale worker tried to finalize an attempt after its lease was fenced out."""


class DeliveryTargetBlocked(RuntimeError):
    """A valid delivery is temporarily outside the pre-Stage-5 destination allowlist."""


DeliveryExecutionState = Literal[
    "succeeded",
    "already_succeeded",
    "retry_scheduled",
    "in_progress",
    "dead_lettered",
    "stale",
]
AttemptOutcome = Literal["succeeded", "transient_failure", "permanent_failure"]


@dataclass(frozen=True, slots=True)
class DeliveryExecutionResult:
    """A durable database decision translated into a later broker ACK or delayed NAK."""

    state: DeliveryExecutionState
    retry_after_seconds: float | None = None

    def __post_init__(self) -> None:
        retrying = self.state in {"retry_scheduled", "in_progress"}
        if retrying != (self.retry_after_seconds is not None):
            msg = "only retryable execution states require retry_after_seconds"
            raise ValueError(msg)
        if self.retry_after_seconds is not None and self.retry_after_seconds <= 0:
            msg = "retry_after_seconds must be positive"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class DeliveryWork:
    """Non-ORM data needed after the short attempt-claim transaction commits."""

    attempt_id: UUID
    attempt_number: int
    generation_attempt_number: int
    dispatch_generation: int
    claim_token: UUID
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


def retry_delay_seconds(
    attempt_number: int,
    *,
    base_seconds: float,
    maximum_seconds: float,
    jitter_ratio: float,
    random_value: float,
) -> float:
    """Return capped exponential delay with bounded downward jitter.

    Attempt one has ``base_seconds`` as its unjittered ceiling. Each later
    attempt doubles that ceiling until ``maximum_seconds``. Jitter selects a
    value uniformly from ``[ceiling * (1 - ratio), ceiling]`` so it never
    violates the configured cap.
    """

    if attempt_number < 1:
        raise ValueError("attempt_number must be positive")
    if base_seconds <= 0 or maximum_seconds < base_seconds:
        raise ValueError("retry bounds are invalid")
    if not 0 <= jitter_ratio <= 1 or not 0 <= random_value <= 1:
        raise ValueError("jitter inputs must be between zero and one")

    ceiling = base_seconds
    for _ in range(attempt_number - 1):
        ceiling = min(maximum_seconds, ceiling * 2)
        if ceiling >= maximum_seconds:
            break
    multiplier = (1 - jitter_ratio) + jitter_ratio * random_value
    return max(MIN_REDELIVERY_DELAY_SECONDS, ceiling * multiplier)


def classify_http_status(status_code: int) -> AttemptOutcome:
    """Classify receiver responses without confusing broker/internal failures."""

    if not 100 <= status_code <= 599:
        return "permanent_failure"
    if 200 <= status_code <= 299:
        return "succeeded"
    if status_code in {408, 425, 429} or 500 <= status_code <= 599:
        return "transient_failure"
    return "permanent_failure"


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
    """Execute one leased, timeout-bounded attempt and persist every retry decision."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        secret_cipher: SecretCipher,
        http_client: httpx.AsyncClient,
        *,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        settings.require_delivery_runtime()
        self._settings = settings
        self._session_factory = session_factory
        self._secret_cipher = secret_cipher
        self._http_client = http_client
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic
        self._random_source = random_source

    def _require_allowed_target(self, target_url: str) -> None:
        hostname = urlsplit(target_url).hostname
        normalized = hostname.lower().rstrip(".") if hostname is not None else ""
        if normalized not in self._settings.delivery_allowed_hosts:
            msg = "delivery target is outside the local/test host allowlist"
            raise DeliveryTargetBlocked(msg)

    def _retry_delay(self, generation_attempt_number: int) -> float:
        return retry_delay_seconds(
            generation_attempt_number,
            base_seconds=self._settings.delivery_retry_base_seconds,
            maximum_seconds=self._settings.delivery_retry_max_seconds,
            jitter_ratio=self._settings.delivery_retry_jitter_ratio,
            random_value=self._random_source(),
        )

    @staticmethod
    def _retry_result(
        seconds: float,
        *,
        state: Literal["retry_scheduled", "in_progress"],
    ) -> DeliveryExecutionResult:
        return DeliveryExecutionResult(
            state=state,
            retry_after_seconds=max(MIN_REDELIVERY_DELAY_SECONDS, seconds),
        )

    @staticmethod
    async def _database_now(session: AsyncSession) -> datetime:
        # PostgreSQL now() is fixed at transaction start. A worker may have waited
        # for the delivery row lock, so leases require the actual server wall clock.
        database_now = cast(
            "datetime | None",
            await session.scalar(select(func.clock_timestamp())),
        )
        if database_now is None:
            raise RuntimeError("PostgreSQL did not return its current time")
        return database_now

    async def _generation_attempt_count(
        self,
        session: AsyncSession,
        delivery_id: UUID,
        dispatch_generation: int,
    ) -> int:
        count = await session.scalar(
            select(func.count())
            .select_from(DeliveryAttempt)
            .where(
                DeliveryAttempt.delivery_id == delivery_id,
                DeliveryAttempt.dispatch_generation == dispatch_generation,
            )
        )
        return int(count or 0)

    @staticmethod
    def _dead_letter(
        delivery: Delivery,
        database_now: datetime,
        reason: Literal["permanent_failure", "attempts_exhausted", "target_blocked"],
    ) -> None:
        delivery.status = "dead_lettered"
        delivery.next_attempt_at = None
        delivery.dead_lettered_at = database_now
        delivery.dead_letter_reason = reason
        delivery.claim_token = None
        delivery.claim_expires_at = None

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
                or outbox.schema_version != message.schema_version
                or outbox.delivery_id != delivery.id
                or outbox.tenant_id != delivery.tenant_id
                or delivery.event_id != message.event_id
                or delivery.endpoint_id != message.endpoint_id
            ):
                raise DeliveryMessageRejected("broker identity does not match PostgreSQL state")

            # Generation stays authoritative in PostgreSQL. Keeping the broker
            # envelope at schema v1 lets old workers parse new initial/replay rows
            # during a controlled rolling cutover instead of TERMing accepted work.
            message_generation = outbox.dispatch_generation
            if message_generation < delivery.dispatch_generation:
                await session.commit()
                return DeliveryExecutionResult("stale")
            if message_generation > delivery.dispatch_generation:
                raise DeliveryMessageRejected("broker generation is ahead of PostgreSQL state")
            if delivery.status == "succeeded":
                await session.commit()
                return DeliveryExecutionResult("already_succeeded")
            if delivery.status == "dead_lettered":
                await session.commit()
                return DeliveryExecutionResult("dead_lettered")

            database_now = await self._database_now(session)
            unfinished_attempt = await session.scalar(
                select(DeliveryAttempt).where(
                    DeliveryAttempt.delivery_id == delivery.id,
                    DeliveryAttempt.finished_at.is_(None),
                )
            )
            if delivery.status == "delivering":
                if (
                    unfinished_attempt is None
                    or delivery.claim_token is None
                    or delivery.claim_expires_at is None
                    or unfinished_attempt.claim_token != delivery.claim_token
                    or unfinished_attempt.dispatch_generation != delivery.dispatch_generation
                ):
                    raise DeliveryMessageRejected("active delivery claim is inconsistent")
                if delivery.claim_expires_at > database_now:
                    remaining = (delivery.claim_expires_at - database_now).total_seconds()
                    await session.commit()
                    return self._retry_result(remaining, state="in_progress")

                generation_attempts = await self._generation_attempt_count(
                    session,
                    delivery.id,
                    delivery.dispatch_generation,
                )
                unfinished_attempt.finished_at = max(database_now, unfinished_attempt.started_at)
                unfinished_attempt.outcome = "abandoned"
                unfinished_attempt.error_code = "worker_lease_expired"
                unfinished_attempt.duration_ms = max(
                    0,
                    round((database_now - unfinished_attempt.started_at).total_seconds() * 1000),
                )
                delivery.claim_token = None
                delivery.claim_expires_at = None
                if generation_attempts >= self._settings.delivery_max_attempts:
                    self._dead_letter(delivery, database_now, "attempts_exhausted")
                    await session.commit()
                    return DeliveryExecutionResult("dead_lettered")
                delay = self._retry_delay(generation_attempts)
                delivery.status = "retry_scheduled"
                delivery.next_attempt_at = database_now + timedelta(seconds=delay)
                delivery.dead_lettered_at = None
                delivery.dead_letter_reason = None
                await session.commit()
                return self._retry_result(delay, state="retry_scheduled")

            if unfinished_attempt is not None:
                raise DeliveryMessageRejected("non-delivering state has an unfinished attempt")
            if delivery.status == "retry_scheduled":
                if delivery.next_attempt_at is None:
                    raise DeliveryMessageRejected("retry schedule is missing its due time")
                if delivery.next_attempt_at > database_now:
                    remaining = (delivery.next_attempt_at - database_now).total_seconds()
                    await session.commit()
                    return self._retry_result(remaining, state="retry_scheduled")
            elif delivery.status != "pending":
                raise DeliveryMessageRejected("delivery state is not executable")

            try:
                self._require_allowed_target(delivery.target_url)
            except DeliveryTargetBlocked:
                self._dead_letter(delivery, database_now, "target_blocked")
                await session.commit()
                return DeliveryExecutionResult("dead_lettered")
            generation_attempts = await self._generation_attempt_count(
                session,
                delivery.id,
                delivery.dispatch_generation,
            )
            if generation_attempts >= self._settings.delivery_max_attempts:
                self._dead_letter(delivery, database_now, "attempts_exhausted")
                await session.commit()
                return DeliveryExecutionResult("dead_lettered")

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
            # Start the lease as close as possible to the commit that publishes
            # it. Dependency loading and secret decryption above must not consume
            # the worker's configured finalization margin.
            claim_started_at = await self._database_now(session)
            claim_token = uuid4()
            attempt = DeliveryAttempt(
                id=uuid4(),
                tenant_id=delivery.tenant_id,
                delivery_id=delivery.id,
                attempt_number=int(last_attempt_number or 0) + 1,
                dispatch_generation=delivery.dispatch_generation,
                claim_token=claim_token,
                started_at=claim_started_at,
            )
            session.add(attempt)
            delivery.status = "delivering"
            delivery.next_attempt_at = None
            delivery.dead_lettered_at = None
            delivery.dead_letter_reason = None
            delivery.claim_token = claim_token
            delivery.claim_expires_at = claim_started_at + timedelta(
                seconds=self._settings.delivery_claim_ttl_seconds
            )
            await session.flush()
            work = DeliveryWork(
                attempt_id=attempt.id,
                attempt_number=attempt.attempt_number,
                generation_attempt_number=generation_attempts + 1,
                dispatch_generation=delivery.dispatch_generation,
                claim_token=claim_token,
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
        outcome: AttemptOutcome,
        response_status_code: int | None,
        error_code: str | None,
        duration_ms: int,
    ) -> DeliveryExecutionResult:
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
            database_now = await self._database_now(session)
            if (
                delivery is None
                or attempt is None
                or attempt.delivery_id != work.delivery_id
                or attempt.finished_at is not None
                or attempt.claim_token != work.claim_token
                or attempt.dispatch_generation != work.dispatch_generation
                or delivery.status != "delivering"
                or delivery.dispatch_generation != work.dispatch_generation
                or delivery.claim_token != work.claim_token
                or delivery.claim_expires_at is None
                or delivery.claim_expires_at <= database_now
            ):
                raise DeliveryClaimLost("attempt ownership changed before finalization")
            attempt.finished_at = max(database_now, attempt.started_at)
            attempt.outcome = outcome
            attempt.response_status_code = response_status_code
            attempt.error_code = error_code
            attempt.duration_ms = duration_ms
            delivery.claim_token = None
            delivery.claim_expires_at = None

            if outcome == "succeeded":
                delivery.status = "succeeded"
                delivery.next_attempt_at = None
                delivery.dead_lettered_at = None
                delivery.dead_letter_reason = None
                result = DeliveryExecutionResult("succeeded")
            elif (
                outcome == "permanent_failure"
                or work.generation_attempt_number >= self._settings.delivery_max_attempts
            ):
                self._dead_letter(
                    delivery,
                    database_now,
                    "permanent_failure" if outcome == "permanent_failure" else "attempts_exhausted",
                )
                result = DeliveryExecutionResult("dead_lettered")
            else:
                delay = self._retry_delay(work.generation_attempt_number)
                delivery.status = "retry_scheduled"
                delivery.next_attempt_at = database_now + timedelta(seconds=delay)
                delivery.dead_lettered_at = None
                delivery.dead_letter_reason = None
                result = self._retry_result(delay, state="retry_scheduled")
            await session.commit()
            return result

    async def execute(self, message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        """Claim, send, and durably finalize one message without holding DB locks over HTTP."""

        claimed = await self._claim_attempt(message)
        if isinstance(claimed, DeliveryExecutionResult):
            return claimed

        timestamp = int(self._clock().astimezone(UTC).timestamp())
        request = build_signed_request(claimed, timestamp, self._settings.version)
        started = self._monotonic()
        response_status_code: int | None = None
        error_code: str | None = None
        outcome: AttemptOutcome
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
                        outcome = classify_http_status(response.status_code)
                    else:
                        outcome = "permanent_failure"
                        error_code = "invalid_http_status"
                    if outcome != "succeeded":
                        error_code = error_code or "http_status"
        except TimeoutError:
            outcome = "transient_failure"
            error_code = "request_timeout"
        except httpx.HTTPError:
            outcome = "transient_failure"
            error_code = "transport_error"

        duration_ms = max(0, round((self._monotonic() - started) * 1000))
        return await self._finish_attempt(
            claimed,
            outcome=outcome,
            response_status_code=response_status_code,
            error_code=error_code,
            duration_ms=duration_ms,
        )


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
