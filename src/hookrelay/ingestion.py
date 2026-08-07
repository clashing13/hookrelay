"""Concurrency-safe, transactional event ingestion."""

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import cast
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.dependencies import AuthenticatedTenant, database_unavailable_problem
from hookrelay.api.errors import ApiProblem
from hookrelay.broker import OUTBOX_SCHEMA_VERSION, OUTBOX_TOPIC
from hookrelay.models import (
    Delivery,
    EndpointSigningSecret,
    Event,
    OutboxMessage,
    WebhookEndpoint,
)
from hookrelay.schemas import (
    DeliveryDetailResponse,
    DeliveryResponse,
    DeliveryStatus,
    EventCreate,
    EventDetailResponse,
    EventResponse,
)

REQUEST_FINGERPRINT_VERSION = 1


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """The committed representation and whether an existing event was replayed."""

    response: EventResponse
    replayed: bool


@dataclass(frozen=True, slots=True)
class EndpointSnapshot:
    """Tenant-validated destination fields copied into a durable delivery."""

    endpoint_id: UUID
    target_url: str
    signing_secret_id: UUID


def request_fingerprint(request: EventCreate) -> bytes:
    """Hash a versioned canonical representation of the accepted operation."""

    canonical = {
        "endpoint_ids": [str(endpoint_id) for endpoint_id in request.canonical_endpoint_ids()],
        "operation": "POST /v1/events",
        "payload": request.payload,
        "type": request.event_type,
        "version": REQUEST_FINGERPRINT_VERSION,
    }
    encoded = json.dumps(
        canonical,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).digest()


def build_outbox_messages(
    *,
    tenant_id: UUID,
    event_id: UUID,
    deliveries: list[Delivery],
) -> list[OutboxMessage]:
    """Build ID-only messages; kept separate so rollback can be tested safely."""

    messages: list[OutboxMessage] = []
    for delivery in deliveries:
        outbox_id = uuid4()
        messages.append(
            OutboxMessage(
                id=outbox_id,
                tenant_id=tenant_id,
                delivery_id=delivery.id,
                schema_version=OUTBOX_SCHEMA_VERSION,
                topic=OUTBOX_TOPIC,
                payload={
                    "delivery_id": str(delivery.id),
                    "endpoint_id": str(delivery.endpoint_id),
                    "event_id": str(event_id),
                    "message_id": str(outbox_id),
                    "schema_version": OUTBOX_SCHEMA_VERSION,
                    "tenant_id": str(tenant_id),
                    "type": OUTBOX_TOPIC,
                },
            )
        )
    return messages


def _idempotency_conflict() -> ApiProblem:
    return ApiProblem(
        status=409,
        code="idempotency_key_reused",
        title="Idempotency key reused",
        detail="The Idempotency-Key was already used for a different request.",
    )


def _resource_not_found() -> ApiProblem:
    return ApiProblem(
        status=404,
        code="resource_not_found",
        title="Resource not found",
        detail="One or more requested resources were not found.",
    )


def _verify_existing_fingerprint(existing: Event, fingerprint: bytes) -> None:
    if (
        existing.request_fingerprint_version != REQUEST_FINGERPRINT_VERSION
        or not hmac.compare_digest(existing.request_fingerprint, fingerprint)
    ):
        raise _idempotency_conflict()


async def _load_existing_event(
    session: AsyncSession,
    tenant_id: UUID,
    idempotency_key: str,
) -> Event | None:
    return cast(
        "Event | None",
        await session.scalar(
            select(Event).where(
                Event.tenant_id == tenant_id,
                Event.idempotency_key == idempotency_key,
            )
        ),
    )


async def _load_endpoint_snapshots(
    session: AsyncSession,
    tenant_id: UUID,
    endpoint_ids: list[UUID],
) -> list[EndpointSnapshot]:
    rows = (
        await session.execute(
            select(
                WebhookEndpoint.id,
                WebhookEndpoint.url,
                EndpointSigningSecret.id.label("signing_secret_id"),
            )
            .join(
                EndpointSigningSecret,
                (EndpointSigningSecret.tenant_id == WebhookEndpoint.tenant_id)
                & (EndpointSigningSecret.endpoint_id == WebhookEndpoint.id),
            )
            .where(
                WebhookEndpoint.tenant_id == tenant_id,
                WebhookEndpoint.id.in_(endpoint_ids),
                WebhookEndpoint.is_active.is_(True),
                EndpointSigningSecret.retired_at.is_(None),
            )
        )
    ).all()
    if len(rows) != len(endpoint_ids):
        raise _resource_not_found()

    snapshots = [
        EndpointSnapshot(
            endpoint_id=row.id,
            target_url=row.url,
            signing_secret_id=row.signing_secret_id,
        )
        for row in rows
    ]
    return sorted(snapshots, key=lambda snapshot: str(snapshot.endpoint_id))


async def _event_deliveries(session: AsyncSession, event: Event) -> list[Delivery]:
    return list(
        await session.scalars(
            select(Delivery)
            .where(
                Delivery.tenant_id == event.tenant_id,
                Delivery.event_id == event.id,
            )
            .order_by(Delivery.endpoint_id)
        )
    )


async def accepted_event_response(session: AsyncSession, event: Event) -> EventResponse:
    """Rebuild the original creation response even if delivery state later changes."""

    deliveries = await _event_deliveries(session, event)
    return EventResponse.from_parts(
        event_id=event.id,
        event_type=event.event_type,
        created_at=event.created_at,
        deliveries=[
            DeliveryResponse(
                id=delivery.id,
                endpoint_id=delivery.endpoint_id,
                status="pending",
            )
            for delivery in deliveries
        ],
    )


async def event_detail_response(session: AsyncSession, event: Event) -> EventDetailResponse:
    """Return current delivery state for an explicit event inspection."""

    deliveries = await _event_deliveries(session, event)
    return EventDetailResponse(
        id=event.id,
        event_type=event.event_type,
        created_at=event.created_at,
        payload=event_payload(event),
        deliveries=[
            DeliveryDetailResponse(
                id=delivery.id,
                endpoint_id=delivery.endpoint_id,
                status=cast(DeliveryStatus, delivery.status),
            )
            for delivery in deliveries
        ],
    )


async def ingest_event(
    session: AsyncSession,
    tenant: AuthenticatedTenant,
    request: EventCreate,
    idempotency_key: str,
) -> IngestionResult:
    """Create event, deliveries, and outbox rows in the caller's transaction."""

    fingerprint = request_fingerprint(request)
    existing = await _load_existing_event(session, tenant.tenant_id, idempotency_key)
    if existing is not None:
        _verify_existing_fingerprint(existing, fingerprint)
        return IngestionResult(
            response=await accepted_event_response(session, existing),
            replayed=True,
        )

    snapshots = await _load_endpoint_snapshots(
        session,
        tenant.tenant_id,
        request.canonical_endpoint_ids(),
    )
    event_id = uuid4()
    inserted = (
        await session.execute(
            postgresql_insert(Event)
            .values(
                id=event_id,
                tenant_id=tenant.tenant_id,
                api_key_id=tenant.api_key_id,
                event_type=request.event_type,
                payload=request.payload,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                request_fingerprint_version=REQUEST_FINGERPRINT_VERSION,
            )
            .on_conflict_do_nothing(index_elements=[Event.tenant_id, Event.idempotency_key])
            .returning(Event.id)
        )
    ).scalar_one_or_none()

    if inserted is None:
        existing = await _load_existing_event(session, tenant.tenant_id, idempotency_key)
        if existing is None:
            raise database_unavailable_problem()
        _verify_existing_fingerprint(existing, fingerprint)
        return IngestionResult(
            response=await accepted_event_response(session, existing),
            replayed=True,
        )

    event = await session.get(Event, event_id)
    if event is None:
        raise database_unavailable_problem()

    deliveries = [
        Delivery(
            id=uuid4(),
            tenant_id=tenant.tenant_id,
            event_id=event_id,
            endpoint_id=snapshot.endpoint_id,
            signing_secret_id=snapshot.signing_secret_id,
            target_url=snapshot.target_url,
            status="pending",
        )
        for snapshot in snapshots
    ]
    session.add_all(deliveries)
    await session.flush()

    outbox_messages = build_outbox_messages(
        tenant_id=tenant.tenant_id,
        event_id=event_id,
        deliveries=deliveries,
    )
    session.add_all(outbox_messages)
    await session.flush()
    return IngestionResult(
        response=await accepted_event_response(session, event),
        replayed=False,
    )


def event_payload(event: Event) -> dict[str, JsonValue]:
    """Narrow JSONB's ORM annotation at the public Pydantic boundary."""

    return cast("dict[str, JsonValue]", event.payload)
