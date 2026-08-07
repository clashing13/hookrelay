"""Tenant-scoped dead-letter replay through a fresh transactional-outbox dispatch."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.errors import ApiProblem
from hookrelay.ingestion import build_delivery_outbox_message
from hookrelay.models import Delivery
from hookrelay.schemas import DeliveryReplayResponse


def _not_found() -> ApiProblem:
    return ApiProblem(
        status=404,
        code="resource_not_found",
        title="Resource not found",
        detail="The requested resource was not found.",
    )


def _not_replayable() -> ApiProblem:
    return ApiProblem(
        status=409,
        code="delivery_not_replayable",
        title="Delivery is not replayable",
        detail="Only a dead-lettered delivery can be replayed.",
    )


def _generation_conflict() -> ApiProblem:
    return ApiProblem(
        status=409,
        code="delivery_generation_conflict",
        title="Delivery generation changed",
        detail="The delivery generation no longer matches the replay precondition.",
    )


async def replay_dead_lettered_delivery(
    session: AsyncSession,
    *,
    tenant_id: UUID,
    delivery_id: UUID,
    expected_dispatch_generation: int,
) -> DeliveryReplayResponse:
    """Start a fresh retry budget without erasing the original delivery history."""

    delivery = await session.scalar(
        select(Delivery)
        .where(
            Delivery.id == delivery_id,
            Delivery.tenant_id == tenant_id,
        )
        .with_for_update()
    )
    if delivery is None:
        raise _not_found()
    if delivery.dispatch_generation != expected_dispatch_generation:
        raise _generation_conflict()
    if delivery.status != "dead_lettered":
        raise _not_replayable()

    delivery.dispatch_generation += 1
    delivery.status = "pending"
    delivery.next_attempt_at = None
    delivery.dead_lettered_at = None
    delivery.dead_letter_reason = None
    delivery.claim_token = None
    delivery.claim_expires_at = None
    session.add(
        build_delivery_outbox_message(
            tenant_id=delivery.tenant_id,
            event_id=delivery.event_id,
            delivery=delivery,
        )
    )
    await session.flush()
    return DeliveryReplayResponse(
        id=delivery.id,
        event_id=delivery.event_id,
        status="pending",
        dispatch_generation=delivery.dispatch_generation,
    )
