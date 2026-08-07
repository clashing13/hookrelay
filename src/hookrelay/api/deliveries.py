"""Authenticated manual replay of tenant-owned dead-lettered deliveries."""

import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.dependencies import (
    AuthenticatedTenant,
    authenticate_tenant,
    database_unavailable_problem,
    get_session,
    require_json_content_type,
)
from hookrelay.api.errors import problem_responses
from hookrelay.replay import replay_dead_lettered_delivery
from hookrelay.schemas import DeliveryReplayRequest, DeliveryReplayResponse

router = APIRouter(prefix="/v1/deliveries", tags=["deliveries"])


@router.post(
    "/{delivery_id}/replay",
    response_model=DeliveryReplayResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_json_content_type)],
    responses=problem_responses(401, 404, 409, 415, 422, 500, 503),
)
async def replay_delivery(
    delivery_id: UUID,
    request: DeliveryReplayRequest,
    response: Response,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> DeliveryReplayResponse:
    """Atomically enqueue a new generation for one dead-lettered delivery."""

    try:
        replayed = await replay_dead_lettered_delivery(
            session,
            tenant_id=tenant.tenant_id,
            delivery_id=delivery_id,
            expected_dispatch_generation=request.expected_dispatch_generation,
        )
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        logging.getLogger("hookrelay.deliveries").warning(
            "delivery_replay_failed",
            extra={"error_type": type(exc).__name__},
        )
        raise database_unavailable_problem() from exc
    except Exception:
        await session.rollback()
        raise

    response.headers["Location"] = f"/v1/events/{replayed.event_id}"
    return replayed
