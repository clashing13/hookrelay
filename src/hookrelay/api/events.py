"""Authenticated durable event submission and inspection."""

import json
import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.dependencies import (
    AuthenticatedTenant,
    authenticate_tenant,
    database_unavailable_problem,
    get_session,
    get_settings_from_request,
    require_idempotency_key,
    require_json_content_type,
)
from hookrelay.api.errors import ApiProblem, problem_responses
from hookrelay.config import Settings
from hookrelay.ingestion import event_detail_response, ingest_event
from hookrelay.models import Event
from hookrelay.schemas import EventCreate, EventDetailResponse, EventResponse

router = APIRouter(prefix="/v1/events", tags=["events"])


@router.post(
    "",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_json_content_type)],
    responses=problem_responses(400, 401, 404, 409, 413, 415, 422, 500, 503),
)
async def create_event(
    request: EventCreate,
    response: Response,
    settings: Annotated[Settings, Depends(get_settings_from_request)],
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> EventResponse:
    """Commit accepted work; separate publisher and worker processes deliver it."""

    encoded_payload = json.dumps(
        request.payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded_payload) > settings.max_event_payload_bytes:
        raise ApiProblem(
            status=413,
            code="event_payload_too_large",
            title="Event payload too large",
            detail="The event payload exceeds the configured byte limit.",
        )

    try:
        result = await ingest_event(session, tenant, request, idempotency_key)
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        logging.getLogger("hookrelay.events").warning(
            "event_ingestion_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc
    except Exception:
        await session.rollback()
        raise

    if result.replayed:
        response.headers["Idempotency-Replayed"] = "true"
    response.headers["Location"] = f"/v1/events/{result.response.id}"
    return result.response


@router.get(
    "/{event_id}",
    response_model=EventDetailResponse,
    responses=problem_responses(401, 404, 500, 503),
)
async def get_event(
    event_id: UUID,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> EventDetailResponse:
    """Inspect one tenant-owned event without exposing outbox or secret data."""

    try:
        event = await session.scalar(
            select(Event).where(
                Event.id == event_id,
                Event.tenant_id == tenant.tenant_id,
            )
        )
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.events").warning(
            "event_lookup_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc

    if event is None:
        raise ApiProblem(
            status=404,
            code="resource_not_found",
            title="Resource not found",
            detail="The requested resource was not found.",
        )
    return await event_detail_response(session, event)
