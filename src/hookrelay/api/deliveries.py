"""Tenant-scoped delivery history, attempt inspection, and manual replay."""

import base64
import binascii
import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Annotated, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from hookrelay.api.dependencies import (
    AuthenticatedTenant,
    authenticate_tenant,
    database_unavailable_problem,
    get_session,
    require_json_content_type,
)
from hookrelay.api.errors import ApiProblem, problem_responses
from hookrelay.models import Delivery, DeliveryAttempt, Event, WebhookEndpoint
from hookrelay.replay import replay_dead_lettered_delivery
from hookrelay.schemas import (
    DeliveryAttemptDetailResponse,
    DeliveryAttemptHistoryResponse,
    DeliveryHistoryResponse,
    DeliveryInspectionResponse,
    DeliveryReplayRequest,
    DeliveryReplayResponse,
    DeliveryStatus,
)

router = APIRouter(prefix="/v1/deliveries", tags=["deliveries"])

CURSOR_VERSION = 1
MAX_CURSOR_LENGTH = 1024
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100

PageLimit = Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)]
OpaqueCursor = Annotated[str | None, Query(min_length=1, max_length=MAX_CURSOR_LENGTH)]
DeliveryStatusFilter = Annotated[DeliveryStatus | None, Query(alias="status")]


def _not_found() -> ApiProblem:
    return ApiProblem(
        status=404,
        code="resource_not_found",
        title="Resource not found",
        detail="The requested resource was not found.",
    )


def _invalid_cursor() -> ApiProblem:
    return ApiProblem(
        status=422,
        code="invalid_cursor",
        title="Invalid pagination cursor",
        detail="The pagination cursor is invalid for this request.",
    )


def _filter_fingerprint(values: dict[str, str | None]) -> str:
    encoded = json.dumps(
        values,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _delivery_filter_fingerprint(
    *,
    delivery_status: DeliveryStatus | None,
    endpoint_id: UUID | None,
    event_id: UUID | None,
) -> str:
    return _filter_fingerprint(
        {
            "endpoint_id": str(endpoint_id) if endpoint_id is not None else None,
            "event_id": str(event_id) if event_id is not None else None,
            "status": delivery_status,
        }
    )


def _attempt_filter_fingerprint(delivery_id: UUID) -> str:
    return _filter_fingerprint({"delivery_id": str(delivery_id)})


def _encode_cursor(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return base64.urlsafe_b64encode(encoded).rstrip(b"=").decode("ascii")


def _decode_cursor(cursor: str) -> dict[str, object]:
    try:
        encoded = cursor.encode("ascii")
        padded = encoded + (b"=" * (-len(encoded) % 4))
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
        loaded: object = json.loads(decoded.decode("utf-8"))
    except (UnicodeEncodeError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError):
        raise _invalid_cursor() from None
    if not isinstance(loaded, dict) or not all(isinstance(key, str) for key in loaded):
        raise _invalid_cursor()
    return cast("dict[str, object]", loaded)


def _encode_delivery_cursor(
    delivery: Delivery,
    *,
    filter_fingerprint: str,
) -> str:
    return _encode_cursor(
        {
            "created_at": delivery.created_at.astimezone(UTC).isoformat(timespec="microseconds"),
            "filter": filter_fingerprint,
            "id": str(delivery.id),
            "kind": "delivery",
            "v": CURSOR_VERSION,
        }
    )


def _decode_delivery_cursor(
    cursor: str,
    *,
    filter_fingerprint: str,
) -> tuple[datetime, UUID]:
    payload = _decode_cursor(cursor)
    if set(payload) != {"created_at", "filter", "id", "kind", "v"}:
        raise _invalid_cursor()
    version = payload["v"]
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version != CURSOR_VERSION
        or payload["kind"] != "delivery"
        or payload["filter"] != filter_fingerprint
        or not isinstance(payload["created_at"], str)
        or not isinstance(payload["id"], str)
    ):
        raise _invalid_cursor()
    try:
        created_at = datetime.fromisoformat(payload["created_at"])
        delivery_id = UUID(payload["id"])
    except ValueError:
        raise _invalid_cursor() from None
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise _invalid_cursor()
    return created_at.astimezone(UTC), delivery_id


def _encode_attempt_cursor(
    attempt: DeliveryAttempt,
    *,
    filter_fingerprint: str,
) -> str:
    return _encode_cursor(
        {
            "attempt_number": attempt.attempt_number,
            "filter": filter_fingerprint,
            "id": str(attempt.id),
            "kind": "attempt",
            "v": CURSOR_VERSION,
        }
    )


def _decode_attempt_cursor(
    cursor: str,
    *,
    filter_fingerprint: str,
) -> tuple[int, UUID]:
    payload = _decode_cursor(cursor)
    if set(payload) != {"attempt_number", "filter", "id", "kind", "v"}:
        raise _invalid_cursor()
    attempt_number = payload["attempt_number"]
    version = payload["v"]
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version != CURSOR_VERSION
        or payload["kind"] != "attempt"
        or payload["filter"] != filter_fingerprint
        or not isinstance(attempt_number, int)
        or isinstance(attempt_number, bool)
        or attempt_number < 1
        or not isinstance(payload["id"], str)
    ):
        raise _invalid_cursor()
    try:
        attempt_id = UUID(payload["id"])
    except ValueError:
        raise _invalid_cursor() from None
    return attempt_number, attempt_id


def _delivery_inspection_response(
    delivery: Delivery,
    *,
    event_type: str,
    endpoint_name: str,
    attempt_count: int,
    last_attempt_at: datetime | None,
) -> DeliveryInspectionResponse:
    return DeliveryInspectionResponse(
        id=delivery.id,
        event_id=delivery.event_id,
        event_type=event_type,
        endpoint_id=delivery.endpoint_id,
        endpoint_name=endpoint_name,
        status=cast(DeliveryStatus, delivery.status),
        dispatch_generation=delivery.dispatch_generation,
        created_at=delivery.created_at,
        next_attempt_at=delivery.next_attempt_at,
        dead_lettered_at=delivery.dead_lettered_at,
        dead_letter_reason=cast(
            "Literal['permanent_failure', 'attempts_exhausted', 'target_blocked'] | None",
            delivery.dead_letter_reason,
        ),
        attempt_count=attempt_count,
        last_attempt_at=last_attempt_at,
        replayable=delivery.status == "dead_lettered",
    )


def _attempt_detail_response(attempt: DeliveryAttempt) -> DeliveryAttemptDetailResponse:
    return DeliveryAttemptDetailResponse(
        id=attempt.id,
        delivery_id=attempt.delivery_id,
        attempt_number=attempt.attempt_number,
        dispatch_generation=attempt.dispatch_generation,
        is_circuit_probe=attempt.is_circuit_probe,
        started_at=attempt.started_at,
        finished_at=attempt.finished_at,
        outcome=cast(
            "Literal['succeeded', 'transient_failure', 'permanent_failure', 'abandoned'] | None",
            attempt.outcome,
        ),
        response_status_code=attempt.response_status_code,
        error_code=attempt.error_code,
        duration_ms=attempt.duration_ms,
    )


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


def _delivery_history_statement(
    tenant_id: UUID,
) -> Select[tuple[Delivery, str, str, int, datetime | None]]:
    attempt_stats = (
        select(
            DeliveryAttempt.delivery_id.label("delivery_id"),
            func.count(DeliveryAttempt.id).label("attempt_count"),
            func.max(DeliveryAttempt.started_at).label("last_attempt_at"),
        )
        .where(DeliveryAttempt.tenant_id == tenant_id)
        .group_by(DeliveryAttempt.delivery_id)
        .subquery()
    )
    return (
        select(
            Delivery,
            Event.event_type,
            WebhookEndpoint.name.label("endpoint_name"),
            func.coalesce(attempt_stats.c.attempt_count, 0).label("attempt_count"),
            attempt_stats.c.last_attempt_at,
        )
        .join(
            Event,
            (Event.id == Delivery.event_id) & (Event.tenant_id == Delivery.tenant_id),
        )
        .join(
            WebhookEndpoint,
            (WebhookEndpoint.id == Delivery.endpoint_id)
            & (WebhookEndpoint.tenant_id == Delivery.tenant_id),
        )
        .outerjoin(
            attempt_stats,
            attempt_stats.c.delivery_id == Delivery.id,
        )
        .where(Delivery.tenant_id == tenant_id)
    )


@router.get(
    "",
    response_model=DeliveryHistoryResponse,
    responses=problem_responses(401, 422, 500, 503),
)
async def list_deliveries(
    response: Response,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
    delivery_status: DeliveryStatusFilter = None,
    endpoint_id: UUID | None = None,
    event_id: UUID | None = None,
    limit: PageLimit = DEFAULT_PAGE_SIZE,
    cursor: OpaqueCursor = None,
) -> DeliveryHistoryResponse:
    """List tenant-owned delivery state with stable reverse keyset pagination."""

    filter_fingerprint = _delivery_filter_fingerprint(
        delivery_status=delivery_status,
        endpoint_id=endpoint_id,
        event_id=event_id,
    )
    position = (
        _decode_delivery_cursor(cursor, filter_fingerprint=filter_fingerprint)
        if cursor is not None
        else None
    )
    statement = _delivery_history_statement(tenant.tenant_id)
    if delivery_status is not None:
        statement = statement.where(Delivery.status == delivery_status)
    if endpoint_id is not None:
        statement = statement.where(Delivery.endpoint_id == endpoint_id)
    if event_id is not None:
        statement = statement.where(Delivery.event_id == event_id)
    if position is not None:
        statement = statement.where(
            or_(
                Delivery.created_at < position[0],
                and_(Delivery.created_at == position[0], Delivery.id < position[1]),
            )
        )
    statement = statement.order_by(Delivery.created_at.desc(), Delivery.id.desc()).limit(limit + 1)

    try:
        rows = (await session.execute(statement)).all()
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.deliveries").warning(
            "delivery_history_lookup_failed",
            extra={"error_type": type(exc).__name__},
        )
        raise database_unavailable_problem() from exc

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    items = [
        _delivery_inspection_response(
            cast(Delivery, row[0]),
            event_type=cast(str, row[1]),
            endpoint_name=cast(str, row[2]),
            attempt_count=int(cast(int, row[3])),
            last_attempt_at=cast("datetime | None", row[4]),
        )
        for row in page_rows
    ]
    next_cursor = None
    if has_more and page_rows:
        next_cursor = _encode_delivery_cursor(
            cast(Delivery, page_rows[-1][0]),
            filter_fingerprint=filter_fingerprint,
        )
    response.headers["Cache-Control"] = "no-store"
    return DeliveryHistoryResponse(items=items, next_cursor=next_cursor)


@router.get(
    "/{delivery_id}",
    response_model=DeliveryInspectionResponse,
    responses=problem_responses(401, 404, 500, 503),
)
async def get_delivery(
    delivery_id: UUID,
    response: Response,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> DeliveryInspectionResponse:
    """Inspect one tenant-owned delivery without exposing delivery credentials."""

    statement = _delivery_history_statement(tenant.tenant_id).where(Delivery.id == delivery_id)
    try:
        row = (await session.execute(statement)).first()
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.deliveries").warning(
            "delivery_lookup_failed",
            extra={"error_type": type(exc).__name__},
        )
        raise database_unavailable_problem() from exc
    if row is None:
        raise _not_found()
    response.headers["Cache-Control"] = "no-store"
    return _delivery_inspection_response(
        cast(Delivery, row[0]),
        event_type=cast(str, row[1]),
        endpoint_name=cast(str, row[2]),
        attempt_count=int(cast(int, row[3])),
        last_attempt_at=cast("datetime | None", row[4]),
    )


@router.get(
    "/{delivery_id}/attempts",
    response_model=DeliveryAttemptHistoryResponse,
    responses=problem_responses(401, 404, 422, 500, 503),
)
async def list_delivery_attempts(
    delivery_id: UUID,
    response: Response,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
    limit: PageLimit = DEFAULT_PAGE_SIZE,
    cursor: OpaqueCursor = None,
) -> DeliveryAttemptHistoryResponse:
    """List immutable attempts across every dispatch generation of one delivery."""

    filter_fingerprint = _attempt_filter_fingerprint(delivery_id)
    position = (
        _decode_attempt_cursor(cursor, filter_fingerprint=filter_fingerprint)
        if cursor is not None
        else None
    )
    try:
        owned_delivery_id = await session.scalar(
            select(Delivery.id).where(
                Delivery.id == delivery_id,
                Delivery.tenant_id == tenant.tenant_id,
            )
        )
        if owned_delivery_id is None:
            raise _not_found()
        statement = select(DeliveryAttempt).where(
            DeliveryAttempt.delivery_id == delivery_id,
            DeliveryAttempt.tenant_id == tenant.tenant_id,
        )
        if position is not None:
            statement = statement.where(
                or_(
                    DeliveryAttempt.attempt_number < position[0],
                    and_(
                        DeliveryAttempt.attempt_number == position[0],
                        DeliveryAttempt.id < position[1],
                    ),
                )
            )
        statement = statement.order_by(
            DeliveryAttempt.attempt_number.desc(),
            DeliveryAttempt.id.desc(),
        ).limit(limit + 1)
        attempts = list(await session.scalars(statement))
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.deliveries").warning(
            "delivery_attempt_history_lookup_failed",
            extra={"error_type": type(exc).__name__},
        )
        raise database_unavailable_problem() from exc

    has_more = len(attempts) > limit
    page_attempts = attempts[:limit]
    next_cursor = None
    if has_more and page_attempts:
        next_cursor = _encode_attempt_cursor(
            page_attempts[-1],
            filter_fingerprint=filter_fingerprint,
        )
    response.headers["Cache-Control"] = "no-store"
    return DeliveryAttemptHistoryResponse(
        items=[_attempt_detail_response(attempt) for attempt in page_attempts],
        next_cursor=next_cursor,
    )


@router.get(
    "/{delivery_id}/attempts/{attempt_id}",
    response_model=DeliveryAttemptDetailResponse,
    responses=problem_responses(401, 404, 500, 503),
)
async def get_delivery_attempt(
    delivery_id: UUID,
    attempt_id: UUID,
    response: Response,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> DeliveryAttemptDetailResponse:
    """Inspect one tenant-owned attempt while keeping claim state private."""

    try:
        attempt = await session.scalar(
            select(DeliveryAttempt).where(
                DeliveryAttempt.id == attempt_id,
                DeliveryAttempt.delivery_id == delivery_id,
                DeliveryAttempt.tenant_id == tenant.tenant_id,
            )
        )
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.deliveries").warning(
            "delivery_attempt_lookup_failed",
            extra={"error_type": type(exc).__name__},
        )
        raise database_unavailable_problem() from exc
    if attempt is None:
        raise _not_found()
    response.headers["Cache-Control"] = "no-store"
    return _attempt_detail_response(attempt)
