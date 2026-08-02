"""Tenant-scoped webhook endpoint creation and inspection."""

import logging
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.dependencies import (
    AuthenticatedTenant,
    authenticate_tenant,
    database_unavailable_problem,
    get_secret_cipher,
    get_session,
    get_settings_from_request,
    require_json_content_type,
)
from hookrelay.api.errors import ApiProblem, InvalidParameter, problem_responses
from hookrelay.config import Settings
from hookrelay.models import EndpointSigningSecret, WebhookEndpoint
from hookrelay.schemas import EndpointCreate, EndpointCreatedResponse, EndpointResponse
from hookrelay.security import SecretCipher, generate_signing_secret

router = APIRouter(prefix="/v1/endpoints", tags=["endpoints"])


def _endpoint_response(endpoint: WebhookEndpoint) -> EndpointResponse:
    return EndpointResponse(
        id=endpoint.id,
        name=endpoint.name,
        url=endpoint.url,
        enabled=endpoint.is_active,
        created_at=endpoint.created_at,
    )


@router.post(
    "",
    response_model=EndpointCreatedResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_json_content_type)],
    responses=problem_responses(401, 415, 422, 500, 503),
)
async def create_endpoint(
    request: EndpointCreate,
    response: Response,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings_from_request)],
    cipher: Annotated[SecretCipher, Depends(get_secret_cipher)],
) -> EndpointCreatedResponse:
    """Store a destination and return its generated signing secret once."""

    if settings.environment in {"staging", "production"} and request.url.scheme != "https":
        raise ApiProblem(
            status=422,
            code="validation_error",
            title="Request validation failed",
            detail="One or more request fields are invalid.",
            errors=[
                InvalidParameter(
                    pointer="/body/url",
                    code="https_required",
                    message="HTTPS is required outside local and test environments.",
                )
            ],
        )

    endpoint_id = uuid4()
    secret_id = uuid4()
    signing_secret = generate_signing_secret()
    endpoint = WebhookEndpoint(
        id=endpoint_id,
        tenant_id=tenant.tenant_id,
        name=request.name,
        url=str(request.url),
    )
    stored_secret = EndpointSigningSecret(
        id=secret_id,
        tenant_id=tenant.tenant_id,
        endpoint_id=endpoint_id,
        version=1,
        encryption_key_version=cipher.key_version,
        ciphertext=cipher.encrypt_endpoint_secret(
            tenant.tenant_id,
            endpoint_id,
            secret_id,
            1,
            signing_secret,
        ),
        secret_hint=signing_secret[-4:],
    )
    try:
        session.add(endpoint)
        await session.flush()
        session.add(stored_secret)
        await session.flush()
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        logging.getLogger("hookrelay.endpoints").warning(
            "endpoint_create_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc

    response.headers["Location"] = f"/v1/endpoints/{endpoint.id}"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    public = _endpoint_response(endpoint)
    return EndpointCreatedResponse(**public.model_dump(), signing_secret=signing_secret)


@router.get(
    "/{endpoint_id}",
    response_model=EndpointResponse,
    responses=problem_responses(401, 404, 500, 503),
)
async def get_endpoint(
    endpoint_id: UUID,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> EndpointResponse:
    """Return tenant-owned endpoint metadata without its signing secret."""

    try:
        endpoint = await session.scalar(
            select(WebhookEndpoint).where(
                WebhookEndpoint.id == endpoint_id,
                WebhookEndpoint.tenant_id == tenant.tenant_id,
            )
        )
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.endpoints").warning(
            "endpoint_lookup_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc
    if endpoint is None:
        raise ApiProblem(
            status=404,
            code="resource_not_found",
            title="Resource not found",
            detail="The requested resource was not found.",
        )
    return _endpoint_response(endpoint)
