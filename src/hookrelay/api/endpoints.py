"""Tenant-scoped webhook endpoint creation and inspection."""

import logging
from datetime import datetime
from typing import Annotated, cast
from uuid import UUID, uuid4

import httpx2 as httpx
from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.dependencies import (
    AuthenticatedTenant,
    authenticate_tenant,
    database_unavailable_problem,
    get_destination_policy,
    get_secret_cipher,
    get_session,
    require_json_content_type,
)
from hookrelay.api.errors import ApiProblem, InvalidParameter, problem_responses
from hookrelay.destination_policy import (
    DestinationPolicy,
    DestinationPolicyBlocked,
    DestinationResolutionError,
)
from hookrelay.models import EndpointSigningSecret, EndpointTrafficControl, WebhookEndpoint
from hookrelay.schemas import (
    EndpointCreate,
    EndpointCreatedResponse,
    EndpointResponse,
    EndpointSigningSecretRotatedResponse,
    EndpointSigningSecretRotateRequest,
)
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
    cipher: Annotated[SecretCipher, Depends(get_secret_cipher)],
    destination_policy: Annotated[DestinationPolicy, Depends(get_destination_policy)],
) -> EndpointCreatedResponse:
    """Store a destination and return its generated signing secret once."""

    try:
        target_url = httpx.URL(str(request.url))
        hostname = destination_policy.validate_url(target_url)
        if not destination_policy.is_local_exemption(hostname):
            port = target_url.port or (443 if target_url.scheme == "https" else 80)
            await destination_policy.resolve(hostname, port)
    except (DestinationPolicyBlocked, DestinationResolutionError) as exc:
        raise ApiProblem(
            status=422,
            code="validation_error",
            title="Request validation failed",
            detail="One or more request fields are invalid.",
            errors=[
                InvalidParameter(
                    pointer="/body/url",
                    code="destination_not_allowed",
                    message="The destination URL cannot be used by this deployment.",
                )
            ],
        ) from exc

    endpoint_id = uuid4()
    secret_id = uuid4()
    signing_secret = generate_signing_secret()
    endpoint = WebhookEndpoint(
        id=endpoint_id,
        tenant_id=tenant.tenant_id,
        name=request.name,
        url=str(target_url),
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
    traffic_control = EndpointTrafficControl(
        tenant_id=tenant.tenant_id,
        endpoint_id=endpoint_id,
    )
    try:
        session.add(endpoint)
        await session.flush()
        session.add_all([stored_secret, traffic_control])
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


@router.post(
    "/{endpoint_id}/signing-secret/rotate",
    response_model=EndpointSigningSecretRotatedResponse,
    dependencies=[Depends(require_json_content_type)],
    responses=problem_responses(401, 404, 409, 415, 422, 500, 503),
)
async def rotate_endpoint_signing_secret(
    endpoint_id: UUID,
    request: EndpointSigningSecretRotateRequest,
    response: Response,
    tenant: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
    cipher: Annotated[SecretCipher, Depends(get_secret_cipher)],
) -> EndpointSigningSecretRotatedResponse:
    """Atomically replace the active secret while retaining delivery snapshots."""

    try:
        endpoint = await session.scalar(
            select(WebhookEndpoint)
            .where(
                WebhookEndpoint.id == endpoint_id,
                WebhookEndpoint.tenant_id == tenant.tenant_id,
            )
            .with_for_update()
        )
        if endpoint is None:
            raise ApiProblem(
                status=404,
                code="resource_not_found",
                title="Resource not found",
                detail="The requested resource was not found.",
            )
        active_secret = await session.scalar(
            select(EndpointSigningSecret)
            .where(
                EndpointSigningSecret.tenant_id == tenant.tenant_id,
                EndpointSigningSecret.endpoint_id == endpoint.id,
                EndpointSigningSecret.retired_at.is_(None),
            )
            .with_for_update()
        )
        if active_secret is None:
            raise RuntimeError("endpoint has no active signing secret")
        if active_secret.version != request.expected_active_version:
            raise ApiProblem(
                status=409,
                code="signing_secret_version_conflict",
                title="Signing secret version conflict",
                detail="The endpoint signing secret changed before this request completed.",
                headers={"HookRelay-Active-Secret-Version": str(active_secret.version)},
            )

        rotated_at = cast(
            "datetime | None",
            await session.scalar(select(func.clock_timestamp())),
        )
        if rotated_at is None:
            raise RuntimeError("PostgreSQL did not return its current time")
        active_secret.retired_at = rotated_at
        await session.flush()

        new_version = active_secret.version + 1
        new_secret_id = uuid4()
        plaintext = generate_signing_secret()
        replacement = EndpointSigningSecret(
            id=new_secret_id,
            tenant_id=tenant.tenant_id,
            endpoint_id=endpoint.id,
            version=new_version,
            encryption_key_version=cipher.key_version,
            ciphertext=cipher.encrypt_endpoint_secret(
                tenant.tenant_id,
                endpoint.id,
                new_secret_id,
                new_version,
                plaintext,
            ),
            secret_hint=plaintext[-4:],
        )
        session.add(replacement)
        await session.flush()
        await session.commit()
    except ApiProblem:
        await session.rollback()
        raise
    except SQLAlchemyError as exc:
        await session.rollback()
        logging.getLogger("hookrelay.endpoints").warning(
            "endpoint_secret_rotation_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc
    except Exception:
        await session.rollback()
        raise

    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return EndpointSigningSecretRotatedResponse(
        endpoint_id=endpoint.id,
        version=replacement.version,
        signing_secret=plaintext,
        created_at=replacement.created_at,
    )


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
