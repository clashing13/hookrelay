"""Protected deployment bootstrap for the first tenant credential."""

import logging
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.dependencies import (
    AuthenticatedTenant,
    authenticate_tenant,
    database_unavailable_problem,
    get_session,
    require_bootstrap_token,
    require_json_content_type,
)
from hookrelay.api.errors import ApiProblem, problem_responses
from hookrelay.models import ApiKey, Tenant
from hookrelay.schemas import (
    IssuedApiKeyResponse,
    TenantBootstrapResponse,
    TenantCreate,
    TenantResponse,
)
from hookrelay.security import generate_api_key

router = APIRouter(prefix="/v1/bootstrap", tags=["bootstrap"])
tenant_router = APIRouter(prefix="/v1/tenant", tags=["tenant"])


@router.post(
    "/tenants",
    response_model=TenantBootstrapResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_bootstrap_token), Depends(require_json_content_type)],
    responses=problem_responses(401, 404, 415, 422, 500, 503),
)
async def create_tenant(
    request: TenantCreate,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TenantBootstrapResponse:
    """Create a tenant and return its initial API key exactly once."""

    generated_key = generate_api_key()
    tenant = Tenant(id=uuid4(), name=request.name)
    api_key = ApiKey(
        id=uuid4(),
        tenant_id=tenant.id,
        name=request.initial_api_key_name,
        public_id=generated_key.public_id,
        secret_hash=generated_key.secret_hash,
        secret_last_four=generated_key.secret_last_four,
    )
    try:
        session.add(tenant)
        await session.flush()
        session.add(api_key)
        await session.flush()
        await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        logging.getLogger("hookrelay.bootstrap").warning(
            "tenant_bootstrap_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc

    response.headers["Location"] = "/v1/tenant"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return TenantBootstrapResponse(
        tenant=TenantResponse(id=tenant.id, name=tenant.name, created_at=tenant.created_at),
        api_key=IssuedApiKeyResponse(
            id=api_key.id,
            name=api_key.name,
            key=generated_key.token,
            created_at=api_key.created_at,
        ),
    )


@tenant_router.get(
    "",
    response_model=TenantResponse,
    responses=problem_responses(401, 404, 500, 503),
)
async def get_tenant(
    tenant_context: Annotated[AuthenticatedTenant, Depends(authenticate_tenant)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TenantResponse:
    """Return only the tenant selected by the authenticated API key."""

    try:
        tenant = await session.scalar(
            select(Tenant).where(
                Tenant.id == tenant_context.tenant_id,
                Tenant.is_active.is_(True),
            )
        )
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.tenants").warning(
            "tenant_lookup_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc
    if tenant is None:
        raise ApiProblem(
            status=404,
            code="resource_not_found",
            title="Resource not found",
            detail="The requested resource was not found.",
        )
    return TenantResponse(id=tenant.id, name=tenant.name, created_at=tenant.created_at)
