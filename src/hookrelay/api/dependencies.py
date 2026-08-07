"""FastAPI dependencies for sessions, authentication, and request contracts."""

import hmac
import logging
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Protocol, cast
from uuid import UUID

from fastapi import Depends, Header, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hookrelay.api.errors import ApiProblem
from hookrelay.config import Settings
from hookrelay.destination_policy import DestinationPolicy
from hookrelay.models import ApiKey, Tenant
from hookrelay.security import (
    DUMMY_API_KEY_SECRET_HASH,
    SecretCipher,
    parse_api_key,
    verify_api_key_secret,
)

IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
bearer_scheme = HTTPBearer(
    auto_error=False,
    scheme_name="HookRelayBearer",
    description="A tenant API key, or the deployment bootstrap token on bootstrap routes.",
)


class SessionDatabase(Protocol):
    """Database behavior required by domain API routes."""

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]: ...


@dataclass(frozen=True, slots=True)
class AuthenticatedTenant:
    """Tenant context derived exclusively from a verified API key."""

    tenant_id: UUID
    api_key_id: UUID


def invalid_credentials_problem() -> ApiProblem:
    return ApiProblem(
        status=401,
        code="invalid_credentials",
        title="Authentication failed",
        detail="A valid HookRelay bearer credential is required.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def database_unavailable_problem() -> ApiProblem:
    return ApiProblem(
        status=503,
        code="database_unavailable",
        title="Database unavailable",
        detail="The request could not be durably processed at this time.",
    )


def get_settings_from_request(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    database = cast(SessionDatabase, request.app.state.database)
    async with database.session_factory() as session:
        yield session


def get_secret_cipher(request: Request) -> SecretCipher:
    return cast(SecretCipher, request.app.state.secret_cipher)


def get_destination_policy(request: Request) -> DestinationPolicy:
    return cast(DestinationPolicy, request.app.state.destination_policy)


async def require_json_content_type(
    content_type: Annotated[str, Header(alias="Content-Type")],
) -> None:
    """Keep JSON mutation routes on one explicit media-type contract."""

    media_type = content_type.split(";", maxsplit=1)[0].strip().lower()
    if media_type != "application/json":
        raise ApiProblem(
            status=415,
            code="unsupported_media_type",
            title="Unsupported media type",
            detail="This endpoint requires an application/json request body.",
        )


async def require_bootstrap_token(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
    settings: Annotated[Settings, Depends(get_settings_from_request)],
) -> None:
    """Protect the explicitly enabled deployment bootstrap surface."""

    if not settings.bootstrap_enabled:
        raise ApiProblem(
            status=404,
            code="bootstrap_disabled",
            title="Resource not found",
            detail="The bootstrap endpoint is disabled.",
        )

    expected = settings.bootstrap_token
    if (
        credentials is None
        or credentials.scheme.lower() != "bearer"
        or expected is None
        or not hmac.compare_digest(
            credentials.credentials.encode("utf-8"),
            expected.get_secret_value().encode("ascii"),
        )
    ):
        raise invalid_credentials_problem()


async def authenticate_tenant(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AuthenticatedTenant:
    """Verify a bearer key and derive tenant scope without accepting a tenant ID."""

    if credentials is None or credentials.scheme.lower() != "bearer":
        raise invalid_credentials_problem()
    parsed = parse_api_key(credentials.credentials)
    if parsed is None:
        raise invalid_credentials_problem()

    try:
        api_key = await session.scalar(
            select(ApiKey)
            .join(Tenant, Tenant.id == ApiKey.tenant_id)
            .where(
                ApiKey.public_id == parsed.public_id,
                Tenant.is_active.is_(True),
            )
        )
    except SQLAlchemyError as exc:
        logging.getLogger("hookrelay.auth").warning(
            "api_key_lookup_failed", extra={"error_type": type(exc).__name__}
        )
        raise database_unavailable_problem() from exc

    expected_hash = api_key.secret_hash if api_key is not None else DUMMY_API_KEY_SECRET_HASH
    secret_is_valid = verify_api_key_secret(parsed.secret, expected_hash)
    if api_key is None or not secret_is_valid or api_key.revoked_at is not None:
        raise invalid_credentials_problem()
    if api_key.expires_at is not None and api_key.expires_at <= datetime.now(UTC):
        raise invalid_credentials_problem()
    return AuthenticatedTenant(tenant_id=api_key.tenant_id, api_key_id=api_key.id)


async def require_idempotency_key(
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> str:
    """Require the documented bounded ASCII grammar before touching PostgreSQL."""

    if IDEMPOTENCY_KEY_PATTERN.fullmatch(idempotency_key) is None:
        raise ApiProblem(
            status=400,
            code="invalid_idempotency_key",
            title="Invalid idempotency key",
            detail="The Idempotency-Key header has an invalid format.",
        )
    return idempotency_key
