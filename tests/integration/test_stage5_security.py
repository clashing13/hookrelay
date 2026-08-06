"""Real-PostgreSQL evidence for Stage 5 endpoint-secret security invariants."""

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx2 import ASGITransport, AsyncClient, Response
from sqlalchemy import func, select

from hookrelay.config import Settings
from hookrelay.database import PostgresDatabase
from hookrelay.main import create_app
from hookrelay.models import Delivery, EndpointSigningSecret, EndpointTrafficControl
from hookrelay.schemas import (
    EndpointCreatedResponse,
    EndpointSigningSecretRotatedResponse,
    TenantBootstrapResponse,
)
from hookrelay.security import SecretCipher

pytestmark = [pytest.mark.integration, pytest.mark.security]

BOOTSTRAP_TOKEN = "hookrelay-stage5-integration-bootstrap-token"


@dataclass(frozen=True, slots=True)
class ProvisionedTenant:
    id: UUID
    api_key: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class ProvisionedEndpoint:
    id: UUID
    signing_secret: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class SecretState:
    id: UUID
    version: int
    ciphertext: bytes = field(repr=False)
    retired_at: datetime | None


def _configured_database_url() -> str:
    database_url = os.getenv("HOOKRELAY_TEST_DATABASE_URL")
    if database_url is None:
        if os.getenv("CI") == "true":
            pytest.fail("CI must configure HOOKRELAY_TEST_DATABASE_URL")
        pytest.skip("HOOKRELAY_TEST_DATABASE_URL is not configured")
    return database_url


def _upgrade_database(database_url: str) -> None:
    environment = os.environ.copy()
    environment["HOOKRELAY_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture(scope="module")
def database_url() -> str:
    """Use only the explicitly configured disposable PostgreSQL database."""

    return _configured_database_url()


@pytest.fixture(scope="module", autouse=True)
def migrated_database(database_url: str) -> None:
    """Apply all reviewed migrations without relying on module execution order."""

    _upgrade_database(database_url)


@pytest.fixture
def stage5_settings(database_url: str) -> Settings:
    return Settings(
        environment="test",
        database_url=database_url,
        database_pool_size=10,
        database_max_overflow=10,
        bootstrap_enabled=True,
        bootstrap_token=BOOTSTRAP_TOKEN,
        _env_file=None,
    )


@pytest_asyncio.fixture
async def running_app(stage5_settings: Settings) -> AsyncIterator[FastAPI]:
    app = create_app(stage5_settings)
    async with LifespanManager(app):
        yield app


@pytest_asyncio.fixture
async def assertion_database(stage5_settings: Settings) -> AsyncIterator[PostgresDatabase]:
    """Use another engine so assertions observe only committed API transactions."""

    database = PostgresDatabase(stage5_settings)
    try:
        yield database
    finally:
        await database.dispose()


@asynccontextmanager
async def _client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


async def _provision_tenant(client: AsyncClient, label: str) -> ProvisionedTenant:
    response = await client.post(
        "/v1/bootstrap/tenants",
        headers={"Authorization": f"Bearer {BOOTSTRAP_TOKEN}"},
        json={
            "name": f"{label}-{uuid4()}",
            "initial_api_key_name": "stage5-integration",
        },
    )
    assert response.status_code == 201, response.text
    issued = TenantBootstrapResponse.model_validate(response.json())
    return ProvisionedTenant(id=issued.tenant.id, api_key=issued.api_key.key)


async def _provision_endpoint(
    client: AsyncClient,
    tenant: ProvisionedTenant,
    label: str,
) -> ProvisionedEndpoint:
    response = await client.post(
        "/v1/endpoints",
        headers={"Authorization": f"Bearer {tenant.api_key}"},
        json={
            "name": f"{label}-{uuid4()}",
            "url": f"https://receiver/{label}/{uuid4()}",
        },
    )
    assert response.status_code == 201, response.text
    created = EndpointCreatedResponse.model_validate(response.json())
    return ProvisionedEndpoint(id=created.id, signing_secret=created.signing_secret)


async def _submit_event(
    client: AsyncClient,
    tenant: ProvisionedTenant,
    endpoint: ProvisionedEndpoint,
    label: str,
) -> UUID:
    response = await client.post(
        "/v1/events",
        headers={
            "Authorization": f"Bearer {tenant.api_key}",
            "Idempotency-Key": f"stage5-{label}-{uuid4()}",
        },
        json={
            "type": "stage5.rotation",
            "payload": {"label": label},
            "endpoint_ids": [str(endpoint.id)],
        },
    )
    assert response.status_code == 201, response.text
    deliveries = response.json()["deliveries"]
    assert len(deliveries) == 1
    return UUID(deliveries[0]["id"])


async def _rotate(
    client: AsyncClient,
    tenant: ProvisionedTenant,
    endpoint_id: UUID,
    expected_version: int,
) -> Response:
    return await client.post(
        f"/v1/endpoints/{endpoint_id}/signing-secret/rotate",
        headers={"Authorization": f"Bearer {tenant.api_key}"},
        json={"expected_active_version": expected_version},
    )


async def _rotate_in_separate_client(
    app: FastAPI,
    tenant: ProvisionedTenant,
    endpoint_id: UUID,
    expected_version: int,
) -> Response:
    async with _client(app) as client:
        return await _rotate(client, tenant, endpoint_id, expected_version)


async def _endpoint_secrets(
    database: PostgresDatabase,
    tenant_id: UUID,
    endpoint_id: UUID,
) -> list[EndpointSigningSecret]:
    async with database.session_factory() as session:
        return list(
            await session.scalars(
                select(EndpointSigningSecret)
                .where(
                    EndpointSigningSecret.tenant_id == tenant_id,
                    EndpointSigningSecret.endpoint_id == endpoint_id,
                )
                .order_by(EndpointSigningSecret.version)
            )
        )


async def _secret_states(
    database: PostgresDatabase,
    tenant_id: UUID,
    endpoint_id: UUID,
) -> list[SecretState]:
    return [
        SecretState(
            id=secret.id,
            version=secret.version,
            ciphertext=secret.ciphertext,
            retired_at=secret.retired_at,
        )
        for secret in await _endpoint_secrets(database, tenant_id, endpoint_id)
    ]


@pytest.mark.asyncio
async def test_endpoint_control_row_and_rotation_preserve_delivery_secret_snapshots(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
    stage5_settings: Settings,
) -> None:
    async with _client(running_app) as client:
        tenant = await _provision_tenant(client, "snapshot")
        endpoint = await _provision_endpoint(client, tenant, "snapshot")

        async with assertion_database.session_factory() as session:
            control = await session.scalar(
                select(EndpointTrafficControl).where(
                    EndpointTrafficControl.tenant_id == tenant.id,
                    EndpointTrafficControl.endpoint_id == endpoint.id,
                )
            )
        assert control is not None
        assert control.rate_window_started_at is None
        assert control.rate_window_count == 0
        assert control.circuit_state == "closed"
        assert control.circuit_consecutive_failures == 0
        assert control.circuit_opened_at is None
        assert control.probe_token is None
        assert control.probe_expires_at is None

        before_rotation = await _submit_event(client, tenant, endpoint, "before")

        initial_secrets = await _endpoint_secrets(assertion_database, tenant.id, endpoint.id)
        assert len(initial_secrets) == 1
        original = initial_secrets[0]
        original_ciphertext = original.ciphertext

        rotated_response = await _rotate(client, tenant, endpoint.id, expected_version=1)
        assert rotated_response.status_code == 200, rotated_response.text
        rotated = EndpointSigningSecretRotatedResponse.model_validate(rotated_response.json())
        after_rotation = await _submit_event(client, tenant, endpoint, "after")

    async with assertion_database.session_factory() as session:
        old_delivery = await session.get(Delivery, before_rotation)
        new_delivery = await session.get(Delivery, after_rotation)
        active_count = await session.scalar(
            select(func.count())
            .select_from(EndpointSigningSecret)
            .where(
                EndpointSigningSecret.tenant_id == tenant.id,
                EndpointSigningSecret.endpoint_id == endpoint.id,
                EndpointSigningSecret.retired_at.is_(None),
            )
        )

    secrets = await _endpoint_secrets(assertion_database, tenant.id, endpoint.id)
    assert [secret.version for secret in secrets] == [1, 2]
    old_secret, new_secret = secrets
    assert old_secret.id == original.id
    assert old_secret.ciphertext == original_ciphertext
    assert old_secret.retired_at is not None
    assert new_secret.id != old_secret.id
    assert new_secret.retired_at is None
    assert active_count == 1

    assert old_delivery is not None
    assert new_delivery is not None
    assert old_delivery.signing_secret_id == old_secret.id
    assert new_delivery.signing_secret_id == new_secret.id

    cipher = SecretCipher(
        stage5_settings.secret_encryption_key_bytes(),
        stage5_settings.secret_encryption_key_version,
    )
    assert (
        cipher.decrypt_endpoint_secret(
            tenant.id,
            endpoint.id,
            old_secret.id,
            old_secret.version,
            old_secret.encryption_key_version,
            old_secret.ciphertext,
        )
        == endpoint.signing_secret
    )
    assert (
        cipher.decrypt_endpoint_secret(
            tenant.id,
            endpoint.id,
            new_secret.id,
            new_secret.version,
            new_secret.encryption_key_version,
            new_secret.ciphertext,
        )
        == rotated.signing_secret
    )
    assert endpoint.signing_secret.encode() not in old_secret.ciphertext
    assert rotated.signing_secret.encode() not in new_secret.ciphertext


@pytest.mark.asyncio
@pytest.mark.concurrency
async def test_concurrent_rotations_with_one_expected_version_have_one_winner(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
) -> None:
    async with _client(running_app) as client:
        tenant = await _provision_tenant(client, "concurrent-rotation")
        endpoint = await _provision_endpoint(client, tenant, "concurrent-rotation")

    responses = await asyncio.gather(
        _rotate_in_separate_client(running_app, tenant, endpoint.id, 1),
        _rotate_in_separate_client(running_app, tenant, endpoint.id, 1),
    )

    assert sorted(response.status_code for response in responses) == [200, 409]
    winner = next(response for response in responses if response.status_code == 200)
    conflict = next(response for response in responses if response.status_code == 409)
    assert EndpointSigningSecretRotatedResponse.model_validate(winner.json()).version == 2
    assert conflict.json()["code"] == "signing_secret_version_conflict"
    assert conflict.headers["hookrelay-active-secret-version"] == "2"

    secrets = await _endpoint_secrets(assertion_database, tenant.id, endpoint.id)
    assert [secret.version for secret in secrets] == [1, 2]
    assert secrets[0].retired_at is not None
    assert secrets[1].retired_at is None
    assert sum(secret.retired_at is None for secret in secrets) == 1


@pytest.mark.asyncio
async def test_cross_tenant_rotation_is_opaque_and_cannot_mutate_secret_rows(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
) -> None:
    async with _client(running_app) as client:
        tenant_a = await _provision_tenant(client, "rotation-owner")
        tenant_b = await _provision_tenant(client, "rotation-outsider")
        endpoint_a = await _provision_endpoint(client, tenant_a, "rotation-owner")
        before = await _secret_states(assertion_database, tenant_a.id, endpoint_a.id)

        response = await _rotate(client, tenant_b, endpoint_a.id, expected_version=1)

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json() == {
        "type": "urn:hookrelay:problem:resource-not-found",
        "title": "Resource not found",
        "status": 404,
        "code": "resource_not_found",
        "detail": "The requested resource was not found.",
    }
    assert "hookrelay-active-secret-version" not in response.headers
    assert str(tenant_a.id) not in response.text
    assert endpoint_a.signing_secret not in response.text
    assert await _secret_states(assertion_database, tenant_a.id, endpoint_a.id) == before
