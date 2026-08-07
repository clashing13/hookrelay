"""Real-PostgreSQL evidence for Stage 2 durability and concurrency invariants."""

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx2 import ASGITransport, AsyncClient, Response
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import hookrelay.ingestion as ingestion_module
from hookrelay.config import Settings
from hookrelay.database import PostgresDatabase
from hookrelay.main import create_app
from hookrelay.models import (
    Delivery,
    DeliveryAttempt,
    EndpointSigningSecret,
    Event,
    OutboxMessage,
)
from hookrelay.schemas import EndpointCreatedResponse, TenantBootstrapResponse

pytestmark = pytest.mark.integration

BOOTSTRAP_TOKEN = "hookrelay-integration-bootstrap-token-2026"
CONCURRENT_REQUESTS = 12


@dataclass(frozen=True, slots=True)
class ProvisionedTenant:
    """Non-secret tenant identifiers plus its one-time test credential."""

    id: UUID
    api_key_id: UUID
    api_key: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class ProvisionedEndpoint:
    """Tenant-owned endpoint metadata needed by ingestion tests."""

    id: UUID
    url: str


@dataclass(frozen=True, slots=True)
class TenantRowCounts:
    """Durable row counts that define a Stage 2 acceptance outcome."""

    events: int
    deliveries: int
    outbox_messages: int
    delivery_attempts: int


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
    """Make this module independent of test collection order."""

    _upgrade_database(database_url)


@pytest_asyncio.fixture
async def running_app(database_url: str) -> AsyncIterator[FastAPI]:
    """Run a Stage 2 app with enough pooled connections for genuine overlap."""

    settings = Settings(
        environment="test",
        database_url=database_url,
        database_pool_size=10,
        database_max_overflow=10,
        bootstrap_enabled=True,
        bootstrap_token=BOOTSTRAP_TOKEN,
        _env_file=None,
    )
    app = create_app(settings)
    async with LifespanManager(app):
        yield app


@pytest_asyncio.fixture
async def assertion_database(database_url: str) -> AsyncIterator[PostgresDatabase]:
    """Use a separate engine so assertions observe only committed API work."""

    database = PostgresDatabase(
        Settings(environment="test", database_url=database_url, _env_file=None)
    )
    try:
        yield database
    finally:
        await database.dispose()


@asynccontextmanager
async def _client(
    app: FastAPI,
    *,
    raise_app_exceptions: bool = True,
) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


async def _provision_tenant(client: AsyncClient, label: str) -> ProvisionedTenant:
    response = await client.post(
        "/v1/bootstrap/tenants",
        headers={"Authorization": f"Bearer {BOOTSTRAP_TOKEN}"},
        json={
            "name": f"{label}-{uuid4()}",
            "initial_api_key_name": "integration",
        },
    )
    assert response.status_code == 201, response.text
    issued = TenantBootstrapResponse.model_validate(response.json())
    return ProvisionedTenant(
        id=issued.tenant.id,
        api_key_id=issued.api_key.id,
        api_key=issued.api_key.key,
    )


async def _provision_endpoint(
    client: AsyncClient,
    tenant: ProvisionedTenant,
    label: str,
) -> ProvisionedEndpoint:
    url = f"https://receiver.example/{label}/{uuid4()}"
    response = await client.post(
        "/v1/endpoints",
        headers={"Authorization": f"Bearer {tenant.api_key}"},
        json={"name": f"{label}-{uuid4()}", "url": url},
    )
    assert response.status_code == 201, response.text
    created = EndpointCreatedResponse.model_validate(response.json())
    assert created.signing_secret.startswith("whsec_")
    return ProvisionedEndpoint(id=created.id, url=created.url)


def _event_body(
    endpoints: list[ProvisionedEndpoint],
    *,
    variant: str = "original",
) -> dict[str, object]:
    return {
        "type": "order.created",
        "payload": {"order_id": "ord-integration", "variant": variant},
        "endpoint_ids": [str(endpoint.id) for endpoint in endpoints],
    }


async def _submit_event(
    client: AsyncClient,
    tenant: ProvisionedTenant,
    idempotency_key: str,
    body: dict[str, object],
) -> Response:
    return await client.post(
        "/v1/events",
        headers={
            "Authorization": f"Bearer {tenant.api_key}",
            "Idempotency-Key": idempotency_key,
        },
        json=body,
    )


async def _submit_event_in_separate_client(
    app: FastAPI,
    tenant: ProvisionedTenant,
    idempotency_key: str,
    body: dict[str, object],
) -> Response:
    async with _client(app) as client:
        return await _submit_event(client, tenant, idempotency_key, body)


async def _tenant_row_counts(
    database: PostgresDatabase,
    tenant_id: UUID,
) -> TenantRowCounts:
    async with database.session_factory() as session:
        event_count = await session.scalar(
            select(func.count()).select_from(Event).where(Event.tenant_id == tenant_id)
        )
        delivery_count = await session.scalar(
            select(func.count()).select_from(Delivery).where(Delivery.tenant_id == tenant_id)
        )
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(OutboxMessage)
            .where(OutboxMessage.tenant_id == tenant_id)
        )
        attempt_count = await session.scalar(
            select(func.count())
            .select_from(DeliveryAttempt)
            .where(DeliveryAttempt.tenant_id == tenant_id)
        )
    return TenantRowCounts(
        events=int(event_count or 0),
        deliveries=int(delivery_count or 0),
        outbox_messages=int(outbox_count or 0),
        delivery_attempts=int(attempt_count or 0),
    )


def _synchronize_first_idempotency_lookup(
    monkeypatch: pytest.MonkeyPatch,
    participants: int,
) -> None:
    """Force every request past its advisory pre-check before any INSERT wins."""

    original = ingestion_module._load_existing_event
    barrier = asyncio.Barrier(participants)
    synchronized_tasks: set[int] = set()

    async def synchronized_load(
        session: AsyncSession,
        tenant_id: UUID,
        idempotency_key: str,
    ) -> Event | None:
        task_id = id(asyncio.current_task())
        if task_id not in synchronized_tasks:
            synchronized_tasks.add(task_id)
            await barrier.wait()
        return await original(session, tenant_id, idempotency_key)

    monkeypatch.setattr(ingestion_module, "_load_existing_event", synchronized_load)


@pytest.mark.asyncio
async def test_ingestion_atomically_commits_event_deliveries_and_outbox(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
) -> None:
    async with _client(running_app) as client:
        tenant = await _provision_tenant(client, "atomic")
        endpoints = [
            await _provision_endpoint(client, tenant, "primary"),
            await _provision_endpoint(client, tenant, "secondary"),
        ]
        response = await _submit_event(
            client,
            tenant,
            f"atomic-{uuid4()}",
            _event_body(endpoints),
        )

    assert response.status_code == 201, response.text
    assert response.headers.get("Idempotency-Replayed") is None
    response_body = response.json()
    event_id = UUID(response_body["id"])
    assert response_body["type"] == "order.created"
    assert {UUID(item["endpoint_id"]) for item in response_body["deliveries"]} == {
        endpoint.id for endpoint in endpoints
    }
    assert {item["status"] for item in response_body["deliveries"]} == {"pending"}

    async with assertion_database.session_factory() as session:
        events = list(
            await session.scalars(
                select(Event).where(Event.tenant_id == tenant.id, Event.id == event_id)
            )
        )
        deliveries = list(
            await session.scalars(
                select(Delivery)
                .where(Delivery.tenant_id == tenant.id, Delivery.event_id == event_id)
                .order_by(Delivery.endpoint_id)
            )
        )
        outbox_messages = list(
            await session.scalars(
                select(OutboxMessage)
                .where(OutboxMessage.tenant_id == tenant.id)
                .order_by(OutboxMessage.delivery_id)
            )
        )
        attempt_count = await session.scalar(
            select(func.count())
            .select_from(DeliveryAttempt)
            .where(DeliveryAttempt.tenant_id == tenant.id)
        )

    assert len(events) == 1
    assert events[0].api_key_id == tenant.api_key_id
    assert len(deliveries) == len(endpoints)
    assert {delivery.target_url for delivery in deliveries} == {
        endpoint.url for endpoint in endpoints
    }
    assert {delivery.status for delivery in deliveries} == {"pending"}
    assert len(outbox_messages) == len(deliveries)
    assert {message.delivery_id for message in outbox_messages} == {
        delivery.id for delivery in deliveries
    }
    assert all(message.published_at is None for message in outbox_messages)
    assert all(message.payload["event_id"] == str(event_id) for message in outbox_messages)
    assert all("signing_secret" not in message.payload for message in outbox_messages)
    assert attempt_count == 0


@pytest.mark.asyncio
async def test_sequential_replay_and_conflict_preserve_one_row_set(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
) -> None:
    async with _client(running_app) as client:
        tenant = await _provision_tenant(client, "sequential")
        endpoint = await _provision_endpoint(client, tenant, "sequential")
        key = f"sequential-{uuid4()}"
        body = _event_body([endpoint])
        created = await _submit_event(client, tenant, key, body)
        replayed = await _submit_event(client, tenant, key, body)
        conflicting = await _submit_event(
            client,
            tenant,
            key,
            _event_body([endpoint], variant="changed"),
        )

    assert created.status_code == 201
    assert created.headers.get("Idempotency-Replayed") is None
    assert replayed.status_code == 201
    assert replayed.headers["Idempotency-Replayed"] == "true"
    assert replayed.json() == created.json()
    assert conflicting.status_code == 409
    assert conflicting.json()["code"] == "idempotency_key_reused"
    assert await _tenant_row_counts(assertion_database, tenant.id) == TenantRowCounts(1, 1, 1, 0)


@pytest.mark.asyncio
@pytest.mark.concurrency
async def test_concurrent_identical_requests_collapse_to_one_event(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _client(running_app) as client:
        tenant = await _provision_tenant(client, "concurrent-replay")
        endpoint = await _provision_endpoint(client, tenant, "concurrent-replay")

    key = f"concurrent-replay-{uuid4()}"
    body = _event_body([endpoint])
    _synchronize_first_idempotency_lookup(monkeypatch, CONCURRENT_REQUESTS)
    responses = await asyncio.gather(
        *(
            _submit_event_in_separate_client(running_app, tenant, key, body)
            for _ in range(CONCURRENT_REQUESTS)
        )
    )

    assert {response.status_code for response in responses} == {201}
    assert all(response.json() == responses[0].json() for response in responses)
    assert sum(response.headers.get("Idempotency-Replayed") is None for response in responses) == 1
    assert (
        sum(response.headers.get("Idempotency-Replayed") == "true" for response in responses)
        == CONCURRENT_REQUESTS - 1
    )
    assert await _tenant_row_counts(assertion_database, tenant.id) == TenantRowCounts(1, 1, 1, 0)


@pytest.mark.asyncio
@pytest.mark.concurrency
async def test_concurrent_conflicting_requests_have_one_winning_fingerprint(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _client(running_app) as client:
        tenant = await _provision_tenant(client, "concurrent-conflict")
        endpoint = await _provision_endpoint(client, tenant, "concurrent-conflict")

    key = f"concurrent-conflict-{uuid4()}"
    variants = ["alpha" if index % 2 == 0 else "beta" for index in range(CONCURRENT_REQUESTS)]
    _synchronize_first_idempotency_lookup(monkeypatch, CONCURRENT_REQUESTS)
    responses = await asyncio.gather(
        *(
            _submit_event_in_separate_client(
                running_app,
                tenant,
                key,
                _event_body([endpoint], variant=variant),
            )
            for variant in variants
        )
    )

    async with assertion_database.session_factory() as session:
        stored_event = await session.scalar(
            select(Event).where(
                Event.tenant_id == tenant.id,
                Event.idempotency_key == key,
            )
        )
    assert stored_event is not None
    winning_variant = stored_event.payload["variant"]
    for variant, response in zip(variants, responses, strict=True):
        if variant == winning_variant:
            assert response.status_code == 201
        else:
            assert response.status_code == 409
            assert response.json()["code"] == "idempotency_key_reused"

    assert sum(response.status_code == 201 for response in responses) == CONCURRENT_REQUESTS // 2
    assert sum(response.status_code == 409 for response in responses) == CONCURRENT_REQUESTS // 2
    assert await _tenant_row_counts(assertion_database, tenant.id) == TenantRowCounts(1, 1, 1, 0)


@pytest.mark.asyncio
async def test_outbox_build_failure_rolls_back_every_row_and_does_not_poison_key(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _client(running_app) as client:
        tenant = await _provision_tenant(client, "rollback")
        endpoint = await _provision_endpoint(client, tenant, "rollback")
        key = f"rollback-{uuid4()}"
        body = _event_body([endpoint])

        def fail_outbox_build(**_kwargs: object) -> list[OutboxMessage]:
            raise RuntimeError("synthetic outbox construction failure")

        with monkeypatch.context() as patch:
            patch.setattr(ingestion_module, "build_outbox_messages", fail_outbox_build)
            async with _client(running_app, raise_app_exceptions=False) as failure_client:
                failed = await _submit_event(failure_client, tenant, key, body)

        assert failed.status_code == 500
        assert failed.json()["code"] == "internal_error"
        assert await _tenant_row_counts(assertion_database, tenant.id) == TenantRowCounts(
            0, 0, 0, 0
        )

        retried = await _submit_event(client, tenant, key, body)

    assert retried.status_code == 201, retried.text
    assert retried.headers.get("Idempotency-Replayed") is None
    assert await _tenant_row_counts(assertion_database, tenant.id) == TenantRowCounts(1, 1, 1, 0)


@pytest.mark.asyncio
@pytest.mark.security
async def test_idempotency_and_resource_access_are_tenant_scoped(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
) -> None:
    async with _client(running_app) as client:
        tenant_a = await _provision_tenant(client, "tenant-a")
        tenant_b = await _provision_tenant(client, "tenant-b")
        endpoint_a = await _provision_endpoint(client, tenant_a, "tenant-a")
        endpoint_b = await _provision_endpoint(client, tenant_b, "tenant-b")
        shared_key = f"tenant-scope-{uuid4()}"

        foreign_endpoint = await _submit_event(
            client,
            tenant_a,
            shared_key,
            _event_body([endpoint_b]),
        )
        assert foreign_endpoint.status_code == 404
        assert await _tenant_row_counts(assertion_database, tenant_a.id) == TenantRowCounts(
            0, 0, 0, 0
        )

    response_a, response_b = await asyncio.gather(
        _submit_event_in_separate_client(
            running_app,
            tenant_a,
            shared_key,
            _event_body([endpoint_a]),
        ),
        _submit_event_in_separate_client(
            running_app,
            tenant_b,
            shared_key,
            _event_body([endpoint_b]),
        ),
    )
    assert response_a.status_code == 201
    assert response_b.status_code == 201
    assert response_a.json()["id"] != response_b.json()["id"]

    async with _client(running_app) as client:
        cross_tenant_read = await client.get(
            f"/v1/events/{response_b.json()['id']}",
            headers={"Authorization": f"Bearer {tenant_a.api_key}"},
        )
    assert cross_tenant_read.status_code == 404
    assert await _tenant_row_counts(assertion_database, tenant_a.id) == TenantRowCounts(1, 1, 1, 0)
    assert await _tenant_row_counts(assertion_database, tenant_b.id) == TenantRowCounts(1, 1, 1, 0)


@pytest.mark.asyncio
@pytest.mark.security
async def test_composite_foreign_keys_reject_cross_tenant_domain_rows(
    running_app: FastAPI,
    assertion_database: PostgresDatabase,
) -> None:
    async with _client(running_app) as client:
        tenant_a = await _provision_tenant(client, "fk-tenant-a")
        tenant_b = await _provision_tenant(client, "fk-tenant-b")
        endpoint_a = await _provision_endpoint(client, tenant_a, "fk-tenant-a")
        endpoint_b = await _provision_endpoint(client, tenant_b, "fk-tenant-b")
        accepted = await _submit_event(
            client,
            tenant_a,
            f"fk-event-{uuid4()}",
            _event_body([endpoint_a]),
        )
    assert accepted.status_code == 201

    event_id = UUID(accepted.json()["id"])
    async with assertion_database.session_factory() as session:
        valid_delivery = await session.scalar(
            select(Delivery).where(
                Delivery.tenant_id == tenant_a.id,
                Delivery.event_id == event_id,
            )
        )
        tenant_b_secret = await session.scalar(
            select(EndpointSigningSecret).where(
                EndpointSigningSecret.tenant_id == tenant_b.id,
                EndpointSigningSecret.endpoint_id == endpoint_b.id,
            )
        )
    assert valid_delivery is not None
    assert tenant_b_secret is not None

    async with assertion_database.session_factory() as session:
        session.add(
            Delivery(
                id=uuid4(),
                tenant_id=tenant_a.id,
                event_id=event_id,
                endpoint_id=endpoint_b.id,
                signing_secret_id=tenant_b_secret.id,
                target_url=endpoint_b.url,
                status="pending",
            )
        )
        with pytest.raises(IntegrityError) as delivery_error:
            await session.commit()
        await session.rollback()
    assert "fk_deliveries_tenant_id_endpoint_id" in str(delivery_error.value.orig)

    completed_at = datetime.now(UTC)
    async with assertion_database.session_factory() as session:
        session.add(
            DeliveryAttempt(
                id=uuid4(),
                tenant_id=tenant_b.id,
                delivery_id=valid_delivery.id,
                attempt_number=1,
                started_at=completed_at,
                finished_at=completed_at,
                outcome="succeeded",
            )
        )
        with pytest.raises(IntegrityError) as attempt_error:
            await session.commit()
        await session.rollback()
    assert "fk_delivery_attempts_tenant_id_delivery_id" in str(attempt_error.value.orig)
    assert await _tenant_row_counts(assertion_database, tenant_a.id) == TenantRowCounts(1, 1, 1, 0)
    assert await _tenant_row_counts(assertion_database, tenant_b.id) == TenantRowCounts(0, 0, 0, 0)
