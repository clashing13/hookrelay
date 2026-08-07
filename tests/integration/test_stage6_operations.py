"""Real-PostgreSQL evidence for tenant operations history and attempt inspection."""

import os
import subprocess
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from httpx2 import AsyncClient
from sqlalchemy import delete, select

from hookrelay.api.dependencies import AuthenticatedTenant, authenticate_tenant
from hookrelay.config import Settings
from hookrelay.database import PostgresDatabase
from hookrelay.main import create_app
from hookrelay.models import (
    ApiKey,
    Delivery,
    DeliveryAttempt,
    EndpointSigningSecret,
    EndpointTrafficControl,
    Event,
    OutboxMessage,
    Tenant,
    WebhookEndpoint,
)
from tests.conftest import api_client

pytestmark = [pytest.mark.integration, pytest.mark.security]


@dataclass(frozen=True, slots=True)
class SeededOperations:
    owner: AuthenticatedTenant
    intruder: AuthenticatedTenant
    owner_endpoint_id: UUID
    second_endpoint_id: UUID
    delivery_ids: tuple[UUID, UUID, UUID]
    event_ids: tuple[UUID, UUID, UUID]
    attempt_ids: tuple[UUID, UUID]
    secret_id: UUID
    target_sentinel: str


@dataclass(slots=True)
class RunningOperations:
    client: AsyncClient
    database: PostgresDatabase
    tenant_holder: dict[str, AuthenticatedTenant]
    seeded_tenant_ids: set[UUID]

    @property
    def selected_tenant(self) -> AuthenticatedTenant:
        return self.tenant_holder["tenant"]

    @selected_tenant.setter
    def selected_tenant(self, value: AuthenticatedTenant) -> None:
        self.tenant_holder["tenant"] = value


def _database_url() -> str:
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
    return _database_url()


@pytest.fixture(scope="module", autouse=True)
def migrated_database(database_url: str) -> None:
    _upgrade_database(database_url)


async def _cleanup(database: PostgresDatabase, tenant_ids: set[UUID]) -> None:
    async with database.session_factory() as session:
        await session.execute(delete(OutboxMessage).where(OutboxMessage.tenant_id.in_(tenant_ids)))
        await session.execute(
            delete(DeliveryAttempt).where(DeliveryAttempt.tenant_id.in_(tenant_ids))
        )
        await session.execute(delete(Delivery).where(Delivery.tenant_id.in_(tenant_ids)))
        await session.execute(delete(Event).where(Event.tenant_id.in_(tenant_ids)))
        await session.execute(
            delete(EndpointTrafficControl).where(EndpointTrafficControl.tenant_id.in_(tenant_ids))
        )
        await session.execute(
            delete(EndpointSigningSecret).where(EndpointSigningSecret.tenant_id.in_(tenant_ids))
        )
        await session.execute(
            delete(WebhookEndpoint).where(WebhookEndpoint.tenant_id.in_(tenant_ids))
        )
        await session.execute(delete(ApiKey).where(ApiKey.tenant_id.in_(tenant_ids)))
        await session.execute(delete(Tenant).where(Tenant.id.in_(tenant_ids)))
        await session.commit()


@pytest_asyncio.fixture
async def running_operations(database_url: str) -> AsyncIterator[RunningOperations]:
    settings = Settings(environment="test", database_url=database_url, _env_file=None)
    database = PostgresDatabase(settings)
    selected = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    tenant_holder = {"tenant": selected}
    app = create_app(settings, database)

    async def authenticated() -> AuthenticatedTenant:
        return tenant_holder["tenant"]

    app.dependency_overrides[authenticate_tenant] = authenticated
    tenant_ids: set[UUID] = set()
    async with api_client(app) as client:
        running = RunningOperations(
            client=client,
            database=database,
            tenant_holder=tenant_holder,
            seeded_tenant_ids=set(),
        )
        yield running
        tenant_ids.add(running.selected_tenant.tenant_id)
        tenant_ids.update(running.seeded_tenant_ids)
        await _cleanup(database, tenant_ids)


def _api_key(tenant_id: UUID, api_key_id: UUID) -> ApiKey:
    return ApiKey(
        id=api_key_id,
        tenant_id=tenant_id,
        name="operations-test",
        public_id=f"ops_{uuid4().hex[:20]}",
        secret_hash=b"h" * 32,
        secret_last_four="test",
    )


async def _seed_operations(running: RunningOperations) -> SeededOperations:
    owner_id = running.selected_tenant.tenant_id
    owner_key_id = running.selected_tenant.api_key_id
    intruder = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    owner_endpoint_id = uuid4()
    second_endpoint_id = uuid4()
    secret_id = uuid4()
    second_secret_id = uuid4()
    same_created_at = datetime(2026, 8, 6, 15, 30, tzinfo=UTC)
    same_attempt_at = datetime(2026, 8, 6, 15, 31, tzinfo=UTC)
    event_ids = (uuid4(), uuid4(), uuid4())
    delivery_ids = (uuid4(), uuid4(), uuid4())
    attempt_ids = (uuid4(), uuid4())
    target_sentinel = "operation-secret-query-must-not-leak"

    async with running.database.session_factory() as session:
        session.add_all(
            [
                Tenant(id=owner_id, name="operations-owner", is_active=True),
                Tenant(id=intruder.tenant_id, name="operations-intruder", is_active=True),
            ]
        )
        await session.flush()
        session.add_all(
            [
                _api_key(owner_id, owner_key_id),
                _api_key(intruder.tenant_id, intruder.api_key_id),
                WebhookEndpoint(
                    id=owner_endpoint_id,
                    tenant_id=owner_id,
                    name="primary",
                    url=f"https://receiver.example/webhook?token={target_sentinel}",
                    is_active=True,
                ),
                WebhookEndpoint(
                    id=second_endpoint_id,
                    tenant_id=owner_id,
                    name="secondary",
                    url="https://secondary.example/webhook",
                    is_active=True,
                ),
            ]
        )
        await session.flush()
        session.add_all(
            [
                EndpointSigningSecret(
                    id=secret_id,
                    tenant_id=owner_id,
                    endpoint_id=owner_endpoint_id,
                    version=1,
                    encryption_key_version=1,
                    ciphertext=b"c" * 29,
                    secret_hint="hide",
                ),
                EndpointSigningSecret(
                    id=second_secret_id,
                    tenant_id=owner_id,
                    endpoint_id=second_endpoint_id,
                    version=1,
                    encryption_key_version=1,
                    ciphertext=b"d" * 29,
                    secret_hint="safe",
                ),
                EndpointTrafficControl(tenant_id=owner_id, endpoint_id=owner_endpoint_id),
                EndpointTrafficControl(tenant_id=owner_id, endpoint_id=second_endpoint_id),
            ]
        )
        await session.flush()
        for index, event_id in enumerate(event_ids):
            session.add(
                Event(
                    id=event_id,
                    tenant_id=owner_id,
                    api_key_id=owner_key_id,
                    event_type=f"operations.event.{index}",
                    payload={"private": f"payload-{index}"},
                    idempotency_key=f"operations-key-{uuid4().hex}",
                    request_fingerprint=bytes([index + 1]) * 32,
                    request_fingerprint_version=1,
                    created_at=same_created_at,
                )
            )
        await session.flush()
        session.add_all(
            [
                Delivery(
                    id=delivery_ids[0],
                    tenant_id=owner_id,
                    event_id=event_ids[0],
                    endpoint_id=owner_endpoint_id,
                    signing_secret_id=secret_id,
                    target_url=f"https://receiver.example/webhook?token={target_sentinel}",
                    status="dead_lettered",
                    dispatch_generation=2,
                    dead_lettered_at=same_created_at + timedelta(minutes=2),
                    dead_letter_reason="permanent_failure",
                    created_at=same_created_at,
                ),
                Delivery(
                    id=delivery_ids[1],
                    tenant_id=owner_id,
                    event_id=event_ids[1],
                    endpoint_id=owner_endpoint_id,
                    signing_secret_id=secret_id,
                    target_url="https://receiver.example/webhook",
                    status="succeeded",
                    dispatch_generation=1,
                    created_at=same_created_at,
                ),
                Delivery(
                    id=delivery_ids[2],
                    tenant_id=owner_id,
                    event_id=event_ids[2],
                    endpoint_id=second_endpoint_id,
                    signing_secret_id=second_secret_id,
                    target_url="https://secondary.example/webhook",
                    status="pending",
                    dispatch_generation=1,
                    created_at=same_created_at,
                ),
            ]
        )
        await session.flush()
        session.add_all(
            [
                DeliveryAttempt(
                    id=attempt_ids[0],
                    tenant_id=owner_id,
                    delivery_id=delivery_ids[0],
                    attempt_number=1,
                    dispatch_generation=1,
                    claim_token=uuid4(),
                    is_circuit_probe=False,
                    started_at=same_attempt_at,
                    finished_at=same_attempt_at + timedelta(milliseconds=20),
                    outcome="transient_failure",
                    error_code="transport_error",
                    duration_ms=20,
                ),
                DeliveryAttempt(
                    id=attempt_ids[1],
                    tenant_id=owner_id,
                    delivery_id=delivery_ids[0],
                    attempt_number=2,
                    dispatch_generation=2,
                    claim_token=uuid4(),
                    is_circuit_probe=True,
                    started_at=same_attempt_at,
                    finished_at=same_attempt_at + timedelta(milliseconds=25),
                    outcome="permanent_failure",
                    response_status_code=400,
                    error_code="http_status",
                    duration_ms=25,
                ),
            ]
        )
        await session.commit()

    # Keep fixture cleanup independent from which tenant is currently selected.
    running.seeded_tenant_ids.update({owner_id, intruder.tenant_id})
    return SeededOperations(
        owner=running.selected_tenant,
        intruder=intruder,
        owner_endpoint_id=owner_endpoint_id,
        second_endpoint_id=second_endpoint_id,
        delivery_ids=delivery_ids,
        event_ids=event_ids,
        attempt_ids=attempt_ids,
        secret_id=secret_id,
        target_sentinel=target_sentinel,
    )


@pytest.mark.asyncio
async def test_equal_timestamp_delivery_pages_have_no_gaps_or_duplicates(
    running_operations: RunningOperations,
) -> None:
    seeded = await _seed_operations(running_operations)
    expected = [str(item) for item in sorted(seeded.delivery_ids, reverse=True)]
    observed: list[str] = []
    cursor: str | None = None

    for _ in range(3):
        params = {"limit": "1"}
        if cursor is not None:
            params["cursor"] = cursor
        response = await running_operations.client.get("/v1/deliveries", params=params)
        assert response.status_code == 200
        body = response.json()
        assert len(body["items"]) == 1
        observed.append(body["items"][0]["id"])
        cursor = body["next_cursor"]

    assert observed == expected
    assert len(set(observed)) == 3
    assert cursor is None

    dead_letters = await running_operations.client.get(
        "/v1/deliveries",
        params={"status": "dead_lettered"},
    )
    by_endpoint = await running_operations.client.get(
        "/v1/deliveries",
        params={"endpoint_id": str(seeded.second_endpoint_id)},
    )
    by_event = await running_operations.client.get(
        "/v1/deliveries",
        params={"event_id": str(seeded.event_ids[1])},
    )
    assert [item["id"] for item in dead_letters.json()["items"]] == [str(seeded.delivery_ids[0])]
    assert [item["id"] for item in by_endpoint.json()["items"]] == [str(seeded.delivery_ids[2])]
    assert [item["id"] for item in by_event.json()["items"]] == [str(seeded.delivery_ids[1])]


@pytest.mark.asyncio
async def test_detail_and_attempt_history_are_safe_and_replay_retains_generations(
    running_operations: RunningOperations,
) -> None:
    seeded = await _seed_operations(running_operations)
    delivery_id = seeded.delivery_ids[0]

    detail = await running_operations.client.get(f"/v1/deliveries/{delivery_id}")
    first_attempt_page = await running_operations.client.get(
        f"/v1/deliveries/{delivery_id}/attempts",
        params={"limit": 1},
    )
    assert detail.status_code == 200
    assert first_attempt_page.status_code == 200
    assert detail.json()["attempt_count"] == 2
    assert detail.json()["replayable"] is True
    cursor = first_attempt_page.json()["next_cursor"]
    assert first_attempt_page.json()["items"][0]["attempt_number"] == 2
    second_attempt_page = await running_operations.client.get(
        f"/v1/deliveries/{delivery_id}/attempts",
        params={"limit": 1, "cursor": cursor},
    )
    assert second_attempt_page.json()["items"][0]["attempt_number"] == 1
    assert second_attempt_page.json()["next_cursor"] is None

    attempt_detail = await running_operations.client.get(
        f"/v1/deliveries/{delivery_id}/attempts/{seeded.attempt_ids[1]}"
    )
    assert attempt_detail.status_code == 200
    forbidden = {
        "tenant_id",
        "target_url",
        "signing_secret_id",
        "claim_token",
        "claim_expires_at",
        "payload",
        "ciphertext",
    }
    assert forbidden.isdisjoint(detail.json())
    assert forbidden.isdisjoint(attempt_detail.json())
    assert seeded.target_sentinel not in detail.text
    assert str(seeded.secret_id) not in detail.text

    replayed = await running_operations.client.post(
        f"/v1/deliveries/{delivery_id}/replay",
        json={"expected_dispatch_generation": 2},
    )
    assert replayed.status_code == 202
    assert replayed.json()["dispatch_generation"] == 3
    retained = await running_operations.client.get(f"/v1/deliveries/{delivery_id}/attempts")
    assert [item["dispatch_generation"] for item in retained.json()["items"]] == [2, 1]

    async with running_operations.database.session_factory() as session:
        outbox = await session.scalar(
            select(OutboxMessage).where(
                OutboxMessage.delivery_id == delivery_id,
                OutboxMessage.dispatch_generation == 3,
            )
        )
    assert outbox is not None
    assert outbox.correlation_id is not None
    assert outbox.traceparent is None


@pytest.mark.asyncio
async def test_cross_tenant_delivery_and_attempt_ids_are_opaque(
    running_operations: RunningOperations,
) -> None:
    seeded = await _seed_operations(running_operations)
    running_operations.selected_tenant = seeded.intruder
    delivery_id = seeded.delivery_ids[0]
    attempt_id = seeded.attempt_ids[0]

    listed = await running_operations.client.get("/v1/deliveries")
    detail = await running_operations.client.get(f"/v1/deliveries/{delivery_id}")
    attempts = await running_operations.client.get(f"/v1/deliveries/{delivery_id}/attempts")
    attempt = await running_operations.client.get(
        f"/v1/deliveries/{delivery_id}/attempts/{attempt_id}"
    )

    assert listed.status_code == 200
    assert listed.json()["items"] == []
    expected = {
        "type": "urn:hookrelay:problem:resource-not-found",
        "title": "Resource not found",
        "status": 404,
        "code": "resource_not_found",
        "detail": "The requested resource was not found.",
    }
    assert detail.json() == expected
    assert attempts.json() == expected
    assert attempt.json() == expected
    combined = detail.text + attempts.text + attempt.text
    assert str(delivery_id) not in combined
    assert str(attempt_id) not in combined
