"""Fast operations API contracts for cursors, safe fields, and opaque misses."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from httpx2 import Response
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.deliveries import (
    _attempt_filter_fingerprint,
    _delivery_filter_fingerprint,
    _encode_attempt_cursor,
    _encode_delivery_cursor,
)
from hookrelay.api.dependencies import AuthenticatedTenant, authenticate_tenant, get_session
from hookrelay.config import Settings
from hookrelay.main import create_app
from hookrelay.models import Delivery, DeliveryAttempt
from tests.conftest import api_client

pytestmark = pytest.mark.security


class StubDatabase:
    async def check_readiness(self) -> None:
        return None

    async def dispose(self) -> None:
        return None


class UnexpectedSession:
    async def execute(self, _statement: object) -> None:
        raise AssertionError("an invalid cursor must fail before a database query")

    async def scalar(self, _statement: object) -> None:
        raise AssertionError("an invalid cursor must fail before a database query")


class RowResult:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    def first(self) -> tuple[object, ...] | None:
        return self._row


class RowSession:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self._row = row

    async def execute(self, _statement: object) -> RowResult:
        return RowResult(self._row)


class ScalarSession:
    def __init__(self, result: object | None) -> None:
        self._result = result

    async def scalar(self, _statement: object) -> object | None:
        return self._result


def _app(session: object, tenant: AuthenticatedTenant) -> FastAPI:
    app = create_app(Settings(environment="test", _env_file=None), StubDatabase())

    async def authenticated() -> AuthenticatedTenant:
        return tenant

    async def session_dependency() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[authenticate_tenant] = authenticated
    app.dependency_overrides[get_session] = session_dependency
    return app


def _assert_problem(response: Response, *, status: int, code: str) -> dict[str, object]:
    body = cast("dict[str, object]", response.json())
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/problem+json")
    assert body["status"] == status
    assert body["code"] == code
    return body


def _delivery(tenant_id: UUID | None = None) -> Delivery:
    delivery = Delivery(
        id=uuid4(),
        tenant_id=tenant_id or uuid4(),
        event_id=uuid4(),
        endpoint_id=uuid4(),
        signing_secret_id=uuid4(),
        target_url="https://private.example.test/hook?credential=do-not-return",
        status="dead_lettered",
        dispatch_generation=3,
        dead_lettered_at=datetime.now(UTC),
        dead_letter_reason="attempts_exhausted",
        claim_token=None,
        claim_expires_at=None,
    )
    delivery.created_at = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)
    return delivery


@pytest.mark.asyncio
async def test_delivery_cursor_is_strict_versioned_and_bound_to_filters() -> None:
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    app = _app(UnexpectedSession(), tenant)
    delivery = _delivery(tenant.tenant_id)
    original_filter = _delivery_filter_fingerprint(
        delivery_status="dead_lettered",
        endpoint_id=None,
        event_id=None,
    )
    cursor = _encode_delivery_cursor(delivery, filter_fingerprint=original_filter)

    async with api_client(app) as client:
        malformed = await client.get("/v1/deliveries", params={"cursor": "not_base64!"})
        filter_mismatch = await client.get(
            "/v1/deliveries",
            params={"status": "succeeded", "cursor": cursor},
        )

    _assert_problem(malformed, status=422, code="invalid_cursor")
    mismatch_body = _assert_problem(filter_mismatch, status=422, code="invalid_cursor")
    assert mismatch_body["detail"] == "The pagination cursor is invalid for this request."


@pytest.mark.asyncio
async def test_attempt_cursor_cannot_be_reused_for_another_delivery() -> None:
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    app = _app(UnexpectedSession(), tenant)
    first_delivery_id = uuid4()
    attempt = DeliveryAttempt(
        id=uuid4(),
        tenant_id=tenant.tenant_id,
        delivery_id=first_delivery_id,
        attempt_number=2,
        dispatch_generation=1,
        is_circuit_probe=False,
        started_at=datetime.now(UTC),
    )
    cursor = _encode_attempt_cursor(
        attempt,
        filter_fingerprint=_attempt_filter_fingerprint(first_delivery_id),
    )

    async with api_client(app) as client:
        response = await client.get(
            f"/v1/deliveries/{uuid4()}/attempts",
            params={"cursor": cursor},
        )

    _assert_problem(response, status=422, code="invalid_cursor")


@pytest.mark.asyncio
async def test_delivery_detail_excludes_snapshot_secrets_claims_urls_and_tenant_id() -> None:
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    delivery = _delivery(tenant.tenant_id)
    sentinel_secret_id = str(delivery.signing_secret_id)
    app = _app(
        RowSession((delivery, "order.created", "primary receiver", 4, datetime.now(UTC))),
        tenant,
    )

    async with api_client(app) as client:
        response = await client.get(f"/v1/deliveries/{delivery.id}")

    assert response.status_code == 200
    body = cast("dict[str, object]", response.json())
    assert body["id"] == str(delivery.id)
    assert body["attempt_count"] == 4
    assert body["replayable"] is True
    forbidden_fields = {
        "tenant_id",
        "target_url",
        "signing_secret_id",
        "claim_token",
        "claim_expires_at",
        "payload",
    }
    assert forbidden_fields.isdisjoint(body)
    assert sentinel_secret_id not in response.text
    assert "do-not-return" not in response.text
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_attempt_detail_excludes_claim_and_cross_tenant_miss_is_opaque() -> None:
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    delivery_id = uuid4()
    sentinel_claim = uuid4()
    attempt = DeliveryAttempt(
        id=uuid4(),
        tenant_id=tenant.tenant_id,
        delivery_id=delivery_id,
        attempt_number=7,
        dispatch_generation=2,
        claim_token=sentinel_claim,
        is_circuit_probe=True,
        started_at=datetime.now(UTC) - timedelta(milliseconds=31),
        finished_at=datetime.now(UTC),
        outcome="succeeded",
        response_status_code=204,
        duration_ms=31,
    )
    owner_app = _app(ScalarSession(attempt), tenant)
    missing_app = _app(ScalarSession(None), tenant)

    async with api_client(owner_app) as client:
        inspected = await client.get(f"/v1/deliveries/{delivery_id}/attempts/{attempt.id}")
    async with api_client(missing_app) as client:
        hidden = await client.get(f"/v1/deliveries/{uuid4()}/attempts/{attempt.id}")

    assert inspected.status_code == 200
    assert inspected.json()["attempt_number"] == 7
    assert "claim_token" not in inspected.json()
    assert str(sentinel_claim) not in inspected.text
    hidden_body = _assert_problem(hidden, status=404, code="resource_not_found")
    assert hidden_body["detail"] == "The requested resource was not found."


def test_openapi_describes_operations_history_and_safe_attempt_shapes() -> None:
    schema = create_app(
        Settings(environment="test", _env_file=None),
        StubDatabase(),
    ).openapi()
    paths = schema["paths"]
    assert "/v1/deliveries" in paths
    assert "/v1/deliveries/{delivery_id}" in paths
    assert "/v1/deliveries/{delivery_id}/attempts" in paths
    assert "/v1/deliveries/{delivery_id}/attempts/{attempt_id}" in paths

    list_parameters = paths["/v1/deliveries"]["get"]["parameters"]
    assert {parameter["name"] for parameter in list_parameters} >= {
        "status",
        "endpoint_id",
        "event_id",
        "limit",
        "cursor",
    }
    components = schema["components"]["schemas"]
    attempt_fields = components["DeliveryAttemptDetailResponse"]["properties"]
    delivery_fields = components["DeliveryInspectionResponse"]["properties"]
    assert "claim_token" not in attempt_fields
    assert "target_url" not in delivery_fields
    assert "signing_secret_id" not in delivery_fields
    assert "tenant_id" not in delivery_fields
