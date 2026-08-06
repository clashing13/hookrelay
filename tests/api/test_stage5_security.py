"""Focused Stage 5 HTTP size-boundary and secret-rotation contracts."""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import Response as ASGIResponse
from httpx2 import Response
from sqlalchemy.ext.asyncio import AsyncSession

import hookrelay.api.events as events_api
from hookrelay.api.dependencies import AuthenticatedTenant, authenticate_tenant, get_session
from hookrelay.config import Settings
from hookrelay.ingestion import IngestionResult
from hookrelay.main import create_app
from hookrelay.models import EndpointSigningSecret, WebhookEndpoint
from hookrelay.schemas import EventCreate, EventResponse
from hookrelay.security import SecretCipher
from tests.conftest import api_client

pytestmark = pytest.mark.security


class StubDatabase:
    """Run the application lifespan without opening PostgreSQL connections."""

    async def check_readiness(self) -> None:
        return None

    async def dispose(self) -> None:
        return None


class RecordingSession:
    """Record whether a size decision reaches transaction finalization."""

    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class RotationSession:
    """Return ordered scalar results and expose every attempted mutation."""

    def __init__(self, results: list[object | None], replacement_created_at: datetime) -> None:
        self.results = results
        self.replacement_created_at = replacement_created_at
        self.scalar_calls = 0
        self.statements: list[object] = []
        self.added: list[object] = []
        self.flushes = 0
        self.commits = 0
        self.rollbacks = 0

    async def scalar(self, statement: object) -> object | None:
        self.scalar_calls += 1
        self.statements.append(statement)
        return self.results.pop(0)

    def add(self, instance: object) -> None:
        self.added.append(instance)

    async def flush(self) -> None:
        self.flushes += 1
        for instance in self.added:
            if isinstance(instance, EndpointSigningSecret):
                instance.created_at = self.replacement_created_at

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _app(settings: Settings) -> FastAPI:
    return create_app(settings, StubDatabase())


def _override_session(app: FastAPI, session: object) -> None:
    async def dependency() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = dependency


def _endpoint(*, endpoint_id: UUID, tenant_id: UUID, created_at: datetime) -> WebhookEndpoint:
    endpoint = WebhookEndpoint(
        id=endpoint_id,
        tenant_id=tenant_id,
        name="rotation-target",
        url="https://receiver.example/webhooks",
        is_active=True,
    )
    endpoint.created_at = created_at
    return endpoint


def _active_secret(
    endpoint: WebhookEndpoint,
    *,
    version: int,
    created_at: datetime,
) -> EndpointSigningSecret:
    secret = EndpointSigningSecret(
        id=uuid4(),
        tenant_id=endpoint.tenant_id,
        endpoint_id=endpoint.id,
        version=version,
        encryption_key_version=1,
        ciphertext=b"existing-ciphertext-is-not-plaintext",
        secret_hint="SAFE",
        retired_at=None,
    )
    secret.created_at = created_at
    return secret


def _assert_too_large(response: Response, *, code: str) -> None:
    body = cast(dict[str, object], response.json())
    assert response.status_code == 413
    assert response.headers["content-type"].startswith("application/problem+json")
    assert body == {
        "type": f"urn:hookrelay:problem:{code.replace('_', '-')}",
        "title": (
            "Request body too large"
            if code == "request_body_too_large"
            else "Event payload too large"
        ),
        "status": 413,
        "code": code,
        "detail": (
            "The request body exceeds the configured byte limit."
            if code == "request_body_too_large"
            else "The event payload exceeds the configured byte limit."
        ),
    }


def _assert_problem(response: Response, *, status: int, code: str) -> dict[str, object]:
    body = cast(dict[str, object], response.json())
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/problem+json")
    assert body["status"] == status
    assert body["code"] == code
    assert body["type"] == f"urn:hookrelay:problem:{code.replace('_', '-')}"
    return body


def _add_echo_route(app: FastAPI) -> None:
    @app.post("/test-only/echo")
    async def echo(request: Request) -> ASGIResponse:
        return ASGIResponse(content=await request.body(), media_type="application/octet-stream")


@pytest.mark.asyncio
async def test_request_body_limit_accepts_exact_bytes_and_rejects_one_more() -> None:
    limit = 1_024
    app = _app(
        Settings(
            environment="test",
            max_request_body_bytes=limit,
            max_event_payload_bytes=limit,
            _env_file=None,
        )
    )
    _add_echo_route(app)

    async with api_client(app) as client:
        accepted = await client.post("/test-only/echo", content=b"a" * limit)
        rejected = await client.post("/test-only/echo", content=b"a" * (limit + 1))

    assert accepted.status_code == 200
    assert accepted.content == b"a" * limit
    _assert_too_large(rejected, code="request_body_too_large")


@pytest.mark.asyncio
async def test_streamed_body_count_defeats_missing_or_false_content_length() -> None:
    limit = 1_024
    app = _app(
        Settings(
            environment="test",
            max_request_body_bytes=limit,
            max_event_payload_bytes=limit,
            _env_file=None,
        )
    )
    _add_echo_route(app)

    async def exact_chunks() -> AsyncIterator[bytes]:
        yield b"a" * 512
        yield b"b" * 512

    async def oversized_chunks() -> AsyncIterator[bytes]:
        yield b"a" * 512
        yield b"b" * 513

    async with api_client(app) as client:
        no_length = await client.post("/test-only/echo", content=exact_chunks())
        false_length = await client.post(
            "/test-only/echo",
            headers={"Content-Length": "1"},
            content=oversized_chunks(),
        )

    assert no_length.status_code == 200
    assert no_length.content == b"a" * 512 + b"b" * 512
    _assert_too_large(false_length, code="request_body_too_large")


@pytest.mark.asyncio
async def test_oversized_declared_length_is_rejected_before_body_consumption() -> None:
    limit = 1_024
    app = _app(
        Settings(
            environment="test",
            max_request_body_bytes=limit,
            max_event_payload_bytes=limit,
            _env_file=None,
        )
    )
    _add_echo_route(app)
    body_was_consumed = False

    async def observed_body() -> AsyncIterator[bytes]:
        nonlocal body_was_consumed
        body_was_consumed = True
        yield b"not-consumed"

    async with api_client(app) as client:
        response = await client.post(
            "/test-only/echo",
            headers={"Content-Length": str(limit + 1)},
            content=observed_body(),
        )

    _assert_too_large(response, code="request_body_too_large")
    assert body_was_consumed is False


@pytest.mark.asyncio
async def test_pathological_numeric_content_length_is_bounded_without_integer_parsing() -> None:
    limit = 1_024
    app = _app(
        Settings(
            environment="test",
            max_request_body_bytes=limit,
            max_event_payload_bytes=limit,
            _env_file=None,
        )
    )
    _add_echo_route(app)
    body_was_consumed = False

    async def observed_body() -> AsyncIterator[bytes]:
        nonlocal body_was_consumed
        body_was_consumed = True
        yield b"not-consumed"

    async with api_client(app) as client:
        response = await client.post(
            "/test-only/echo",
            headers={"Content-Length": "9" * 5_000},
            content=observed_body(),
        )

    _assert_too_large(response, code="request_body_too_large")
    assert body_was_consumed is False


@pytest.mark.asyncio
async def test_event_payload_limit_uses_canonical_utf8_bytes_before_ingestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload_at_limit = {"message": "é"}
    payload_bytes = json.dumps(
        payload_at_limit,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    settings = Settings(
        environment="test",
        max_request_body_bytes=1_024,
        max_event_payload_bytes=len(payload_bytes),
        _env_file=None,
    )
    app = _app(settings)
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    session = RecordingSession()
    ingestion_calls = 0

    async def authenticated() -> AuthenticatedTenant:
        return tenant

    async def fake_ingest_event(
        called_session: AsyncSession,
        called_tenant: AuthenticatedTenant,
        request: EventCreate,
        idempotency_key: str,
    ) -> IngestionResult:
        nonlocal ingestion_calls
        ingestion_calls += 1
        assert called_session is cast(AsyncSession, session)
        assert called_tenant == tenant
        assert request.payload == payload_at_limit
        assert idempotency_key == "limit-test-1"
        return IngestionResult(
            response=EventResponse.from_parts(
                event_id=uuid4(),
                event_type=request.event_type,
                created_at=datetime.now(UTC),
                deliveries=[],
            ),
            replayed=False,
        )

    app.dependency_overrides[authenticate_tenant] = authenticated
    _override_session(app, session)
    monkeypatch.setattr(events_api, "ingest_event", fake_ingest_event)
    endpoint_id = str(uuid4())

    async with api_client(app) as client:
        accepted = await client.post(
            "/v1/events",
            headers={"Idempotency-Key": "limit-test-1"},
            json={
                "type": "limit.test",
                "payload": payload_at_limit,
                "endpoint_ids": [endpoint_id],
            },
        )
        rejected = await client.post(
            "/v1/events",
            headers={"Idempotency-Key": "limit-test-2"},
            json={
                "type": "limit.test",
                "payload": {"message": "éx"},
                "endpoint_ids": [endpoint_id],
            },
        )

    assert accepted.status_code == 201
    assert ingestion_calls == 1
    assert session.commits == 1
    assert session.rollbacks == 0
    _assert_too_large(rejected, code="event_payload_too_large")


@pytest.mark.asyncio
async def test_signing_secret_rotation_returns_plaintext_once_and_stores_only_ciphertext() -> None:
    settings = Settings(environment="test", _env_file=None)
    app = _app(settings)
    created_at = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
    rotated_at = datetime(2026, 8, 5, 12, 5, tzinfo=UTC)
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    endpoint = _endpoint(
        endpoint_id=uuid4(),
        tenant_id=tenant.tenant_id,
        created_at=created_at,
    )
    previous = _active_secret(endpoint, version=1, created_at=created_at)
    session = RotationSession(
        [endpoint, previous, rotated_at, endpoint],
        replacement_created_at=rotated_at,
    )

    async def authenticated() -> AuthenticatedTenant:
        return tenant

    app.dependency_overrides[authenticate_tenant] = authenticated
    _override_session(app, session)

    async with api_client(app) as client:
        rotated = await client.post(
            f"/v1/endpoints/{endpoint.id}/signing-secret/rotate",
            json={"expected_active_version": 1},
        )
        inspected = await client.get(f"/v1/endpoints/{endpoint.id}")

    assert rotated.status_code == 200
    assert rotated.headers["cache-control"] == "no-store"
    assert rotated.headers["pragma"] == "no-cache"
    body = cast(dict[str, object], rotated.json())
    plaintext = cast(str, body["signing_secret"])
    assert plaintext.startswith("whsec_")
    assert body["endpoint_id"] == str(endpoint.id)
    assert body["version"] == 2
    assert body["created_at"] == "2026-08-05T12:05:00Z"

    assert previous.version == 1
    assert previous.retired_at == rotated_at
    assert len(session.added) == 1
    replacement = cast(EndpointSigningSecret, session.added[0])
    assert replacement.tenant_id == tenant.tenant_id
    assert replacement.endpoint_id == endpoint.id
    assert replacement.version == 2
    assert replacement.encryption_key_version == settings.secret_encryption_key_version
    assert replacement.secret_hint == plaintext[-4:]
    assert plaintext.encode() not in replacement.ciphertext
    assert plaintext not in repr(replacement)
    assert plaintext not in str(vars(replacement))

    cipher = SecretCipher(
        settings.secret_encryption_key_bytes(),
        settings.secret_encryption_key_version,
    )
    assert (
        cipher.decrypt_endpoint_secret(
            replacement.tenant_id,
            replacement.endpoint_id,
            replacement.id,
            replacement.version,
            replacement.encryption_key_version,
            replacement.ciphertext,
        )
        == plaintext
    )
    assert session.flushes == 2
    assert session.commits == 1
    assert session.rollbacks == 0

    assert inspected.status_code == 200
    assert "signing_secret" not in inspected.json()
    assert plaintext not in inspected.text


@pytest.mark.asyncio
async def test_signing_secret_rotation_conflict_reveals_only_current_version() -> None:
    app = _app(Settings(environment="test", _env_file=None))
    created_at = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    endpoint = _endpoint(
        endpoint_id=uuid4(),
        tenant_id=tenant.tenant_id,
        created_at=created_at,
    )
    current = _active_secret(endpoint, version=3, created_at=created_at)
    session = RotationSession([endpoint, current], replacement_created_at=created_at)

    async def authenticated() -> AuthenticatedTenant:
        return tenant

    app.dependency_overrides[authenticate_tenant] = authenticated
    _override_session(app, session)

    async with api_client(app) as client:
        response = await client.post(
            f"/v1/endpoints/{endpoint.id}/signing-secret/rotate",
            json={"expected_active_version": 2},
        )

    body = _assert_problem(response, status=409, code="signing_secret_version_conflict")
    assert body["detail"] == "The endpoint signing secret changed before this request completed."
    assert response.headers["hookrelay-active-secret-version"] == "3"
    assert current.secret_hint not in response.text
    assert current.ciphertext.hex() not in response.text
    assert current.retired_at is None
    assert session.added == []
    assert session.flushes == 0
    assert session.commits == 0
    assert session.rollbacks == 1


@pytest.mark.asyncio
async def test_cross_tenant_signing_secret_rotation_is_opaque_and_non_mutating() -> None:
    app = _app(Settings(environment="test", _env_file=None))
    authenticated_tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    other_tenant_id = uuid4()
    endpoint_id = uuid4()
    other_tenant_endpoint = _endpoint(
        endpoint_id=endpoint_id,
        tenant_id=other_tenant_id,
        created_at=datetime(2026, 8, 5, 12, 0, tzinfo=UTC),
    )
    session = RotationSession([None], replacement_created_at=other_tenant_endpoint.created_at)

    async def authenticated() -> AuthenticatedTenant:
        return authenticated_tenant

    app.dependency_overrides[authenticate_tenant] = authenticated
    _override_session(app, session)

    async with api_client(app) as client:
        response = await client.post(
            f"/v1/endpoints/{endpoint_id}/signing-secret/rotate",
            json={"expected_active_version": 1},
        )

    body = _assert_problem(response, status=404, code="resource_not_found")
    assert body == {
        "type": "urn:hookrelay:problem:resource-not-found",
        "title": "Resource not found",
        "status": 404,
        "code": "resource_not_found",
        "detail": "The requested resource was not found.",
    }
    assert "hookrelay-active-secret-version" not in response.headers
    assert str(other_tenant_id) not in response.text
    assert session.scalar_calls == 1
    endpoint_lookup = str(session.statements[0])
    assert "webhook_endpoints.id" in endpoint_lookup
    assert "webhook_endpoints.tenant_id" in endpoint_lookup
    assert session.added == []
    assert session.flushes == 0
    assert session.commits == 0
    assert session.rollbacks == 1


def test_event_openapi_documents_payload_too_large_response() -> None:
    app = _app(Settings(environment="test", _env_file=None))
    event_responses = cast(
        dict[str, object],
        app.openapi()["paths"]["/v1/events"]["post"]["responses"],
    )

    response = cast(dict[str, object], event_responses["413"])
    assert "application/problem+json" in cast(dict[str, object], response["content"])


@pytest.mark.asyncio
async def test_endpoint_creation_rejects_metadata_literal_before_database_mutation() -> None:
    settings = Settings(
        environment="test",
        delivery_allowed_hosts=frozenset(),
        _env_file=None,
    )
    app = _app(settings)
    tenant = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())
    session = RecordingSession()

    async def authenticated() -> AuthenticatedTenant:
        return tenant

    app.dependency_overrides[authenticate_tenant] = authenticated
    _override_session(app, session)

    async with api_client(app) as client:
        response = await client.post(
            "/v1/endpoints",
            json={
                "name": "blocked-metadata",
                "url": "http://169.254.169.254/latest/meta-data",
            },
        )

    body = _assert_problem(response, status=422, code="validation_error")
    assert body["errors"] == [
        {
            "pointer": "/body/url",
            "code": "destination_not_allowed",
            "message": "The destination URL cannot be used by this deployment.",
        }
    ]
    assert session.commits == 0
    assert session.rollbacks == 0
