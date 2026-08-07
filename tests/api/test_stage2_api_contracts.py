"""Fast Stage 2 HTTP, authentication, error, and secret-disclosure contracts."""

from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx2 import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.dependencies import AuthenticatedTenant, authenticate_tenant, get_session
from hookrelay.config import Settings
from hookrelay.main import create_app
from hookrelay.models import ApiKey, EndpointSigningSecret, Tenant, WebhookEndpoint
from hookrelay.security import GeneratedApiKey, generate_api_key, parse_api_key
from tests.conftest import api_client

pytestmark = pytest.mark.security


class StubDatabase:
    """Satisfy application lifecycle without opening PostgreSQL connections."""

    def __init__(self) -> None:
        self.disposed = False

    async def check_readiness(self) -> None:
        return None

    async def dispose(self) -> None:
        self.disposed = True


class ScalarSession:
    """Return one authentication lookup result and record query count."""

    def __init__(self, result: object | None) -> None:
        self.result = result
        self.scalar_calls = 0

    async def scalar(self, _statement: object) -> object | None:
        self.scalar_calls += 1
        return self.result


class SequenceSession:
    """Return deterministic values for routes that make multiple scalar queries."""

    def __init__(self, results: Iterable[object | None]) -> None:
        self.results = list(results)

    async def scalar(self, _statement: object) -> object | None:
        return self.results.pop(0)


class RecordingSession:
    """Minimal write-session double that applies relevant server defaults."""

    def __init__(self) -> None:
        self.added: list[object] = []
        self.commits = 0
        self.rollbacks = 0

    def add(self, instance: object) -> None:
        self.added.append(instance)

    def add_all(self, instances: Iterable[object]) -> None:
        self.added.extend(instances)

    async def flush(self) -> None:
        created_at = datetime.now(UTC)
        for instance in self.added:
            if isinstance(instance, (ApiKey, EndpointSigningSecret, Tenant, WebhookEndpoint)):
                instance.created_at = created_at
            if isinstance(instance, (Tenant, WebhookEndpoint)):
                instance.is_active = True

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _app(settings: Settings | None = None) -> FastAPI:
    return create_app(settings or Settings(environment="test", _env_file=None), StubDatabase())


def _override_session(app: FastAPI, session: object) -> None:
    async def dependency() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = dependency


def _stored_api_key(
    generated: GeneratedApiKey,
    *,
    revoked_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> ApiKey:
    return ApiKey(
        id=uuid4(),
        tenant_id=uuid4(),
        name="test",
        public_id=generated.public_id,
        secret_hash=generated.secret_hash,
        secret_last_four=generated.secret_last_four,
        revoked_at=revoked_at,
        expires_at=expires_at,
    )


def _invalid_credentials_body() -> dict[str, object]:
    return {
        "type": "urn:hookrelay:problem:invalid-credentials",
        "title": "Authentication failed",
        "status": 401,
        "code": "invalid_credentials",
        "detail": "A valid HookRelay bearer credential is required.",
    }


def _assert_problem(response: Response, *, status: int, code: str) -> dict[str, object]:
    body = cast(dict[str, object], response.json())
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/problem+json")
    assert body["status"] == status
    assert body["code"] == code
    assert body["type"] == f"urn:hookrelay:problem:{code.replace('_', '-')}"
    return body


@pytest.mark.asyncio
async def test_missing_malformed_and_unknown_keys_share_one_sanitized_401(
    capsys: pytest.CaptureFixture[str],
) -> None:
    session = ScalarSession(None)
    app = _app()
    _override_session(app, session)
    unknown_key = generate_api_key().token
    cases = [
        {},
        {"Authorization": "Basic not-a-bearer-token"},
        {"Authorization": "Bearer malformed"},
        {"Authorization": f"Bearer {unknown_key}"},
    ]

    async with api_client(app) as client:
        responses = [await client.get("/v1/tenant", headers=headers) for headers in cases]

    assert all(response.json() == _invalid_credentials_body() for response in responses)
    assert all(response.headers["www-authenticate"] == "Bearer" for response in responses)
    assert all(
        response.headers["content-type"].startswith("application/problem+json")
        for response in responses
    )
    assert session.scalar_calls == 1
    assert unknown_key not in "".join(response.text for response in responses)
    assert unknown_key not in capsys.readouterr().err


@pytest.mark.asyncio
async def test_revoked_expired_and_wrong_secrets_are_indistinguishable() -> None:
    now = datetime.now(UTC)
    generated = generate_api_key()
    other = generate_api_key()
    parsed_other = parse_api_key(other.token)
    assert parsed_other is not None
    wrong_secret_token = f"hrk_{generated.public_id}.{parsed_other.secret}"
    scenarios = [
        (_stored_api_key(generated, revoked_at=now), generated.token),
        (_stored_api_key(generated, expires_at=now - timedelta(seconds=1)), generated.token),
        (_stored_api_key(generated), wrong_secret_token),
    ]

    for stored_key, presented_token in scenarios:
        app = _app()
        _override_session(app, ScalarSession(stored_key))
        async with api_client(app) as client:
            response = await client.get(
                "/v1/tenant",
                headers={"Authorization": f"Bearer {presented_token}"},
            )

        assert response.json() == _invalid_credentials_body()
        assert response.headers["www-authenticate"] == "Bearer"
        assert presented_token not in response.text


@pytest.mark.asyncio
async def test_bootstrap_returns_api_key_once_without_persisting_the_raw_value() -> None:
    bootstrap_token = "bootstrap_" + "A" * 40
    settings = Settings(
        environment="test",
        bootstrap_enabled=True,
        bootstrap_token=bootstrap_token,
        _env_file=None,
    )
    app = _app(settings)
    write_session = RecordingSession()
    _override_session(app, write_session)

    async with api_client(app) as client:
        created = await client.post(
            "/v1/bootstrap/tenants",
            headers={"Authorization": f"Bearer {bootstrap_token}"},
            json={"name": "Acme", "initial_api_key_name": "production"},
        )

        assert created.status_code == 201
        raw_key = cast(str, created.json()["api_key"]["key"])
        parsed = parse_api_key(raw_key)
        assert parsed is not None
        stored_key = next(item for item in write_session.added if isinstance(item, ApiKey))
        tenant = next(item for item in write_session.added if isinstance(item, Tenant))
        assert stored_key.public_id == parsed.public_id
        assert stored_key.secret_hash != parsed.secret.encode()
        assert raw_key not in repr(stored_key)
        assert raw_key not in str(vars(stored_key))
        assert created.headers["cache-control"] == "no-store"
        assert created.headers["pragma"] == "no-cache"
        assert write_session.commits == 1

        read_session = SequenceSession([stored_key, tenant])
        _override_session(app, read_session)
        inspected = await client.get(
            "/v1/tenant",
            headers={"Authorization": f"Bearer {raw_key}"},
        )

    assert inspected.status_code == 200
    assert inspected.json() == {
        "id": str(tenant.id),
        "name": "Acme",
        "created_at": tenant.created_at.isoformat().replace("+00:00", "Z"),
    }
    assert raw_key not in inspected.text
    assert "api_key" not in inspected.json()


@pytest.mark.asyncio
async def test_endpoint_signing_secret_is_one_time_and_ciphertext_only_at_rest() -> None:
    tenant_context = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())

    async def authenticated() -> AuthenticatedTenant:
        return tenant_context

    app = _app()
    app.dependency_overrides[authenticate_tenant] = authenticated
    write_session = RecordingSession()
    _override_session(app, write_session)

    async with api_client(app) as client:
        created = await client.post(
            "/v1/endpoints",
            json={"name": "orders", "url": "https://receiver.example/webhooks"},
        )

        assert created.status_code == 201
        raw_secret = cast(str, created.json()["signing_secret"])
        endpoint = next(item for item in write_session.added if isinstance(item, WebhookEndpoint))
        stored_secret = next(
            item for item in write_session.added if isinstance(item, EndpointSigningSecret)
        )
        assert raw_secret.startswith("whsec_")
        assert raw_secret.encode() not in stored_secret.ciphertext
        assert raw_secret not in repr(stored_secret)
        assert raw_secret not in str(vars(stored_secret))
        assert stored_secret.encryption_key_version == 1
        assert created.headers["cache-control"] == "no-store"
        assert created.headers["pragma"] == "no-cache"

        _override_session(app, ScalarSession(endpoint))
        inspected = await client.get(f"/v1/endpoints/{endpoint.id}")

    assert inspected.status_code == 200
    assert "signing_secret" not in inspected.json()
    assert raw_secret not in inspected.text


@pytest.mark.asyncio
async def test_problem_details_cover_media_type_json_validation_and_unknown_routes() -> None:
    bootstrap_token = "bootstrap_" + "B" * 40
    tenant_context = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())

    async def authenticated() -> AuthenticatedTenant:
        return tenant_context

    app = _app(
        Settings(
            environment="test",
            bootstrap_enabled=True,
            bootstrap_token=bootstrap_token,
            _env_file=None,
        )
    )
    app.dependency_overrides[authenticate_tenant] = authenticated
    _override_session(app, RecordingSession())

    async with api_client(app) as client:
        missing_media_type = await client.post("/v1/events", content=b"{}")
        unsupported = await client.post(
            "/v1/events",
            content="{}",
            headers={"Content-Type": "text/plain"},
        )
        malformed = await client.post(
            "/v1/bootstrap/tenants",
            content=b'{"name":',
            headers={
                "Authorization": f"Bearer {bootstrap_token}",
                "Content-Type": "application/json",
            },
        )
        injected_value = "do-not-reflect-this-tenant"
        invalid = await client.post(
            "/v1/bootstrap/tenants",
            headers={"Authorization": f"Bearer {bootstrap_token}"},
            json={"name": "Acme", "tenant_id": injected_value},
        )
        missing = await client.get("/definitely-not-a-route")

    _assert_problem(missing_media_type, status=415, code="unsupported_media_type")
    _assert_problem(unsupported, status=415, code="unsupported_media_type")
    malformed_body = _assert_problem(malformed, status=400, code="invalid_json")
    assert "errors" not in malformed_body
    invalid_body = _assert_problem(invalid, status=422, code="validation_error")
    errors = cast(list[dict[str, str]], invalid_body["errors"])
    assert {error["pointer"] for error in errors} == {"/body/tenant_id"}
    assert injected_value not in invalid.text
    _assert_problem(missing, status=404, code="resource_not_found")


@pytest.mark.asyncio
async def test_event_idempotency_header_is_required_and_strictly_validated() -> None:
    tenant_context = AuthenticatedTenant(tenant_id=uuid4(), api_key_id=uuid4())

    async def authenticated() -> AuthenticatedTenant:
        return tenant_context

    app = _app()
    app.dependency_overrides[authenticate_tenant] = authenticated
    _override_session(app, RecordingSession())
    request_body = {
        "type": "order.created",
        "payload": {"order_id": "ord_123"},
        "endpoint_ids": [str(uuid4())],
    }

    async with api_client(app) as client:
        missing = await client.post("/v1/events", json=request_body)
        invalid = await client.post(
            "/v1/events",
            headers={"Idempotency-Key": "short"},
            json=request_body,
        )

    _assert_problem(missing, status=400, code="idempotency_key_required")
    _assert_problem(invalid, status=400, code="invalid_idempotency_key")


@pytest.mark.asyncio
async def test_unexpected_errors_are_sanitized_in_response_and_logs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    leaked_value = "postgresql://private-user:private-password@database/events"
    app = _app()

    @app.get("/test-only/unexpected")
    async def explode() -> None:
        raise RuntimeError(leaked_value)

    async with LifespanManager(app) as manager:
        transport = ASGITransport(app=manager.app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/test-only/unexpected")

    body = _assert_problem(response, status=500, code="internal_error")
    assert body["detail"] == "The request could not be completed."
    assert leaked_value not in response.text
    assert leaked_value not in capsys.readouterr().err


def test_openapi_describes_stage2_security_errors_and_one_time_secret_shapes() -> None:
    schema = _app().openapi()
    paths = cast(dict[str, dict[str, object]], schema["paths"])
    expected_paths = {
        "/v1/bootstrap/tenants",
        "/v1/tenant",
        "/v1/endpoints",
        "/v1/endpoints/{endpoint_id}",
        "/v1/events",
        "/v1/events/{event_id}",
    }

    assert expected_paths <= paths.keys()
    security_schemes = schema["components"]["securitySchemes"]
    assert security_schemes["HookRelayBearer"]["type"] == "http"
    assert security_schemes["HookRelayBearer"]["scheme"] == "bearer"

    event_post = cast(dict[str, object], paths["/v1/events"]["post"])
    assert event_post["security"] == [{"HookRelayBearer": []}]
    parameters = cast(list[dict[str, object]], event_post["parameters"])
    content_type_parameter = next(
        parameter for parameter in parameters if parameter["name"] == "Content-Type"
    )
    idempotency_parameter = next(
        parameter for parameter in parameters if parameter["name"] == "Idempotency-Key"
    )
    assert content_type_parameter["in"] == "header"
    assert content_type_parameter["required"] is True
    assert idempotency_parameter["in"] == "header"
    assert idempotency_parameter["required"] is True

    responses = cast(dict[str, dict[str, object]], event_post["responses"])
    for status_code in ("400", "401", "404", "409", "415", "422", "503"):
        content = cast(dict[str, object], responses[status_code]["content"])
        assert "application/problem+json" in content

    components = schema["components"]["schemas"]
    assert "signing_secret" in components["EndpointCreatedResponse"]["properties"]
    assert "signing_secret" not in components["EndpointResponse"]["properties"]
    assert "key" in components["IssuedApiKeyResponse"]["properties"]
    assert "key" not in components["TenantResponse"]["properties"]
