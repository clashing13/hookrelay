"""API behavior tests for PostgreSQL readiness and failure isolation."""

import asyncio

import pytest

from hookrelay.config import Settings
from hookrelay.main import create_app
from tests.conftest import api_client


class StubDatabase:
    """Controllable database boundary for fast API contract tests."""

    def __init__(self, *failures: Exception | None) -> None:
        self._failures = list(failures)
        self.disposed = False

    async def check_readiness(self) -> None:
        if self._failures:
            failure = self._failures.pop(0)
            if failure is not None:
                raise failure

    async def dispose(self) -> None:
        self.disposed = True


class SlowDatabase(StubDatabase):
    async def check_readiness(self) -> None:
        await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_readiness_contract_when_database_is_usable() -> None:
    app = create_app(Settings(environment="test", _env_file=None), StubDatabase())

    async with api_client(app) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "hookrelay",
        "version": "0.2.0",
    }


@pytest.mark.asyncio
async def test_readiness_failure_is_sanitized_and_liveness_stays_healthy() -> None:
    leaked_secret = "postgresql+asyncpg://user:password@private-host/hookrelay"
    database = StubDatabase(RuntimeError(leaked_secret), RuntimeError(leaked_secret))
    app = create_app(Settings(environment="test", _env_file=None), database)

    async with api_client(app) as client:
        readiness_response = await client.get("/health/ready")
        liveness_response = await client.get("/health/live")

    assert readiness_response.status_code == 503
    assert readiness_response.json() == {
        "status": "unavailable",
        "service": "hookrelay",
        "version": "0.2.0",
    }
    assert leaked_secret not in readiness_response.text
    assert liveness_response.status_code == 200


@pytest.mark.asyncio
async def test_readiness_recovers_without_application_restart() -> None:
    database = StubDatabase(ConnectionError("database is down"), None)
    app = create_app(Settings(environment="test", _env_file=None), database)

    async with api_client(app) as client:
        unavailable_response = await client.get("/health/ready")
        recovered_response = await client.get("/health/ready")

    assert unavailable_response.status_code == 503
    assert recovered_response.status_code == 200


@pytest.mark.asyncio
async def test_readiness_query_has_a_bounded_timeout() -> None:
    settings = Settings(
        environment="test",
        readiness_timeout_seconds=0.01,
        _env_file=None,
    )
    app = create_app(settings, SlowDatabase())

    async with api_client(app) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503


@pytest.mark.asyncio
async def test_application_disposes_database_during_shutdown() -> None:
    database = StubDatabase()
    app = create_app(Settings(environment="test", _env_file=None), database)

    async with api_client(app):
        assert not database.disposed

    assert database.disposed
