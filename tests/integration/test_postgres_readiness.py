"""Integration test that executes readiness through a real PostgreSQL engine."""

import os

import pytest

from hookrelay.config import Settings
from hookrelay.main import create_app
from tests.conftest import api_client

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_readiness_executes_against_real_postgresql() -> None:
    database_url = os.getenv("HOOKRELAY_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("HOOKRELAY_TEST_DATABASE_URL is not configured")

    settings = Settings(
        environment="test",
        database_url=database_url,
        readiness_timeout_seconds=5,
        _env_file=None,
    )
    app = create_app(settings)

    async with api_client(app) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
