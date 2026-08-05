"""API contract tests for process liveness."""

import pytest
from httpx2 import AsyncClient


@pytest.mark.asyncio
async def test_liveness_contract(client: AsyncClient) -> None:
    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "hookrelay",
        "version": "0.4.0",
    }


@pytest.mark.asyncio
async def test_openapi_contains_liveness_route(client: AsyncClient) -> None:
    response = await client.get("/openapi.json")

    assert response.status_code == 200
    assert "/health/live" in response.json()["paths"]
    assert "/health/ready" in response.json()["paths"]
