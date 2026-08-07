"""Shared asynchronous API fixtures."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx2 import ASGITransport, AsyncClient

from hookrelay.config import Settings
from hookrelay.main import create_app


@asynccontextmanager
async def api_client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Run ASGI lifespan and expose an entirely asynchronous HTTP client."""

    async with LifespanManager(app) as manager:
        transport = ASGITransport(app=manager.app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as test_client:
            yield test_client


@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    settings = Settings(environment="test", _env_file=None)
    app = create_app(settings)
    async with api_client(app) as test_client:
        yield test_client
