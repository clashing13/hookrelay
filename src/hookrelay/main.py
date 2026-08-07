"""FastAPI application factory and Uvicorn entry point."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from hookrelay.api.endpoints import router as endpoints_router
from hookrelay.api.errors import register_exception_handlers
from hookrelay.api.events import router as events_router
from hookrelay.api.health import router as health_router
from hookrelay.api.tenants import router as bootstrap_router
from hookrelay.api.tenants import tenant_router
from hookrelay.config import Settings, get_settings
from hookrelay.database import DatabaseHealth, PostgresDatabase
from hookrelay.logging import configure_logging
from hookrelay.security import SecretCipher


def create_app(
    settings: Settings | None = None,
    database: DatabaseHealth | None = None,
) -> FastAPI:
    """Build an application instance with explicit, testable dependencies."""

    app_settings = settings or get_settings()
    logger = configure_logging(app_settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app_database = database or PostgresDatabase(app_settings)
        app.state.settings = app_settings
        app.state.database = app_database
        app.state.secret_cipher = SecretCipher(
            app_settings.secret_encryption_key_bytes(),
            app_settings.secret_encryption_key_version,
        )
        logger.info(
            "application_started",
            extra={"service": app_settings.service_name, "version": app_settings.version},
        )
        try:
            yield
        finally:
            await app_database.dispose()
            logger.info(
                "application_stopped",
                extra={"service": app_settings.service_name},
            )

    app = FastAPI(
        title="HookRelay API",
        version=app_settings.version,
        lifespan=lifespan,
    )
    register_exception_handlers(app)
    app.include_router(health_router)
    app.include_router(bootstrap_router)
    app.include_router(tenant_router)
    app.include_router(endpoints_router)
    app.include_router(events_router)
    return app


app = create_app()


def run() -> None:
    """Start the development server using environment-backed settings."""

    settings = get_settings()
    uvicorn.run(
        "hookrelay.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        loop="asyncio",
    )
