"""FastAPI application factory and Uvicorn entry point."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from hookrelay.api.health import router as health_router
from hookrelay.config import Settings, get_settings
from hookrelay.logging import configure_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build an application instance with explicit, testable dependencies."""

    app_settings = settings or get_settings()
    logger = configure_logging(app_settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = app_settings
        logger.info(
            "application_started",
            extra={"service": app_settings.service_name, "version": app_settings.version},
        )
        try:
            yield
        finally:
            logger.info("application_stopped", extra={"service": app_settings.service_name})

    app = FastAPI(
        title="HookRelay API",
        version=app_settings.version,
        lifespan=lifespan,
    )
    app.include_router(health_router)
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
    )
