"""FastAPI application factory and Uvicorn entry point."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from hookrelay.api.deliveries import router as deliveries_router
from hookrelay.api.endpoints import router as endpoints_router
from hookrelay.api.errors import register_exception_handlers
from hookrelay.api.events import router as events_router
from hookrelay.api.health import router as health_router
from hookrelay.api.tenants import router as bootstrap_router
from hookrelay.api.tenants import tenant_router
from hookrelay.config import Settings, get_settings
from hookrelay.console import ConsoleSecurityHeadersMiddleware, mount_console
from hookrelay.database import DatabaseHealth, PostgresDatabase
from hookrelay.destination_policy import DestinationPolicy
from hookrelay.logging import configure_logging
from hookrelay.metrics import HookRelayMetrics, build_metrics_endpoint
from hookrelay.observability import ObservabilityMiddleware, Telemetry
from hookrelay.request_limits import RequestBodyLimitMiddleware
from hookrelay.security import SecretCipher


def create_app(
    settings: Settings | None = None,
    database: DatabaseHealth | None = None,
) -> FastAPI:
    """Build an application instance with explicit, testable dependencies."""

    app_settings = settings or get_settings()
    logger = configure_logging(app_settings)
    telemetry = Telemetry.from_settings(app_settings, service_role="api")
    metrics = HookRelayMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app_database = database or PostgresDatabase(app_settings)
        app.state.settings = app_settings
        app.state.database = app_database
        app.state.metrics = metrics
        app.state.telemetry = telemetry
        app.state.secret_cipher = SecretCipher(
            app_settings.secret_encryption_key_bytes(),
            app_settings.secret_encryption_key_version,
        )
        app.state.destination_policy = DestinationPolicy(
            environment=app_settings.environment,
            local_exempt_hosts=app_settings.delivery_allowed_hosts,
            dns_timeout_seconds=app_settings.delivery_dns_timeout_seconds,
        )
        logger.info(
            "application_started",
            extra={"service": app_settings.service_name, "version": app_settings.version},
        )
        try:
            yield
        finally:
            try:
                await telemetry.shutdown()
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
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_bytes=app_settings.max_request_body_bytes,
    )
    app.add_middleware(ConsoleSecurityHeadersMiddleware)
    app.add_middleware(
        ObservabilityMiddleware,
        telemetry=telemetry,
        metrics=metrics,
    )
    register_exception_handlers(app)
    app.include_router(health_router)
    app.include_router(bootstrap_router)
    app.include_router(tenant_router)
    app.include_router(endpoints_router)
    app.include_router(events_router)
    app.include_router(deliveries_router)
    if app_settings.metrics_enabled:
        app.add_route(
            "/metrics",
            build_metrics_endpoint(metrics),
            methods=["GET"],
            include_in_schema=False,
        )
    mount_console(app)
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
