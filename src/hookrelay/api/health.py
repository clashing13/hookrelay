"""Process liveness and dependency readiness endpoints."""

import asyncio
import logging
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from hookrelay.config import Settings
from hookrelay.database import DatabaseHealth

router = APIRouter(prefix="/health", tags=["health"])


class LivenessResponse(BaseModel):
    """Stable response returned when the API process can answer requests."""

    status: Literal["ok"] = "ok"
    service: str
    version: str


class ReadinessUnavailableResponse(BaseModel):
    """Sanitized response returned when dependency-backed work is unsafe."""

    status: Literal["unavailable"] = "unavailable"
    service: str
    version: str


@router.get("/live", response_model=LivenessResponse)
async def liveness(request: Request) -> LivenessResponse:
    """Report process liveness without contacting external dependencies."""

    settings = request.app.state.settings
    return LivenessResponse(service=settings.service_name, version=settings.version)


@router.get(
    "/ready",
    response_model=LivenessResponse,
    responses={503: {"model": ReadinessUnavailableResponse}},
)
async def readiness(request: Request) -> LivenessResponse | JSONResponse:
    """Report whether PostgreSQL can support dependency-backed traffic right now."""

    settings: Settings = request.app.state.settings
    database: DatabaseHealth = request.app.state.database

    try:
        await asyncio.wait_for(
            database.check_readiness(),
            timeout=settings.readiness_timeout_seconds,
        )
    except Exception as exc:
        logging.getLogger("hookrelay.health").warning(
            "database_readiness_failed",
            extra={"error_type": type(exc).__name__},
        )
        response = ReadinessUnavailableResponse(
            service=settings.service_name,
            version=settings.version,
        )
        return JSONResponse(status_code=503, content=response.model_dump(mode="json"))

    return LivenessResponse(service=settings.service_name, version=settings.version)
