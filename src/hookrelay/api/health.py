"""Dependency-free process health endpoints."""

from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/health", tags=["health"])


class LivenessResponse(BaseModel):
    """Stable response returned when the API process can answer requests."""

    status: Literal["ok"] = "ok"
    service: str
    version: str


@router.get("/live", response_model=LivenessResponse)
async def liveness(request: Request) -> LivenessResponse:
    """Report process liveness without contacting external dependencies."""

    settings = request.app.state.settings
    return LivenessResponse(service=settings.service_name, version=settings.version)
