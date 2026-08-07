"""Configurable local-only webhook receiver used by Compose and end-to-end tests."""

import asyncio
import base64
import hashlib
from datetime import UTC, datetime
from functools import lru_cache

import uvicorn
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ReceiverSettings(BaseSettings):
    """Isolated configuration so this test tool never needs production credentials."""

    model_config = SettingsConfigDict(
        env_prefix="HOOKRELAY_RECEIVER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = "127.0.0.1"
    port: int = Field(default=9000, ge=1, le=65535)
    response_status_code: int = Field(default=204, ge=200, le=599)
    delay_seconds: float = Field(default=0.0, ge=0, le=120)
    max_captured_requests: int = Field(default=1000, ge=1, le=10_000)
    max_body_bytes: int = Field(default=1_048_576, ge=1024, le=10_485_760)


class CapturedRequest(BaseModel):
    """Inspection-safe evidence preserving the exact request bytes."""

    model_config = ConfigDict(extra="forbid")

    sequence: int
    received_at: datetime
    headers: dict[str, str]
    body_base64: str
    body_sha256: str


class ReceiverState:
    """A bounded in-memory request ledger for local demonstrations only."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.requests: list[CapturedRequest] = []
        self.next_sequence = 1

    def append(self, body: bytes, headers: dict[str, str]) -> CapturedRequest:
        captured = CapturedRequest(
            sequence=self.next_sequence,
            received_at=datetime.now(UTC),
            headers=headers,
            body_base64=base64.b64encode(body).decode("ascii"),
            body_sha256=hashlib.sha256(body).hexdigest(),
        )
        self.next_sequence += 1
        self.requests.append(captured)
        if len(self.requests) > self.limit:
            del self.requests[: len(self.requests) - self.limit]
        return captured


def create_test_receiver(settings: ReceiverSettings | None = None) -> FastAPI:
    """Create an intentionally small receiver whose behavior is environment-configurable."""

    receiver_settings = settings or get_receiver_settings()
    receiver_state = ReceiverState(receiver_settings.max_captured_requests)
    app = FastAPI(title="HookRelay test receiver", version="0.5.0")
    app.state.receiver_settings = receiver_settings
    app.state.receiver_state = receiver_state

    @app.get("/health/live")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/webhooks")
    async def receive_webhook(request: Request) -> Response:
        body = await request.body()
        if len(body) > receiver_settings.max_body_bytes:
            return Response(status_code=413)
        receiver_state.append(
            body,
            {name.lower(): value for name, value in request.headers.items()},
        )
        if receiver_settings.delay_seconds:
            await asyncio.sleep(receiver_settings.delay_seconds)
        return Response(status_code=receiver_settings.response_status_code)

    @app.get("/requests", response_model=list[CapturedRequest])
    async def list_requests() -> list[CapturedRequest]:
        return list(receiver_state.requests)

    @app.get("/requests/{delivery_id}", response_model=list[CapturedRequest])
    async def requests_for_delivery(delivery_id: str) -> list[CapturedRequest]:
        return [
            item
            for item in receiver_state.requests
            if item.headers.get("hookrelay-delivery-id") == delivery_id
        ]

    @app.delete("/requests", status_code=204)
    async def clear_requests() -> Response:
        receiver_state.requests.clear()
        return Response(status_code=204)

    return app


@lru_cache
def get_receiver_settings() -> ReceiverSettings:
    return ReceiverSettings()


app = create_test_receiver()


def run() -> None:
    """Run the local test receiver as a separate process."""

    settings = get_receiver_settings()
    uvicorn.run(
        "hookrelay.test_receiver:app",
        host=settings.host,
        port=settings.port,
        log_level="info",
        loop="asyncio",
    )
