"""Same-origin packaging for the deliberately small operations console."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import FastAPI
from starlette.staticfiles import StaticFiles
from starlette.types import Message, Receive, Scope, Send

CONSOLE_PREFIX = "/console"
CONSOLE_ASSET_DIRECTORY = Path(__file__).with_name("console_dist")

_CONSOLE_SECURITY_HEADERS = (
    (
        b"content-security-policy",
        b"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
        b"connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; "
        b"form-action 'none'; frame-ancestors 'none'",
    ),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"x-frame-options", b"DENY"),
    (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
)


class ConsoleSecurityHeadersMiddleware:
    """Attach restrictive browser policy only to console asset responses."""

    def __init__(self, app: Callable[[Scope, Receive, Send], Awaitable[None]]) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not str(scope.get("path", "")).startswith(CONSOLE_PREFIX):
            await self._app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                existing = {name.lower() for name, _value in headers}
                headers.extend(
                    (name, value)
                    for name, value in _CONSOLE_SECURITY_HEADERS
                    if name not in existing
                )
                message = {**message, "headers": headers}
            await send(message)

        await self._app(scope, receive, send_with_headers)


def mount_console(app: FastAPI, directory: Path = CONSOLE_ASSET_DIRECTORY) -> bool:
    """Mount built assets when present and leave API-only development unaffected."""

    index = directory / "index.html"
    if not index.is_file():
        return False
    app.mount(
        CONSOLE_PREFIX,
        StaticFiles(directory=str(directory), html=True, check_dir=True),
        name="operations-console",
    )
    return True


def console_security_headers() -> dict[str, str]:
    """Expose the fixed policy for focused tests and documentation."""

    return {
        name.decode("ascii"): value.decode("ascii") for name, value in _CONSOLE_SECURITY_HEADERS
    }
