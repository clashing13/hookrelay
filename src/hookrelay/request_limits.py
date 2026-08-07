"""Bound request memory use before FastAPI parses untrusted bodies."""

from collections import deque
from collections.abc import Sequence

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from hookrelay.api.errors import PROBLEM_MEDIA_TYPE


def _request_too_large_response() -> JSONResponse:
    """Build the same sanitized Problem Details shape used by API handlers."""

    return JSONResponse(
        status_code=413,
        content={
            "type": "urn:hookrelay:problem:request-body-too-large",
            "title": "Request body too large",
            "status": 413,
            "code": "request_body_too_large",
            "detail": "The request body exceeds the configured byte limit.",
        },
        media_type=PROBLEM_MEDIA_TYPE,
    )


def _declared_content_length_exceeds(
    headers: Sequence[tuple[bytes, bytes]],
    maximum: int,
) -> bool:
    """Compare one decimal length hint without parsing an unbounded integer."""

    values = [value.strip() for name, value in headers if name.lower() == b"content-length"]
    if len(values) != 1 or not values[0].isdigit():
        return False

    significant_digits = values[0].lstrip(b"0") or b"0"
    maximum_digits = str(maximum).encode("ascii")
    return len(significant_digits) > len(maximum_digits) or (
        len(significant_digits) == len(maximum_digits) and significant_digits > maximum_digits
    )


class RequestBodyLimitMiddleware:
    """Reject oversized HTTP bodies before routing, authentication, or JSON parsing.

    ``Content-Length`` provides an early rejection path but is never trusted as the
    byte count. Buffering and replaying the bounded ASGI messages also covers requests
    with no length, a false length, or chunked transfer encoding.
    """

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        if max_body_bytes < 1:
            msg = "max_body_bytes must be positive"
            raise ValueError(msg)
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if _declared_content_length_exceeds(
            scope.get("headers", []),
            self.max_body_bytes,
        ):
            await _request_too_large_response()(scope, receive, send)
            return

        buffered_messages: deque[Message] = deque()
        received_bytes = 0
        while True:
            message = await receive()
            buffered_messages.append(message)
            if message["type"] != "http.request":
                break

            received_bytes += len(message.get("body", b""))
            if received_bytes > self.max_body_bytes:
                await _request_too_large_response()(scope, receive, send)
                return
            if not message.get("more_body", False):
                break

        async def replay_receive() -> Message:
            if buffered_messages:
                return buffered_messages.popleft()
            return await receive()

        await self.app(scope, replay_receive, send)
