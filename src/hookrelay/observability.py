"""Correlation, W3C propagation, and explicit OpenTelemetry lifecycle helpers."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Literal, Protocol, cast
from uuid import UUID, uuid4

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.trace import NoOpTracerProvider, Span, SpanKind, Status, StatusCode, Tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from hookrelay.config import Settings

ServiceRole = Literal["api", "outbox", "worker"]

HTTP_CORRELATION_HEADER = "X-Correlation-ID"
NATS_CORRELATION_HEADER = "HookRelay-Correlation-Id"
CORRELATION_SCOPE_KEY = "hookrelay.correlation_id"
_CORRELATION_ID: ContextVar[str | None] = ContextVar(
    "hookrelay_correlation_id",
    default=None,
)
_TRACE_PROPAGATOR = TraceContextTextMapPropagator()


class RequestMetrics(Protocol):
    """Small request-metric seam used without coupling middleware to a registry."""

    def observe_api_request(
        self,
        *,
        method: str,
        route: str,
        status_code: int,
        duration_seconds: float,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class PersistedTraceContext:
    """Bounded context fields safe to snapshot beside an outbox message."""

    correlation_id: UUID
    traceparent: str | None


class _BoundedBatchSpanProcessor(BatchSpanProcessor):
    """Apply HookRelay's configured timeout to the SDK's shutdown drain."""

    def __init__(
        self,
        span_exporter: SpanExporter,
        *,
        shutdown_timeout_millis: int,
        max_queue_size: int,
        schedule_delay_millis: int,
        max_export_batch_size: int,
        export_timeout_millis: int,
    ) -> None:
        super().__init__(
            span_exporter,
            max_queue_size=max_queue_size,
            schedule_delay_millis=schedule_delay_millis,
            max_export_batch_size=max_export_batch_size,
            export_timeout_millis=export_timeout_millis,
        )
        self._shutdown_timeout_millis = shutdown_timeout_millis

    def shutdown(self) -> None:
        # The public SDK processor currently hard-codes a 30-second drain. Its
        # internal batch primitive exposes the timeout and the dependency is pinned.
        self._batch_processor.shutdown(timeout_millis=self._shutdown_timeout_millis)


def _new_correlation_id() -> str:
    return str(uuid4())


def normalize_correlation_id(value: str | None) -> str:
    """Normalize one UUID correlation identifier or replace it with a fresh UUID."""

    if value is not None:
        try:
            return str(UUID(value.strip()))
        except (ValueError, AttributeError):
            pass
    return _new_correlation_id()


def current_correlation_id() -> str | None:
    """Return the correlation identifier bound to the current async context."""

    return _CORRELATION_ID.get()


@contextmanager
def correlation_scope(value: str | None = None) -> Iterator[None]:
    """Bind one validated correlation identifier and restore the previous context."""

    token: Token[str | None] = _CORRELATION_ID.set(normalize_correlation_id(value))
    try:
        yield
    finally:
        _CORRELATION_ID.reset(token)


def current_trace_fields() -> dict[str, str]:
    """Return fixed-width trace identifiers for structured logs when a span is active."""

    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return {}
    return {
        "trace_id": f"{span_context.trace_id:032x}",
        "span_id": f"{span_context.span_id:016x}",
    }


def _string_carrier(headers: Mapping[str, object] | None) -> dict[str, str]:
    if headers is None:
        return {}
    normalized: dict[str, str] = {}
    for name, value in headers.items():
        if isinstance(value, str):
            normalized[name.lower()] = value
        elif isinstance(value, (list, tuple)) and value and isinstance(value[0], str):
            normalized[name.lower()] = value[0]
    return normalized


def extract_trace_context(headers: Mapping[str, object] | None) -> Context:
    """Extract only the W3C Trace Context fields from an untrusted carrier."""

    return _TRACE_PROPAGATOR.extract(_string_carrier(headers))


def inject_message_context(headers: dict[str, str]) -> None:
    """Add current W3C trace and bounded correlation context to NATS headers."""

    _TRACE_PROPAGATOR.inject(headers)
    correlation_id = current_correlation_id()
    if correlation_id is not None:
        headers[NATS_CORRELATION_HEADER] = correlation_id


def capture_persisted_trace_context() -> PersistedTraceContext:
    """Snapshot the current request context for an outbox row without payload changes."""

    carrier: dict[str, str] = {}
    _TRACE_PROPAGATOR.inject(carrier)
    traceparent = carrier.get("traceparent")
    if traceparent is not None:
        # Persist only the sampled bit. Newer OTel SDKs may also emit the W3C
        # random-trace-id bit (03); the durable column intentionally accepts the
        # stable 00/01 subset so rows remain readable by older consumers.
        parts = traceparent.split("-")
        parts[3] = f"{int(parts[3], 16) & 1:02x}"
        traceparent = "-".join(parts)
    return PersistedTraceContext(
        correlation_id=UUID(normalize_correlation_id(current_correlation_id())),
        traceparent=traceparent,
    )


def persisted_trace_headers(context: PersistedTraceContext) -> dict[str, str]:
    """Rebuild a safe NATS/OTel carrier from an authoritative outbox snapshot."""

    headers = {NATS_CORRELATION_HEADER: str(context.correlation_id)}
    if context.traceparent is not None:
        headers["traceparent"] = context.traceparent
    return headers


def message_correlation_id(headers: Mapping[str, object] | None) -> str:
    """Read a NATS correlation header case-insensitively, replacing unsafe values."""

    carrier = _string_carrier(headers)
    return normalize_correlation_id(carrier.get(NATS_CORRELATION_HEADER.lower()))


class Telemetry:
    """Own one process-local tracer provider and its bounded exporter lifecycle."""

    def __init__(self, provider: TracerProvider | None, tracer: Tracer) -> None:
        self._provider = provider
        self.tracer = tracer
        self._shutdown = False

    @classmethod
    def disabled(cls) -> Telemetry:
        """Return a true no-op tracer without threads, queues, or network work."""

        provider = NoOpTracerProvider()
        return cls(None, provider.get_tracer("hookrelay"))

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        service_role: ServiceRole,
        span_exporter: SpanExporter | None = None,
    ) -> Telemetry:
        """Build a disabled tracer or an OTLP/HTTP batch pipeline for one process."""

        if not settings.telemetry_enabled and span_exporter is None:
            return cls.disabled()

        resource = Resource.create(
            {
                "service.name": f"{settings.service_name}-{service_role}",
                "service.version": settings.version,
                "deployment.environment.name": settings.environment,
                "hookrelay.service.role": service_role,
            }
        )
        provider = TracerProvider(resource=resource, shutdown_on_exit=False)
        if span_exporter is None:
            endpoint = settings.otel_exporter_otlp_endpoint
            if endpoint is None:
                raise ValueError("enabled telemetry requires an OTLP endpoint")
            exporter: SpanExporter = OTLPSpanExporter(
                endpoint=endpoint,
                timeout=settings.otel_export_timeout_seconds,
            )
            timeout_millis = round(settings.otel_export_timeout_seconds * 1000)
            provider.add_span_processor(
                _BoundedBatchSpanProcessor(
                    exporter,
                    shutdown_timeout_millis=timeout_millis,
                    max_queue_size=settings.otel_batch_max_queue_size,
                    schedule_delay_millis=round(settings.otel_batch_schedule_delay_seconds * 1000),
                    max_export_batch_size=settings.otel_batch_max_export_batch_size,
                    export_timeout_millis=timeout_millis,
                )
            )
        else:
            provider.add_span_processor(SimpleSpanProcessor(span_exporter))
        tracer = provider.get_tracer(f"hookrelay.{service_role}", settings.version)
        return cls(provider, tracer)

    def start_as_current_span(
        self,
        name: str,
        *,
        kind: SpanKind = SpanKind.INTERNAL,
        parent_context: Context | None = None,
        attributes: Mapping[str, str | bool | int | float] | None = None,
    ) -> AbstractContextManager[Span]:
        """Start one manual span without installing a process-global provider."""

        return cast(
            "AbstractContextManager[Span]",
            self.tracer.start_as_current_span(
                name,
                context=parent_context,
                kind=kind,
                attributes=dict(attributes or {}),
                record_exception=False,
                set_status_on_exception=False,
            ),
        )

    async def force_flush(self, timeout_seconds: float) -> bool:
        """Flush queued spans off the event loop within the caller's budget."""

        if self._provider is None or self._shutdown:
            return True
        timeout_millis = max(1, round(timeout_seconds * 1000))
        return await asyncio.to_thread(self._provider.force_flush, timeout_millis)

    async def shutdown(self) -> None:
        """Stop the process-local span pipeline exactly once without blocking asyncio."""

        if self._provider is None or self._shutdown:
            return
        self._shutdown = True
        await asyncio.to_thread(self._provider.shutdown)


NOOP_TELEMETRY = Telemetry.disabled()


class ObservabilityMiddleware:
    """Pure-ASGI request context, safe server spans, and low-cardinality metrics."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        telemetry: Telemetry,
        metrics: RequestMetrics | None = None,
    ) -> None:
        self.app = app
        self.telemetry = telemetry
        self.metrics = metrics

    @staticmethod
    def _headers(scope: Scope) -> dict[str, str]:
        return {
            name.decode("latin-1").lower(): value.decode("latin-1")
            for name, value in scope.get("headers", [])
        }

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = self._headers(scope)
        correlation_id = normalize_correlation_id(headers.get(HTTP_CORRELATION_HEADER.lower()))
        scope[CORRELATION_SCOPE_KEY] = correlation_id
        parent_context = extract_trace_context(headers)
        method = str(scope.get("method", "UNKNOWN")).upper()
        status_code = 500
        started = time.monotonic()

        async def observed_send(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                response_headers = list(message.get("headers", []))
                if not any(
                    name.lower() == HTTP_CORRELATION_HEADER.lower().encode("ascii")
                    for name, _value in response_headers
                ):
                    response_headers.append(
                        (HTTP_CORRELATION_HEADER.encode("ascii"), correlation_id.encode("ascii"))
                    )
                message["headers"] = response_headers
            await send(message)

        with correlation_scope(correlation_id):
            with self.telemetry.start_as_current_span(
                f"HTTP {method}",
                kind=SpanKind.SERVER,
                parent_context=parent_context,
                attributes={"http.request.method": method},
            ) as span:
                try:
                    await self.app(scope, receive, observed_send)
                except Exception as exc:
                    span.set_attribute("error.type", type(exc).__name__)
                    span.set_status(Status(StatusCode.ERROR))
                    raise
                finally:
                    route_object = scope.get("route")
                    route = getattr(route_object, "path", None)
                    safe_route = (
                        route if isinstance(route, str) and route.startswith("/") else "unmatched"
                    )
                    span.update_name(f"{method} {safe_route}")
                    span.set_attribute("http.route", safe_route)
                    span.set_attribute("http.response.status_code", status_code)
                    if status_code >= 500:
                        span.set_status(Status(StatusCode.ERROR))
                    duration_seconds = max(0.0, time.monotonic() - started)
                    if self.metrics is not None:
                        self.metrics.observe_api_request(
                            method=method,
                            route=safe_route,
                            status_code=status_code,
                            duration_seconds=duration_seconds,
                        )
