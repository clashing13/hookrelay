"""Focused Stage 6 correlation, tracing, logging, and metric contracts."""

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from httpx2 import ASGITransport, AsyncClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import generate_latest
from starlette.types import Message, Receive, Scope, Send

from hookrelay.api.errors import register_exception_handlers
from hookrelay.broker import DeliveryRequestedMessage, JetStreamBroker, encode_delivery_message
from hookrelay.config import Settings
from hookrelay.logging import JsonFormatter
from hookrelay.metrics import HookRelayMetrics, PrometheusServer
from hookrelay.observability import (
    HTTP_CORRELATION_HEADER,
    NATS_CORRELATION_HEADER,
    ObservabilityMiddleware,
    Telemetry,
    capture_persisted_trace_context,
    correlation_scope,
    current_correlation_id,
    inject_message_context,
    normalize_correlation_id,
)

TRACEPARENT_PATTERN = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")
PERSISTED_TRACEPARENT_PATTERN = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$")


def _settings(**changes: object) -> Settings:
    return Settings.model_validate(
        {
            "environment": "test",
            "telemetry_enabled": True,
            "otel_exporter_otlp_endpoint": "http://collector:4318/v1/traces",
            "_env_file": None,
            **changes,
        }
    )


def _message() -> DeliveryRequestedMessage:
    return DeliveryRequestedMessage(
        type="delivery.requested",
        schema_version=1,
        message_id=uuid4(),
        tenant_id=uuid4(),
        event_id=uuid4(),
        endpoint_id=uuid4(),
        delivery_id=uuid4(),
    )


def test_correlation_ids_are_canonical_uuids_and_context_is_restored() -> None:
    expected = uuid4()

    assert normalize_correlation_id(str(expected).upper()) == str(expected)
    assert UUID(normalize_correlation_id("not-a-uuid"))
    assert current_correlation_id() is None
    with correlation_scope(str(expected).upper()):
        assert current_correlation_id() == str(expected)
        assert capture_persisted_trace_context().correlation_id == expected
    assert current_correlation_id() is None


@pytest.mark.asyncio
async def test_manual_otel_span_propagates_w3c_and_persisted_context() -> None:
    exporter = InMemorySpanExporter()
    telemetry = Telemetry.from_settings(
        _settings(),
        service_role="api",
        span_exporter=exporter,
    )
    correlation_id = uuid4()

    with correlation_scope(str(correlation_id)):
        with telemetry.start_as_current_span("accept event"):
            headers: dict[str, str] = {}
            inject_message_context(headers)
            persisted = capture_persisted_trace_context()

    assert headers[NATS_CORRELATION_HEADER] == str(correlation_id)
    assert TRACEPARENT_PATTERN.fullmatch(headers["traceparent"]) is not None
    assert persisted.correlation_id == correlation_id
    assert persisted.traceparent is not None
    assert PERSISTED_TRACEPARENT_PATTERN.fullmatch(persisted.traceparent) is not None
    assert persisted.traceparent.rsplit("-", 1)[0] == headers["traceparent"].rsplit("-", 1)[0]
    assert [span.name for span in exporter.get_finished_spans()] == ["accept event"]
    await telemetry.shutdown()


@pytest.mark.asyncio
async def test_json_logs_add_safe_process_and_active_context_fields() -> None:
    exporter = InMemorySpanExporter()
    settings = _settings()
    telemetry = Telemetry.from_settings(settings, service_role="worker", span_exporter=exporter)
    formatter = JsonFormatter(settings, "worker")
    correlation_id = uuid4()
    record = logging.LogRecord(
        "hookrelay.worker",
        logging.INFO,
        __file__,
        1,
        "delivery_finished",
        (),
        None,
    )
    delivery_id = str(uuid4())
    record.delivery_id = delivery_id
    record.service = "untrusted-override"

    with correlation_scope(str(correlation_id)):
        with telemetry.start_as_current_span("finish"):
            body = cast(dict[str, object], json.loads(formatter.format(record)))

    assert body["service"] == "hookrelay"
    assert body["version"] == "0.6.0"
    assert body["environment"] == "test"
    assert body["service_role"] == "worker"
    assert body["correlation_id"] == str(correlation_id)
    assert re.fullmatch(r"[0-9a-f]{32}", cast(str, body["trace_id"]))
    assert re.fullmatch(r"[0-9a-f]{16}", cast(str, body["span_id"]))
    assert body["delivery_id"] == delivery_id
    await telemetry.shutdown()


def test_custom_metrics_registry_uses_only_bounded_labels() -> None:
    metrics = HookRelayMetrics()
    metrics.observe_api_request(
        method="CUSTOM-UNTRUSTED",
        route="not-a-route",
        status_code=999,
        duration_seconds=0.25,
    )
    metrics.observe_outbox_claimed(2)
    metrics.observe_outbox_published(duplicate=True)
    metrics.observe_worker_message("not-a-state")
    metrics.observe_broker_disposition("ack", succeeded=True)
    metrics.observe_attempt(
        outcome="succeeded",
        circuit_probe=False,
        duration_seconds=0.1,
    )
    metrics.observe_delivery_deferral("rate_limited")

    exposition = generate_latest(metrics.registry).decode("utf-8")
    assert 'method="OTHER",route="unmatched",status_class="other"' in exposition
    assert 'state="interrupted"' in exposition
    assert 'circuit_probe="false",outcome="succeeded"' in exposition
    assert 'reason="rate_limited"' in exposition
    assert "python_gc" not in exposition


@pytest.mark.asyncio
async def test_prometheus_listener_serves_only_the_custom_metrics_path() -> None:
    metrics = HookRelayMetrics()
    metrics.observe_outbox_claimed(1)
    server = await PrometheusServer(metrics.registry, host="127.0.0.1", port=0).start()
    assert server.bound_port is not None
    reader, writer = await asyncio.open_connection(
        "127.0.0.1",
        server.bound_port,
    )
    writer.write(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    await server.close()

    assert response.startswith(b"HTTP/1.1 200 OK")
    assert b"hookrelay_outbox_messages_claimed_total 1.0" in response
    assert b"X-Content-Type-Options: nosniff" in response


@pytest.mark.asyncio
async def test_request_middleware_does_not_consume_body_and_returns_correlation() -> None:
    metrics = HookRelayMetrics()
    sent: list[Message] = []

    @dataclass(frozen=True)
    class Route:
        path: str = "/v1/events/{event_id}"

    async def inner(scope: Scope, _receive: Receive, send: Send) -> None:
        scope["route"] = Route()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    async def forbidden_receive() -> Message:
        raise AssertionError("observability middleware must not consume the body")

    async def capture_send(message: Message) -> None:
        sent.append(message)

    middleware = ObservabilityMiddleware(
        inner,
        telemetry=Telemetry.disabled(),
        metrics=metrics,
    )
    scope = cast(
        Scope,
        {
            "type": "http",
            "method": "GET",
            "headers": [(b"x-correlation-id", b"unsafe")],
        },
    )
    await middleware(scope, forbidden_receive, capture_send)

    response_headers = dict(cast(list[tuple[bytes, bytes]], sent[0]["headers"]))
    assert UUID(response_headers[HTTP_CORRELATION_HEADER.encode("ascii")].decode("ascii"))
    exposition = generate_latest(metrics.registry).decode("utf-8")
    assert 'route="/v1/events/{event_id}"' in exposition


@pytest.mark.asyncio
async def test_unexpected_500_preserves_correlation_after_middleware_unwinds() -> None:
    app = FastAPI()
    app.add_middleware(
        ObservabilityMiddleware,
        telemetry=Telemetry.disabled(),
        metrics=None,
    )
    register_exception_handlers(app)

    @app.get("/explode")
    async def explode() -> None:
        raise RuntimeError("private failure detail")

    records: list[logging.LogRecord] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("hookrelay.api")
    handler = CaptureHandler()
    previous_propagate = logger.propagate
    logger.addHandler(handler)
    logger.propagate = False
    correlation_id = str(uuid4())
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://testserver",
        ) as client:
            response = await client.get(
                "/explode",
                headers={HTTP_CORRELATION_HEADER: correlation_id},
            )
    finally:
        logger.removeHandler(handler)
        logger.propagate = previous_propagate

    assert response.status_code == 500
    assert response.headers[HTTP_CORRELATION_HEADER] == correlation_id
    assert response.json()["code"] == "internal_error"
    assert records[0].__dict__["correlation_id"] == correlation_id
    assert "private failure detail" not in response.text


@dataclass(frozen=True)
class _Ack:
    stream: str
    seq: int = 1
    duplicate: bool = False


class _JetStream:
    def __init__(self) -> None:
        self.data: bytes | None = None
        self.headers: dict[str, str] | None = None

    async def publish(self, _subject: str, data: bytes, **kwargs: object) -> _Ack:
        self.data = data
        self.headers = cast(dict[str, str], kwargs["headers"])
        return _Ack(stream="HOOKRELAY_DELIVERIES_V1")


class _Broker(JetStreamBroker):
    def __init__(self, settings: Settings, jetstream: _JetStream) -> None:
        super().__init__(settings, client_name="test")
        self._test_jetstream = jetstream

    @property
    def jetstream(self) -> Any:
        return self._test_jetstream


@pytest.mark.asyncio
async def test_nats_headers_propagate_context_without_changing_broker_payload() -> None:
    exporter = InMemorySpanExporter()
    telemetry = Telemetry.from_settings(
        _settings(),
        service_role="outbox",
        span_exporter=exporter,
    )
    settings = _settings()
    jetstream = _JetStream()
    broker = _Broker(settings, jetstream)
    message = _message()
    correlation_id = uuid4()

    with correlation_scope(str(correlation_id)):
        with telemetry.start_as_current_span("publish"):
            await broker.publish(message)

    assert jetstream.data == encode_delivery_message(message)
    assert jetstream.headers is not None
    assert jetstream.headers[NATS_CORRELATION_HEADER] == str(correlation_id)
    assert TRACEPARENT_PATTERN.fullmatch(jetstream.headers["traceparent"]) is not None
    decoded = cast(dict[str, object], json.loads(jetstream.data))
    assert "traceparent" not in decoded
    assert "correlation_id" not in decoded
    await telemetry.shutdown()
