"""Low-cardinality Prometheus metrics and a process-local async listener."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Literal

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.exposition import CONTENT_TYPE_LATEST
from starlette.requests import Request
from starlette.responses import Response

from hookrelay.config import Settings

_HTTP_METHODS = frozenset({"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"})
_DELIVERY_STATES = frozenset(
    {
        "succeeded",
        "already_succeeded",
        "retry_scheduled",
        "in_progress",
        "dead_lettered",
        "stale",
        "invalid",
        "target_blocked",
        "claim_lost",
        "rejected",
        "interrupted",
    }
)
_ATTEMPT_OUTCOMES = frozenset({"succeeded", "transient_failure", "permanent_failure", "abandoned"})
_DEFERRAL_REASONS = frozenset({"rate_limited", "circuit_open", "circuit_probe_active"})
_BROKER_DISPOSITIONS = frozenset({"ack", "nak", "term"})


class HookRelayMetrics:
    """Own a custom registry so processes and tests never share metric state."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self._api_requests = Counter(
            "hookrelay_api_requests_total",
            "Completed HookRelay API requests.",
            ("method", "route", "status_class"),
            registry=self.registry,
        )
        self._api_request_duration = Histogram(
            "hookrelay_api_request_duration_seconds",
            "HookRelay API request duration.",
            ("method", "route"),
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
            registry=self.registry,
        )
        self._outbox_claimed = Counter(
            "hookrelay_outbox_messages_claimed_total",
            "Outbox rows claimed after the claim transaction committed.",
            registry=self.registry,
        )
        self._outbox_published = Counter(
            "hookrelay_outbox_messages_published_total",
            "Outbox messages finalized after a JetStream PubAck.",
            registry=self.registry,
        )
        self._outbox_duplicates = Counter(
            "hookrelay_outbox_duplicate_observations_total",
            "JetStream PubAcks marked as duplicate observations.",
            registry=self.registry,
        )
        self._outbox_failures = Counter(
            "hookrelay_outbox_publish_failures_total",
            "Bounded outbox publish scans interrupted by failure.",
            registry=self.registry,
        )
        self._worker_messages = Counter(
            "hookrelay_worker_message_results_total",
            "Durable worker message observations by bounded result state.",
            ("state",),
            registry=self.registry,
        )
        self._worker_in_flight = Gauge(
            "hookrelay_worker_messages_in_flight",
            "Messages currently executing in this worker process.",
            registry=self.registry,
        )
        self._broker_dispositions = Counter(
            "hookrelay_worker_broker_dispositions_total",
            "Broker ACK, delayed NAK, and TERM observations.",
            ("disposition", "result"),
            registry=self.registry,
        )
        self._attempts = Counter(
            "hookrelay_delivery_attempts_total",
            "Durably completed or abandoned HTTP attempts.",
            ("outcome", "circuit_probe"),
            registry=self.registry,
        )
        self._attempt_duration = Histogram(
            "hookrelay_delivery_attempt_duration_seconds",
            "Persisted HTTP attempt duration by bounded outcome.",
            ("outcome", "circuit_probe"),
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
            registry=self.registry,
        )
        self._deferrals = Counter(
            "hookrelay_delivery_deferrals_total",
            "Rate-limit and circuit deferrals that created no HTTP attempt.",
            ("reason",),
            registry=self.registry,
        )

    @staticmethod
    def _status_class(status_code: int) -> str:
        return f"{status_code // 100}xx" if 100 <= status_code <= 599 else "other"

    def observe_api_request(
        self,
        *,
        method: str,
        route: str,
        status_code: int,
        duration_seconds: float,
    ) -> None:
        safe_method = method if method in _HTTP_METHODS else "OTHER"
        safe_route = route if route.startswith("/") and len(route) <= 200 else "unmatched"
        self._api_requests.labels(
            method=safe_method,
            route=safe_route,
            status_class=self._status_class(status_code),
        ).inc()
        self._api_request_duration.labels(method=safe_method, route=safe_route).observe(
            max(0.0, duration_seconds)
        )

    def observe_outbox_claimed(self, count: int) -> None:
        if count > 0:
            self._outbox_claimed.inc(count)

    def observe_outbox_published(self, *, duplicate: bool) -> None:
        self._outbox_published.inc()
        if duplicate:
            self._outbox_duplicates.inc()

    def observe_outbox_failure(self) -> None:
        self._outbox_failures.inc()

    def observe_worker_message(self, state: str) -> None:
        safe_state = state if state in _DELIVERY_STATES else "interrupted"
        self._worker_messages.labels(state=safe_state).inc()

    def worker_message_started(self) -> None:
        self._worker_in_flight.inc()

    def worker_message_finished(self) -> None:
        self._worker_in_flight.dec()

    def observe_broker_disposition(self, disposition: str, *, succeeded: bool) -> None:
        if disposition not in _BROKER_DISPOSITIONS:
            raise ValueError("broker disposition is not part of the bounded metric vocabulary")
        self._broker_dispositions.labels(
            disposition=disposition,
            result="succeeded" if succeeded else "failed",
        ).inc()

    def observe_attempt(
        self,
        *,
        outcome: str,
        circuit_probe: bool,
        duration_seconds: float,
    ) -> None:
        if outcome not in _ATTEMPT_OUTCOMES:
            raise ValueError("attempt outcome is not part of the bounded metric vocabulary")
        probe = "true" if circuit_probe else "false"
        self._attempts.labels(outcome=outcome, circuit_probe=probe).inc()
        self._attempt_duration.labels(outcome=outcome, circuit_probe=probe).observe(
            max(0.0, duration_seconds)
        )

    def observe_delivery_deferral(self, reason: str) -> None:
        if reason not in _DEFERRAL_REASONS:
            raise ValueError("deferral reason is not part of the bounded metric vocabulary")
        self._deferrals.labels(reason=reason).inc()


class PrometheusServer:
    """Serve one custom registry without adding an HTTP framework to workers."""

    def __init__(self, registry: CollectorRegistry, *, host: str, port: int) -> None:
        self._registry = registry
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None

    @property
    def bound_port(self) -> int | None:
        if self._server is None or not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> PrometheusServer:
        if self._server is None:
            self._server = await asyncio.start_server(
                self._handle_connection,
                host=self._host,
                port=self._port,
                limit=8192,
            )
        return self

    async def close(self) -> None:
        if self._server is None:
            return
        server = self._server
        self._server = None
        server.close()
        await server.wait_closed()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        status = "400 Bad Request"
        content_type = "text/plain; charset=utf-8"
        body = b"Bad Request\n"
        include_body = True
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
            first_line = request.split(b"\r\n", maxsplit=1)[0].decode("ascii")
            method, target, version = first_line.split(" ", maxsplit=2)
            if version not in {"HTTP/1.0", "HTTP/1.1"}:
                raise ValueError("unsupported HTTP version")
            include_body = method != "HEAD"
            if method not in {"GET", "HEAD"}:
                status = "405 Method Not Allowed"
                body = b"Method Not Allowed\n"
            elif target.split("?", maxsplit=1)[0] != "/metrics":
                status = "404 Not Found"
                body = b"Not Found\n"
            else:
                status = "200 OK"
                content_type = CONTENT_TYPE_LATEST
                body = generate_latest(self._registry)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, ValueError):
            pass

        response_body = body if include_body else b""
        response = (
            f"HTTP/1.1 {status}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n"
            "X-Content-Type-Options: nosniff\r\n"
            "\r\n"
        ).encode("ascii") + response_body
        try:
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


async def start_metrics_server(
    settings: Settings,
    metrics: HookRelayMetrics,
    *,
    service_role: Literal["outbox", "worker"],
) -> PrometheusServer | None:
    """Start the configured publisher/worker listener, or remain explicitly disabled."""

    if not settings.metrics_enabled:
        return None
    port = (
        settings.outbox_metrics_port if service_role == "outbox" else settings.worker_metrics_port
    )
    return await PrometheusServer(metrics.registry, host=settings.metrics_host, port=port).start()


def build_metrics_endpoint(
    metrics: HookRelayMetrics,
) -> Callable[[Request], Awaitable[Response]]:
    """Build the exact `/metrics` endpoint main registers on the API listener."""

    async def metrics_endpoint(_request: Request) -> Response:
        return Response(
            content=generate_latest(metrics.registry),
            media_type=CONTENT_TYPE_LATEST,
            headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
        )

    return metrics_endpoint
