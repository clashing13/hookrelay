"""Real-PostgreSQL evidence for shared Stage 5 delivery traffic controls."""

import asyncio
import os
import subprocess
import sys
from collections import Counter
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import cast
from uuid import UUID, uuid4

import httpx2 as httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.broker import DeliveryRequestedMessage
from hookrelay.config import Settings
from hookrelay.database import PostgresDatabase
from hookrelay.delivery import DeliveryExecutionResult, DeliveryExecutor, DeliveryWork
from hookrelay.main import create_app
from hookrelay.models import Delivery, DeliveryAttempt, EndpointTrafficControl, OutboxMessage
from hookrelay.schemas import EndpointCreatedResponse, TenantBootstrapResponse
from hookrelay.security import SecretCipher

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

BOOTSTRAP_TOKEN = "hookrelay-stage5-traffic-control-bootstrap-token"


@dataclass(frozen=True, slots=True)
class SeededEndpoint:
    endpoint_id: UUID
    messages: tuple[DeliveryRequestedMessage, ...]


@dataclass(frozen=True, slots=True)
class SeededScenario:
    tenant_id: UUID
    endpoints: dict[str, SeededEndpoint]


def _required_url(environment_name: str) -> str:
    value = os.getenv(environment_name)
    if value is None:
        if os.getenv("CI") == "true":
            pytest.fail(f"CI must configure {environment_name}")
        pytest.skip(f"{environment_name} is not configured")
    return value


@pytest.fixture(scope="module")
def database_url() -> str:
    return _required_url("HOOKRELAY_TEST_DATABASE_URL")


@pytest.fixture(scope="module", autouse=True)
def migrated_database(database_url: str) -> None:
    """Apply the current reviewed migration head without relying on module order."""

    environment = os.environ.copy()
    environment["HOOKRELAY_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture
def stage5_settings(database_url: str) -> Settings:
    return Settings(
        environment="test",
        database_url=database_url,
        database_pool_size=20,
        database_max_overflow=20,
        bootstrap_enabled=True,
        bootstrap_token=BOOTSTRAP_TOKEN,
        delivery_worker_concurrency=8,
        nats_max_ack_pending=8,
        delivery_http_timeout_seconds=5,
        delivery_claim_ttl_seconds=7,
        delivery_finalization_margin_seconds=1,
        delivery_max_attempts=3,
        delivery_retry_base_seconds=0.1,
        delivery_retry_max_seconds=0.1,
        delivery_retry_jitter_ratio=0,
        delivery_rate_limit_requests=2,
        delivery_rate_limit_window_seconds=60,
        delivery_circuit_failure_threshold=2,
        delivery_circuit_cooldown_seconds=7,
        delivery_allowed_hosts=frozenset({"receiver"}),
        _env_file=None,
    )


@pytest_asyncio.fixture
async def database(stage5_settings: Settings) -> AsyncIterator[PostgresDatabase]:
    database = PostgresDatabase(stage5_settings)
    try:
        yield database
    finally:
        await database.dispose()


@asynccontextmanager
async def _api_client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            yield client


async def _seed_scenario(
    settings: Settings,
    database: PostgresDatabase,
    delivery_counts: dict[str, int],
) -> SeededScenario:
    """Create one unique tenant and unique events through the public API."""

    endpoint_ids: dict[str, UUID] = {}
    delivery_ids: dict[str, list[UUID]] = {label: [] for label in delivery_counts}
    async with _api_client(settings) as client:
        tenant_response = await client.post(
            "/v1/bootstrap/tenants",
            headers={"Authorization": f"Bearer {BOOTSTRAP_TOKEN}"},
            json={
                "name": f"stage5-traffic-{uuid4()}",
                "initial_api_key_name": "traffic-control-test",
            },
        )
        assert tenant_response.status_code == 201, tenant_response.text
        tenant = TenantBootstrapResponse.model_validate(tenant_response.json())

        for label, count in delivery_counts.items():
            endpoint_response = await client.post(
                "/v1/endpoints",
                headers={"Authorization": f"Bearer {tenant.api_key.key}"},
                json={
                    "name": f"{label}-{uuid4()}",
                    "url": f"http://receiver/{label}",
                },
            )
            assert endpoint_response.status_code == 201, endpoint_response.text
            endpoint = EndpointCreatedResponse.model_validate(endpoint_response.json())
            endpoint_ids[label] = endpoint.id

            for event_number in range(count):
                event_response = await client.post(
                    "/v1/events",
                    headers={
                        "Authorization": f"Bearer {tenant.api_key.key}",
                        "Idempotency-Key": f"stage5-{label}-{uuid4()}",
                    },
                    json={
                        "type": "traffic.test",
                        "payload": {"endpoint": label, "sequence": event_number},
                        "endpoint_ids": [str(endpoint.id)],
                    },
                )
                assert event_response.status_code == 201, event_response.text
                event_body = cast("dict[str, object]", event_response.json())
                deliveries = cast("list[dict[str, object]]", event_body["deliveries"])
                assert len(deliveries) == 1
                delivery_ids[label].append(UUID(cast("str", deliveries[0]["id"])))

    all_delivery_ids = [
        delivery_id
        for endpoint_delivery_ids in delivery_ids.values()
        for delivery_id in endpoint_delivery_ids
    ]
    async with database.session_factory() as session:
        outboxes = list(
            await session.scalars(
                select(OutboxMessage).where(OutboxMessage.delivery_id.in_(all_delivery_ids))
            )
        )
    assert len(outboxes) == len(all_delivery_ids)
    message_by_delivery = {
        outbox.delivery_id: DeliveryRequestedMessage.model_validate(outbox.payload)
        for outbox in outboxes
    }
    return SeededScenario(
        tenant_id=tenant.tenant.id,
        endpoints={
            label: SeededEndpoint(
                endpoint_id=endpoint_ids[label],
                messages=tuple(message_by_delivery[delivery_id] for delivery_id in ids),
            )
            for label, ids in delivery_ids.items()
        },
    )


@asynccontextmanager
async def _executor_pool(
    settings: Settings,
    database: PostgresDatabase,
    handler: Callable[[httpx.Request], Coroutine[None, None, httpx.Response]],
    *,
    size: int,
) -> AsyncIterator[list[DeliveryExecutor]]:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        trust_env=False,
    ) as http_client:
        cipher = SecretCipher(
            settings.secret_encryption_key_bytes(),
            settings.secret_encryption_key_version,
        )
        yield [
            DeliveryExecutor(
                settings,
                database.session_factory,
                cipher,
                http_client,
                random_source=lambda: 1,
            )
            for _ in range(size)
        ]


def _observe_next_control_lock(
    monkeypatch: pytest.MonkeyPatch,
    executor: DeliveryExecutor,
) -> asyncio.Event:
    """Signal immediately before an executor tries to acquire the endpoint row."""

    original_lock = executor._lock_traffic_control
    lock_started = asyncio.Event()

    async def observed_lock(
        session: AsyncSession,
        delivery: Delivery,
    ) -> EndpointTrafficControl:
        lock_started.set()
        return await original_lock(session, delivery)

    monkeypatch.setattr(executor, "_lock_traffic_control", observed_lock)
    return lock_started


async def _wait_for_completed(
    tasks: Sequence[asyncio.Task[DeliveryExecutionResult]],
    expected: int,
) -> None:
    deadline = asyncio.get_running_loop().time() + 3
    while sum(task.done() for task in tasks) < expected:
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"only {sum(task.done() for task in tasks)} tasks completed")
        await asyncio.sleep(0.01)


class _GatedRateHandler:
    def __init__(self, *, limited_path: str, allowed: int) -> None:
        self.limited_path = limited_path
        self.allowed = allowed
        self.calls: Counter[str] = Counter()
        self.limit_reached = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls[path] += 1
        if path == self.limited_path:
            if self.calls[path] == self.allowed:
                self.limit_reached.set()
            await self.release.wait()
        return httpx.Response(204, request=request)


class _GatedBreakerHandler:
    def __init__(self, *, failures_before_probe: int) -> None:
        self.failures_before_probe = failures_before_probe
        self.calls = 0
        self.probe_started = asyncio.Event()
        self.release_probe = asyncio.Event()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.calls <= self.failures_before_probe:
            return httpx.Response(503, request=request)
        self.probe_started.set()
        await self.release_probe.wait()
        return httpx.Response(204, request=request)


@pytest.mark.asyncio
async def test_shared_fixed_window_limits_concurrent_workers_per_endpoint(
    stage5_settings: Settings,
    database: PostgresDatabase,
) -> None:
    settings = stage5_settings.model_copy(update={"delivery_max_attempts": 1})
    seeded = await _seed_scenario(settings, database, {"limited": 4, "independent": 1})
    limited = seeded.endpoints["limited"]
    independent = seeded.endpoints["independent"]
    messages = [*limited.messages, *independent.messages]
    handler = _GatedRateHandler(limited_path="/limited", allowed=2)

    tasks: list[asyncio.Task[DeliveryExecutionResult]] = []
    results: list[DeliveryExecutionResult]
    async with _executor_pool(settings, database, handler, size=len(messages)) as executors:
        tasks = [
            asyncio.create_task(executor.execute(message))
            for executor, message in zip(executors, messages, strict=True)
        ]
        try:
            await asyncio.wait_for(handler.limit_reached.wait(), timeout=3)
            # Two allowed limited requests remain gated; the two denials and the
            # independent endpoint must finish without waiting for HTTP release.
            await _wait_for_completed(tasks, expected=3)
            handler.release.set()
            results = list(await asyncio.wait_for(asyncio.gather(*tasks), timeout=5))
        finally:
            handler.release.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    limited_results = results[: len(limited.messages)]
    assert Counter(result.state for result in limited_results) == {
        "succeeded": 2,
        "retry_scheduled": 2,
    }
    assert results[-1].state == "succeeded"
    assert handler.calls == Counter({"/limited": 2, "/independent": 1})

    deferred_ids = {
        message.delivery_id
        for message, result in zip(limited.messages, limited_results, strict=True)
        if result.state == "retry_scheduled"
    }
    all_ids = [message.delivery_id for message in messages]
    async with database.session_factory() as session:
        deliveries = {
            delivery.id: delivery
            for delivery in await session.scalars(select(Delivery).where(Delivery.id.in_(all_ids)))
        }
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt).where(DeliveryAttempt.delivery_id.in_(all_ids))
            )
        )
        limited_control = await session.get(
            EndpointTrafficControl,
            (seeded.tenant_id, limited.endpoint_id),
        )
        independent_control = await session.get(
            EndpointTrafficControl,
            (seeded.tenant_id, independent.endpoint_id),
        )

    attempt_counts = Counter(attempt.delivery_id for attempt in attempts)
    assert len(deferred_ids) == 2
    assert all(attempt_counts[delivery_id] == 0 for delivery_id in deferred_ids)
    assert all(deliveries[delivery_id].status == "retry_scheduled" for delivery_id in deferred_ids)
    assert all(deliveries[delivery_id].next_attempt_at is not None for delivery_id in deferred_ids)
    assert limited_control is not None and limited_control.rate_window_count == 2
    assert independent_control is not None and independent_control.rate_window_count == 1


@pytest.mark.asyncio
async def test_concurrent_cooldown_claims_lease_one_probe_and_success_closes_breaker(
    stage5_settings: Settings,
    database: PostgresDatabase,
) -> None:
    settings = stage5_settings.model_copy(
        update={
            "delivery_rate_limit_requests": 100,
            "delivery_circuit_failure_threshold": 2,
        }
    )
    seeded = await _seed_scenario(settings, database, {"breaker": 6})
    endpoint = seeded.endpoints["breaker"]
    opening_messages = endpoint.messages[:2]
    competing_messages = endpoint.messages[2:]
    handler = _GatedBreakerHandler(failures_before_probe=2)

    tasks: list[asyncio.Task[DeliveryExecutionResult]] = []
    concurrent_results: list[DeliveryExecutionResult]
    async with _executor_pool(settings, database, handler, size=4) as executors:
        opening_results = [
            await executor.execute(message)
            for executor, message in zip(executors, opening_messages, strict=False)
        ]
        assert [result.state for result in opening_results] == [
            "retry_scheduled",
            "retry_scheduled",
        ]

        async with database.session_factory() as session:
            control = await session.get(
                EndpointTrafficControl,
                (seeded.tenant_id, endpoint.endpoint_id),
            )
            database_now = await session.scalar(select(func.clock_timestamp()))
            assert control is not None
            assert database_now is not None
            assert control.circuit_state == "open"
            assert control.circuit_consecutive_failures == 2
            control.circuit_opened_at = database_now - timedelta(
                seconds=settings.delivery_circuit_cooldown_seconds
            )
            await session.commit()

        tasks = [
            asyncio.create_task(executor.execute(message))
            for executor, message in zip(executors, competing_messages, strict=True)
        ]
        try:
            await asyncio.wait_for(handler.probe_started.wait(), timeout=3)
            await _wait_for_completed(tasks, expected=3)

            competitor_ids = [message.delivery_id for message in competing_messages]
            async with database.session_factory() as session:
                in_flight_control = await session.get(
                    EndpointTrafficControl,
                    (seeded.tenant_id, endpoint.endpoint_id),
                )
                in_flight_deliveries = list(
                    await session.scalars(select(Delivery).where(Delivery.id.in_(competitor_ids)))
                )
                in_flight_attempts = list(
                    await session.scalars(
                        select(DeliveryAttempt).where(
                            DeliveryAttempt.delivery_id.in_(competitor_ids)
                        )
                    )
                )

            assert in_flight_control is not None
            assert in_flight_control.circuit_state == "half_open"
            assert in_flight_control.probe_token is not None
            assert len(in_flight_attempts) == 1
            assert in_flight_attempts[0].is_circuit_probe
            assert in_flight_attempts[0].finished_at is None
            assert in_flight_control.probe_token == in_flight_attempts[0].claim_token
            assert Counter(delivery.status for delivery in in_flight_deliveries) == {
                "delivering": 1,
                "retry_scheduled": 3,
            }
            assert handler.calls == 3

            handler.release_probe.set()
            concurrent_results = list(await asyncio.wait_for(asyncio.gather(*tasks), timeout=5))
        finally:
            handler.release_probe.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    assert Counter(result.state for result in concurrent_results) == {
        "succeeded": 1,
        "retry_scheduled": 3,
    }
    all_ids = [message.delivery_id for message in endpoint.messages]
    competitor_ids = [message.delivery_id for message in competing_messages]
    async with database.session_factory() as session:
        final_control = await session.get(
            EndpointTrafficControl,
            (seeded.tenant_id, endpoint.endpoint_id),
        )
        final_attempts = list(
            await session.scalars(
                select(DeliveryAttempt).where(DeliveryAttempt.delivery_id.in_(all_ids))
            )
        )

    competitor_attempts = [
        attempt for attempt in final_attempts if attempt.delivery_id in competitor_ids
    ]
    assert final_control is not None
    assert final_control.circuit_state == "closed"
    assert final_control.circuit_consecutive_failures == 0
    assert final_control.circuit_opened_at is None
    assert final_control.probe_token is None
    assert final_control.probe_expires_at is None
    assert len(final_attempts) == 3
    assert len(competitor_attempts) == 1
    assert competitor_attempts[0].is_circuit_probe
    assert competitor_attempts[0].outcome == "succeeded"


@pytest.mark.asyncio
async def test_rate_admission_refreshes_database_clock_after_control_lock_wait(
    stage5_settings: Settings,
    database: PostgresDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    window_seconds = 0.5
    settings = stage5_settings.model_copy(
        update={
            "delivery_rate_limit_requests": 1,
            "delivery_rate_limit_window_seconds": window_seconds,
        }
    )
    seeded = await _seed_scenario(settings, database, {"clock-window": 1})
    endpoint = seeded.endpoints["clock-window"]
    message = endpoint.messages[0]
    handler = _GatedBreakerHandler(failures_before_probe=1)

    claim_task: asyncio.Task[DeliveryWork | DeliveryExecutionResult] | None = None
    async with _executor_pool(settings, database, handler, size=1) as executors:
        executor = executors[0]
        lock_started = _observe_next_control_lock(monkeypatch, executor)
        try:
            async with database.session_factory() as blocker:
                control = await blocker.scalar(
                    select(EndpointTrafficControl)
                    .where(
                        EndpointTrafficControl.tenant_id == seeded.tenant_id,
                        EndpointTrafficControl.endpoint_id == endpoint.endpoint_id,
                    )
                    .with_for_update()
                )
                window_started_at = await blocker.scalar(select(func.clock_timestamp()))
                assert control is not None
                assert window_started_at is not None
                control.rate_window_started_at = window_started_at
                control.rate_window_count = settings.delivery_rate_limit_requests
                await blocker.flush()
                window_ends_at = window_started_at + timedelta(seconds=window_seconds)

                claim_task = asyncio.create_task(executor._claim_attempt(message))
                await asyncio.wait_for(lock_started.wait(), timeout=2)
                assert not claim_task.done()
                await asyncio.sleep(window_seconds + 0.15)
                lock_release_time = await blocker.scalar(select(func.clock_timestamp()))
                assert lock_release_time is not None
                assert lock_release_time >= window_ends_at
                await blocker.commit()

            claimed = await asyncio.wait_for(claim_task, timeout=3)
        finally:
            if claim_task is not None and not claim_task.done():
                claim_task.cancel()
            if claim_task is not None:
                await asyncio.gather(claim_task, return_exceptions=True)

    assert isinstance(claimed, DeliveryWork)
    async with database.session_factory() as session:
        final_control = await session.get(
            EndpointTrafficControl,
            (seeded.tenant_id, endpoint.endpoint_id),
        )
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == message.delivery_id)
            )
        )
    assert final_control is not None
    assert final_control.rate_window_started_at is not None
    assert final_control.rate_window_started_at >= lock_release_time
    assert final_control.rate_window_count == 1
    assert len(attempts) == 1


@pytest.mark.asyncio
async def test_breaker_open_time_refreshes_database_clock_after_control_lock_wait(
    stage5_settings: Settings,
    database: PostgresDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = stage5_settings.model_copy(
        update={
            "delivery_rate_limit_requests": 100,
            "delivery_circuit_failure_threshold": 1,
        }
    )
    seeded = await _seed_scenario(settings, database, {"clock-breaker": 1})
    endpoint = seeded.endpoints["clock-breaker"]
    message = endpoint.messages[0]
    handler = _GatedBreakerHandler(failures_before_probe=1)

    finish_task: asyncio.Task[DeliveryExecutionResult] | None = None
    async with _executor_pool(settings, database, handler, size=1) as executors:
        executor = executors[0]
        claimed = await executor._claim_attempt(message)
        assert isinstance(claimed, DeliveryWork)
        lock_started = _observe_next_control_lock(monkeypatch, executor)
        try:
            async with database.session_factory() as blocker:
                control = await blocker.scalar(
                    select(EndpointTrafficControl)
                    .where(
                        EndpointTrafficControl.tenant_id == seeded.tenant_id,
                        EndpointTrafficControl.endpoint_id == endpoint.endpoint_id,
                    )
                    .with_for_update()
                )
                assert control is not None
                finish_task = asyncio.create_task(
                    executor._finish_attempt(
                        claimed,
                        outcome="transient_failure",
                        response_status_code=None,
                        error_code="injected_transport_failure",
                        duration_ms=0,
                    )
                )
                await asyncio.wait_for(lock_started.wait(), timeout=2)
                assert not finish_task.done()
                await asyncio.sleep(0.25)
                lock_release_time = await blocker.scalar(select(func.clock_timestamp()))
                assert lock_release_time is not None
                await blocker.commit()

            result = await asyncio.wait_for(finish_task, timeout=3)
        finally:
            if finish_task is not None and not finish_task.done():
                finish_task.cancel()
            if finish_task is not None:
                await asyncio.gather(finish_task, return_exceptions=True)

    assert result.state == "retry_scheduled"
    async with database.session_factory() as session:
        final_control = await session.get(
            EndpointTrafficControl,
            (seeded.tenant_id, endpoint.endpoint_id),
        )
    assert final_control is not None
    assert final_control.circuit_state == "open"
    assert final_control.circuit_consecutive_failures == 1
    assert final_control.circuit_opened_at is not None
    assert final_control.circuit_opened_at >= lock_release_time
