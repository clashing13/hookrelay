"""Real PostgreSQL, JetStream, and HTTP evidence for Stage 4 recovery policy."""

import asyncio
import os
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
import uvicorn
from asgi_lifespan import LifespanManager
from fastapi import FastAPI, Request, Response
from httpx2 import ASGITransport, AsyncClient
from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hookrelay.broker import DeliveryRequestedMessage, JetStreamBroker, decode_delivery_message
from hookrelay.config import Settings
from hookrelay.database import PostgresDatabase
from hookrelay.delivery import (
    DeliveryClaimLost,
    DeliveryExecutionResult,
    DeliveryExecutor,
    DeliveryWork,
    build_http_client,
)
from hookrelay.main import create_app
from hookrelay.models import Delivery, DeliveryAttempt, OutboxMessage
from hookrelay.outbox import TransactionalOutboxPublisher
from hookrelay.schemas import (
    DeliveryReplayResponse,
    EndpointCreatedResponse,
    TenantBootstrapResponse,
)
from hookrelay.security import SecretCipher
from hookrelay.worker import DeliveryBrokerMessage, DeliveryWorker

pytestmark = pytest.mark.integration

BOOTSTRAP_TOKEN = "hookrelay-stage4-integration-bootstrap-token"


@dataclass(frozen=True, slots=True)
class SeededDelivery:
    """Tenant credentials and identifiers produced through the real API."""

    tenant_id: UUID
    api_key: str = field(repr=False)
    event_id: UUID
    endpoint_id: UUID
    delivery_id: UUID
    outbox_id: UUID


@dataclass(slots=True)
class ReceiverLedger:
    """Deterministic response sequence and exact bodies observed over TCP."""

    statuses: tuple[int, ...]
    delay_seconds: float = 0
    bodies: list[bytes] = field(default_factory=list)

    def record(self, body: bytes) -> int:
        index = len(self.bodies)
        self.bodies.append(body)
        return self.statuses[min(index, len(self.statuses) - 1)]


@dataclass(frozen=True, slots=True)
class RunningReceiver:
    url: str
    ledger: ReceiverLedger


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


@pytest.fixture(scope="module")
def nats_url() -> str:
    return _required_url("HOOKRELAY_TEST_NATS_URL")


@pytest.fixture(scope="module", autouse=True)
def migrated_database(database_url: str) -> None:
    """Apply the Stage 4 revision without depending on test-module ordering."""

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
def stage4_settings(database_url: str, nats_url: str) -> Settings:
    suffix = uuid4().hex[:12]
    return Settings(
        environment="test",
        database_url=database_url,
        database_pool_size=10,
        database_max_overflow=10,
        bootstrap_enabled=True,
        bootstrap_token=BOOTSTRAP_TOKEN,
        nats_url=nats_url,
        nats_stream_name=f"HR4_TEST_{suffix}",
        nats_subject=f"hookrelay.stage4.{suffix}.delivery",
        nats_consumer_name=f"HR4_CONSUMER_{suffix}",
        nats_duplicate_window_seconds=120,
        nats_stream_max_bytes=1_048_576,
        nats_ack_wait_seconds=6,
        nats_max_ack_pending=4,
        outbox_batch_size=1,
        outbox_claim_ttl_seconds=5,
        delivery_worker_concurrency=2,
        delivery_http_timeout_seconds=0.5,
        delivery_claim_ttl_seconds=2,
        delivery_finalization_margin_seconds=0.5,
        delivery_max_attempts=3,
        delivery_retry_base_seconds=0.1,
        delivery_retry_max_seconds=0.1,
        delivery_retry_jitter_ratio=0,
        delivery_allowed_hosts=frozenset({"127.0.0.1"}),
        _env_file=None,
    )


@pytest_asyncio.fixture
async def database(stage4_settings: Settings) -> AsyncIterator[PostgresDatabase]:
    database = PostgresDatabase(stage4_settings)
    try:
        yield database
    finally:
        await database.dispose()


@pytest_asyncio.fixture(autouse=True)
async def isolated_pending_outbox(database: PostgresDatabase) -> AsyncIterator[None]:
    """Lease older unpublished rows so each publisher sees only this test's work."""

    quarantine_token = uuid4()
    async with database.session_factory() as session:
        await session.execute(
            update(OutboxMessage)
            .where(
                OutboxMessage.published_at.is_(None),
                or_(
                    OutboxMessage.claim_expires_at.is_(None),
                    OutboxMessage.claim_expires_at <= func.now(),
                ),
            )
            .values(
                claim_token=quarantine_token,
                claim_expires_at=func.now() + timedelta(hours=1),
            )
        )
        await session.commit()
    try:
        yield
    finally:
        async with database.session_factory() as session:
            await session.execute(
                update(OutboxMessage)
                .where(OutboxMessage.claim_token == quarantine_token)
                .values(claim_token=None, claim_expires_at=None)
            )
            await session.commit()


@pytest_asyncio.fixture
async def broker(stage4_settings: Settings) -> AsyncIterator[JetStreamBroker]:
    broker = JetStreamBroker(stage4_settings, client_name=f"stage4-test-{uuid4().hex[:10]}")
    await broker.connect()
    try:
        yield broker
    finally:
        try:
            await broker.jetstream.delete_stream(stage4_settings.nats_stream_name)
        except NotFoundError:
            pass
        await broker.close()


@asynccontextmanager
async def _api_client(settings: Settings) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


async def _wait_until_server_started(server: uvicorn.Server, task: asyncio.Task[None]) -> None:
    deadline = asyncio.get_running_loop().time() + 5
    while not server.started:
        if task.done():
            await task
            raise AssertionError("receiver stopped before reporting startup")
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("receiver did not start within five seconds")
        await asyncio.sleep(0.01)


@asynccontextmanager
async def _receiver(
    *statuses: int,
    delay_seconds: float = 0,
    port: int | None = None,
) -> AsyncIterator[RunningReceiver]:
    assert statuses
    ledger = ReceiverLedger(tuple(statuses), delay_seconds=delay_seconds)
    app = FastAPI()

    @app.post("/webhooks")
    async def receive(request: Request) -> Response:
        status_code = ledger.record(await request.body())
        if ledger.delay_seconds:
            await asyncio.sleep(ledger.delay_seconds)
        return Response(status_code=status_code)

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port or 0))
    listener.listen(128)
    port = int(listener.getsockname()[1])
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
        )
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        await _wait_until_server_started(server, task)
        yield RunningReceiver(url=f"http://127.0.0.1:{port}/webhooks", ledger=ledger)
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, timeout=5)
        finally:
            listener.close()


async def _seed_delivery(
    settings: Settings,
    database: PostgresDatabase,
    target_url: str,
) -> SeededDelivery:
    async with _api_client(settings) as client:
        tenant_response = await client.post(
            "/v1/bootstrap/tenants",
            headers={"Authorization": f"Bearer {BOOTSTRAP_TOKEN}"},
            json={
                "name": f"stage4-{uuid4()}",
                "initial_api_key_name": "recovery-test",
            },
        )
        assert tenant_response.status_code == 201, tenant_response.text
        tenant = TenantBootstrapResponse.model_validate(tenant_response.json())

        endpoint_response = await client.post(
            "/v1/endpoints",
            headers={"Authorization": f"Bearer {tenant.api_key.key}"},
            json={"name": "stage4-receiver", "url": target_url},
        )
        assert endpoint_response.status_code == 201, endpoint_response.text
        endpoint = EndpointCreatedResponse.model_validate(endpoint_response.json())

        event_response = await client.post(
            "/v1/events",
            headers={
                "Authorization": f"Bearer {tenant.api_key.key}",
                "Idempotency-Key": f"stage4-{uuid4()}",
            },
            json={
                "type": "order.created",
                "payload": {"order_id": f"ord-{uuid4()}"},
                "endpoint_ids": [str(endpoint.id)],
            },
        )
        assert event_response.status_code == 201, event_response.text
        event_body = cast(dict[str, object], event_response.json())
        event_id = UUID(cast(str, event_body["id"]))
        response_deliveries = cast(list[dict[str, object]], event_body["deliveries"])
        assert len(response_deliveries) == 1
        delivery_id = UUID(cast(str, response_deliveries[0]["id"]))

    async with database.session_factory() as session:
        outbox = await session.scalar(
            select(OutboxMessage).where(
                OutboxMessage.delivery_id == delivery_id,
                OutboxMessage.dispatch_generation == 1,
            )
        )
    assert outbox is not None
    return SeededDelivery(
        tenant_id=tenant.tenant.id,
        api_key=tenant.api_key.key,
        event_id=event_id,
        endpoint_id=endpoint.id,
        delivery_id=delivery_id,
        outbox_id=outbox.id,
    )


async def _publish_one(
    settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    result = await TransactionalOutboxPublisher(
        settings,
        database.session_factory,
        broker,
    ).publish_available_once()
    assert result.claimed == 1
    assert result.published == 1


async def _fetch_one(
    subscription: JetStreamContext.PullSubscription,
    *,
    wait_seconds: float = 3,
) -> Msg:
    messages = await asyncio.wait_for(
        subscription.fetch(batch=1, timeout=wait_seconds),
        timeout=wait_seconds + 1,
    )
    assert len(messages) == 1
    return messages[0]


async def _wait_for_empty_consumer(broker: JetStreamBroker, settings: Settings) -> None:
    deadline = asyncio.get_running_loop().time() + 5
    while True:
        consumer = await broker.jetstream.consumer_info(
            settings.nats_stream_name,
            settings.nats_consumer_name,
        )
        stream = await broker.jetstream.stream_info(settings.nats_stream_name)
        if (
            (consumer.num_ack_pending or 0) == 0
            and (consumer.num_pending or 0) == 0
            and stream.state.messages == 0
        ):
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("JetStream did not observe the terminal ACK")
        await asyncio.sleep(0.02)


@asynccontextmanager
async def _delivery_runtime(
    settings: Settings,
    database: PostgresDatabase,
) -> AsyncIterator[tuple[DeliveryWorker, DeliveryExecutor]]:
    http_client = build_http_client(settings)
    executor = DeliveryExecutor(
        settings,
        database.session_factory,
        SecretCipher(
            settings.secret_encryption_key_bytes(),
            settings.secret_encryption_key_version,
        ),
        http_client,
        random_source=lambda: 1,
    )
    try:
        yield DeliveryWorker(settings, executor), executor
    finally:
        await http_client.aclose()


async def _message_for_outbox(
    database: PostgresDatabase,
    outbox_id: UUID,
) -> DeliveryRequestedMessage:
    async with database.session_factory() as session:
        outbox = await session.get(OutboxMessage, outbox_id)
    assert outbox is not None
    return DeliveryRequestedMessage.model_validate(outbox.payload)


async def _force_dead_letter(database: PostgresDatabase, delivery_id: UUID) -> None:
    async with database.session_factory() as session:
        delivery = await session.get(Delivery, delivery_id)
        database_now = await session.scalar(select(func.now()))
        assert delivery is not None
        assert database_now is not None
        delivery.status = "dead_lettered"
        delivery.next_attempt_at = None
        delivery.dead_lettered_at = database_now
        delivery.dead_letter_reason = "permanent_failure"
        delivery.claim_token = None
        delivery.claim_expires_at = None
        await session.commit()


@pytest.mark.asyncio
@pytest.mark.nats
async def test_transient_delayed_nak_waits_then_succeeds_over_real_http(
    stage4_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    settings = stage4_settings.model_copy(
        update={
            "delivery_retry_base_seconds": 0.5,
            "delivery_retry_max_seconds": 0.5,
            "delivery_retry_jitter_ratio": 0,
        }
    )
    async with _receiver(503, 204) as receiver:
        seeded = await _seed_delivery(settings, database, receiver.url)
        await _publish_one(settings, database, broker)
        subscription = await broker.pull_subscription()

        async with _delivery_runtime(settings, database) as (worker, _executor):
            first = await _fetch_one(subscription)
            await worker.process_message(cast(DeliveryBrokerMessage, first))

            async with database.session_factory() as session:
                delivery = await session.get(Delivery, seeded.delivery_id)
                attempts = list(
                    await session.scalars(
                        select(DeliveryAttempt).where(
                            DeliveryAttempt.delivery_id == seeded.delivery_id
                        )
                    )
                )
            assert delivery is not None
            assert delivery.status == "retry_scheduled"
            assert delivery.next_attempt_at is not None
            assert len(attempts) == 1
            assert attempts[0].outcome == "transient_failure"
            assert attempts[0].response_status_code == 503

            with pytest.raises(TimeoutError):
                await subscription.fetch(batch=1, timeout=0.1)

            redelivered = await _fetch_one(subscription, wait_seconds=2)
            assert redelivered.metadata.num_delivered >= 2
            await worker.process_message(cast(DeliveryBrokerMessage, redelivered))
            await _wait_for_empty_consumer(broker, settings)

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        )
    assert delivery is not None
    assert delivery.status == "succeeded"
    assert delivery.next_attempt_at is None
    assert [attempt.outcome for attempt in attempts] == ["transient_failure", "succeeded"]
    assert [attempt.response_status_code for attempt in attempts] == [503, 204]
    assert len(receiver.ledger.bodies) == 2
    assert receiver.ledger.bodies[0] == receiver.ledger.bodies[1]


@pytest.mark.asyncio
@pytest.mark.nats
async def test_permanent_http_failure_dead_letters_and_acknowledges(
    stage4_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    async with _receiver(400) as receiver:
        seeded = await _seed_delivery(stage4_settings, database, receiver.url)
        await _publish_one(stage4_settings, database, broker)
        subscription = await broker.pull_subscription()
        message = await _fetch_one(subscription)

        async with _delivery_runtime(stage4_settings, database) as (worker, _executor):
            await worker.process_message(cast(DeliveryBrokerMessage, message))
        await _wait_for_empty_consumer(broker, stage4_settings)

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempt = await session.scalar(
            select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == seeded.delivery_id)
        )
    assert delivery is not None
    assert delivery.status == "dead_lettered"
    assert delivery.dead_lettered_at is not None
    assert delivery.dead_letter_reason == "permanent_failure"
    assert delivery.next_attempt_at is None
    assert attempt is not None
    assert attempt.outcome == "permanent_failure"
    assert attempt.response_status_code == 400
    assert len(receiver.ledger.bodies) == 1


@pytest.mark.asyncio
async def test_policy_blocked_target_is_persistently_dead_lettered_without_http_attempt(
    stage4_settings: Settings,
    database: PostgresDatabase,
) -> None:
    seeded = await _seed_delivery(
        stage4_settings,
        database,
        "http://blocked.example/webhooks",
    )
    message = await _message_for_outbox(database, seeded.outbox_id)

    async with _delivery_runtime(stage4_settings, database) as (_worker, executor):
        result = await executor.execute(message)

    assert result.state == "dead_lettered"
    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempt_count = await session.scalar(
            select(func.count())
            .select_from(DeliveryAttempt)
            .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
        )
    assert delivery is not None
    assert delivery.status == "dead_lettered"
    assert delivery.dead_letter_reason == "target_blocked"
    assert delivery.dead_lettered_at is not None
    assert attempt_count == 0


@pytest.mark.asyncio
@pytest.mark.nats
async def test_stopped_destination_recovers_without_losing_the_persisted_retry(
    stage4_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    settings = stage4_settings.model_copy(
        update={
            "delivery_retry_base_seconds": 0.5,
            "delivery_retry_max_seconds": 0.5,
            "delivery_retry_jitter_ratio": 0,
        }
    )
    port_probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port_probe.bind(("127.0.0.1", 0))
    receiver_port = int(port_probe.getsockname()[1])
    port_probe.close()
    seeded = await _seed_delivery(
        settings,
        database,
        f"http://127.0.0.1:{receiver_port}/webhooks",
    )
    await _publish_one(settings, database, broker)
    subscription = await broker.pull_subscription()

    async with _delivery_runtime(settings, database) as (worker, _executor):
        unavailable_message = await _fetch_one(subscription)
        await worker.process_message(cast(DeliveryBrokerMessage, unavailable_message))
        async with database.session_factory() as session:
            delivery = await session.get(Delivery, seeded.delivery_id)
            first_attempt = await session.scalar(
                select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == seeded.delivery_id)
            )
        assert delivery is not None and delivery.status == "retry_scheduled"
        assert first_attempt is not None
        assert first_attempt.outcome == "transient_failure"
        assert first_attempt.error_code in {"transport_error", "request_timeout"}

        async with _receiver(204, port=receiver_port) as receiver:
            recovered_message = await _fetch_one(subscription, wait_seconds=2)
            await worker.process_message(cast(DeliveryBrokerMessage, recovered_message))
            assert len(receiver.ledger.bodies) == 1
    await _wait_for_empty_consumer(broker, settings)

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        )
    assert delivery is not None and delivery.status == "succeeded"
    assert [attempt.outcome for attempt in attempts] == ["transient_failure", "succeeded"]


@pytest.mark.asyncio
@pytest.mark.nats
async def test_exact_max_attempts_dead_letters_without_a_fourth_request(
    stage4_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    settings = stage4_settings.model_copy(
        update={
            "delivery_max_attempts": 3,
            "delivery_retry_base_seconds": 0.1,
            "delivery_retry_max_seconds": 0.1,
            "delivery_retry_jitter_ratio": 0,
        }
    )
    deliveries: list[int] = []
    async with _receiver(503) as receiver:
        seeded = await _seed_delivery(settings, database, receiver.url)
        await _publish_one(settings, database, broker)
        subscription = await broker.pull_subscription()

        async with _delivery_runtime(settings, database) as (worker, _executor):
            for _ in range(settings.delivery_max_attempts):
                message = await _fetch_one(subscription, wait_seconds=2)
                deliveries.append(message.metadata.num_delivered)
                await worker.process_message(cast(DeliveryBrokerMessage, message))
        await _wait_for_empty_consumer(broker, settings)

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        )
    assert delivery is not None
    assert delivery.status == "dead_lettered"
    assert delivery.dead_letter_reason == "attempts_exhausted"
    assert deliveries == [1, 2, 3]
    assert [attempt.attempt_number for attempt in attempts] == [1, 2, 3]
    assert all(attempt.dispatch_generation == 1 for attempt in attempts)
    assert all(attempt.outcome == "transient_failure" for attempt in attempts)
    assert len(receiver.ledger.bodies) == settings.delivery_max_attempts


@pytest.mark.asyncio
@pytest.mark.nats
async def test_manual_replay_resets_budget_but_keeps_global_attempt_history(
    stage4_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    settings = stage4_settings.model_copy(
        update={
            "delivery_max_attempts": 2,
            "delivery_retry_base_seconds": 0.1,
            "delivery_retry_max_seconds": 0.1,
            "delivery_retry_jitter_ratio": 0,
        }
    )
    async with _receiver(503, 503, 204) as receiver:
        seeded = await _seed_delivery(settings, database, receiver.url)
        await _publish_one(settings, database, broker)
        subscription = await broker.pull_subscription()
        async with _delivery_runtime(settings, database) as (worker, _executor):
            for _ in range(settings.delivery_max_attempts):
                message = await _fetch_one(subscription, wait_seconds=2)
                await worker.process_message(cast(DeliveryBrokerMessage, message))
            await _wait_for_empty_consumer(broker, settings)

            async with _api_client(settings) as client:
                replay = await client.post(
                    f"/v1/deliveries/{seeded.delivery_id}/replay",
                    headers={"Authorization": f"Bearer {seeded.api_key}"},
                    json={"expected_dispatch_generation": 1},
                )
            assert replay.status_code == 202, replay.text
            assert (
                await TransactionalOutboxPublisher(
                    settings,
                    database.session_factory,
                    broker,
                ).publish_available_once()
            ).published == 1

            replay_message = await _fetch_one(subscription)
            await worker.process_message(cast(DeliveryBrokerMessage, replay_message))
            await _wait_for_empty_consumer(broker, settings)

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        )
    assert delivery is not None
    assert delivery.status == "succeeded"
    assert delivery.dispatch_generation == 2
    assert [attempt.attempt_number for attempt in attempts] == [1, 2, 3]
    assert [attempt.dispatch_generation for attempt in attempts] == [1, 1, 2]
    assert [attempt.outcome for attempt in attempts] == [
        "transient_failure",
        "transient_failure",
        "succeeded",
    ]
    assert len(receiver.ledger.bodies) == 3


@pytest.mark.asyncio
async def test_expired_claim_is_abandoned_and_fences_the_stale_worker(
    stage4_settings: Settings,
    database: PostgresDatabase,
) -> None:
    seeded = await _seed_delivery(
        stage4_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    message = await _message_for_outbox(database, seeded.outbox_id)

    async with _delivery_runtime(stage4_settings, database) as (_worker, executor):
        claimed = await executor._claim_attempt(message)
        assert isinstance(claimed, DeliveryWork)

        async with database.session_factory() as session:
            delivery = await session.get(Delivery, seeded.delivery_id)
            assert delivery is not None
            delivery.claim_expires_at = delivery.created_at + timedelta(microseconds=1)
            await session.commit()

        recovered = await executor._claim_attempt(message)
        assert isinstance(recovered, DeliveryExecutionResult)
        assert recovered.state == "retry_scheduled"
        assert recovered.retry_after_seconds == pytest.approx(0.1)

        with pytest.raises(DeliveryClaimLost):
            await executor._finish_attempt(
                claimed,
                outcome="succeeded",
                response_status_code=204,
                error_code=None,
                duration_ms=0,
            )

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempt = await session.scalar(
            select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == seeded.delivery_id)
        )
    assert delivery is not None
    assert delivery.status == "retry_scheduled"
    assert delivery.claim_token is None
    assert delivery.claim_expires_at is None
    assert delivery.next_attempt_at is not None
    assert attempt is not None
    assert attempt.finished_at is not None
    assert attempt.outcome == "abandoned"
    assert attempt.error_code == "worker_lease_expired"


@pytest.mark.asyncio
async def test_claim_uses_fresh_database_clock_after_waiting_for_row_lock(
    stage4_settings: Settings,
    database: PostgresDatabase,
) -> None:
    """A transaction-start timestamp must not defer work that became due while blocked."""

    seeded = await _seed_delivery(
        stage4_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    message = await _message_for_outbox(database, seeded.outbox_id)
    waiter_transaction_started = asyncio.Event()
    transaction_starts: list[datetime] = []

    @asynccontextmanager
    async def lock_waiting_session() -> AsyncIterator[AsyncSession]:
        async with database.session_factory() as session:
            transaction_start = await session.scalar(select(func.now()))
            assert transaction_start is not None
            transaction_starts.append(transaction_start)
            waiter_transaction_started.set()
            yield session

    http_client = build_http_client(stage4_settings)
    executor = DeliveryExecutor(
        stage4_settings,
        cast("async_sessionmaker[AsyncSession]", lock_waiting_session),
        SecretCipher(
            stage4_settings.secret_encryption_key_bytes(),
            stage4_settings.secret_encryption_key_version,
        ),
        http_client,
        random_source=lambda: 1,
    )
    claim_task: asyncio.Task[DeliveryWork | DeliveryExecutionResult] | None = None
    try:
        async with database.session_factory() as blocker:
            delivery = await blocker.scalar(
                select(Delivery).where(Delivery.id == seeded.delivery_id).with_for_update()
            )
            assert delivery is not None

            claim_task = asyncio.create_task(executor._claim_attempt(message))
            try:
                await asyncio.wait_for(waiter_transaction_started.wait(), timeout=2)
                database_now = await blocker.scalar(select(func.clock_timestamp()))
                assert database_now is not None
                delivery.status = "retry_scheduled"
                delivery.next_attempt_at = database_now + timedelta(seconds=0.25)
                await blocker.flush()
                await asyncio.sleep(0.4)
            finally:
                await blocker.commit()

        claimed = await asyncio.wait_for(claim_task, timeout=2)
    except BaseException:
        if claim_task is not None and not claim_task.done():
            claim_task.cancel()
            await asyncio.gather(claim_task, return_exceptions=True)
        raise
    finally:
        await http_client.aclose()

    assert isinstance(claimed, DeliveryWork)
    assert len(transaction_starts) == 1
    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempt = await session.scalar(
            select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == seeded.delivery_id)
        )
    assert delivery is not None
    assert delivery.status == "delivering"
    assert attempt is not None
    assert attempt.started_at >= transaction_starts[0] + timedelta(seconds=0.25)


@pytest.mark.asyncio
@pytest.mark.nats
async def test_replay_publishes_fresh_generation_and_acks_stale_broker_work(
    stage4_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    async with _receiver(204) as receiver:
        seeded = await _seed_delivery(stage4_settings, database, receiver.url)
        await _publish_one(stage4_settings, database, broker)
        await _force_dead_letter(database, seeded.delivery_id)

        async with _api_client(stage4_settings) as client:
            detail_response = await client.get(
                f"/v1/events/{seeded.event_id}",
                headers={"Authorization": f"Bearer {seeded.api_key}"},
            )
            assert detail_response.status_code == 200
            detail_delivery = cast(list[dict[str, object]], detail_response.json()["deliveries"])[0]
            assert detail_delivery["status"] == "dead_lettered"
            assert detail_delivery["dispatch_generation"] == 1
            assert detail_delivery["dead_letter_reason"] == "permanent_failure"
            response = await client.post(
                f"/v1/deliveries/{seeded.delivery_id}/replay",
                headers={"Authorization": f"Bearer {seeded.api_key}"},
                json={"expected_dispatch_generation": 1},
            )
        assert response.status_code == 202, response.text
        replayed = DeliveryReplayResponse.model_validate(response.json())
        assert replayed.id == seeded.delivery_id
        assert replayed.dispatch_generation == 2
        assert response.headers["Location"] == f"/v1/events/{seeded.event_id}"

        async with database.session_factory() as session:
            outboxes = list(
                await session.scalars(
                    select(OutboxMessage)
                    .where(OutboxMessage.delivery_id == seeded.delivery_id)
                    .order_by(OutboxMessage.dispatch_generation)
                )
            )
        assert [outbox.dispatch_generation for outbox in outboxes] == [1, 2]
        assert outboxes[0].id == seeded.outbox_id
        assert outboxes[1].id != seeded.outbox_id
        assert outboxes[1].published_at is None

        await _publish_one(stage4_settings, database, broker)
        subscription = await broker.pull_subscription()
        async with _delivery_runtime(stage4_settings, database) as (worker, _executor):
            old_message = await _fetch_one(subscription)
            assert decode_delivery_message(old_message.data).message_id == outboxes[0].id
            await worker.process_message(cast(DeliveryBrokerMessage, old_message))
            assert receiver.ledger.bodies == []

            replay_message = await _fetch_one(subscription)
            assert decode_delivery_message(replay_message.data).message_id == outboxes[1].id
            await worker.process_message(cast(DeliveryBrokerMessage, replay_message))
        await _wait_for_empty_consumer(broker, stage4_settings)

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == seeded.delivery_id)
            )
        )
    assert delivery is not None
    assert delivery.status == "succeeded"
    assert delivery.dispatch_generation == 2
    assert len(attempts) == 1
    assert attempts[0].dispatch_generation == 2
    assert attempts[0].outcome == "succeeded"
    assert len(receiver.ledger.bodies) == 1


@pytest.mark.asyncio
@pytest.mark.concurrency
@pytest.mark.security
async def test_concurrent_replay_has_one_winner_and_remains_tenant_scoped(
    stage4_settings: Settings,
    database: PostgresDatabase,
) -> None:
    owner = await _seed_delivery(
        stage4_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    intruder = await _seed_delivery(
        stage4_settings,
        database,
        "http://127.0.0.1:65531/webhooks",
    )
    await _force_dead_letter(database, owner.delivery_id)

    async with _api_client(stage4_settings) as client:
        owner_headers = {"Authorization": f"Bearer {owner.api_key}"}
        first, second = await asyncio.gather(
            client.post(
                f"/v1/deliveries/{owner.delivery_id}/replay",
                headers=owner_headers,
                json={"expected_dispatch_generation": 1},
            ),
            client.post(
                f"/v1/deliveries/{owner.delivery_id}/replay",
                headers=owner_headers,
                json={"expected_dispatch_generation": 1},
            ),
        )
        cross_tenant = await client.post(
            f"/v1/deliveries/{owner.delivery_id}/replay",
            headers={"Authorization": f"Bearer {intruder.api_key}"},
            json={"expected_dispatch_generation": 1},
        )

    assert sorted((first.status_code, second.status_code)) == [202, 409]
    conflict = first if first.status_code == 409 else second
    assert conflict.json()["code"] == "delivery_generation_conflict"
    assert cross_tenant.status_code == 404
    assert cross_tenant.json()["code"] == "resource_not_found"

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, owner.delivery_id)
        outboxes = list(
            await session.scalars(
                select(OutboxMessage)
                .where(OutboxMessage.delivery_id == owner.delivery_id)
                .order_by(OutboxMessage.dispatch_generation)
            )
        )
    assert delivery is not None
    assert delivery.status == "pending"
    assert delivery.dispatch_generation == 2
    assert [outbox.dispatch_generation for outbox in outboxes] == [1, 2]


@pytest.mark.asyncio
async def test_stale_replay_precondition_cannot_advance_a_newly_dead_lettered_generation(
    stage4_settings: Settings,
    database: PostgresDatabase,
) -> None:
    seeded = await _seed_delivery(
        stage4_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    await _force_dead_letter(database, seeded.delivery_id)
    headers = {"Authorization": f"Bearer {seeded.api_key}"}
    async with _api_client(stage4_settings) as client:
        accepted = await client.post(
            f"/v1/deliveries/{seeded.delivery_id}/replay",
            headers=headers,
            json={"expected_dispatch_generation": 1},
        )
        assert accepted.status_code == 202
        await _force_dead_letter(database, seeded.delivery_id)
        ambiguous_retry = await client.post(
            f"/v1/deliveries/{seeded.delivery_id}/replay",
            headers=headers,
            json={"expected_dispatch_generation": 1},
        )

    assert ambiguous_retry.status_code == 409
    assert ambiguous_retry.json()["code"] == "delivery_generation_conflict"
    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(OutboxMessage)
            .where(OutboxMessage.delivery_id == seeded.delivery_id)
        )
    assert delivery is not None
    assert delivery.status == "dead_lettered"
    assert delivery.dispatch_generation == 2
    assert outbox_count == 2


@pytest.mark.asyncio
@pytest.mark.nats
async def test_hard_killed_worker_is_abandoned_then_recovered_by_a_new_process(
    stage4_settings: Settings,
    database: PostgresDatabase,
) -> None:
    """Kill an OS process after receiver capture and prove fenced at-least-once recovery."""

    settings = stage4_settings.model_copy(
        update={
            "nats_ack_wait_seconds": 8,
            "delivery_worker_concurrency": 1,
            "delivery_http_timeout_seconds": 3,
            "delivery_claim_ttl_seconds": 5,
            "delivery_retry_base_seconds": 0.1,
            "delivery_retry_max_seconds": 0.1,
            "delivery_retry_jitter_ratio": 0,
        }
    )
    broker = JetStreamBroker(settings, client_name=f"stage4-kill-test-{uuid4().hex[:10]}")
    await broker.connect()
    worker_environment = os.environ.copy()
    worker_environment.update(
        {
            "HOOKRELAY_ENVIRONMENT": "test",
            "HOOKRELAY_DATABASE_URL": settings.database_url.get_secret_value(),
            "HOOKRELAY_DATABASE_POOL_SIZE": "2",
            "HOOKRELAY_DATABASE_MAX_OVERFLOW": "0",
            "HOOKRELAY_NATS_URL": settings.nats_server_url(),
            "HOOKRELAY_NATS_STREAM_NAME": settings.nats_stream_name,
            "HOOKRELAY_NATS_SUBJECT": settings.nats_subject,
            "HOOKRELAY_NATS_CONSUMER_NAME": settings.nats_consumer_name,
            "HOOKRELAY_NATS_DUPLICATE_WINDOW_SECONDS": str(settings.nats_duplicate_window_seconds),
            "HOOKRELAY_NATS_STREAM_MAX_BYTES": str(settings.nats_stream_max_bytes),
            "HOOKRELAY_NATS_ACK_WAIT_SECONDS": str(settings.nats_ack_wait_seconds),
            "HOOKRELAY_NATS_MAX_ACK_PENDING": str(settings.nats_max_ack_pending),
            "HOOKRELAY_DELIVERY_WORKER_CONCURRENCY": "1",
            "HOOKRELAY_DELIVERY_HTTP_TIMEOUT_SECONDS": str(settings.delivery_http_timeout_seconds),
            "HOOKRELAY_DELIVERY_CLAIM_TTL_SECONDS": str(settings.delivery_claim_ttl_seconds),
            "HOOKRELAY_DELIVERY_FINALIZATION_MARGIN_SECONDS": str(
                settings.delivery_finalization_margin_seconds
            ),
            "HOOKRELAY_DELIVERY_MAX_ATTEMPTS": str(settings.delivery_max_attempts),
            "HOOKRELAY_DELIVERY_RETRY_BASE_SECONDS": str(settings.delivery_retry_base_seconds),
            "HOOKRELAY_DELIVERY_RETRY_MAX_SECONDS": str(settings.delivery_retry_max_seconds),
            "HOOKRELAY_DELIVERY_RETRY_JITTER_RATIO": "0",
            "HOOKRELAY_DELIVERY_ALLOWED_HOSTS": '["127.0.0.1"]',
        }
    )

    first_worker: asyncio.subprocess.Process | None = None
    replacement_worker: asyncio.subprocess.Process | None = None
    try:
        async with _receiver(204, delay_seconds=10) as receiver:
            seeded = await _seed_delivery(settings, database, receiver.url)
            await _publish_one(settings, database, broker)
            command = [
                sys.executable,
                "-c",
                "from hookrelay.worker import run; run()",
            ]
            first_worker = await asyncio.create_subprocess_exec(
                *command,
                env=worker_environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            capture_deadline = asyncio.get_running_loop().time() + 5
            while True:
                async with database.session_factory() as session:
                    delivery = await session.get(Delivery, seeded.delivery_id)
                    unfinished = await session.scalar(
                        select(func.count())
                        .select_from(DeliveryAttempt)
                        .where(
                            DeliveryAttempt.delivery_id == seeded.delivery_id,
                            DeliveryAttempt.finished_at.is_(None),
                        )
                    )
                if (
                    delivery is not None
                    and delivery.status == "delivering"
                    and int(unfinished or 0) == 1
                    and len(receiver.ledger.bodies) == 1
                ):
                    break
                if first_worker.returncode is not None:
                    raise AssertionError("worker exited before the delivery was captured")
                if asyncio.get_running_loop().time() >= capture_deadline:
                    raise AssertionError("worker did not reach the receiver before kill deadline")
                await asyncio.sleep(0.02)

            first_worker.kill()
            await asyncio.wait_for(first_worker.wait(), timeout=5)
            assert first_worker.returncode not in {None, 0}

            # The original receiver handler may still be sleeping, but new requests can
            # now complete immediately. This models the destination recovering while a
            # replacement HookRelay worker starts.
            receiver.ledger.delay_seconds = 0
            replacement_worker = await asyncio.create_subprocess_exec(
                *command,
                env=worker_environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            recovery_deadline = asyncio.get_running_loop().time() + 15
            while True:
                async with database.session_factory() as session:
                    delivery = await session.get(Delivery, seeded.delivery_id)
                    attempts = list(
                        await session.scalars(
                            select(DeliveryAttempt)
                            .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
                            .order_by(DeliveryAttempt.attempt_number)
                        )
                    )
                if delivery is not None and delivery.status == "succeeded":
                    break
                if replacement_worker.returncode is not None:
                    raise AssertionError("replacement worker exited before recovery")
                if asyncio.get_running_loop().time() >= recovery_deadline:
                    raise AssertionError("replacement worker did not recover the delivery")
                await asyncio.sleep(0.05)

            await _wait_for_empty_consumer(broker, settings)
            assert [attempt.outcome for attempt in attempts] == ["abandoned", "succeeded"]
            assert attempts[0].error_code == "worker_lease_expired"
            assert len(receiver.ledger.bodies) == 2
            assert receiver.ledger.bodies[0] == receiver.ledger.bodies[1]
    finally:
        for process in (first_worker, replacement_worker):
            if process is not None and process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=5)
        try:
            await broker.jetstream.delete_stream(settings.nats_stream_name)
        except NotFoundError:
            pass
        await broker.close()
