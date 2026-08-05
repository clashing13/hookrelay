"""Real PostgreSQL, JetStream, and TCP-receiver evidence for the Stage 3 pipeline."""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
import uvicorn
from asgi_lifespan import LifespanManager
from httpx2 import ASGITransport, AsyncClient
from nats.aio.msg import Msg
from nats.js.api import AckPolicy, RetentionPolicy, StorageType
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError
from sqlalchemy import func, or_, select, text, update

from hookrelay.broker import (
    BrokerTopologyError,
    DeliveryRequestedMessage,
    JetStreamBroker,
    PublishReceipt,
    decode_delivery_message,
)
from hookrelay.config import Settings
from hookrelay.database import PostgresDatabase
from hookrelay.delivery import DeliveryExecutionResult, DeliveryExecutor, build_http_client
from hookrelay.main import create_app
from hookrelay.models import Delivery, DeliveryAttempt, Event, OutboxMessage
from hookrelay.outbox import OutboxClaimLost, TransactionalOutboxPublisher
from hookrelay.schemas import EndpointCreatedResponse, TenantBootstrapResponse
from hookrelay.security import SecretCipher
from hookrelay.test_receiver import ReceiverSettings, ReceiverState, create_test_receiver
from hookrelay.worker import DeliveryBrokerMessage, DeliveryWorker

pytestmark = [pytest.mark.integration, pytest.mark.nats]

BOOTSTRAP_TOKEN = "hookrelay-stage3-integration-bootstrap-token"


@dataclass(frozen=True, slots=True)
class SeededDelivery:
    """Public response values plus authoritative identifiers used by pipeline assertions."""

    tenant_id: UUID
    event_id: UUID
    event_created_at: datetime
    endpoint_id: UUID
    delivery_id: UUID
    outbox_id: UUID
    signing_secret: str
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class RunningReceiver:
    """An actual loopback TCP receiver and its exact-byte capture ledger."""

    url: str
    state: ReceiverState


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
    """Apply reviewed revisions without making test ordering a hidden dependency."""

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
def stage3_settings(database_url: str, nats_url: str) -> Settings:
    suffix = uuid4().hex[:12]
    return Settings(
        environment="test",
        database_url=database_url,
        database_pool_size=10,
        database_max_overflow=10,
        bootstrap_enabled=True,
        bootstrap_token=BOOTSTRAP_TOKEN,
        nats_url=nats_url,
        nats_stream_name=f"HR_TEST_{suffix}",
        nats_subject=f"hookrelay.test.{suffix}.delivery",
        nats_consumer_name=f"HR_CONSUMER_{suffix}",
        nats_duplicate_window_seconds=120,
        nats_stream_max_bytes=1_048_576,
        nats_ack_wait_seconds=6,
        nats_max_ack_pending=4,
        outbox_batch_size=1,
        outbox_claim_ttl_seconds=5,
        delivery_worker_concurrency=2,
        delivery_http_timeout_seconds=0.5,
        delivery_allowed_hosts=frozenset({"127.0.0.1"}),
        _env_file=None,
    )


@pytest_asyncio.fixture
async def database(stage3_settings: Settings) -> AsyncIterator[PostgresDatabase]:
    database = PostgresDatabase(stage3_settings)
    try:
        yield database
    finally:
        await database.dispose()


@pytest_asyncio.fixture(autouse=True)
async def isolated_pending_outbox(
    database: PostgresDatabase,
) -> AsyncIterator[None]:
    """Temporarily lease pre-existing work so this test publishes only its own row."""

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
async def broker(stage3_settings: Settings) -> AsyncIterator[JetStreamBroker]:
    broker = JetStreamBroker(stage3_settings, client_name=f"test-{uuid4().hex[:10]}")
    await broker.connect()
    try:
        yield broker
    finally:
        try:
            await broker.jetstream.delete_stream(stage3_settings.nats_stream_name)
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
async def _receiver_socket(*, delay_seconds: float = 0) -> AsyncIterator[RunningReceiver]:
    settings = ReceiverSettings(
        host="127.0.0.1",
        port=9000,
        response_status_code=204,
        delay_seconds=delay_seconds,
        _env_file=None,
    )
    app = create_test_receiver(settings)
    state = cast(ReceiverState, app.state.receiver_state)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
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
        yield RunningReceiver(url=f"http://127.0.0.1:{port}/webhooks", state=state)
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
    payload: dict[str, object] = {
        "order_id": f"ord-{uuid4()}",
        "nested": {"a": True, "z": 2},
    }
    async with _api_client(settings) as client:
        tenant_response = await client.post(
            "/v1/bootstrap/tenants",
            headers={"Authorization": f"Bearer {BOOTSTRAP_TOKEN}"},
            json={
                "name": f"stage3-{uuid4()}",
                "initial_api_key_name": "pipeline-test",
            },
        )
        assert tenant_response.status_code == 201, tenant_response.text
        tenant = TenantBootstrapResponse.model_validate(tenant_response.json())

        endpoint_response = await client.post(
            "/v1/endpoints",
            headers={"Authorization": f"Bearer {tenant.api_key.key}"},
            json={"name": "loopback-receiver", "url": target_url},
        )
        assert endpoint_response.status_code == 201, endpoint_response.text
        endpoint = EndpointCreatedResponse.model_validate(endpoint_response.json())

        event_response = await client.post(
            "/v1/events",
            headers={
                "Authorization": f"Bearer {tenant.api_key.key}",
                "Idempotency-Key": f"stage3-{uuid4()}",
            },
            json={
                "type": "order.created",
                "payload": payload,
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
        event = await session.get(Event, event_id)
        delivery = await session.get(Delivery, delivery_id)
        outbox = await session.scalar(
            select(OutboxMessage).where(OutboxMessage.delivery_id == delivery_id)
        )
    assert event is not None
    assert delivery is not None
    assert outbox is not None
    return SeededDelivery(
        tenant_id=tenant.tenant.id,
        event_id=event.id,
        event_created_at=event.created_at,
        endpoint_id=endpoint.id,
        delivery_id=delivery.id,
        outbox_id=outbox.id,
        signing_secret=endpoint.signing_secret,
        payload=payload,
    )


async def _fetch_one(subscription: JetStreamContext.PullSubscription) -> Msg:
    messages = await asyncio.wait_for(subscription.fetch(batch=1, timeout=5), timeout=6)
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
            raise AssertionError(
                "JetStream did not observe the final ACK before the bounded deadline"
            )
        await asyncio.sleep(0.02)


class InjectedPublishFailure(RuntimeError):
    """A deterministic failure before or immediately after a real JetStream PubAck."""


class _FailingPublisher:
    def __init__(self, broker: JetStreamBroker, *, publish_before_failure: bool) -> None:
        self._broker = broker
        self._publish_before_failure = publish_before_failure

    async def publish(self, message: DeliveryRequestedMessage) -> PublishReceipt:
        if self._publish_before_failure:
            await self._broker.publish(message)
        raise InjectedPublishFailure


class _GatedPublisher:
    """Hold the first publisher after its claim transaction has committed."""

    def __init__(self) -> None:
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(self, _message: DeliveryRequestedMessage) -> PublishReceipt:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return PublishReceipt(stream="test", sequence=self.calls, duplicate=False)


class _ClaimStealingPublisher:
    """Simulate another publisher reclaiming ownership after a real PubAck."""

    def __init__(self, database: PostgresDatabase, broker: JetStreamBroker) -> None:
        self._database = database
        self._broker = broker
        self.replacement_token = uuid4()

    async def publish(self, message: DeliveryRequestedMessage) -> PublishReceipt:
        receipt = await self._broker.publish(message)
        async with self._database.session_factory() as session:
            await session.execute(
                update(OutboxMessage)
                .where(OutboxMessage.id == message.message_id)
                .values(
                    claim_token=self.replacement_token,
                    claim_expires_at=func.now() + timedelta(minutes=1),
                )
            )
            await session.commit()
        return receipt


class _StopAfterFirstPublisher:
    def __init__(self, broker: JetStreamBroker, stop_event: asyncio.Event) -> None:
        self._broker = broker
        self._stop_event = stop_event

    async def publish(self, message: DeliveryRequestedMessage) -> PublishReceipt:
        receipt = await self._broker.publish(message)
        self._stop_event.set()
        return receipt


class _PubAckMarkerProbe:
    """Observe the durable marker after real PubAck but before the caller can set it."""

    def __init__(self, broker: JetStreamBroker, database: PostgresDatabase) -> None:
        self._broker = broker
        self._database = database
        self.puback_received = False
        self.published_at_after_puback: datetime | None = None

    async def publish(self, message: DeliveryRequestedMessage) -> PublishReceipt:
        receipt = await self._broker.publish(message)
        self.puback_received = True
        async with self._database.session_factory() as session:
            outbox = await session.get(OutboxMessage, message.message_id)
        assert outbox is not None
        self.published_at_after_puback = outbox.published_at
        return receipt


class _AckAfterDatabaseProbe:
    """Delegate the real ACK after recording the committed database state it observes."""

    def __init__(
        self,
        message: Msg,
        database: PostgresDatabase,
        seeded: SeededDelivery,
    ) -> None:
        self.data = message.data
        self._message = message
        self._database = database
        self._seeded = seeded
        self.called = False
        self.delivery_status_at_ack: str | None = None
        self.completed_attempts_at_ack: int | None = None

    async def ack_sync(self, seconds: float = 1.0, /) -> object:
        async with self._database.session_factory() as session:
            delivery = await session.get(Delivery, self._seeded.delivery_id)
            completed_attempts = await session.scalar(
                select(func.count())
                .select_from(DeliveryAttempt)
                .where(
                    DeliveryAttempt.delivery_id == self._seeded.delivery_id,
                    DeliveryAttempt.finished_at.is_not(None),
                    DeliveryAttempt.outcome == "succeeded",
                )
            )
        self.called = True
        self.delivery_status_at_ack = delivery.status if delivery is not None else None
        self.completed_attempts_at_ack = int(completed_attempts or 0)
        return await self._message.ack_sync(seconds)

    async def in_progress(self) -> None:
        await self._message.in_progress()

    async def nak(self, delay: float | None = None) -> None:
        await self._message.nak(delay)

    async def term(self) -> None:
        await self._message.term()


class _RecordingExecutor:
    def __init__(self, executor: DeliveryExecutor) -> None:
        self._executor = executor
        self.results: list[DeliveryExecutionResult] = []

    async def execute(self, message: DeliveryRequestedMessage) -> DeliveryExecutionResult:
        result = await self._executor.execute(message)
        self.results.append(result)
        return result


def _expected_body(seeded: SeededDelivery) -> bytes:
    created_at = seeded.event_created_at.astimezone(UTC).isoformat(timespec="microseconds")
    if created_at.endswith("+00:00"):
        created_at = f"{created_at[:-6]}Z"
    return json.dumps(
        {
            "created_at": created_at,
            "delivery_id": str(seeded.delivery_id),
            "id": str(seeded.event_id),
            "payload": seeded.payload,
            "schema_version": 1,
            "type": "order.created",
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


@pytest.mark.asyncio
async def test_jetstream_topology_is_idempotent_and_rejects_drift(
    stage3_settings: Settings,
    broker: JetStreamBroker,
) -> None:
    await broker.ensure_topology()
    await broker.ensure_topology()

    stream = await broker.jetstream.stream_info(stage3_settings.nats_stream_name)
    consumer = await broker.jetstream.consumer_info(
        stage3_settings.nats_stream_name,
        stage3_settings.nats_consumer_name,
    )
    assert stream.config.subjects == [stage3_settings.nats_subject]
    assert stream.config.retention == RetentionPolicy.WORK_QUEUE
    assert stream.config.storage == StorageType.FILE
    assert stream.config.max_bytes == stage3_settings.nats_stream_max_bytes
    assert consumer.config.durable_name == stage3_settings.nats_consumer_name
    assert consumer.config.ack_policy == AckPolicy.EXPLICIT
    assert consumer.config.max_ack_pending == stage3_settings.nats_max_ack_pending

    drift_settings = stage3_settings.model_copy(
        update={"nats_stream_max_bytes": stage3_settings.nats_stream_max_bytes * 2}
    )
    drift_broker = JetStreamBroker(drift_settings, client_name="stage3-drift-test")
    try:
        with pytest.raises(BrokerTopologyError, match="max_bytes"):
            await drift_broker.connect()
    finally:
        await drift_broker.close()


@pytest.mark.asyncio
async def test_publish_acknowledgement_precedes_published_marker(
    stage3_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    seeded = await _seed_delivery(
        stage3_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    puback_probe = _PubAckMarkerProbe(broker, database)
    publisher = TransactionalOutboxPublisher(
        stage3_settings,
        database.session_factory,
        puback_probe,
    )

    result = await publisher.publish_available_once()

    assert result.claimed == 1
    assert result.published == 1
    assert result.duplicates == 0
    assert puback_probe.puback_received
    assert puback_probe.published_at_after_puback is None
    async with database.session_factory() as session:
        outbox = await session.get(OutboxMessage, seeded.outbox_id)
    assert outbox is not None
    assert outbox.published_at is not None
    assert outbox.claim_token is None
    assert outbox.claim_expires_at is None
    stream = await broker.jetstream.stream_info(stage3_settings.nats_stream_name)
    assert stream.state.messages == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "publish_before_failure",
    [False, True],
    ids=["failed-before-puback", "ambiguous-after-puback"],
)
async def test_failed_or_ambiguous_publish_remains_recoverable(
    publish_before_failure: bool,
    stage3_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    seeded = await _seed_delivery(
        stage3_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    failing = TransactionalOutboxPublisher(
        stage3_settings,
        database.session_factory,
        _FailingPublisher(broker, publish_before_failure=publish_before_failure),
    )

    with pytest.raises(InjectedPublishFailure):
        await failing.publish_available_once()

    async with database.session_factory() as session:
        after_failure = await session.get(OutboxMessage, seeded.outbox_id)
    assert after_failure is not None
    assert after_failure.published_at is None
    assert after_failure.claim_token is None
    assert after_failure.claim_expires_at is None
    before_retry = await broker.jetstream.stream_info(stage3_settings.nats_stream_name)
    assert before_retry.state.messages == int(publish_before_failure)

    recovered = TransactionalOutboxPublisher(
        stage3_settings,
        database.session_factory,
        broker,
    )
    result = await recovered.publish_available_once()

    assert result.claimed == 1
    assert result.published == 1
    assert result.duplicates == int(publish_before_failure)
    async with database.session_factory() as session:
        after_retry = await session.get(OutboxMessage, seeded.outbox_id)
    assert after_retry is not None
    assert after_retry.published_at is not None
    after_retry_stream = await broker.jetstream.stream_info(stage3_settings.nats_stream_name)
    assert after_retry_stream.state.messages == 1


@pytest.mark.asyncio
async def test_two_publishers_cannot_claim_the_same_unexpired_row(
    stage3_settings: Settings,
    database: PostgresDatabase,
) -> None:
    seeded = await _seed_delivery(
        stage3_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    gated = _GatedPublisher()
    first_publisher = TransactionalOutboxPublisher(
        stage3_settings,
        database.session_factory,
        gated,
    )
    second_publisher = TransactionalOutboxPublisher(
        stage3_settings,
        database.session_factory,
        gated,
    )

    first_task = asyncio.create_task(first_publisher.publish_available_once())
    await asyncio.wait_for(gated.entered.wait(), timeout=2)
    second_result = await second_publisher.publish_available_once()
    gated.release.set()
    first_result = await asyncio.wait_for(first_task, timeout=2)

    assert first_result.published == 1
    assert second_result.claimed == 0
    assert gated.calls == 1
    async with database.session_factory() as session:
        outbox = await session.get(OutboxMessage, seeded.outbox_id)
    assert outbox is not None and outbox.published_at is not None


@pytest.mark.asyncio
async def test_expired_claim_is_reclaimed_and_published(
    stage3_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    seeded = await _seed_delivery(
        stage3_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    async with database.session_factory() as session:
        await session.execute(
            update(OutboxMessage)
            .where(OutboxMessage.id == seeded.outbox_id)
            .values(
                claim_token=uuid4(),
                claim_expires_at=func.now() + text("interval '100 milliseconds'"),
            )
        )
        await session.commit()
    await asyncio.sleep(0.2)

    publisher = TransactionalOutboxPublisher(
        stage3_settings,
        database.session_factory,
        broker,
    )
    result = await publisher.publish_available_once()

    assert result.published == 1
    async with database.session_factory() as session:
        outbox = await session.get(OutboxMessage, seeded.outbox_id)
    assert outbox is not None
    assert outbox.published_at is not None
    assert outbox.claim_token is None
    assert outbox.claim_expires_at is None


@pytest.mark.asyncio
async def test_lost_claim_token_cannot_finalize_published_marker(
    stage3_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    seeded = await _seed_delivery(
        stage3_settings,
        database,
        "http://127.0.0.1:65530/webhooks",
    )
    stealing_publisher = _ClaimStealingPublisher(database, broker)
    publisher = TransactionalOutboxPublisher(
        stage3_settings,
        database.session_factory,
        stealing_publisher,
    )

    try:
        with pytest.raises(OutboxClaimLost):
            await publisher.publish_available_once()
        async with database.session_factory() as session:
            outbox = await session.get(OutboxMessage, seeded.outbox_id)
        assert outbox is not None
        assert outbox.published_at is None
        assert outbox.claim_token == stealing_publisher.replacement_token
    finally:
        async with database.session_factory() as session:
            await session.execute(
                update(OutboxMessage)
                .where(OutboxMessage.id == seeded.outbox_id)
                .values(claim_token=None, claim_expires_at=None)
            )
            await session.commit()


@pytest.mark.asyncio
async def test_shutdown_between_batch_items_releases_unpublished_claims(
    stage3_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    seeded = [
        await _seed_delivery(
            stage3_settings,
            database,
            "http://127.0.0.1:65530/webhooks",
        )
        for _ in range(2)
    ]
    batch_settings = stage3_settings.model_copy(update={"outbox_batch_size": 2})
    stop_event = asyncio.Event()
    publisher = TransactionalOutboxPublisher(
        batch_settings,
        database.session_factory,
        _StopAfterFirstPublisher(broker, stop_event),
    )

    result = await publisher.publish_available_once(stop_event)

    assert stop_event.is_set()
    assert result.claimed == 2
    assert result.published == 1
    async with database.session_factory() as session:
        rows = list(
            await session.scalars(
                select(OutboxMessage).where(
                    OutboxMessage.id.in_([item.outbox_id for item in seeded])
                )
            )
        )
    assert sum(row.published_at is not None for row in rows) == 1
    assert sum(row.published_at is None for row in rows) == 1
    assert all(row.claim_token is None for row in rows)
    assert all(row.claim_expires_at is None for row in rows)


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_api_to_jetstream_worker_delivers_exact_signed_body_then_acks(
    stage3_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    fixed_clock = datetime(2026, 8, 3, 12, 0, 0, tzinfo=UTC)
    async with _receiver_socket() as receiver:
        seeded = await _seed_delivery(stage3_settings, database, receiver.url)
        publisher = TransactionalOutboxPublisher(
            stage3_settings,
            database.session_factory,
            broker,
        )
        published = await publisher.publish_available_once()
        assert published.published == 1

        worker_broker = JetStreamBroker(stage3_settings, client_name="stage3-e2e-worker")
        http_client = build_http_client(stage3_settings)
        await worker_broker.connect()
        try:
            subscription = await worker_broker.pull_subscription()
            broker_message = await _fetch_one(subscription)
            executor = DeliveryExecutor(
                stage3_settings,
                database.session_factory,
                SecretCipher(
                    stage3_settings.secret_encryption_key_bytes(),
                    stage3_settings.secret_encryption_key_version,
                ),
                http_client,
                clock=lambda: fixed_clock,
            )
            worker = DeliveryWorker(stage3_settings, executor)
            observed_ack = _AckAfterDatabaseProbe(broker_message, database, seeded)

            await worker.process_message(cast(DeliveryBrokerMessage, observed_ack))

            assert observed_ack.called
            assert observed_ack.delivery_status_at_ack == "succeeded"
            assert observed_ack.completed_attempts_at_ack == 1
            await _wait_for_empty_consumer(worker_broker, stage3_settings)
        finally:
            await http_client.aclose()
            await worker_broker.close()

    assert len(receiver.state.requests) == 1
    captured = receiver.state.requests[0]
    body = base64.b64decode(captured.body_base64)
    expected_body = _expected_body(seeded)
    assert body == expected_body
    assert captured.body_sha256 == hashlib.sha256(expected_body).hexdigest()
    assert captured.headers["content-type"] == "application/json"
    assert captured.headers["hookrelay-delivery-id"] == str(seeded.delivery_id)
    assert captured.headers["hookrelay-event-id"] == str(seeded.event_id)
    assert captured.headers["hookrelay-timestamp"] == str(int(fixed_clock.timestamp()))
    assert captured.headers["hookrelay-webhook-version"] == "1"
    expected_signature = (
        "v1="
        + hmac.new(
            seeded.signing_secret.encode("utf-8"),
            str(int(fixed_clock.timestamp())).encode("ascii") + b"." + expected_body,
            hashlib.sha256,
        ).hexdigest()
    )
    assert captured.headers["hookrelay-signature"] == expected_signature

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        outbox = await session.get(OutboxMessage, seeded.outbox_id)
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        )
    assert delivery is not None and delivery.status == "succeeded"
    assert outbox is not None and outbox.published_at is not None
    assert len(attempts) == 1
    assert attempts[0].attempt_number == 1
    assert attempts[0].finished_at is not None
    assert attempts[0].outcome == "succeeded"
    assert attempts[0].response_status_code == 204
    assert attempts[0].error_code is None
    assert attempts[0].duration_ms is not None and attempts[0].duration_ms >= 0


@pytest.mark.asyncio
async def test_http_deadline_records_a_bounded_unacknowledged_failure(
    stage3_settings: Settings,
    database: PostgresDatabase,
) -> None:
    async with _receiver_socket(delay_seconds=2) as receiver:
        seeded = await _seed_delivery(stage3_settings, database, receiver.url)
        async with database.session_factory() as session:
            outbox = await session.get(OutboxMessage, seeded.outbox_id)
        assert outbox is not None
        message = DeliveryRequestedMessage.model_validate(outbox.payload)
        # Keep the transport timeout above the executor's outer deadline so this
        # test deterministically exercises HookRelay's own total-request budget.
        http_client = AsyncClient(timeout=5, follow_redirects=False, trust_env=False)
        try:
            executor = DeliveryExecutor(
                stage3_settings,
                database.session_factory,
                SecretCipher(
                    stage3_settings.secret_encryption_key_bytes(),
                    stage3_settings.secret_encryption_key_version,
                ),
                http_client,
            )
            started = asyncio.get_running_loop().time()
            result = await executor.execute(message)
            elapsed = asyncio.get_running_loop().time() - started
        finally:
            await http_client.aclose()

    assert result.state == "retry_scheduled"
    assert result.retry_after_seconds is not None
    assert elapsed < 1.5
    assert len(receiver.state.requests) == 1
    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempts = list(
            await session.scalars(
                select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == seeded.delivery_id)
            )
        )
    assert delivery is not None and delivery.status == "retry_scheduled"
    assert delivery.next_attempt_at is not None
    assert len(attempts) == 1
    assert attempts[0].outcome == "transient_failure"
    assert attempts[0].error_code == "request_timeout"
    assert attempts[0].response_status_code is None
    assert attempts[0].finished_at is not None


@pytest.mark.asyncio
async def test_real_redelivery_of_succeeded_work_skips_second_http_attempt(
    stage3_settings: Settings,
    database: PostgresDatabase,
    broker: JetStreamBroker,
) -> None:
    fixed_clock = datetime(2026, 8, 3, 12, 0, 0, tzinfo=UTC)
    async with _receiver_socket() as receiver:
        seeded = await _seed_delivery(stage3_settings, database, receiver.url)
        publisher = TransactionalOutboxPublisher(
            stage3_settings,
            database.session_factory,
            broker,
        )
        assert (await publisher.publish_available_once()).published == 1

        worker_broker = JetStreamBroker(stage3_settings, client_name="stage3-redelivery-worker")
        http_client = build_http_client(stage3_settings)
        await worker_broker.connect()
        try:
            subscription = await worker_broker.pull_subscription()
            first_message = await _fetch_one(subscription)
            executor = DeliveryExecutor(
                stage3_settings,
                database.session_factory,
                SecretCipher(
                    stage3_settings.secret_encryption_key_bytes(),
                    stage3_settings.secret_encryption_key_version,
                ),
                http_client,
                clock=lambda: fixed_clock,
            )

            first_result = await executor.execute(decode_delivery_message(first_message.data))
            assert first_result.state == "succeeded"
            assert len(receiver.state.requests) == 1

            # This explicit NAK simulates a success ACK lost after the durable commit.
            await first_message.nak()
            redelivered = await _fetch_one(subscription)
            assert redelivered.metadata.num_delivered >= 2

            recording_executor = _RecordingExecutor(executor)
            worker = DeliveryWorker(stage3_settings, recording_executor)
            await worker.process_message(redelivered)

            assert [result.state for result in recording_executor.results] == ["already_succeeded"]
            assert len(receiver.state.requests) == 1
            await _wait_for_empty_consumer(worker_broker, stage3_settings)
        finally:
            await http_client.aclose()
            await worker_broker.close()

    async with database.session_factory() as session:
        delivery = await session.get(Delivery, seeded.delivery_id)
        attempt_count = await session.scalar(
            select(func.count())
            .select_from(DeliveryAttempt)
            .where(DeliveryAttempt.delivery_id == seeded.delivery_id)
        )
    assert delivery is not None and delivery.status == "succeeded"
    assert attempt_count == 1
