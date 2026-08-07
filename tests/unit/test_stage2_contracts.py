"""Pure Stage 2 ingestion and request-schema contracts."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from hookrelay.ingestion import (
    OUTBOX_SCHEMA_VERSION,
    OUTBOX_TOPIC,
    build_outbox_messages,
    request_fingerprint,
)
from hookrelay.models import Delivery
from hookrelay.schemas import (
    DeliveryDetailResponse,
    DeliveryResponse,
    EndpointCreate,
    EventCreate,
    EventDetailResponse,
    EventResponse,
    TenantCreate,
)


def _event_request(
    endpoint_ids: list[UUID],
    *,
    event_type: str = "order.created",
    payload: object | None = None,
) -> EventCreate:
    return EventCreate.model_validate(
        {
            "type": event_type,
            "payload": {"order_id": "ord_123"} if payload is None else payload,
            "endpoint_ids": endpoint_ids,
        }
    )


def test_request_fingerprint_is_stable_for_object_and_endpoint_order() -> None:
    first_endpoint = UUID("00000000-0000-0000-0000-000000000001")
    second_endpoint = UUID("00000000-0000-0000-0000-000000000002")
    first = _event_request(
        [second_endpoint, first_endpoint],
        payload={"nested": {"b": 2, "a": 1}, "order_id": "ord_123"},
    )
    equivalent = _event_request(
        [first_endpoint, second_endpoint],
        payload={"order_id": "ord_123", "nested": {"a": 1, "b": 2}},
    )

    first_fingerprint = request_fingerprint(first)

    assert len(first_fingerprint) == 32
    assert first_fingerprint == request_fingerprint(equivalent)
    assert first.canonical_endpoint_ids() == [first_endpoint, second_endpoint]


def test_request_fingerprint_binds_type_payload_and_destination_set() -> None:
    first_endpoint = uuid4()
    second_endpoint = uuid4()
    baseline = _event_request([first_endpoint])
    variants = [
        _event_request([first_endpoint], event_type="order.updated"),
        _event_request([first_endpoint], payload={"order_id": "ord_999"}),
        _event_request([first_endpoint, second_endpoint]),
    ]

    baseline_fingerprint = request_fingerprint(baseline)

    assert all(request_fingerprint(variant) != baseline_fingerprint for variant in variants)


@pytest.mark.parametrize(
    "body",
    [
        {"type": "order created", "payload": {}, "endpoint_ids": [uuid4()]},
        {"type": "order.created", "payload": [], "endpoint_ids": [uuid4()]},
        {"type": "order.created", "payload": {}, "endpoint_ids": []},
        {
            "type": "order.created",
            "payload": {},
            "endpoint_ids": [UUID("00000000-0000-0000-0000-000000000001")] * 2,
        },
        {
            "type": "order.created",
            "payload": {"amount": float("nan")},
            "endpoint_ids": [uuid4()],
        },
        {
            "type": "order.created",
            "payload": {"nested": [1, {"amount": float("inf")}]},
            "endpoint_ids": [uuid4()],
        },
        {
            "type": "order.created",
            "payload": {},
            "endpoint_ids": [uuid4()],
            "tenant_id": str(uuid4()),
        },
    ],
)
def test_event_schema_rejects_ambiguous_or_unsafe_inputs(body: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        EventCreate.model_validate(body)


def test_event_schema_bounds_fanout() -> None:
    with pytest.raises(ValidationError):
        _event_request([uuid4() for _ in range(101)])


@pytest.mark.parametrize(
    "url",
    [
        "ftp://receiver.example/webhooks",
        "https://user:password@receiver.example/webhooks",
        "https://receiver.example/webhooks#fragment",
    ],
)
def test_endpoint_schema_rejects_ambiguous_destination_urls(url: str) -> None:
    with pytest.raises(ValidationError):
        EndpointCreate.model_validate({"name": "orders", "url": url})


def test_names_are_trimmed_and_blank_names_are_rejected() -> None:
    tenant = TenantCreate.model_validate(
        {"name": "  Acme  ", "initial_api_key_name": "  production  "}
    )
    endpoint = EndpointCreate.model_validate(
        {"name": "  orders  ", "url": "https://receiver.example/webhooks"}
    )

    assert tenant.name == "Acme"
    assert tenant.initial_api_key_name == "production"
    assert endpoint.name == "orders"
    with pytest.raises(ValidationError):
        TenantCreate.model_validate({"name": "   "})
    with pytest.raises(ValidationError):
        EndpointCreate.model_validate({"name": "\t", "url": "https://receiver.example"})


def test_event_response_uses_public_type_alias_and_separates_live_delivery_status() -> None:
    event_id = uuid4()
    endpoint_id = uuid4()
    delivery_id = uuid4()
    created_at = datetime.now(UTC)
    accepted = EventResponse.from_parts(
        event_id=event_id,
        event_type="order.created",
        created_at=created_at,
        deliveries=[DeliveryResponse(id=delivery_id, endpoint_id=endpoint_id, status="pending")],
    )
    detail = EventDetailResponse(
        id=event_id,
        event_type="order.created",
        created_at=created_at,
        payload={"order_id": "ord_123"},
        deliveries=[
            DeliveryDetailResponse(id=delivery_id, endpoint_id=endpoint_id, status="succeeded")
        ],
    )

    accepted_json = accepted.model_dump(mode="json", by_alias=True)
    detail_json = detail.model_dump(mode="json", by_alias=True)

    assert accepted_json["type"] == "order.created"
    assert "event_type" not in accepted_json
    assert accepted_json["deliveries"][0]["status"] == "pending"
    assert detail_json["deliveries"][0]["status"] == "succeeded"


def test_outbox_builder_emits_one_id_only_message_per_delivery() -> None:
    tenant_id = uuid4()
    event_id = uuid4()
    first = Delivery(
        id=uuid4(),
        tenant_id=tenant_id,
        event_id=event_id,
        endpoint_id=uuid4(),
        signing_secret_id=uuid4(),
        target_url="https://receiver.example/one",
        status="pending",
    )
    second = Delivery(
        id=uuid4(),
        tenant_id=tenant_id,
        event_id=event_id,
        endpoint_id=uuid4(),
        signing_secret_id=uuid4(),
        target_url="https://receiver.example/two",
        status="pending",
    )

    messages = build_outbox_messages(
        tenant_id=tenant_id,
        event_id=event_id,
        deliveries=[first, second],
    )

    assert len(messages) == 2
    assert {message.delivery_id for message in messages} == {first.id, second.id}
    for message in messages:
        assert message.topic == OUTBOX_TOPIC
        assert message.schema_version == OUTBOX_SCHEMA_VERSION
        assert set(message.payload) == {
            "delivery_id",
            "endpoint_id",
            "event_id",
            "message_id",
            "schema_version",
            "tenant_id",
            "type",
        }
        assert "payload" not in message.payload
        assert "secret" not in message.payload
