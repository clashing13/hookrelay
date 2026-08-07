"""Typed public request and response contracts for durable ingestion and delivery state."""

import math
from datetime import datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator


class StrictModel(BaseModel):
    """Reject mass-assignment fields instead of silently discarding them."""

    model_config = ConfigDict(extra="forbid")


TrimmedName = Annotated[str, Field(min_length=1, max_length=100)]


class TenantCreate(StrictModel):
    name: TrimmedName
    initial_api_key_name: TrimmedName = "default"

    @field_validator("name", "initial_api_key_name")
    @classmethod
    def strip_names(cls, value: str) -> str:
        value = value.strip()
        if not value:
            msg = "name cannot be blank"
            raise ValueError(msg)
        return value


class TenantResponse(StrictModel):
    id: UUID
    name: str
    created_at: datetime


class IssuedApiKeyResponse(StrictModel):
    id: UUID
    name: str
    key: str = Field(repr=False)
    created_at: datetime


class TenantBootstrapResponse(StrictModel):
    tenant: TenantResponse
    api_key: IssuedApiKeyResponse


class EndpointCreate(StrictModel):
    name: TrimmedName
    url: HttpUrl

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            msg = "name cannot be blank"
            raise ValueError(msg)
        return value

    @field_validator("url")
    @classmethod
    def reject_ambiguous_url_components(cls, value: HttpUrl) -> HttpUrl:
        if value.username is not None or value.password is not None or value.fragment is not None:
            msg = "URL userinfo and fragments are not supported"
            raise ValueError(msg)
        if len(str(value)) > 2048:
            msg = "URL must be at most 2048 characters"
            raise ValueError(msg)
        return value


class EndpointResponse(StrictModel):
    id: UUID
    name: str
    url: str
    enabled: bool
    created_at: datetime


class EndpointCreatedResponse(EndpointResponse):
    signing_secret: str = Field(repr=False)


class EndpointSigningSecretRotateRequest(StrictModel):
    """Optimistic precondition for one tenant-scoped signing-secret rotation."""

    expected_active_version: int = Field(ge=1, le=2_147_483_647)


class EndpointSigningSecretRotatedResponse(StrictModel):
    """The replacement signing secret, returned exactly once."""

    endpoint_id: UUID
    version: int = Field(ge=2)
    signing_secret: str = Field(repr=False)
    created_at: datetime


DeliveryStatus = Literal[
    "pending",
    "delivering",
    "retry_scheduled",
    "succeeded",
    "dead_lettered",
]


class DeliveryResponse(StrictModel):
    id: UUID
    endpoint_id: UUID
    status: Literal["pending"]


class DeliveryDetailResponse(StrictModel):
    id: UUID
    endpoint_id: UUID
    status: DeliveryStatus
    dispatch_generation: int = Field(default=1, ge=1)
    next_attempt_at: datetime | None = None
    dead_letter_reason: (
        Literal[
            "permanent_failure",
            "attempts_exhausted",
            "target_blocked",
        ]
        | None
    ) = None


class DeliveryReplayRequest(StrictModel):
    """Optimistic precondition that prevents one operator action replaying twice."""

    expected_dispatch_generation: int = Field(ge=1)


class DeliveryReplayResponse(StrictModel):
    """Accepted asynchronous retry cycle for one dead-lettered delivery."""

    id: UUID
    event_id: UUID
    status: Literal["pending"]
    dispatch_generation: int = Field(ge=2)


class DeliveryInspectionResponse(StrictModel):
    """Tenant-safe current delivery state for operations inspection."""

    id: UUID
    event_id: UUID
    event_type: str
    endpoint_id: UUID
    endpoint_name: str
    status: DeliveryStatus
    dispatch_generation: int = Field(ge=1)
    created_at: datetime
    next_attempt_at: datetime | None = None
    dead_lettered_at: datetime | None = None
    dead_letter_reason: (
        Literal[
            "permanent_failure",
            "attempts_exhausted",
            "target_blocked",
        ]
        | None
    ) = None
    attempt_count: int = Field(ge=0)
    last_attempt_at: datetime | None = None
    replayable: bool


class DeliveryHistoryResponse(StrictModel):
    """One bounded page of tenant-owned deliveries in reverse creation order."""

    items: list[DeliveryInspectionResponse]
    next_cursor: str | None = None


DeliveryAttemptOutcome = Literal[
    "succeeded",
    "transient_failure",
    "permanent_failure",
    "abandoned",
]


class DeliveryAttemptDetailResponse(StrictModel):
    """Immutable attempt evidence without claims, secrets, URLs, or event bodies."""

    id: UUID
    delivery_id: UUID
    attempt_number: int = Field(ge=1)
    dispatch_generation: int = Field(ge=1)
    is_circuit_probe: bool
    started_at: datetime
    finished_at: datetime | None = None
    outcome: DeliveryAttemptOutcome | None = None
    response_status_code: int | None = Field(default=None, ge=100, le=599)
    error_code: str | None = Field(default=None, max_length=100)
    duration_ms: int | None = Field(default=None, ge=0)


class DeliveryAttemptHistoryResponse(StrictModel):
    """One bounded page of lifetime-monotonic attempts for a delivery."""

    items: list[DeliveryAttemptDetailResponse]
    next_cursor: str | None = None


class EventCreate(StrictModel):
    event_type: Annotated[
        str,
        Field(alias="type", min_length=1, max_length=100, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$"),
    ]
    payload: dict[str, JsonValue]
    endpoint_ids: Annotated[list[UUID], Field(min_length=1, max_length=100)]

    @field_validator("endpoint_ids")
    @classmethod
    def require_unique_endpoint_ids(cls, value: list[UUID]) -> list[UUID]:
        if len(value) != len(set(value)):
            msg = "endpoint_ids must be unique"
            raise ValueError(msg)
        return value

    @field_validator("payload")
    @classmethod
    def reject_non_finite_numbers(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Keep request fingerprinting and PostgreSQL JSONB representation deterministic."""

        def contains_non_finite(item: JsonValue) -> bool:
            if isinstance(item, float):
                return not math.isfinite(item)
            if isinstance(item, list):
                return any(contains_non_finite(member) for member in item)
            if isinstance(item, dict):
                return any(contains_non_finite(member) for member in item.values())
            return False

        if contains_non_finite(value):
            msg = "payload numbers must be finite"
            raise ValueError(msg)
        return value

    def canonical_endpoint_ids(self) -> list[UUID]:
        return sorted(self.endpoint_ids, key=str)


class EventResponse(StrictModel):
    id: UUID
    event_type: str = Field(serialization_alias="type")
    created_at: datetime
    deliveries: list[DeliveryResponse]

    @classmethod
    def from_parts(
        cls,
        *,
        event_id: UUID,
        event_type: str,
        created_at: datetime,
        deliveries: list[DeliveryResponse],
    ) -> Self:
        return cls(
            id=event_id,
            event_type=event_type,
            created_at=created_at,
            deliveries=deliveries,
        )


class EventDetailResponse(StrictModel):
    id: UUID
    event_type: str = Field(serialization_alias="type")
    created_at: datetime
    payload: dict[str, JsonValue]
    deliveries: list[DeliveryDetailResponse]
