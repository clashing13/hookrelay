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
