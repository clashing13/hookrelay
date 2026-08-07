"""SQLAlchemy mappings for HookRelay's durable ingestion and delivery domain."""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Shared declarative base exposed to Alembic."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class CreatedAtMixin:
    """Give immutable records a PostgreSQL-generated creation timestamp."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Tenant(CreatedAtMixin, Base):
    """An isolated HookRelay customer boundary."""

    __tablename__ = "tenants"
    __table_args__ = (CheckConstraint("btrim(name) <> ''", name="name_not_blank"),)

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )


class ApiKey(CreatedAtMixin, Base):
    """A revocable tenant credential; the raw secret is never persisted."""

    __tablename__ = "api_keys"
    __table_args__ = (
        UniqueConstraint("tenant_id", "id"),
        UniqueConstraint("public_id"),
        CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        CheckConstraint("octet_length(secret_hash) = 32", name="secret_hash_length"),
        CheckConstraint("char_length(secret_last_four) = 4", name="secret_hint_length"),
        CheckConstraint(
            "expires_at IS NULL OR expires_at >= created_at",
            name="expiry_not_before_creation",
        ),
        CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="revocation_not_before_creation",
        ),
        Index("ix_api_keys_tenant_id_revoked_at", "tenant_id", "revoked_at"),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    public_id: Mapped[str] = mapped_column(String(24), nullable=False)
    secret_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    secret_last_four: Mapped[str] = mapped_column(String(4), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WebhookEndpoint(CreatedAtMixin, Base):
    """A tenant-owned destination whose secrets are versioned separately."""

    __tablename__ = "webhook_endpoints"
    __table_args__ = (
        UniqueConstraint("tenant_id", "id"),
        CheckConstraint("btrim(name) <> ''", name="name_not_blank"),
        CheckConstraint("btrim(url) <> ''", name="url_not_blank"),
        CheckConstraint("char_length(url) <= 2048", name="url_length"),
        Index("ix_webhook_endpoints_tenant_id_is_active", "tenant_id", "is_active"),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )


class EndpointTrafficControl(Base):
    """Shared rate-limit and circuit-breaker state for one tenant endpoint."""

    __tablename__ = "endpoint_traffic_controls"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["webhook_endpoints.tenant_id", "webhook_endpoints.id"],
            ondelete="CASCADE",
        ),
        CheckConstraint("rate_window_count >= 0", name="rate_window_count_nonnegative"),
        CheckConstraint(
            "rate_window_started_at IS NOT NULL OR rate_window_count = 0",
            name="rate_window_state_consistent",
        ),
        CheckConstraint(
            "circuit_state IN ('closed', 'open', 'half_open')",
            name="circuit_state_valid",
        ),
        CheckConstraint(
            "circuit_consecutive_failures >= 0",
            name="circuit_consecutive_failures_nonnegative",
        ),
        CheckConstraint(
            "circuit_state = 'closed' OR circuit_consecutive_failures > 0",
            name="nonclosed_circuit_has_failures",
        ),
        CheckConstraint(
            "((circuit_state = 'closed' AND circuit_opened_at IS NULL "
            "AND probe_token IS NULL AND probe_expires_at IS NULL) OR "
            "(circuit_state = 'open' AND circuit_opened_at IS NOT NULL "
            "AND probe_token IS NULL AND probe_expires_at IS NULL) OR "
            "(circuit_state = 'half_open' AND circuit_opened_at IS NOT NULL "
            "AND probe_token IS NOT NULL AND probe_expires_at IS NOT NULL))",
            name="circuit_state_consistent",
        ),
        CheckConstraint(
            "probe_expires_at IS NULL OR probe_expires_at > circuit_opened_at",
            name="probe_expiry_after_circuit_opened",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    endpoint_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    rate_window_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rate_window_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    circuit_state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="closed", server_default="closed"
    )
    circuit_consecutive_failures: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    circuit_opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    probe_token: Mapped[UUID | None] = mapped_column(PostgreSQLUUID(as_uuid=True))
    probe_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class EndpointSigningSecret(CreatedAtMixin, Base):
    """One encrypted secret version retained for accepted delivery snapshots."""

    __tablename__ = "endpoint_signing_secrets"
    __table_args__ = (
        UniqueConstraint("tenant_id", "endpoint_id", "id"),
        UniqueConstraint("endpoint_id", "version"),
        ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["webhook_endpoints.tenant_id", "webhook_endpoints.id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint("version > 0", name="version_positive"),
        CheckConstraint(
            "encryption_key_version > 0",
            name="encryption_key_version_positive",
        ),
        CheckConstraint("octet_length(ciphertext) >= 29", name="ciphertext_minimum_length"),
        CheckConstraint("char_length(secret_hint) = 4", name="secret_hint_length"),
        CheckConstraint(
            "retired_at IS NULL OR retired_at >= created_at",
            name="retirement_not_before_creation",
        ),
        Index(
            "uq_endpoint_signing_secrets_active_endpoint",
            "endpoint_id",
            unique=True,
            postgresql_where=text("retired_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    endpoint_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    encryption_key_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    secret_hint: Mapped[str] = mapped_column(String(4), nullable=False)
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Event(CreatedAtMixin, Base):
    """The immutable producer event and tenant-scoped idempotency record."""

    __tablename__ = "events"
    __table_args__ = (
        UniqueConstraint("tenant_id", "id"),
        UniqueConstraint("tenant_id", "idempotency_key"),
        ForeignKeyConstraint(
            ["tenant_id", "api_key_id"],
            ["api_keys.tenant_id", "api_keys.id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint("btrim(event_type) <> ''", name="event_type_not_blank"),
        CheckConstraint(
            "event_type ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$'",
            name="event_type_format",
        ),
        CheckConstraint(
            "char_length(idempotency_key) BETWEEN 8 AND 128",
            name="idempotency_key_length",
        ),
        CheckConstraint(
            "idempotency_key ~ '^[A-Za-z0-9._:-]{8,128}$'",
            name="idempotency_key_format",
        ),
        CheckConstraint("octet_length(request_fingerprint) = 32", name="fingerprint_length"),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_is_object"),
        CheckConstraint("request_fingerprint_version > 0", name="fingerprint_version_positive"),
        Index("ix_events_tenant_id_created_at", "tenant_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    api_key_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    request_fingerprint_version: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=1, server_default="1"
    )


class Delivery(CreatedAtMixin, Base):
    """One eventual delivery of an event to one webhook endpoint."""

    __tablename__ = "deliveries"
    __table_args__ = (
        UniqueConstraint("tenant_id", "id"),
        UniqueConstraint("event_id", "endpoint_id"),
        ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["webhook_endpoints.tenant_id", "webhook_endpoints.id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "endpoint_id", "signing_secret_id"],
            [
                "endpoint_signing_secrets.tenant_id",
                "endpoint_signing_secrets.endpoint_id",
                "endpoint_signing_secrets.id",
            ],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "status IN ('pending', 'delivering', 'retry_scheduled', 'succeeded', 'dead_lettered')",
            name="status_valid",
        ),
        CheckConstraint("char_length(target_url) <= 2048", name="target_url_length"),
        CheckConstraint("btrim(target_url) <> ''", name="target_url_not_blank"),
        CheckConstraint("dispatch_generation > 0", name="dispatch_generation_positive"),
        CheckConstraint(
            "((status = 'delivering' AND claim_token IS NOT NULL "
            "AND claim_expires_at IS NOT NULL) OR "
            "(status <> 'delivering' AND claim_token IS NULL "
            "AND claim_expires_at IS NULL))",
            name="claim_state_consistent",
        ),
        CheckConstraint(
            "(status = 'retry_scheduled') = (next_attempt_at IS NOT NULL)",
            name="retry_schedule_consistent",
        ),
        CheckConstraint(
            "((status = 'dead_lettered' AND dead_lettered_at IS NOT NULL "
            "AND dead_letter_reason IS NOT NULL) OR "
            "(status <> 'dead_lettered' AND dead_lettered_at IS NULL "
            "AND dead_letter_reason IS NULL))",
            name="dead_letter_state_consistent",
        ),
        CheckConstraint(
            "dead_letter_reason IS NULL OR "
            "dead_letter_reason IN "
            "('permanent_failure', 'attempts_exhausted', 'target_blocked')",
            name="dead_letter_reason_valid",
        ),
        CheckConstraint(
            "claim_expires_at IS NULL OR claim_expires_at > created_at",
            name="claim_expiry_after_creation",
        ),
        Index(
            "ix_deliveries_tenant_id_status_created_at",
            "tenant_id",
            "status",
            "created_at",
            "id",
        ),
        Index(
            "ix_deliveries_tenant_id_created_at_id",
            "tenant_id",
            "created_at",
            "id",
        ),
        Index(
            "ix_deliveries_tenant_id_endpoint_id_created_at_id",
            "tenant_id",
            "endpoint_id",
            "created_at",
            "id",
        ),
        Index(
            "ix_deliveries_retry_scheduled_next_attempt_at",
            "next_attempt_at",
            postgresql_where=text("status = 'retry_scheduled'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    event_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    endpoint_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    signing_secret_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    target_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", server_default="pending"
    )
    dispatch_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_lettered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_letter_reason: Mapped[str | None] = mapped_column(String(32))
    claim_token: Mapped[UUID | None] = mapped_column(PostgreSQLUUID(as_uuid=True))
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DeliveryAttempt(Base):
    """An immutable-identity record whose completion fields describe one HTTP attempt."""

    __tablename__ = "delivery_attempts"
    __table_args__ = (
        UniqueConstraint("delivery_id", "attempt_number"),
        ForeignKeyConstraint(
            ["tenant_id", "delivery_id"],
            ["deliveries.tenant_id", "deliveries.id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint("attempt_number > 0", name="attempt_number_positive"),
        CheckConstraint("dispatch_generation > 0", name="dispatch_generation_positive"),
        CheckConstraint(
            "outcome IS NULL OR outcome IN ('succeeded', 'transient_failure', "
            "'permanent_failure', 'abandoned')",
            name="outcome_valid",
        ),
        CheckConstraint(
            "response_status_code IS NULL OR response_status_code BETWEEN 100 AND 599",
            name="response_status_code_valid",
        ),
        CheckConstraint("duration_ms IS NULL OR duration_ms >= 0", name="duration_ms_nonnegative"),
        CheckConstraint(
            "(finished_at IS NULL AND outcome IS NULL) OR "
            "(finished_at IS NOT NULL AND outcome IS NOT NULL)",
            name="completion_consistent",
        ),
        CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at",
            name="finish_not_before_start",
        ),
        CheckConstraint(
            "finished_at IS NOT NULL OR claim_token IS NOT NULL",
            name="active_attempt_has_claim",
        ),
        Index("ix_delivery_attempts_tenant_id_started_at", "tenant_id", "started_at"),
        Index(
            "uq_delivery_attempts_unfinished_delivery",
            "delivery_id",
            unique=True,
            postgresql_where=text("finished_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    delivery_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    dispatch_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    claim_token: Mapped[UUID | None] = mapped_column(PostgreSQLUUID(as_uuid=True))
    is_circuit_probe: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(32))
    response_status_code: Mapped[int | None] = mapped_column(SmallInteger)
    error_code: Mapped[str | None] = mapped_column(String(100))
    duration_ms: Mapped[int | None] = mapped_column(BigInteger)


class OutboxMessage(CreatedAtMixin, Base):
    """A delivery dispatch fact committed atomically with its event."""

    __tablename__ = "outbox_messages"
    __table_args__ = (
        UniqueConstraint("delivery_id", "topic", "dispatch_generation"),
        CheckConstraint("dispatch_generation > 0", name="dispatch_generation_positive"),
        CheckConstraint("schema_version > 0", name="schema_version_positive"),
        CheckConstraint("topic = 'delivery.requested'", name="topic_valid"),
        ForeignKeyConstraint(
            ["tenant_id", "delivery_id"],
            ["deliveries.tenant_id", "deliveries.id"],
            ondelete="RESTRICT",
        ),
        Index(
            "ix_outbox_messages_unpublished_created_at",
            "created_at",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index(
            "ix_outbox_messages_unpublished_claim_expires_at",
            "claim_expires_at",
            "created_at",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
        CheckConstraint(
            "(claim_token IS NULL) = (claim_expires_at IS NULL)",
            name="claim_fields_consistent",
        ),
        CheckConstraint(
            "claim_expires_at IS NULL OR claim_expires_at > created_at",
            name="claim_expiry_after_creation",
        ),
        CheckConstraint(
            "published_at IS NULL OR claim_token IS NULL",
            name="published_message_not_claimed",
        ),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_is_object"),
        CheckConstraint(
            "traceparent IS NULL OR traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$'",
            name="traceparent_canonical",
        ),
    )

    id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    delivery_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False)
    dispatch_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    schema_version: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=1, server_default="1"
    )
    topic: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    correlation_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        nullable=False,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    traceparent: Mapped[str | None] = mapped_column(String(55))
    claim_token: Mapped[UUID | None] = mapped_column(PostgreSQLUUID(as_uuid=True))
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
