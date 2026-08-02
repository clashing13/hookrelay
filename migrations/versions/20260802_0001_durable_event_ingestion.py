"""Add the durable multi-tenant event-ingestion domain.

Revision ID: 20260802_0001
Revises:
Create Date: 2026-08-02

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic.
revision: str = "20260802_0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create tenant, credential, ingestion, delivery, and outbox tables."""

    op.create_table(
        "tenants",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("btrim(name) <> ''", name=op.f("ck_tenants_name_not_blank")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tenants")),
    )
    op.create_table(
        "api_keys",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("public_id", sa.String(length=24), nullable=False),
        sa.Column("secret_hash", sa.LargeBinary(length=32), nullable=False),
        sa.Column("secret_last_four", sa.String(length=4), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "expires_at IS NULL OR expires_at >= created_at",
            name=op.f("ck_api_keys_expiry_not_before_creation"),
        ),
        sa.CheckConstraint(
            "btrim(name) <> ''",
            name=op.f("ck_api_keys_name_not_blank"),
        ),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name=op.f("ck_api_keys_revocation_not_before_creation"),
        ),
        sa.CheckConstraint(
            "octet_length(secret_hash) = 32",
            name=op.f("ck_api_keys_secret_hash_length"),
        ),
        sa.CheckConstraint(
            "char_length(secret_last_four) = 4",
            name=op.f("ck_api_keys_secret_hint_length"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_api_keys_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api_keys")),
        sa.UniqueConstraint("public_id", name=op.f("uq_api_keys_public_id")),
        sa.UniqueConstraint("tenant_id", "id", name=op.f("uq_api_keys_tenant_id_id")),
    )
    op.create_index(
        "ix_api_keys_tenant_id_revoked_at",
        "api_keys",
        ["tenant_id", "revoked_at"],
        unique=False,
    )
    op.create_table(
        "webhook_endpoints",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "btrim(name) <> ''",
            name=op.f("ck_webhook_endpoints_name_not_blank"),
        ),
        sa.CheckConstraint(
            "char_length(url) <= 2048",
            name=op.f("ck_webhook_endpoints_url_length"),
        ),
        sa.CheckConstraint(
            "btrim(url) <> ''",
            name=op.f("ck_webhook_endpoints_url_not_blank"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_webhook_endpoints_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_webhook_endpoints")),
        sa.UniqueConstraint(
            "tenant_id",
            "id",
            name=op.f("uq_webhook_endpoints_tenant_id_id"),
        ),
    )
    op.create_index(
        "ix_webhook_endpoints_tenant_id_is_active",
        "webhook_endpoints",
        ["tenant_id", "is_active"],
        unique=False,
    )
    op.create_table(
        "endpoint_signing_secrets",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("encryption_key_version", sa.SmallInteger(), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("secret_hint", sa.String(length=4), nullable=False),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "octet_length(ciphertext) >= 29",
            name=op.f("ck_endpoint_signing_secrets_ciphertext_minimum_length"),
        ),
        sa.CheckConstraint(
            "encryption_key_version > 0",
            name=op.f("ck_endpoint_signing_secrets_encryption_key_version_positive"),
        ),
        sa.CheckConstraint(
            "retired_at IS NULL OR retired_at >= created_at",
            name=op.f("ck_endpoint_signing_secrets_retirement_not_before_creation"),
        ),
        sa.CheckConstraint(
            "char_length(secret_hint) = 4",
            name=op.f("ck_endpoint_signing_secrets_secret_hint_length"),
        ),
        sa.CheckConstraint(
            "version > 0",
            name=op.f("ck_endpoint_signing_secrets_version_positive"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["webhook_endpoints.tenant_id", "webhook_endpoints.id"],
            name=op.f("fk_endpoint_signing_secrets_tenant_id_endpoint_id_webhook_endpoints"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_endpoint_signing_secrets_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_endpoint_signing_secrets")),
        sa.UniqueConstraint(
            "endpoint_id",
            "version",
            name=op.f("uq_endpoint_signing_secrets_endpoint_id_version"),
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "endpoint_id",
            "id",
            name=op.f("uq_endpoint_signing_secrets_tenant_id_endpoint_id_id"),
        ),
    )
    op.create_index(
        "uq_endpoint_signing_secrets_active_endpoint",
        "endpoint_signing_secrets",
        ["endpoint_id"],
        unique=True,
        postgresql_where=sa.text("retired_at IS NULL"),
    )
    op.create_table(
        "events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("api_key_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_fingerprint", sa.LargeBinary(length=32), nullable=False),
        sa.Column(
            "request_fingerprint_version",
            sa.SmallInteger(),
            server_default="1",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "btrim(event_type) <> ''",
            name=op.f("ck_events_event_type_not_blank"),
        ),
        sa.CheckConstraint(
            "event_type ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$'",
            name=op.f("ck_events_event_type_format"),
        ),
        sa.CheckConstraint(
            "octet_length(request_fingerprint) = 32",
            name=op.f("ck_events_fingerprint_length"),
        ),
        sa.CheckConstraint(
            "request_fingerprint_version > 0",
            name=op.f("ck_events_fingerprint_version_positive"),
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[A-Za-z0-9._:-]{8,128}$'",
            name=op.f("ck_events_idempotency_key_format"),
        ),
        sa.CheckConstraint(
            "char_length(idempotency_key) BETWEEN 8 AND 128",
            name=op.f("ck_events_idempotency_key_length"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name=op.f("ck_events_payload_is_object"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "api_key_id"],
            ["api_keys.tenant_id", "api_keys.id"],
            name=op.f("fk_events_tenant_id_api_key_id_api_keys"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_events_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_events")),
        sa.UniqueConstraint("tenant_id", "id", name=op.f("uq_events_tenant_id_id")),
        sa.UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name=op.f("uq_events_tenant_id_idempotency_key"),
        ),
    )
    op.create_index(
        "ix_events_tenant_id_created_at",
        "events",
        ["tenant_id", "created_at"],
        unique=False,
    )
    op.create_table(
        "deliveries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("signing_secret_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'delivering', 'retry_scheduled', 'succeeded', 'dead_lettered')",
            name=op.f("ck_deliveries_status_valid"),
        ),
        sa.CheckConstraint(
            "char_length(target_url) <= 2048",
            name=op.f("ck_deliveries_target_url_length"),
        ),
        sa.CheckConstraint(
            "btrim(target_url) <> ''",
            name=op.f("ck_deliveries_target_url_not_blank"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["webhook_endpoints.tenant_id", "webhook_endpoints.id"],
            name=op.f("fk_deliveries_tenant_id_endpoint_id_webhook_endpoints"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            name=op.f("fk_deliveries_tenant_id_event_id_events"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "endpoint_id", "signing_secret_id"],
            [
                "endpoint_signing_secrets.tenant_id",
                "endpoint_signing_secrets.endpoint_id",
                "endpoint_signing_secrets.id",
            ],
            name=op.f(
                "fk_deliveries_tenant_id_endpoint_id_signing_secret_id_endpoint_signing_secrets"
            ),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_deliveries_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_deliveries")),
        sa.UniqueConstraint(
            "event_id",
            "endpoint_id",
            name=op.f("uq_deliveries_event_id_endpoint_id"),
        ),
        sa.UniqueConstraint("tenant_id", "id", name=op.f("uq_deliveries_tenant_id_id")),
    )
    op.create_index(
        "ix_deliveries_tenant_id_status_created_at",
        "deliveries",
        ["tenant_id", "status", "created_at"],
        unique=False,
    )
    op.create_table(
        "delivery_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(length=32), nullable=True),
        sa.Column("response_status_code", sa.SmallInteger(), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.CheckConstraint(
            "attempt_number > 0",
            name=op.f("ck_delivery_attempts_attempt_number_positive"),
        ),
        sa.CheckConstraint(
            "(finished_at IS NULL AND outcome IS NULL) OR "
            "(finished_at IS NOT NULL AND outcome IS NOT NULL)",
            name=op.f("ck_delivery_attempts_completion_consistent"),
        ),
        sa.CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0",
            name=op.f("ck_delivery_attempts_duration_ms_nonnegative"),
        ),
        sa.CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at",
            name=op.f("ck_delivery_attempts_finish_not_before_start"),
        ),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('succeeded', 'transient_failure', "
            "'permanent_failure', 'abandoned')",
            name=op.f("ck_delivery_attempts_outcome_valid"),
        ),
        sa.CheckConstraint(
            "response_status_code IS NULL OR response_status_code BETWEEN 100 AND 599",
            name=op.f("ck_delivery_attempts_response_status_code_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "delivery_id"],
            ["deliveries.tenant_id", "deliveries.id"],
            name=op.f("fk_delivery_attempts_tenant_id_delivery_id_deliveries"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_delivery_attempts_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_delivery_attempts")),
        sa.UniqueConstraint(
            "delivery_id",
            "attempt_number",
            name=op.f("uq_delivery_attempts_delivery_id_attempt_number"),
        ),
    )
    op.create_index(
        "ix_delivery_attempts_tenant_id_started_at",
        "delivery_attempts",
        ["tenant_id", "started_at"],
        unique=False,
    )
    op.create_table(
        "outbox_messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("schema_version", sa.SmallInteger(), server_default="1", nullable=False),
        sa.Column("topic", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name=op.f("ck_outbox_messages_payload_is_object"),
        ),
        sa.CheckConstraint(
            "schema_version > 0",
            name=op.f("ck_outbox_messages_schema_version_positive"),
        ),
        sa.CheckConstraint(
            "topic = 'delivery.requested'",
            name=op.f("ck_outbox_messages_topic_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "delivery_id"],
            ["deliveries.tenant_id", "deliveries.id"],
            name=op.f("fk_outbox_messages_tenant_id_delivery_id_deliveries"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_outbox_messages_tenant_id_tenants"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_messages")),
        sa.UniqueConstraint(
            "delivery_id",
            "topic",
            name=op.f("uq_outbox_messages_delivery_id_topic"),
        ),
    )
    op.create_index(
        "ix_outbox_messages_unpublished_created_at",
        "outbox_messages",
        ["created_at", "id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Drop the Stage 2 domain in dependency-safe reverse order."""

    op.drop_index(
        "ix_outbox_messages_unpublished_created_at",
        table_name="outbox_messages",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_table("outbox_messages")
    op.drop_index(
        "ix_delivery_attempts_tenant_id_started_at",
        table_name="delivery_attempts",
    )
    op.drop_table("delivery_attempts")
    op.drop_index(
        "ix_deliveries_tenant_id_status_created_at",
        table_name="deliveries",
    )
    op.drop_table("deliveries")
    op.drop_index("ix_events_tenant_id_created_at", table_name="events")
    op.drop_table("events")
    op.drop_index(
        "uq_endpoint_signing_secrets_active_endpoint",
        table_name="endpoint_signing_secrets",
        postgresql_where=sa.text("retired_at IS NULL"),
    )
    op.drop_table("endpoint_signing_secrets")
    op.drop_index(
        "ix_webhook_endpoints_tenant_id_is_active",
        table_name="webhook_endpoints",
    )
    op.drop_table("webhook_endpoints")
    op.drop_index("ix_api_keys_tenant_id_revoked_at", table_name="api_keys")
    op.drop_table("api_keys")
    op.drop_table("tenants")
