"""Add operations-history indexes and durable observability context.

Revision ID: 20260806_0005
Revises: 20260805_0004
Create Date: 2026-08-06

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic.
revision: str = "20260806_0005"
down_revision: str | Sequence[str] | None = "20260805_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Support bounded tenant history queries and cross-process correlation."""

    op.drop_index(
        "ix_deliveries_tenant_id_status_created_at",
        table_name="deliveries",
    )
    op.create_index(
        "ix_deliveries_tenant_id_status_created_at",
        "deliveries",
        ["tenant_id", "status", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_deliveries_tenant_id_created_at_id",
        "deliveries",
        ["tenant_id", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_deliveries_tenant_id_endpoint_id_created_at_id",
        "deliveries",
        ["tenant_id", "endpoint_id", "created_at", "id"],
        unique=False,
    )

    op.add_column(
        "outbox_messages",
        sa.Column(
            "correlation_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.execute("UPDATE outbox_messages SET correlation_id = id")
    op.alter_column(
        "outbox_messages",
        "correlation_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
        server_default=sa.text("gen_random_uuid()"),
    )
    op.add_column(
        "outbox_messages",
        sa.Column("traceparent", sa.String(length=55), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_outbox_messages_traceparent_canonical"),
        "outbox_messages",
        "traceparent IS NULL OR traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$'",
    )


def downgrade() -> None:
    """Remove derived indexes and optional observability metadata."""

    op.drop_constraint(
        op.f("ck_outbox_messages_traceparent_canonical"),
        "outbox_messages",
        type_="check",
    )
    op.drop_column("outbox_messages", "traceparent")
    op.drop_column("outbox_messages", "correlation_id")

    op.drop_index(
        "ix_deliveries_tenant_id_endpoint_id_created_at_id",
        table_name="deliveries",
    )
    op.drop_index(
        "ix_deliveries_tenant_id_created_at_id",
        table_name="deliveries",
    )
    op.drop_index(
        "ix_deliveries_tenant_id_status_created_at",
        table_name="deliveries",
    )
    op.create_index(
        "ix_deliveries_tenant_id_status_created_at",
        "deliveries",
        ["tenant_id", "status", "created_at"],
        unique=False,
    )
