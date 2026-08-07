"""Add shared per-endpoint rate-limit and circuit-breaker state.

Revision ID: 20260805_0004
Revises: 20260804_0003
Create Date: 2026-08-05

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic.
revision: str = "20260805_0004"
down_revision: str | Sequence[str] | None = "20260804_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create and backfill one shared traffic-control row per endpoint."""

    # Repair early Stage 4 development databases that recorded revision 0003
    # before its duration widening was exercised against PostgreSQL.
    op.alter_column(
        "delivery_attempts",
        "duration_ms",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    op.drop_constraint(
        op.f("ck_deliveries_dead_letter_reason_valid"),
        "deliveries",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_dead_letter_reason_valid"),
        "deliveries",
        "dead_letter_reason IS NULL OR dead_letter_reason IN "
        "('permanent_failure', 'attempts_exhausted', 'target_blocked')",
    )
    op.add_column(
        "delivery_attempts",
        sa.Column("is_circuit_probe", sa.Boolean(), server_default="false", nullable=False),
    )

    op.create_table(
        "endpoint_traffic_controls",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("rate_window_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rate_window_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "circuit_state",
            sa.String(length=16),
            server_default="closed",
            nullable=False,
        ),
        sa.Column(
            "circuit_consecutive_failures",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column("circuit_opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("probe_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("probe_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "circuit_consecutive_failures >= 0",
            name=op.f("ck_endpoint_traffic_controls_circuit_consecutive_failures_nonnegative"),
        ),
        sa.CheckConstraint(
            "circuit_state = 'closed' OR circuit_consecutive_failures > 0",
            name=op.f("ck_endpoint_traffic_controls_nonclosed_circuit_has_failures"),
        ),
        sa.CheckConstraint(
            "((circuit_state = 'closed' AND circuit_opened_at IS NULL "
            "AND probe_token IS NULL AND probe_expires_at IS NULL) OR "
            "(circuit_state = 'open' AND circuit_opened_at IS NOT NULL "
            "AND probe_token IS NULL AND probe_expires_at IS NULL) OR "
            "(circuit_state = 'half_open' AND circuit_opened_at IS NOT NULL "
            "AND probe_token IS NOT NULL AND probe_expires_at IS NOT NULL))",
            name=op.f("ck_endpoint_traffic_controls_circuit_state_consistent"),
        ),
        sa.CheckConstraint(
            "circuit_state IN ('closed', 'open', 'half_open')",
            name=op.f("ck_endpoint_traffic_controls_circuit_state_valid"),
        ),
        sa.CheckConstraint(
            "probe_expires_at IS NULL OR probe_expires_at > circuit_opened_at",
            name=op.f("ck_endpoint_traffic_controls_probe_expiry_after_circuit_opened"),
        ),
        sa.CheckConstraint(
            "rate_window_count >= 0",
            name=op.f("ck_endpoint_traffic_controls_rate_window_count_nonnegative"),
        ),
        sa.CheckConstraint(
            "rate_window_started_at IS NOT NULL OR rate_window_count = 0",
            name=op.f("ck_endpoint_traffic_controls_rate_window_state_consistent"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["webhook_endpoints.tenant_id", "webhook_endpoints.id"],
            name=op.f("fk_endpoint_traffic_controls_tenant_id_endpoint_id_webhook_endpoints"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "endpoint_id",
            name=op.f("pk_endpoint_traffic_controls"),
        ),
    )

    op.execute(
        """
        INSERT INTO endpoint_traffic_controls (tenant_id, endpoint_id)
        SELECT tenant_id, id
        FROM webhook_endpoints
        """
    )


def downgrade() -> None:
    """Remove the derived per-endpoint traffic-control state."""

    op.drop_table("endpoint_traffic_controls")
    op.drop_column("delivery_attempts", "is_circuit_probe")
