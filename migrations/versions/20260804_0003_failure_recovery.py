"""Add durable delivery retry, lease, dead-letter, and replay state.

Revision ID: 20260804_0003
Revises: 20260803_0002
Create Date: 2026-08-04

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic.
revision: str = "20260804_0003"
down_revision: str | Sequence[str] | None = "20260803_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Make PostgreSQL authoritative for retries and recoverable worker ownership."""

    op.add_column(
        "deliveries",
        sa.Column("dispatch_generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "deliveries",
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "deliveries",
        sa.Column("dead_lettered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "deliveries",
        sa.Column("dead_letter_reason", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "deliveries",
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "deliveries",
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "delivery_attempts",
        sa.Column("dispatch_generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "delivery_attempts",
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.alter_column(
        "delivery_attempts",
        "duration_ms",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )

    # A Stage 3 worker could have stopped after committing an unfinished attempt.
    # Preserve that evidence as abandoned and make the delivery immediately retryable.
    op.execute(
        """
        UPDATE delivery_attempts
        SET finished_at = GREATEST(started_at, now()),
            outcome = 'abandoned',
            error_code = 'stage4_migration_recovery',
            duration_ms = GREATEST(
                0,
                FLOOR(EXTRACT(EPOCH FROM (now() - started_at)) * 1000)
            )::bigint
        WHERE finished_at IS NULL
        """
    )
    op.execute(
        """
        UPDATE deliveries
        SET status = 'retry_scheduled', next_attempt_at = now()
        WHERE status = 'delivering'
        """
    )
    op.execute(
        """
        UPDATE deliveries
        SET next_attempt_at = now()
        WHERE status = 'retry_scheduled' AND next_attempt_at IS NULL
        """
    )
    op.execute(
        """
        UPDATE deliveries
        SET dead_lettered_at = now(), dead_letter_reason = 'attempts_exhausted'
        WHERE status = 'dead_lettered'
        """
    )

    op.create_check_constraint(
        op.f("ck_deliveries_dispatch_generation_positive"),
        "deliveries",
        "dispatch_generation > 0",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_claim_state_consistent"),
        "deliveries",
        "((status = 'delivering' AND claim_token IS NOT NULL "
        "AND claim_expires_at IS NOT NULL) OR "
        "(status <> 'delivering' AND claim_token IS NULL "
        "AND claim_expires_at IS NULL))",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_retry_schedule_consistent"),
        "deliveries",
        "(status = 'retry_scheduled') = (next_attempt_at IS NOT NULL)",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_dead_letter_state_consistent"),
        "deliveries",
        "((status = 'dead_lettered' AND dead_lettered_at IS NOT NULL "
        "AND dead_letter_reason IS NOT NULL) OR "
        "(status <> 'dead_lettered' AND dead_lettered_at IS NULL "
        "AND dead_letter_reason IS NULL))",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_dead_letter_reason_valid"),
        "deliveries",
        "dead_letter_reason IS NULL OR "
        "dead_letter_reason IN "
        "('permanent_failure', 'attempts_exhausted', 'target_blocked')",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_claim_expiry_after_creation"),
        "deliveries",
        "claim_expires_at IS NULL OR claim_expires_at > created_at",
    )
    op.create_index(
        "ix_deliveries_retry_scheduled_next_attempt_at",
        "deliveries",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'retry_scheduled'"),
    )
    op.create_check_constraint(
        op.f("ck_delivery_attempts_dispatch_generation_positive"),
        "delivery_attempts",
        "dispatch_generation > 0",
    )
    op.create_check_constraint(
        op.f("ck_delivery_attempts_active_attempt_has_claim"),
        "delivery_attempts",
        "finished_at IS NOT NULL OR claim_token IS NOT NULL",
    )
    op.create_index(
        "uq_delivery_attempts_unfinished_delivery",
        "delivery_attempts",
        ["delivery_id"],
        unique=True,
        postgresql_where=sa.text("finished_at IS NULL"),
    )

    op.add_column(
        "outbox_messages",
        sa.Column("dispatch_generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_outbox_messages_dispatch_generation_positive"),
        "outbox_messages",
        "dispatch_generation > 0",
    )
    op.drop_constraint(
        op.f("uq_outbox_messages_delivery_id_topic"),
        "outbox_messages",
        type_="unique",
    )
    op.create_unique_constraint(
        op.f("uq_outbox_messages_delivery_id_topic_dispatch_generation"),
        "outbox_messages",
        ["delivery_id", "topic", "dispatch_generation"],
    )


def downgrade() -> None:
    """Remove Stage 4 state, refusing to discard replay generations silently."""

    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM deliveries
                WHERE status IN ('delivering', 'retry_scheduled', 'dead_lettered')
                   OR dispatch_generation <> 1
            ) OR EXISTS (
                SELECT 1
                FROM outbox_messages
                WHERE dispatch_generation <> 1
            ) OR EXISTS (
                SELECT 1
                FROM delivery_attempts
                WHERE dispatch_generation <> 1
                   OR duration_ms > 2147483647
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade Stage 4 while Stage 4 delivery state or envelopes exist';
            END IF;
        END
        $$
        """
    )
    op.drop_constraint(
        op.f("uq_outbox_messages_delivery_id_topic_dispatch_generation"),
        "outbox_messages",
        type_="unique",
    )
    op.drop_constraint(
        op.f("ck_outbox_messages_dispatch_generation_positive"),
        "outbox_messages",
        type_="check",
    )
    op.drop_column("outbox_messages", "dispatch_generation")
    op.create_unique_constraint(
        op.f("uq_outbox_messages_delivery_id_topic"),
        "outbox_messages",
        ["delivery_id", "topic"],
    )

    op.drop_index(
        "uq_delivery_attempts_unfinished_delivery",
        table_name="delivery_attempts",
        postgresql_where=sa.text("finished_at IS NULL"),
    )
    op.drop_constraint(
        op.f("ck_delivery_attempts_active_attempt_has_claim"),
        "delivery_attempts",
        type_="check",
    )
    op.drop_column("delivery_attempts", "claim_token")
    op.drop_constraint(
        op.f("ck_delivery_attempts_dispatch_generation_positive"),
        "delivery_attempts",
        type_="check",
    )
    op.drop_column("delivery_attempts", "dispatch_generation")
    op.alter_column(
        "delivery_attempts",
        "duration_ms",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )

    op.drop_index(
        "ix_deliveries_retry_scheduled_next_attempt_at",
        table_name="deliveries",
        postgresql_where=sa.text("status = 'retry_scheduled'"),
    )
    op.drop_constraint(
        op.f("ck_deliveries_claim_expiry_after_creation"),
        "deliveries",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_deliveries_dead_letter_reason_valid"),
        "deliveries",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_deliveries_dead_letter_state_consistent"),
        "deliveries",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_deliveries_retry_schedule_consistent"),
        "deliveries",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_deliveries_claim_state_consistent"),
        "deliveries",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_deliveries_dispatch_generation_positive"),
        "deliveries",
        type_="check",
    )
    op.drop_column("deliveries", "claim_expires_at")
    op.drop_column("deliveries", "claim_token")
    op.drop_column("deliveries", "dead_letter_reason")
    op.drop_column("deliveries", "dead_lettered_at")
    op.drop_column("deliveries", "next_attempt_at")
    op.drop_column("deliveries", "dispatch_generation")
