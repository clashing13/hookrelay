"""Add recoverable publisher claims to transactional-outbox rows.

Revision ID: 20260803_0002
Revises: 20260802_0001
Create Date: 2026-08-03

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic.
revision: str = "20260803_0002"
down_revision: str | Sequence[str] | None = "20260802_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Let multiple publishers claim short batches without holding locks over NATS I/O."""

    op.add_column(
        "outbox_messages",
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "outbox_messages",
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_outbox_messages_claim_fields_consistent"),
        "outbox_messages",
        "(claim_token IS NULL) = (claim_expires_at IS NULL)",
    )
    op.create_check_constraint(
        op.f("ck_outbox_messages_claim_expiry_after_creation"),
        "outbox_messages",
        "claim_expires_at IS NULL OR claim_expires_at > created_at",
    )
    op.create_check_constraint(
        op.f("ck_outbox_messages_published_message_not_claimed"),
        "outbox_messages",
        "published_at IS NULL OR claim_token IS NULL",
    )
    op.create_index(
        "ix_outbox_messages_unpublished_claim_expires_at",
        "outbox_messages",
        ["claim_expires_at", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Return to the Stage 2 outbox shape without disturbing domain rows."""

    op.drop_index(
        "ix_outbox_messages_unpublished_claim_expires_at",
        table_name="outbox_messages",
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_constraint(
        op.f("ck_outbox_messages_published_message_not_claimed"),
        "outbox_messages",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_outbox_messages_claim_expiry_after_creation"),
        "outbox_messages",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_outbox_messages_claim_fields_consistent"),
        "outbox_messages",
        type_="check",
    )
    op.drop_column("outbox_messages", "claim_expires_at")
    op.drop_column("outbox_messages", "claim_token")
