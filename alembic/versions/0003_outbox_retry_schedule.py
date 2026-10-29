"""Persist outbox retry scheduling for publisher recovery."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0003_outbox_retry_schedule"
down_revision: str | None = "0002_tenant_slug_name"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox_events",
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.drop_index("ix_outbox_pending", table_name="outbox_events")
    op.create_index(
        "ix_outbox_pending",
        "outbox_events",
        ["published_at", "next_attempt_at", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_pending", table_name="outbox_events")
    op.create_index(
        "ix_outbox_pending",
        "outbox_events",
        ["published_at", "created_at"],
        unique=False,
    )
    op.drop_column("outbox_events", "next_attempt_at")
