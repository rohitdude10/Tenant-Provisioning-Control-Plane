"""Enforce tenant slug format and reject blank names."""

from collections.abc import Sequence

from alembic import op

revision: str = "0002_tenant_slug_name"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_tenants_slug_format",
        "tenants",
        "slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'",
    )
    op.create_check_constraint(
        "ck_tenants_name_not_blank",
        "tenants",
        "length(btrim(name)) > 0",
    )
    op.create_index("ix_processed_updates_task", "processed_updates", ["task_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_processed_updates_task", table_name="processed_updates")
    op.drop_constraint("ck_tenants_name_not_blank", "tenants", type_="check")
    op.drop_constraint("ck_tenants_slug_format", "tenants", type_="check")
