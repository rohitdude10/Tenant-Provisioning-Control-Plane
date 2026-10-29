from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.time import utc_now
from app.db import Base
from app.domain import TASK_STATUSES, TASK_TYPES, TENANT_SLUG_REGEX, TENANT_STATUSES


class Tenant(Base):
    __tablename__ = "tenants"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_tenants_slug"),
        CheckConstraint("length(name) > 0", name="ck_tenants_name_nonempty"),
        CheckConstraint("version >= 1", name="ck_tenants_version_positive"),
        CheckConstraint(
            "length(btrim(name)) > 0",
            name="ck_tenants_name_not_blank",
        ),
        CheckConstraint(
            f"slug ~ '{TENANT_SLUG_REGEX}'",
            name="ck_tenants_slug_format",
        ),
        CheckConstraint(
            "status IN (" + ", ".join(f"'{status}'" for status in sorted(TENANT_STATUSES)) + ")",
            name="ck_tenants_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    slug: Mapped[str] = mapped_column(String(28), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    tasks: Mapped[list["Task"]] = relationship(back_populates="tenant")


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint(
            "type IN (" + ", ".join(f"'{task_type}'" for task_type in sorted(TASK_TYPES)) + ")",
            name="ck_tasks_type",
        ),
        CheckConstraint(
            "status IN (" + ", ".join(f"'{status}'" for status in sorted(TASK_STATUSES)) + ")",
            name="ck_tasks_status",
        ),
        Index("ix_tasks_tenant_created", "tenant_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="accepted")
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    tenant: Mapped[Tenant] = relationship(back_populates="tasks")


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_outbox_pending", "published_at", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False, unique=True
    )
    body: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(String(1000))


class ProcessedUpdate(Base):
    __tablename__ = "processed_updates"
    __table_args__ = (Index("ix_processed_updates_task", "task_id"),)

    update_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    task_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now
    )
