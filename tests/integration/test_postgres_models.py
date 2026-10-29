import os
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, inspect, select
from sqlalchemy.exc import IntegrityError

from app.core.time import format_timestamp, utc_now
from app.db import SessionLocal, engine, transaction_scope
from app.models import OutboxEvent, ProcessedUpdate, Task, Tenant

pytestmark = pytest.mark.integration
if not os.getenv("TEST_DATABASE_URL"):
    pytestmark = pytest.mark.skip(
        reason="Set TEST_DATABASE_URL to run PostgreSQL model integration tests"
    )
else:
    database_name = os.getenv("TEST_DATABASE_URL", "").rsplit("/", 1)[-1].split("?", 1)[0]
    if not database_name.endswith("_test"):
        raise RuntimeError(
            "TEST_DATABASE_URL database name must end with '_test' to protect local data"
        )


def _tenant(slug: str, *, name: str = "Model test", status: str = "provisioning") -> Tenant:
    now = utc_now()
    return Tenant(
        id=uuid4(),
        slug=slug,
        name=name,
        status=status,
        version=1,
        created_at=now,
        updated_at=now,
    )


def _expect_constraint_error(session, record, constraint_name: str | None = None) -> None:
    with pytest.raises(IntegrityError) as error:
        with session.begin_nested():
            session.add(record)
            session.flush()
    if constraint_name is not None:
        assert error.value.orig.diag.constraint_name == constraint_name


def _cleanup(tenant_id) -> None:
    with transaction_scope() as session:
        task_ids = list(session.scalars(select(Task.id).where(Task.tenant_id == tenant_id)))
        session.execute(delete(ProcessedUpdate).where(ProcessedUpdate.task_id.in_(task_ids)))
        session.execute(delete(OutboxEvent).where(OutboxEvent.task_id.in_(task_ids)))
        session.execute(delete(Task).where(Task.tenant_id == tenant_id))
        session.execute(delete(Tenant).where(Tenant.id == tenant_id))


def test_postgres_enforces_slug_regex_and_nonblank_name():
    with SessionLocal() as session:
        _expect_constraint_error(
            session,
            _tenant("Invalid_slug"),
            "ck_tenants_slug_format",
        )
        _expect_constraint_error(
            session,
            _tenant(f"test-{uuid4().hex[:12]}", name="   "),
            "ck_tenants_name_not_blank",
        )
        _expect_constraint_error(
            session,
            _tenant(f"test-{uuid4().hex[:12]}", status="unknown"),
            "ck_tenants_status",
        )


def test_slug_uniqueness_includes_destroyed_tenants():
    slug = f"test-{uuid4().hex[:12]}"
    with SessionLocal() as session:
        with session.begin():
            session.add(_tenant(slug, status="destroyed"))
        _expect_constraint_error(
            session,
            _tenant(slug, status="provisioning"),
            "uq_tenants_slug",
        )
        tenant_id = session.scalar(select(Tenant.id).where(Tenant.slug == slug))
    _cleanup(tenant_id)


def test_postgres_enforces_task_status_type_and_tenant_foreign_key():
    task_id = uuid4()
    with SessionLocal() as session:
        _expect_constraint_error(
            session,
            Task(
                id=task_id,
                type="rebuild",
                tenant_id=uuid4(),
                status="accepted",
                payload={},
                created_at=utc_now(),
                updated_at=utc_now(),
            ),
            "ck_tasks_type",
        )
        _expect_constraint_error(
            session,
            Task(
                id=task_id,
                type="deploy",
                tenant_id=uuid4(),
                status="queued",
                payload={},
                created_at=utc_now(),
                updated_at=utc_now(),
            ),
            "ck_tasks_status",
        )
        _expect_constraint_error(
            session,
            Task(
                id=task_id,
                type="deploy",
                tenant_id=uuid4(),
                status="accepted",
                payload={},
                created_at=utc_now(),
                updated_at=utc_now(),
            ),
            None,
        )


def test_outbox_and_processed_update_uniqueness_and_foreign_keys():
    tenant = _tenant(f"test-{uuid4().hex[:12]}")
    task = Task(
        id=uuid4(),
        type="deploy",
        tenant_id=tenant.id,
        status="accepted",
        payload={},
        created_at=utc_now(),
        updated_at=utc_now(),
    )
    with SessionLocal.begin() as session:
        session.add_all([tenant, task])
        session.flush()
        first_event = OutboxEvent(task_id=task.id, body={"id": str(task.id)}, created_at=utc_now())
        session.add(first_event)
        session.flush()
        _expect_constraint_error(
            session,
            OutboxEvent(task_id=task.id, body={"id": str(task.id)}, created_at=utc_now()),
        )
        _expect_constraint_error(
            session,
            ProcessedUpdate(update_id="shared-id", task_id=uuid4(), processed_at=utc_now()),
        )
        session.add(ProcessedUpdate(update_id="shared-id", task_id=task.id, processed_at=utc_now()))
        session.flush()
        _expect_constraint_error(
            session,
            ProcessedUpdate(update_id="shared-id", task_id=task.id, processed_at=utc_now()),
        )
    _cleanup(tenant.id)


def test_indexes_support_task_outbox_and_processed_update_lookups():
    inspector = inspect(engine)
    expected = {
        "tasks": {"ix_tasks_tenant_created"},
        "outbox_events": {"ix_outbox_pending"},
        "processed_updates": {"ix_processed_updates_task"},
    }

    for table, index_names in expected.items():
        existing = {index["name"] for index in inspector.get_indexes(table)}
        assert index_names <= existing


def test_timestamp_columns_round_trip_as_aware_utc_and_format_with_milliseconds():
    tenant = _tenant(f"test-{uuid4().hex[:12]}")
    try:
        with transaction_scope() as session:
            session.add(tenant)
        with SessionLocal() as session:
            stored = session.get(Tenant, tenant.id)
            assert stored is not None
            assert stored.created_at.tzinfo is not None
            assert stored.created_at.utcoffset() == timedelta(0)
            assert format_timestamp(stored.created_at).endswith("Z")
            assert len(format_timestamp(stored.created_at).split(".")[-1][:-1]) == 3
    finally:
        _cleanup(tenant.id)


def test_transaction_scope_commits_on_success_and_rolls_back_on_error():
    committed = _tenant(f"test-{uuid4().hex[:12]}")
    rolled_back = _tenant(f"test-{uuid4().hex[:12]}")
    with transaction_scope() as session:
        session.add(committed)

    with pytest.raises(RuntimeError, match="rollback probe"):
        with transaction_scope() as session:
            session.add(rolled_back)
            raise RuntimeError("rollback probe")

    try:
        with SessionLocal() as session:
            assert session.get(Tenant, committed.id) is not None
            assert session.get(Tenant, rolled_back.id) is None
    finally:
        _cleanup(committed.id)


@pytest.mark.parametrize(
    ("current_status", "incoming_status", "allowed"),
    [
        ("accepted", "in_progress", True),
        ("accepted", "done", False),
        ("accepted", "failed", False),
        ("in_progress", "in_progress", False),
        ("in_progress", "done", True),
        ("in_progress", "failed", True),
        ("done", "in_progress", False),
        ("done", "done", False),
        ("done", "failed", False),
        ("failed", "in_progress", False),
        ("failed", "done", False),
        ("failed", "failed", False),
    ],
)
def test_postgres_applies_only_permitted_task_transitions(current_status, incoming_status, allowed):
    from app.schemas import WorkerUpdate
    from app.services import apply_worker_update

    tenant_status = {
        "accepted": "provisioning",
        "in_progress": "provisioning",
        "done": "active",
        "failed": "failed",
    }[current_status]
    tenant = _tenant(f"test-{uuid4().hex[:12]}", status=tenant_status)
    now = utc_now()
    task = Task(
        id=uuid4(),
        type="deploy",
        tenant_id=tenant.id,
        status=current_status,
        payload={},
        created_at=now,
        updated_at=now,
    )
    with transaction_scope() as session:
        session.add_all([tenant, task])

    try:
        update = WorkerUpdate(
            update_id=f"{task.id}:{incoming_status}:{uuid4()}",
            task_id=task.id,
            status=incoming_status,
            occurred_at=now,
        )
        with SessionLocal() as session:
            applied = apply_worker_update(session, update)
        with SessionLocal() as session:
            saved_task = session.get(Task, task.id)
            saved_tenant = session.get(Tenant, tenant.id)
            assert saved_task is not None
            assert saved_tenant is not None
            assert applied is allowed
            assert saved_task.status == (incoming_status if allowed else current_status)
            if allowed and incoming_status == "done":
                assert saved_tenant.status == "active"
            elif allowed and incoming_status == "failed":
                assert saved_tenant.status == "failed"
            else:
                assert saved_tenant.status == tenant_status
    finally:
        _cleanup(tenant.id)
