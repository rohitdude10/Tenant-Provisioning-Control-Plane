import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from app.core.errors import DomainError
from app.db import SessionLocal
from app.models import OutboxEvent, ProcessedUpdate, Task, Tenant
from app.schemas import TenantCreate, TenantPatch, WorkerUpdate
from app.services import apply_worker_update, create_tenant, update_tenant

pytestmark = pytest.mark.integration
if not os.getenv("TEST_DATABASE_URL"):
    pytestmark = pytest.mark.skip(
        reason="Set TEST_DATABASE_URL to run PostgreSQL integration tests"
    )
else:
    database_name = os.getenv("TEST_DATABASE_URL", "").rsplit("/", 1)[-1].split("?", 1)[0]
    if not database_name.endswith("_test"):
        raise RuntimeError(
            "TEST_DATABASE_URL database name must end with '_test' to protect local data"
        )


def _cleanup(tenant_id):
    with SessionLocal.begin() as session:
        task_ids = list(session.scalars(select(Task.id).where(Task.tenant_id == tenant_id)))
        session.execute(delete(ProcessedUpdate).where(ProcessedUpdate.task_id.in_(task_ids)))
        session.execute(delete(OutboxEvent).where(OutboxEvent.task_id.in_(task_ids)))
        session.execute(delete(Task).where(Task.tenant_id == tenant_id))
        session.execute(delete(Tenant).where(Tenant.id == tenant_id))


def test_same_version_patch_has_only_one_winner():
    slug = f"test-{uuid4().hex[:12]}"
    with SessionLocal() as session:
        tenant, deploy = create_tenant(session, TenantCreate(slug=slug, name="Concurrency"))
        apply_worker_update(
            session,
            WorkerUpdate(
                update_id=f"{deploy.id}:in_progress",
                task_id=deploy.id,
                status="in_progress",
                occurred_at=deploy.created_at,
            ),
        )
        apply_worker_update(
            session,
            WorkerUpdate(
                update_id=f"{deploy.id}:done",
                task_id=deploy.id,
                status="done",
                occurred_at=deploy.created_at,
            ),
        )
        tenant_id = tenant.id
        expected_version = tenant.version

    barrier = Barrier(2)

    def patch(name: str):
        barrier.wait()
        with SessionLocal() as session:
            try:
                update_tenant(session, tenant_id, TenantPatch(version=expected_version, name=name))
                return "success"
            except DomainError as exc:
                return exc.code

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(patch, ("Winner A", "Winner B")))
        assert results.count("success") == 1
        assert results.count("tenant_version_conflict") == 1
    finally:
        _cleanup(tenant_id)


def test_concurrent_duplicate_slug_creates_have_one_winner():
    slug = f"test-{uuid4().hex[:12]}"
    barrier = Barrier(2)

    def create(name: str):
        barrier.wait()
        with SessionLocal() as session:
            try:
                tenant, _ = create_tenant(session, TenantCreate(slug=slug, name=name))
                return "success", tenant.id
            except DomainError as exc:
                return exc.code, None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(create, ("Duplicate A", "Duplicate B")))
    winners = [tenant_id for result, tenant_id in results if result == "success"]
    assert len(winners) == 1
    assert [result for result, _ in results].count("tenant_already_exists") == 1
    _cleanup(winners[0])


def test_worker_updates_are_idempotent_and_stale_progress_cannot_regress():
    slug = f"test-{uuid4().hex[:12]}"
    with SessionLocal() as session:
        tenant, deploy = create_tenant(session, TenantCreate(slug=slug, name="Lifecycle"))
        tenant_id = tenant.id
        in_progress = WorkerUpdate(
            update_id=f"{deploy.id}:in_progress",
            task_id=deploy.id,
            status="in_progress",
            occurred_at=deploy.created_at,
        )
        done = WorkerUpdate(
            update_id=f"{deploy.id}:done",
            task_id=deploy.id,
            status="done",
            occurred_at=deploy.created_at,
        )
        assert apply_worker_update(session, in_progress)
        assert apply_worker_update(session, done)
        assert not apply_worker_update(session, done)
        assert not apply_worker_update(
            session,
            WorkerUpdate(
                update_id=f"{deploy.id}:late-progress",
                task_id=deploy.id,
                status="in_progress",
                occurred_at=deploy.created_at,
            ),
        )

    try:
        with SessionLocal() as session:
            saved_task = session.get(Task, deploy.id)
            saved_tenant = session.get(Tenant, tenant_id)
            assert saved_task is not None and saved_task.status == "done"
            assert saved_tenant is not None and saved_tenant.status == "active"
            assert saved_tenant.version == 2
    finally:
        _cleanup(tenant_id)
