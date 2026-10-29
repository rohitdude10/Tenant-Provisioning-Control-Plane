import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.core.time import utc_now
from app.db import SessionLocal, transaction_scope
from app.models import OutboxEvent, ProcessedUpdate, Task, Tenant
from app.schemas import WorkerUpdate
from app.services import apply_worker_update

pytestmark = pytest.mark.integration
if not os.getenv("TEST_DATABASE_URL"):
    pytestmark = pytest.mark.skip(
        reason="Set TEST_DATABASE_URL to run HTTP/PostgreSQL integration tests"
    )
else:
    database_name = os.getenv("TEST_DATABASE_URL", "").rsplit("/", 1)[-1].split("?", 1)[0]
    if not database_name.endswith("_test"):
        raise RuntimeError(
            "TEST_DATABASE_URL database name must end with '_test' to protect local data"
        )


@pytest.fixture
def api_client():
    from app.main import app

    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest.fixture
def tenant_ids():
    created: list[UUID] = []
    yield created
    with transaction_scope() as session:
        task_ids = list(session.scalars(select(Task.id).where(Task.tenant_id.in_(created))))
        if task_ids:
            session.execute(delete(ProcessedUpdate).where(ProcessedUpdate.task_id.in_(task_ids)))
            session.execute(delete(OutboxEvent).where(OutboxEvent.task_id.in_(task_ids)))
            session.execute(delete(Task).where(Task.id.in_(task_ids)))
        if created:
            session.execute(delete(Tenant).where(Tenant.id.in_(created)))


def _create_tenant(api_client, tenant_ids: list[UUID], *, name: str = "API test") -> dict:
    response = api_client.post(
        "/tenants",
        json={"slug": f"test-{uuid4().hex[:12]}", "name": name},
    )
    assert response.status_code == 202, response.text
    tenant = response.json()
    tenant_ids.append(UUID(tenant["id"]))
    return tenant


def _complete_task(task_id: UUID, status: str = "done") -> None:
    now = utc_now()
    with SessionLocal() as session:
        assert apply_worker_update(
            session,
            WorkerUpdate(
                update_id=f"{task_id}:in_progress",
                task_id=task_id,
                status="in_progress",
                occurred_at=now,
            ),
        )
    with SessionLocal() as session:
        assert apply_worker_update(
            session,
            WorkerUpdate(
                update_id=f"{task_id}:{status}",
                task_id=task_id,
                status=status,
                occurred_at=now,
            ),
        )


def _task_for_tenant(tenant_id: UUID, task_type: str) -> Task:
    with SessionLocal() as session:
        task = session.scalar(
            select(Task)
            .where(Task.tenant_id == tenant_id, Task.type == task_type)
            .order_by(Task.created_at.desc())
        )
        assert task is not None
        return task


def _make_active(api_client, tenant_ids: list[UUID], name: str = "API test") -> dict:
    tenant = _create_tenant(api_client, tenant_ids, name=name)
    deploy_task = _task_for_tenant(UUID(tenant["id"]), "deploy")
    _complete_task(deploy_task.id)
    response = api_client.get(f"/tenants/{tenant['id']}")
    assert response.status_code == 200
    return response.json()


def _outbox_for_task(task_id: UUID) -> OutboxEvent | None:
    with SessionLocal() as session:
        return session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task_id))


def test_post_tenant_creates_provisioning_tenant_accepted_task_and_outbox_atomically(
    api_client, tenant_ids
):
    tenant = _create_tenant(api_client, tenant_ids, name="New Tenant")
    assert tenant["status"] == "provisioning"
    assert tenant["version"] == 1
    assert tenant["name"] == "New Tenant"
    assert tenant["created_at"].endswith("Z")
    assert len(tenant["created_at"].rsplit(".", 1)[-1][:-1]) == 3

    task = _task_for_tenant(UUID(tenant["id"]), "deploy")
    event = _outbox_for_task(task.id)
    assert task.status == "accepted"
    assert event is not None
    assert event.body["id"] == str(task.id)
    assert event.body["type"] == "deploy"
    assert event.body["tenant_id"] == tenant["id"]
    assert event.body["status"] == "accepted"


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"slug": "bad_slug", "name": "Invalid"},
        {"slug": "valid-slug", "name": "   "},
        {"slug": "valid-slug", "name": "Valid", "extra": "forbidden"},
    ],
)
def test_post_tenant_rejects_invalid_request_body_with_structured_error(api_client, body):
    response = api_client.post("/tenants", json=body)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["error"]["details"]


def test_post_tenant_translates_duplicate_slug_constraint_including_destroyed(
    api_client, tenant_ids
):
    slug = f"test-{uuid4().hex[:12]}"
    destroyed = Tenant(
        id=uuid4(),
        slug=slug,
        name="Previously destroyed",
        status="destroyed",
        version=3,
        created_at=utc_now(),
        updated_at=utc_now(),
    )
    with transaction_scope() as session:
        session.add(destroyed)
    tenant_ids.append(destroyed.id)

    response = api_client.post("/tenants", json={"slug": slug, "name": "Duplicate"})

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "tenant_already_exists"


def test_get_tenant_list_detail_and_not_found(api_client, tenant_ids):
    tenant = _create_tenant(api_client, tenant_ids)

    detail = api_client.get(f"/tenants/{tenant['id']}")
    listing = api_client.get("/tenants?limit=1&offset=0")
    missing = api_client.get(f"/tenants/{uuid4()}")

    assert detail.status_code == 200
    assert detail.json()["id"] == tenant["id"]
    assert listing.status_code == 200
    assert len(listing.json()["items"]) == 1
    assert listing.json()["limit"] == 1
    assert listing.json()["offset"] == 0
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "tenant_not_found"


def test_patch_requires_expected_version_and_only_updates_active_tenants(api_client, tenant_ids):
    provisioning = _create_tenant(api_client, tenant_ids)
    no_version = api_client.patch(
        f"/tenants/{provisioning['id']}",
        json={"name": "Changed"},
    )
    not_active = api_client.patch(
        f"/tenants/{provisioning['id']}",
        json={"version": 1, "name": "Changed"},
    )
    assert no_version.status_code == 422
    assert no_version.json()["error"]["code"] == "validation_error"
    assert not_active.status_code == 409
    assert not_active.json()["error"]["code"] == "tenant_update_not_allowed"


def test_patch_updates_name_version_status_and_creates_update_task_and_outbox(
    api_client, tenant_ids
):
    active = _make_active(api_client, tenant_ids, name="Original")

    response = api_client.patch(
        f"/tenants/{active['id']}",
        json={"version": active["version"], "name": "Updated"},
    )

    assert response.status_code == 202
    updated = response.json()
    assert updated["status"] == "updating"
    assert updated["version"] == active["version"] + 1
    assert updated["name"] == "Updated"
    task = _task_for_tenant(UUID(active["id"]), "update")
    event = _outbox_for_task(task.id)
    assert task.status == "accepted"
    assert task.payload == {"name": "Updated"}
    assert event is not None and event.body["type"] == "update"


def test_patch_stale_version_returns_version_conflict(api_client, tenant_ids):
    active = _make_active(api_client, tenant_ids)
    first = api_client.patch(
        f"/tenants/{active['id']}",
        json={"version": active["version"], "name": "First"},
    )
    stale = api_client.patch(
        f"/tenants/{active['id']}",
        json={"version": active["version"], "name": "Stale"},
    )

    assert first.status_code == 202
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "tenant_version_conflict"


def test_concurrent_patch_requests_with_same_version_create_exactly_one_task(
    api_client, tenant_ids
):
    active = _make_active(api_client, tenant_ids)
    barrier = Barrier(2)

    def patch(name: str):
        barrier.wait()
        return api_client.patch(
            f"/tenants/{active['id']}",
            json={"version": active["version"], "name": name},
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(patch, ("Concurrent A", "Concurrent B")))

    assert sorted(response.status_code for response in responses) == [202, 409]
    conflict = next(response for response in responses if response.status_code == 409)
    assert conflict.json()["error"]["code"] == "tenant_version_conflict"
    with SessionLocal() as session:
        update_tasks = list(
            session.scalars(
                select(Task).where(Task.tenant_id == UUID(active["id"]), Task.type == "update")
            )
        )
        update_outbox = list(
            session.scalars(
                select(OutboxEvent)
                .join(Task, OutboxEvent.task_id == Task.id)
                .where(Task.tenant_id == UUID(active["id"]), Task.type == "update")
            )
        )
    assert len(update_tasks) == 1
    assert len(update_outbox) == 1


@pytest.mark.parametrize("initial_status", ["active", "failed"])
def test_delete_is_soft_and_creates_destroy_task_and_outbox(api_client, tenant_ids, initial_status):
    tenant = _make_active(api_client, tenant_ids)
    if initial_status == "failed":
        deploy = _task_for_tenant(UUID(tenant["id"]), "deploy")
        # A completed deploy is terminal; create a failed tenant state through a failed update.
        patch = api_client.patch(
            f"/tenants/{tenant['id']}",
            json={"version": tenant["version"], "name": "Before failure"},
        )
        assert patch.status_code == 202
        update_task = _task_for_tenant(UUID(tenant["id"]), "update")
        _complete_task(update_task.id, "failed")
        tenant = api_client.get(f"/tenants/{tenant['id']}").json()
        assert tenant["status"] == "failed"
        assert deploy.status == "done"

    response = api_client.delete(f"/tenants/{tenant['id']}")
    assert response.status_code == 202
    assert response.json()["status"] == "destroying"

    destroy_task = _task_for_tenant(UUID(tenant["id"]), "destroy")
    assert destroy_task.status == "accepted"
    assert _outbox_for_task(destroy_task.id) is not None
    assert api_client.get(f"/tenants/{tenant['id']}").status_code == 200
    assert api_client.delete(f"/tenants/{tenant['id']}").json()["error"]["code"] == (
        "tenant_update_not_allowed"
    )


def test_delete_rejects_provisioning_and_missing_tenants(api_client, tenant_ids):
    tenant = _create_tenant(api_client, tenant_ids)
    provisioning = api_client.delete(f"/tenants/{tenant['id']}")
    missing = api_client.delete(f"/tenants/{uuid4()}")

    assert provisioning.status_code == 409
    assert provisioning.json()["error"]["code"] == "tenant_update_not_allowed"
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "tenant_not_found"


def test_task_list_detail_not_found_and_read_only_methods(api_client, tenant_ids):
    tenant = _create_tenant(api_client, tenant_ids)
    task = _task_for_tenant(UUID(tenant["id"]), "deploy")

    listing = api_client.get("/tasks?limit=10&offset=0")
    detail = api_client.get(f"/tasks/{task.id}")
    missing = api_client.get(f"/tasks/{uuid4()}")
    not_read_only = api_client.patch(f"/tasks/{task.id}", json={"status": "done"})

    assert listing.status_code == 200
    assert any(item["id"] == str(task.id) for item in listing.json()["items"])
    assert detail.status_code == 200
    assert detail.json()["status"] == "accepted"
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "task_not_found"
    assert not_read_only.status_code == 405
    assert not_read_only.json()["error"]["code"] == "http_error"


@pytest.mark.parametrize("operation", ["create", "patch"])
def test_task_or_outbox_failure_rolls_back_tenant_mutation(
    api_client, tenant_ids, monkeypatch, operation
):
    import app.services as services

    if operation == "patch":
        active = _make_active(api_client, tenant_ids, name="Before")
        tenant_id = UUID(active["id"])
        expected_version = active["version"]
    else:
        tenant_id = None
        expected_version = None
        slug = f"test-{uuid4().hex[:12]}"

    def fail_task_creation(*args, **kwargs):
        raise RuntimeError("injected task creation failure")

    monkeypatch.setattr(services, "_create_task", fail_task_creation)
    if operation == "create":
        response = api_client.post(
            "/tenants",
            json={"slug": slug, "name": "Rollback"},
        )
        assert response.status_code == 500
        with SessionLocal() as session:
            assert session.scalar(select(Tenant.id).where(Tenant.slug == slug)) is None
    else:
        response = api_client.patch(
            f"/tenants/{tenant_id}",
            json={"version": expected_version, "name": "Must rollback"},
        )
        assert response.status_code == 500
        with SessionLocal() as session:
            saved = session.get(Tenant, tenant_id)
            assert saved is not None
            assert saved.status == "active"
            assert saved.name == "Before"
            assert saved.version == expected_version
            assert (
                session.scalar(
                    select(Task.id).where(Task.tenant_id == tenant_id, Task.type == "update")
                )
                is None
            )
