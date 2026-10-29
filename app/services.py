import logging
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.errors import DomainError
from app.core.time import format_timestamp, utc_now
from app.domain import can_transition_task, can_transition_tenant, tenant_status_for_task
from app.models import OutboxEvent, ProcessedUpdate, Task, Tenant
from app.schemas import TaskEvent, TenantCreate, TenantPatch, WorkerUpdate

logger = logging.getLogger(__name__)


def _task_event(task: Task) -> dict[str, Any]:
    return TaskEvent(
        id=task.id,
        type=task.type,
        tenant_id=task.tenant_id,
        status=task.status,
        created_at=format_timestamp(task.created_at),
        payload=task.payload,
    ).model_dump(mode="json")


def _create_task(session: Session, tenant: Tenant, task_type: str, payload: dict[str, Any]) -> Task:
    now = utc_now()
    task = Task(
        id=uuid4(),
        type=task_type,
        tenant_id=tenant.id,
        status="accepted",
        payload=payload,
        created_at=now,
        updated_at=now,
    )
    session.add(task)
    session.flush()
    session.add(OutboxEvent(task_id=task.id, body=_task_event(task), created_at=now))
    return task


def create_tenant(session: Session, request: TenantCreate) -> tuple[Tenant, Task]:
    now = utc_now()
    tenant = Tenant(
        id=uuid4(),
        slug=request.slug,
        name=request.name,
        status="provisioning",
        version=1,
        created_at=now,
        updated_at=now,
    )
    try:
        with session.begin():
            session.add(tenant)
            session.flush()
            task = _create_task(session, tenant, "deploy", {})
    except IntegrityError as exc:
        constraint = getattr(getattr(exc.orig, "diag", None), "constraint_name", None)
        if constraint == "uq_tenants_slug":
            raise DomainError(
                "tenant_already_exists", f"Tenant slug '{request.slug}' already exists", 409
            ) from exc
        raise
    return tenant, task


def list_tenants(session: Session, limit: int, offset: int) -> list[Tenant]:
    return list(
        session.scalars(
            select(Tenant).order_by(Tenant.created_at, Tenant.id).limit(limit).offset(offset)
        )
    )


def get_tenant(session: Session, tenant_id: UUID) -> Tenant:
    tenant = session.get(Tenant, tenant_id)
    if tenant is None:
        raise DomainError("tenant_not_found", "Tenant was not found", 404)
    return tenant


def update_tenant(session: Session, tenant_id: UUID, request: TenantPatch) -> tuple[Tenant, Task]:
    now = utc_now()
    with session.begin():
        tenant = session.get(Tenant, tenant_id)
        if tenant is None:
            raise DomainError("tenant_not_found", "Tenant was not found", 404)
        if tenant.version != request.version:
            raise DomainError(
                "tenant_version_conflict",
                f"Expected version {request.version}, current version is {tenant.version}",
                409,
            )
        if not can_transition_tenant(tenant.status, "updating"):
            raise DomainError(
                "tenant_update_not_allowed",
                f"Tenant cannot be updated while status is '{tenant.status}'",
                409,
            )
        payload = {"name": request.name}
        result = session.execute(
            update(Tenant)
            .where(
                Tenant.id == tenant_id,
                Tenant.version == request.version,
                Tenant.status == "active",
            )
            .values(
                status="updating",
                name=request.name,
                version=Tenant.version + 1,
                updated_at=now,
            )
        )
        if result.rowcount != 1:
            raise DomainError(
                "tenant_version_conflict",
                "Tenant changed during this update; reload and retry",
                409,
            )
        task = _create_task(session, tenant, "update", payload)
        refreshed = session.get(Tenant, tenant_id)
        if refreshed is None:
            raise RuntimeError("Tenant disappeared within its update transaction")
    return refreshed, task


def delete_tenant(session: Session, tenant_id: UUID) -> tuple[Tenant, Task]:
    now = utc_now()
    with session.begin():
        tenant = session.get(Tenant, tenant_id)
        if tenant is None:
            raise DomainError("tenant_not_found", "Tenant was not found", 404)
        if not can_transition_tenant(tenant.status, "destroying"):
            raise DomainError(
                "tenant_update_not_allowed",
                f"Tenant cannot be deleted while status is '{tenant.status}'",
                409,
            )
        result = session.execute(
            update(Tenant)
            .where(
                Tenant.id == tenant_id,
                Tenant.version == tenant.version,
                Tenant.status == tenant.status,
            )
            .values(status="destroying", version=Tenant.version + 1, updated_at=now)
        )
        if result.rowcount != 1:
            raise DomainError(
                "tenant_version_conflict",
                "Tenant changed during this delete; reload and retry",
                409,
            )
        task = _create_task(session, tenant, "destroy", {})
        refreshed = session.get(Tenant, tenant_id)
        if refreshed is None:
            raise RuntimeError("Tenant disappeared within its delete transaction")
    return refreshed, task


def list_tasks(session: Session, limit: int, offset: int) -> list[Task]:
    return list(
        session.scalars(select(Task).order_by(Task.created_at, Task.id).limit(limit).offset(offset))
    )


def get_task(session: Session, task_id: UUID) -> Task:
    task = session.get(Task, task_id)
    if task is None:
        raise DomainError("task_not_found", "Task was not found", 404)
    return task


def apply_worker_update(session: Session, update_message: WorkerUpdate) -> bool:
    with session.begin():
        task = session.scalar(
            select(Task).where(Task.id == update_message.task_id).with_for_update()
        )
        if task is None:
            raise ValueError(f"Worker update refers to unknown task {update_message.task_id}")
        if session.get(ProcessedUpdate, update_message.update_id) is not None:
            return False
        session.add(
            ProcessedUpdate(
                update_id=update_message.update_id, task_id=task.id, processed_at=utc_now()
            )
        )
        if not can_transition_task(task.status, update_message.status):
            logger.info(
                "Ignoring stale task update id=%s task_id=%s current=%s incoming=%s",
                update_message.update_id,
                task.id,
                task.status,
                update_message.status,
            )
            return False
        now = utc_now()
        task.status = update_message.status
        task.updated_at = now
        if update_message.status in {"done", "failed"}:
            tenant = session.scalar(
                select(Tenant).where(Tenant.id == task.tenant_id).with_for_update()
            )
            if tenant is None:
                raise RuntimeError(f"Task {task.id} references a missing tenant")
            target_status = tenant_status_for_task(task.type, update_message.status)
            if not can_transition_tenant(tenant.status, target_status):
                raise RuntimeError(
                    f"Cannot move tenant {tenant.id} from {tenant.status} to {target_status}"
                )
            tenant.status = target_status
            tenant.version += 1
            tenant.updated_at = now
    return True
