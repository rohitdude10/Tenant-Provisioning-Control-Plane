from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.api.dependencies import get_session
from app.schemas import Page, TaskResponse, TenantCreate, TenantPatch, TenantResponse
from app.services import (
    create_tenant,
    delete_tenant,
    get_task,
    get_tenant,
    list_tasks,
    list_tenants,
    update_tenant,
)

router = APIRouter()
SessionDep = Annotated[Session, Depends(get_session)]


@router.post("/tenants", response_model=TenantResponse, status_code=status.HTTP_202_ACCEPTED)
def post_tenant(request: TenantCreate, session: SessionDep) -> TenantResponse:
    tenant, _ = create_tenant(session, request)
    return TenantResponse.model_validate(tenant)


@router.get("/tenants", response_model=Page[TenantResponse])
def get_tenants(
    session: SessionDep,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> Page[TenantResponse]:
    return Page[TenantResponse](
        items=[
            TenantResponse.model_validate(item) for item in list_tenants(session, limit, offset)
        ],
        limit=limit,
        offset=offset,
    )


@router.get("/tenants/{tenant_id}", response_model=TenantResponse)
def get_tenant_by_id(tenant_id: UUID, session: SessionDep) -> TenantResponse:
    return TenantResponse.model_validate(get_tenant(session, tenant_id))


@router.patch(
    "/tenants/{tenant_id}", response_model=TenantResponse, status_code=status.HTTP_202_ACCEPTED
)
def patch_tenant(tenant_id: UUID, request: TenantPatch, session: SessionDep) -> TenantResponse:
    tenant, _ = update_tenant(session, tenant_id, request)
    return TenantResponse.model_validate(tenant)


@router.delete(
    "/tenants/{tenant_id}", response_model=TenantResponse, status_code=status.HTTP_202_ACCEPTED
)
def remove_tenant(tenant_id: UUID, session: SessionDep) -> TenantResponse:
    tenant, _ = delete_tenant(session, tenant_id)
    return TenantResponse.model_validate(tenant)


@router.get("/tasks", response_model=Page[TaskResponse])
def get_tasks(
    session: SessionDep,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> Page[TaskResponse]:
    return Page[TaskResponse](
        items=[TaskResponse.model_validate(item) for item in list_tasks(session, limit, offset)],
        limit=limit,
        offset=offset,
    )


@router.get("/tasks/{task_id}", response_model=TaskResponse)
def get_task_by_id(task_id: UUID, session: SessionDep) -> TaskResponse:
    return TaskResponse.model_validate(get_task(session, task_id))


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
