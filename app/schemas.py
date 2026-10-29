import re
from datetime import datetime
from typing import Any, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from app.core.time import format_timestamp
from app.domain import TENANT_SLUG_REGEX

SLUG_PATTERN = re.compile(TENANT_SLUG_REGEX)


class TenantCreate(BaseModel):
    slug: str = Field(min_length=3, max_length=28)
    name: str = Field(min_length=1, max_length=255)

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        if SLUG_PATTERN.fullmatch(value) is None:
            raise ValueError("slug must match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")
        return value

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name must not be empty")
        return value


class TenantPatch(BaseModel):
    version: int = Field(ge=1)
    name: str | None = Field(default=None, min_length=1, max_length=255)

    model_config = ConfigDict(extra="forbid")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is not None:
            value = value.strip()
            if not value:
                raise ValueError("name must not be empty")
        return value

    @model_validator(mode="after")
    def require_mutation(self) -> "TenantPatch":
        if self.name is None:
            raise ValueError("at least one mutable field is required")
        return self


class TenantResponse(BaseModel):
    id: UUID
    slug: str
    name: str
    status: str
    version: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

    @field_serializer("created_at", "updated_at")
    def serialize_timestamp(self, value: datetime) -> str:
        return format_timestamp(value)


class TaskResponse(BaseModel):
    id: UUID
    type: Literal["deploy", "update", "destroy"]
    tenant_id: UUID
    status: Literal["accepted", "in_progress", "done", "failed"]
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

    @field_serializer("created_at", "updated_at")
    def serialize_timestamp(self, value: datetime) -> str:
        return format_timestamp(value)


ItemT = TypeVar("ItemT")


class Page(BaseModel, Generic[ItemT]):
    items: list[ItemT]
    limit: int
    offset: int


class TaskEvent(BaseModel):
    id: UUID
    type: Literal["deploy", "update", "destroy"]
    tenant_id: UUID
    status: Literal["accepted", "in_progress", "done", "failed"]
    created_at: str
    payload: dict[str, Any]


class WorkerUpdate(BaseModel):
    update_id: str = Field(min_length=1, max_length=200)
    task_id: UUID
    status: Literal["in_progress", "done", "failed"]
    occurred_at: datetime
