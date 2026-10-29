from typing import Literal

TENANT_SLUG_REGEX = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"
TENANT_STATUSES = frozenset(
    {"provisioning", "active", "updating", "destroying", "destroyed", "failed"}
)
TASK_STATUSES = frozenset({"accepted", "in_progress", "done", "failed"})
TASK_TYPES = frozenset({"deploy", "update", "destroy"})

TaskStatus = Literal["accepted", "in_progress", "done", "failed"]

TENANT_TRANSITIONS: dict[str, set[str]] = {
    "provisioning": {"active", "failed"},
    "active": {"updating", "destroying"},
    "updating": {"active", "failed"},
    "destroying": {"destroyed", "failed"},
    "failed": {"destroying"},
    "destroyed": set(),
}

TASK_TRANSITIONS: dict[str, set[str]] = {
    "accepted": {"in_progress"},
    "in_progress": {"done", "failed"},
    "done": set(),
    "failed": set(),
}


def can_transition_task(current: str, target: TaskStatus) -> bool:
    return target in TASK_TRANSITIONS.get(current, set())


def can_transition_tenant(current: str, target: str) -> bool:
    return target in TENANT_TRANSITIONS.get(current, set())


def tenant_status_for_task(task_type: str, outcome: str) -> str:
    if outcome == "failed":
        return "failed"
    return {"deploy": "active", "update": "active", "destroy": "destroyed"}[task_type]
