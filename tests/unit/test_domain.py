from itertools import product

import pytest

from app.domain import can_transition_task, can_transition_tenant, tenant_status_for_task


@pytest.mark.parametrize(
    ("current", "target"),
    list(product(("accepted", "in_progress", "done", "failed"), ("in_progress", "done", "failed"))),
)
def test_task_transition_table_allows_only_declared_edges(current, target):
    allowed = {
        ("accepted", "in_progress"),
        ("in_progress", "done"),
        ("in_progress", "failed"),
    }

    assert can_transition_task(current, target) is ((current, target) in allowed)


@pytest.mark.parametrize(
    ("current", "target"),
    list(
        product(
            ("provisioning", "active", "updating", "destroying", "destroyed", "failed"),
            ("provisioning", "active", "updating", "destroying", "destroyed", "failed"),
        )
    ),
)
def test_tenant_transition_table_covers_every_state_pair(current, target):
    allowed = {
        ("provisioning", "active"),
        ("provisioning", "failed"),
        ("active", "updating"),
        ("active", "destroying"),
        ("updating", "active"),
        ("updating", "failed"),
        ("destroying", "destroyed"),
        ("destroying", "failed"),
        ("failed", "destroying"),
    }

    assert can_transition_tenant(current, target) is ((current, target) in allowed)


def test_terminal_task_outcome_maps_to_tenant_state():
    assert tenant_status_for_task("deploy", "done") == "active"
    assert tenant_status_for_task("update", "done") == "active"
    assert tenant_status_for_task("destroy", "done") == "destroyed"
    for task_type in ("deploy", "update", "destroy"):
        assert tenant_status_for_task(task_type, "failed") == "failed"
