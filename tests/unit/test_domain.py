from app.domain import can_transition_task, can_transition_tenant, tenant_status_for_task


def test_task_transition_table_allows_forward_progress_only():
    assert can_transition_task("accepted", "in_progress")
    assert can_transition_task("in_progress", "done")
    assert can_transition_task("in_progress", "failed")
    assert not can_transition_task("done", "in_progress")
    assert not can_transition_task("failed", "done")


def test_terminal_task_outcome_maps_to_tenant_state():
    assert tenant_status_for_task("deploy", "done") == "active"
    assert tenant_status_for_task("update", "done") == "active"
    assert tenant_status_for_task("destroy", "done") == "destroyed"
    for task_type in ("deploy", "update", "destroy"):
        assert tenant_status_for_task(task_type, "failed") == "failed"


def test_tenant_transition_guards_match_the_state_machine():
    assert can_transition_tenant("active", "updating")
    assert can_transition_tenant("active", "destroying")
    assert can_transition_tenant("failed", "destroying")
    assert can_transition_tenant("provisioning", "active")
    assert can_transition_tenant("updating", "failed")
    assert not can_transition_tenant("provisioning", "destroying")
    assert not can_transition_tenant("destroyed", "active")
