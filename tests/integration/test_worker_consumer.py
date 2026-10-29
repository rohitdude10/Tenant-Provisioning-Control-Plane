import json
import os
from types import SimpleNamespace
from uuid import UUID, uuid4

import pika
import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError

from app.core.time import utc_now
from app.db import SessionLocal, transaction_scope
from app.messaging.rabbit import (
    UPDATE_RETRY_ATTEMPT_HEADER,
    inbound_retry_delays,
    inbound_retry_queue_name,
)
from app.models import OutboxEvent, ProcessedUpdate, Task, Tenant
from app.processes import inbound_consumer, worker_simulator
from app.schemas import TenantCreate, TenantPatch, WorkerUpdate
from app.services import (
    apply_worker_update,
    create_tenant,
    delete_tenant,
    update_tenant,
)

pytestmark = pytest.mark.integration
if not os.getenv("TEST_DATABASE_URL"):
    pytestmark = pytest.mark.skip(
        reason="Set TEST_DATABASE_URL to run worker/consumer integration tests"
    )
else:
    database_name = os.getenv("TEST_DATABASE_URL", "").rsplit("/", 1)[-1].split("?", 1)[0]
    if not database_name.endswith("_test"):
        raise RuntimeError(
            "TEST_DATABASE_URL database name must end with '_test' to protect local data"
        )


class FakeChannel:
    def __init__(self) -> None:
        self.acks: list[int] = []
        self.nacks: list[tuple[int, bool]] = []

    def basic_ack(self, *, delivery_tag: int) -> None:
        self.acks.append(delivery_tag)

    def basic_nack(self, *, delivery_tag: int, requeue: bool) -> None:
        self.nacks.append((delivery_tag, requeue))


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


def _create_tenant(
    tenant_ids: list[UUID], name: str = "Messaging integration"
) -> tuple[UUID, UUID]:
    with SessionLocal() as session:
        tenant, task = create_tenant(
            session,
            TenantCreate(slug=f"test-{uuid4().hex[:12]}", name=name),
        )
        tenant_ids.append(tenant.id)
        return tenant.id, task.id


def _apply_status(task_id: UUID, status: str) -> None:
    with SessionLocal() as session:
        assert apply_worker_update(
            session,
            WorkerUpdate(
                update_id=f"setup:{task_id}:{status}:{uuid4()}",
                task_id=task_id,
                status=status,
                occurred_at=utc_now(),
            ),
        )


def _prepare_task(task_type: str, tenant_ids: list[UUID]) -> tuple[UUID, dict]:
    tenant_id, deploy_id = _create_tenant(tenant_ids)
    if task_type == "deploy":
        task_id = deploy_id
    else:
        _apply_status(deploy_id, "in_progress")
        _apply_status(deploy_id, "done")
        with SessionLocal() as session:
            tenant = session.get(Tenant, tenant_id)
            assert tenant is not None
            expected_version = tenant.version
        with SessionLocal() as session:
            if task_type == "update":
                _, task = update_tenant(
                    session,
                    tenant_id,
                    TenantPatch(version=expected_version, name="Updated integration tenant"),
                )
            else:
                _, task = delete_tenant(session, tenant_id)
        task_id = task.id

    with SessionLocal() as session:
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task_id))
        assert event is not None
        return tenant_id, event.body


def _run_worker(
    monkeypatch, task_event: dict, *, fail_rate: float = 0.0
) -> tuple[list[dict], FakeChannel]:
    updates: list[dict] = []
    monkeypatch.setattr(
        worker_simulator,
        "publish_json",
        lambda channel, queue, body, **kwargs: updates.append(body),
    )
    monkeypatch.setattr(worker_simulator.time, "sleep", lambda _: None)
    channel = FakeChannel()
    worker_simulator.handle_delivery(
        channel,
        SimpleNamespace(delivery_tag=1),
        SimpleNamespace(),
        json.dumps(task_event).encode(),
        SimpleNamespace(fail_rate=fail_rate, min_delay_ms=0, max_delay_ms=0),
    )
    return updates, channel


def _consume_updates(updates: list[dict]) -> FakeChannel:
    channel = FakeChannel()
    for delivery_tag, body in enumerate(updates, start=1):
        inbound_consumer.handle_delivery(
            channel,
            SimpleNamespace(delivery_tag=delivery_tag),
            SimpleNamespace(headers={}, message_id=body["update_id"]),
            json.dumps(body).encode(),
        )
    return channel


def _saved_state(task_id: UUID, tenant_id: UUID) -> tuple[str, str, int]:
    with SessionLocal() as session:
        task = session.get(Task, task_id)
        tenant = session.get(Tenant, tenant_id)
        assert task is not None and tenant is not None
        return task.status, tenant.status, tenant.version


@pytest.mark.parametrize(
    ("task_type", "expected_tenant_status", "expected_tenant_version"),
    [
        ("deploy", "active", 2),
        ("update", "active", 4),
        ("destroy", "destroyed", 4),
    ],
)
def test_worker_and_consumer_complete_each_task_type(
    monkeypatch, tenant_ids, task_type, expected_tenant_status, expected_tenant_version
):
    tenant_id, task_event = _prepare_task(task_type, tenant_ids)

    updates, worker_channel = _run_worker(monkeypatch, task_event)
    consumer_channel = _consume_updates(updates)

    assert [update["status"] for update in updates] == ["in_progress", "done"]
    assert len({update["update_id"] for update in updates}) == 2
    assert worker_channel.acks == [1]
    assert worker_channel.nacks == []
    assert consumer_channel.acks == [1, 2]
    assert _saved_state(UUID(task_event["id"]), tenant_id) == (
        "done",
        expected_tenant_status,
        expected_tenant_version,
    )


@pytest.mark.parametrize("task_type", ["deploy", "update", "destroy"])
def test_failed_task_outcome_moves_tenant_to_failed(monkeypatch, tenant_ids, task_type):
    tenant_id, task_event = _prepare_task(task_type, tenant_ids)

    updates, _ = _run_worker(monkeypatch, task_event, fail_rate=1.0)
    _consume_updates(updates)

    assert [update["status"] for update in updates] == ["in_progress", "failed"]
    task_status, tenant_status, tenant_version = _saved_state(UUID(task_event["id"]), tenant_id)
    assert (task_status, tenant_status) == ("failed", "failed")
    assert tenant_version == (2 if task_type == "deploy" else 4)


def test_worker_retries_task_with_stable_update_ids_after_publish_failure(monkeypatch, tenant_ids):
    _, task_id = _create_tenant(tenant_ids)
    with SessionLocal() as session:
        task_event = session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task_id)).body

    attempted: list[dict] = []
    successful: list[dict] = []

    def publish(channel, queue, body, **kwargs):
        attempted.append(body)
        if len(attempted) == 1:
            raise pika.exceptions.AMQPConnectionError("broker unavailable")
        successful.append(body)

    monkeypatch.setattr(worker_simulator, "publish_json", publish)
    monkeypatch.setattr(worker_simulator.time, "sleep", lambda _: None)
    delivery = SimpleNamespace(delivery_tag=9)
    args = SimpleNamespace(fail_rate=0.0, min_delay_ms=0, max_delay_ms=0)

    first_channel = FakeChannel()
    worker_simulator.handle_delivery(
        first_channel, delivery, SimpleNamespace(), json.dumps(task_event).encode(), args
    )
    second_channel = FakeChannel()
    worker_simulator.handle_delivery(
        second_channel, delivery, SimpleNamespace(), json.dumps(task_event).encode(), args
    )

    assert first_channel.nacks == [(9, True)]
    assert first_channel.acks == []
    assert second_channel.acks == [9]
    assert successful[0]["update_id"] == attempted[0]["update_id"]
    assert [update["status"] for update in successful] == ["in_progress", "done"]
    assert successful[0]["update_id"] != successful[1]["update_id"]


def test_malformed_task_is_dead_lettered_and_worker_continues(monkeypatch):
    channel = FakeChannel()
    worker_simulator.handle_delivery(
        channel,
        SimpleNamespace(delivery_tag=1),
        SimpleNamespace(),
        b'{"id":"invalid"}',
        SimpleNamespace(fail_rate=0, min_delay_ms=0, max_delay_ms=0),
    )
    assert channel.nacks == [(1, False)]


def test_duplicate_updates_and_restart_do_not_change_state_twice(monkeypatch, tenant_ids):
    tenant_id, task_event = _prepare_task("deploy", tenant_ids)
    updates, _ = _run_worker(monkeypatch, task_event)
    for update in updates:
        body = json.dumps(update).encode()
        inbound_consumer.handle_delivery(
            FakeChannel(),
            SimpleNamespace(delivery_tag=1),
            SimpleNamespace(headers={}, message_id=update["update_id"]),
            body,
        )

    before = _saved_state(UUID(task_event["id"]), tenant_id)
    restarted_consumer_channel = FakeChannel()
    terminal_update = updates[-1]
    inbound_consumer.handle_delivery(
        restarted_consumer_channel,
        SimpleNamespace(delivery_tag=2),
        SimpleNamespace(headers={}, message_id=terminal_update["update_id"]),
        json.dumps(terminal_update).encode(),
    )

    assert restarted_consumer_channel.acks == [2]
    assert restarted_consumer_channel.nacks == []
    assert _saved_state(UUID(task_event["id"]), tenant_id) == before


def test_out_of_order_stale_progress_is_ignored_and_early_terminal_is_dead_lettered(
    tenant_ids,
):
    tenant_id, task_id = _create_tenant(tenant_ids)
    early_terminal = WorkerUpdate(
        update_id=f"{task_id}:done",
        task_id=task_id,
        status="done",
        occurred_at=utc_now(),
    ).model_dump(mode="json")
    early_channel = FakeChannel()
    inbound_consumer.handle_delivery(
        early_channel,
        SimpleNamespace(delivery_tag=1),
        SimpleNamespace(headers={}, message_id=early_terminal["update_id"]),
        json.dumps(early_terminal).encode(),
    )
    assert early_channel.nacks == [(1, False)]
    assert _saved_state(task_id, tenant_id) == ("accepted", "provisioning", 1)

    progress = WorkerUpdate(
        update_id=f"{task_id}:in_progress",
        task_id=task_id,
        status="in_progress",
        occurred_at=utc_now(),
    ).model_dump(mode="json")
    inbound_consumer.handle_delivery(
        FakeChannel(),
        SimpleNamespace(delivery_tag=2),
        SimpleNamespace(headers={}, message_id=progress["update_id"]),
        json.dumps(progress).encode(),
    )
    terminal_channel = FakeChannel()
    inbound_consumer.handle_delivery(
        terminal_channel,
        SimpleNamespace(delivery_tag=3),
        SimpleNamespace(headers={}, message_id=early_terminal["update_id"]),
        json.dumps(early_terminal).encode(),
    )
    stale_progress = {**progress, "update_id": f"{task_id}:late-progress"}
    stale_channel = FakeChannel()
    inbound_consumer.handle_delivery(
        stale_channel,
        SimpleNamespace(delivery_tag=4),
        SimpleNamespace(headers={}, message_id=stale_progress["update_id"]),
        json.dumps(stale_progress).encode(),
    )

    assert terminal_channel.acks == [3]
    assert stale_channel.acks == [4]
    assert _saved_state(task_id, tenant_id) == ("done", "active", 2)


def test_database_failure_uses_durable_delayed_retry_then_succeeds(monkeypatch, tenant_ids):
    tenant_id, task_event = _prepare_task("deploy", tenant_ids)
    update = WorkerUpdate(
        update_id=f"{task_event['id']}:in_progress",
        task_id=task_event["id"],
        status="in_progress",
        occurred_at=utc_now(),
    )
    publications: list[tuple[str, dict, dict]] = []

    def publish(channel, queue, body, *, message_id=None, headers=None):
        publications.append((queue, body, headers or {}))

    monkeypatch.setattr(inbound_consumer, "publish_json", publish)
    monkeypatch.setattr(
        inbound_consumer,
        "_apply",
        lambda message: (_ for _ in ()).throw(
            OperationalError("update", {}, RuntimeError("database temporarily unavailable"))
        ),
    )
    first_channel = FakeChannel()
    inbound_consumer.handle_delivery(
        first_channel,
        SimpleNamespace(delivery_tag=1),
        SimpleNamespace(headers={}, message_id=update.update_id),
        json.dumps(update.model_dump(mode="json")).encode(),
    )

    assert first_channel.acks == [1]
    assert first_channel.nacks == []
    retry_queue, retry_body, retry_headers = publications[0]
    assert retry_queue == inbound_retry_queue_name(inbound_retry_delays()[0])
    assert retry_body["update_id"] == update.update_id
    assert retry_headers[UPDATE_RETRY_ATTEMPT_HEADER] == 1

    monkeypatch.undo()
    second_channel = FakeChannel()
    inbound_consumer.handle_delivery(
        second_channel,
        SimpleNamespace(delivery_tag=2),
        SimpleNamespace(headers=retry_headers, message_id=update.update_id),
        json.dumps(retry_body).encode(),
    )
    assert second_channel.acks == [2]
    assert _saved_state(UUID(task_event["id"]), tenant_id) == (
        "in_progress",
        "provisioning",
        1,
    )


def test_database_retry_limit_dead_letters_and_broker_retry_failure_requeues(
    monkeypatch, tenant_ids
):
    _, task_event = _prepare_task("deploy", tenant_ids)
    update = WorkerUpdate(
        update_id=f"{task_event['id']}:in_progress",
        task_id=task_event["id"],
        status="in_progress",
        occurred_at=utc_now(),
    )
    monkeypatch.setattr(
        inbound_consumer,
        "_apply",
        lambda message: (_ for _ in ()).throw(
            OperationalError("update", {}, RuntimeError("database unavailable"))
        ),
    )
    exhausted_channel = FakeChannel()
    inbound_consumer.handle_delivery(
        exhausted_channel,
        SimpleNamespace(delivery_tag=1),
        SimpleNamespace(
            headers={UPDATE_RETRY_ATTEMPT_HEADER: len(inbound_retry_delays())},
            message_id=update.update_id,
        ),
        json.dumps(update.model_dump(mode="json")).encode(),
    )
    assert exhausted_channel.nacks == [(1, False)]

    monkeypatch.setattr(
        inbound_consumer,
        "publish_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            pika.exceptions.AMQPConnectionError("broker unavailable")
        ),
    )
    broker_failure_channel = FakeChannel()
    inbound_consumer.handle_delivery(
        broker_failure_channel,
        SimpleNamespace(delivery_tag=2),
        SimpleNamespace(headers={}, message_id=update.update_id),
        json.dumps(update.model_dump(mode="json")).encode(),
    )
    assert broker_failure_channel.acks == []
    assert broker_failure_channel.nacks == [(2, True)]


def test_unknown_task_update_is_dead_lettered_without_crashing_consumer():
    update = WorkerUpdate(
        update_id=f"unknown:{uuid4()}",
        task_id=uuid4(),
        status="in_progress",
        occurred_at=utc_now(),
    )
    channel = FakeChannel()
    inbound_consumer.handle_delivery(
        channel,
        SimpleNamespace(delivery_tag=1),
        SimpleNamespace(headers={}, message_id=update.update_id),
        json.dumps(update.model_dump(mode="json")).encode(),
    )
    assert channel.nacks == [(1, False)]
