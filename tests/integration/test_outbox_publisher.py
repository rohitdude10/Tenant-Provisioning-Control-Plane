import os
import threading
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pika
import pytest
from sqlalchemy import delete, event, select
from sqlalchemy.orm import Session

from app.core.time import utc_now
from app.db import SessionLocal, transaction_scope
from app.models import OutboxEvent, ProcessedUpdate, Task, Tenant
from app.processes import outbox_publisher

pytestmark = pytest.mark.integration
if not os.getenv("TEST_DATABASE_URL"):
    pytestmark = pytest.mark.skip(
        reason="Set TEST_DATABASE_URL to run PostgreSQL outbox publisher tests"
    )
else:
    database_name = os.getenv("TEST_DATABASE_URL", "").rsplit("/", 1)[-1].split("?", 1)[0]
    if not database_name.endswith("_test"):
        raise RuntimeError(
            "TEST_DATABASE_URL database name must end with '_test' to protect local data"
        )


class FakeChannel:
    def __init__(self) -> None:
        self.confirmations_enabled = False

    def confirm_delivery(self) -> None:
        self.confirmations_enabled = True


class FakeConnection:
    def __init__(self, channel: FakeChannel | None = None) -> None:
        self.fake_channel = channel or FakeChannel()
        self.closed = False

    def channel(self) -> FakeChannel:
        return self.fake_channel

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def pending_outbox_event():
    tenant_id = uuid4()
    task_id = uuid4()
    now = datetime(1900, 1, 1, tzinfo=UTC)
    with transaction_scope() as session:
        session.add(
            Tenant(
                id=tenant_id,
                slug=f"test-{uuid4().hex[:12]}",
                name="Outbox publisher test",
                status="provisioning",
                version=1,
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            Task(
                id=task_id,
                type="deploy",
                tenant_id=tenant_id,
                status="accepted",
                payload={},
                created_at=now,
                updated_at=now,
            )
        )
        session.flush()
        session.add(
            OutboxEvent(
                task_id=task_id,
                body={
                    "id": str(task_id),
                    "type": "deploy",
                    "tenant_id": str(tenant_id),
                    "status": "accepted",
                    "created_at": now.isoformat(),
                    "payload": {},
                },
                created_at=now,
            )
        )
    yield task_id
    with transaction_scope() as session:
        task_ids = list(session.scalars(select(Task.id).where(Task.tenant_id == tenant_id)))
        if task_ids:
            session.execute(delete(ProcessedUpdate).where(ProcessedUpdate.task_id.in_(task_ids)))
            session.execute(delete(OutboxEvent).where(OutboxEvent.task_id.in_(task_ids)))
            session.execute(delete(Task).where(Task.id.in_(task_ids)))
        session.execute(delete(Tenant).where(Tenant.id == tenant_id))


def _outbox_row(task_id):
    with SessionLocal() as session:
        return session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == task_id))


def _successful_broker(monkeypatch, published, *, before_publish=None):
    channel = FakeChannel()
    connection = FakeConnection(channel)
    monkeypatch.setattr(outbox_publisher, "connect", lambda: connection)
    monkeypatch.setattr(outbox_publisher, "declare_topology", lambda unused_channel: None)

    def publish(_channel, routing_key, body, *, message_id=None):
        assert channel.confirmations_enabled
        if before_publish is not None:
            before_publish()
        published.append((routing_key, body, message_id))

    monkeypatch.setattr(outbox_publisher, "publish_json", publish)
    return connection


def test_publisher_confirms_durable_task_event_then_marks_outbox_published(
    monkeypatch, pending_outbox_event
):
    published = []
    connection = _successful_broker(monkeypatch, published)

    assert outbox_publisher.publish_once()

    row = _outbox_row(pending_outbox_event)
    assert connection.closed
    assert len(published) == 1
    routing_key, body, message_id = published[0]
    assert routing_key == outbox_publisher.TASK_QUEUE
    assert body["id"] == str(pending_outbox_event)
    assert body["type"] == "deploy"
    assert body["tenant_id"]
    assert body["status"] == "accepted"
    assert message_id == str(pending_outbox_event)
    assert row is not None
    assert row.published_at is not None
    assert row.attempts == 1
    assert row.next_attempt_at is None
    assert row.last_error is None


def test_broker_unavailability_persists_retry_schedule_and_error(monkeypatch, pending_outbox_event):
    def fail_connect():
        raise pika.exceptions.AMQPConnectionError("broker unavailable")

    monkeypatch.setattr(outbox_publisher, "connect", fail_connect)

    assert outbox_publisher.publish_once()

    row = _outbox_row(pending_outbox_event)
    assert row is not None
    assert row.published_at is None
    assert row.attempts == 1
    assert row.next_attempt_at is not None
    assert row.next_attempt_at > utc_now()
    assert row.last_error
    assert outbox_publisher.publish_once() is False


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 1.0), (2, 2.0), (3, 4.0), (4, 5.0), (12, 5.0), (10_000, 5.0)],
)
def test_retry_delay_is_exponential_and_capped(attempt, expected):
    assert outbox_publisher.retry_delay_seconds(attempt, 1.0, 5.0) == expected


def test_retry_after_broker_recovery_increments_attempt_count(monkeypatch, pending_outbox_event):
    monkeypatch.setattr(
        outbox_publisher,
        "connect",
        lambda: (_ for _ in ()).throw(pika.exceptions.AMQPConnectionError("broker unavailable")),
    )
    assert outbox_publisher.publish_once()
    with transaction_scope() as session:
        row = session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == pending_outbox_event))
        assert row is not None
        row.next_attempt_at = utc_now() - timedelta(seconds=1)

    published = []
    _successful_broker(monkeypatch, published)
    assert outbox_publisher.publish_once()

    row = _outbox_row(pending_outbox_event)
    assert row is not None
    assert row.published_at is not None
    assert row.attempts == 2
    assert row.next_attempt_at is None
    assert len(published) == 1


def test_unpublished_event_is_recovered_by_a_fresh_publisher_invocation(
    monkeypatch, pending_outbox_event
):
    monkeypatch.setattr(
        outbox_publisher,
        "connect",
        lambda: (_ for _ in ()).throw(pika.exceptions.AMQPConnectionError("broker unavailable")),
    )
    assert outbox_publisher.publish_once()
    with transaction_scope() as session:
        row = session.scalar(select(OutboxEvent).where(OutboxEvent.task_id == pending_outbox_event))
        assert row is not None
        assert row.published_at is None
        row.next_attempt_at = utc_now() - timedelta(seconds=1)

    published = []
    _successful_broker(monkeypatch, published)
    assert outbox_publisher.publish_once()
    recovered = _outbox_row(pending_outbox_event)
    assert recovered is not None
    assert recovered.published_at is not None
    assert recovered.attempts == 2
    assert len(published) == 1


def test_confirmed_publish_then_database_rollback_can_publish_duplicate(
    monkeypatch, pending_outbox_event
):
    published = []
    _successful_broker(monkeypatch, published)
    failed_commit = True

    def fail_first_commit(_session: Session) -> None:
        nonlocal failed_commit
        if failed_commit:
            failed_commit = False
            raise RuntimeError("simulated crash before published marker commits")

    event.listen(SessionLocal, "before_commit", fail_first_commit)
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            outbox_publisher.publish_once()
    finally:
        event.remove(SessionLocal, "before_commit", fail_first_commit)

    assert _outbox_row(pending_outbox_event).published_at is None
    assert outbox_publisher.publish_once()
    assert len(published) == 2
    assert published[0][2] == published[1][2] == str(pending_outbox_event)
    assert _outbox_row(pending_outbox_event).published_at is not None


def test_skip_locked_prevents_two_publishers_claiming_same_event(monkeypatch, pending_outbox_event):
    first_publish_started = threading.Event()
    release_first_publish = threading.Event()
    published = []
    channel = FakeChannel()
    connection = FakeConnection(channel)
    monkeypatch.setattr(outbox_publisher, "connect", lambda: connection)
    monkeypatch.setattr(outbox_publisher, "declare_topology", lambda unused_channel: None)

    def publish(_channel, _routing_key, _body, *, message_id=None):
        assert channel.confirmations_enabled
        first_publish_started.set()
        assert release_first_publish.wait(timeout=10)
        published.append(message_id)

    monkeypatch.setattr(outbox_publisher, "publish_json", publish)
    result: list[bool] = []
    first = threading.Thread(target=lambda: result.append(outbox_publisher.publish_once()))
    first.start()
    try:
        assert first_publish_started.wait(timeout=10)
        assert outbox_publisher.publish_once() is False
    finally:
        release_first_publish.set()
        first.join(timeout=10)

    assert not first.is_alive()
    assert result == [True]
    assert published == [str(pending_outbox_event)]
    assert _outbox_row(pending_outbox_event).published_at is not None


def test_task_and_outbox_rollback_together_when_transaction_fails():
    tenant_id = uuid4()
    task_id = uuid4()
    slug = f"test-{uuid4().hex[:12]}"
    now = utc_now()

    with pytest.raises(RuntimeError, match="rollback probe"):
        with transaction_scope() as session:
            tenant = Tenant(
                id=tenant_id,
                slug=slug,
                name="Rollback outbox",
                status="provisioning",
                version=1,
                created_at=now,
                updated_at=now,
            )
            task = Task(
                id=task_id,
                type="deploy",
                tenant_id=tenant_id,
                status="accepted",
                payload={},
                created_at=now,
                updated_at=now,
            )
            session.add_all(
                [
                    tenant,
                    task,
                    OutboxEvent(
                        task_id=task_id,
                        body={"id": str(task_id)},
                        created_at=now,
                    ),
                ]
            )
            raise RuntimeError("rollback probe")

    with SessionLocal() as session:
        assert session.get(Tenant, tenant_id) is None
        assert session.get(Task, task_id) is None
        assert session.scalar(select(OutboxEvent.id).where(OutboxEvent.task_id == task_id)) is None
