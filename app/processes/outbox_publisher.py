import logging
import math
import time
from datetime import timedelta
from typing import Any

import pika
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.core.time import utc_now
from app.db import SessionLocal, check_database
from app.messaging.rabbit import (
    TASK_QUEUE,
    connect,
    declare_topology,
    publish_json,
    retry_connection,
)
from app.models import OutboxEvent

logger = logging.getLogger(__name__)
TRANSIENT_BROKER_ERRORS = (pika.exceptions.AMQPError, OSError, TimeoutError)
RETRYABLE_INFRASTRUCTURE_ERRORS = (SQLAlchemyError, *TRANSIENT_BROKER_ERRORS)


def retry_delay_seconds(attempt: int, base_seconds: float, maximum_seconds: float) -> float:
    if attempt < 1:
        raise ValueError("attempt must be at least one")
    if base_seconds <= 0 or maximum_seconds <= 0:
        raise ValueError("retry delays must be positive")
    if base_seconds >= maximum_seconds:
        return maximum_seconds
    cap_exponent = math.ceil(math.log2(maximum_seconds / base_seconds))
    if attempt - 1 >= cap_exponent:
        return maximum_seconds
    return min(base_seconds * (2 ** (attempt - 1)), maximum_seconds)


def _validate_task_event(event: OutboxEvent) -> tuple[dict[str, Any], str]:
    body = event.body
    required_fields = {"id", "type", "tenant_id", "status"}
    if not isinstance(body, dict) or not required_fields.issubset(body):
        raise ValueError(f"Outbox event {event.id} is missing required task event fields")
    task_id = str(event.task_id)
    if str(body["id"]) != task_id:
        raise ValueError(f"Outbox event {event.id} has an event ID that does not match its task ID")
    return body, task_id


def publish_once() -> bool:
    settings = get_settings()
    now = utc_now()
    with SessionLocal.begin() as session:
        event = session.scalar(
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                or_(
                    OutboxEvent.next_attempt_at.is_(None),
                    OutboxEvent.next_attempt_at <= now,
                ),
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return False
        body, message_id = _validate_task_event(event)
        attempt = event.attempts + 1
        try:
            connection = connect()
            try:
                channel = connection.channel()
                declare_topology(channel)
                channel.confirm_delivery()
                publish_json(channel, TASK_QUEUE, body, message_id=message_id)
            finally:
                connection.close()
        except TRANSIENT_BROKER_ERRORS as exc:
            delay = retry_delay_seconds(
                attempt,
                settings.outbox_retry_base_seconds,
                settings.outbox_retry_max_seconds,
            )
            event.attempts = attempt
            event.last_error = str(exc)[:1000]
            event.next_attempt_at = now + timedelta(seconds=delay)
            logger.warning(
                "Outbox event id=%s publish attempt %s failed: %s; retrying in %.1f seconds",
                event.id,
                attempt,
                exc,
                delay,
            )
            return True

        event.attempts = attempt
        event.published_at = utc_now()
        event.next_attempt_at = None
        event.last_error = None
        logger.info(
            "Published outbox event id=%s task_id=%s attempt=%s",
            event.id,
            message_id,
            attempt,
        )
    return True


def run() -> None:
    settings = get_settings()
    logger.info(
        "Outbox publisher started; poll interval %.2fs; retry backoff %.2f..%.2fs",
        settings.outbox_poll_seconds,
        settings.outbox_retry_base_seconds,
        settings.outbox_retry_max_seconds,
    )
    retry_delay = 1.0
    while True:
        try:
            check_database()
            if not publish_once():
                time.sleep(settings.outbox_poll_seconds)
            else:
                retry_delay = 1.0
        except KeyboardInterrupt:
            raise
        except RETRYABLE_INFRASTRUCTURE_ERRORS:
            logger.exception(
                "Outbox publisher infrastructure failure; retrying in %.1f seconds", retry_delay
            )
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30.0)


def main() -> None:
    logging.basicConfig(level=get_settings().log_level.upper())
    retry_connection(run, "outbox publisher")


if __name__ == "__main__":
    main()
