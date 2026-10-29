import logging
import time

import pika
from sqlalchemy import select
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


def publish_once() -> bool:
    with SessionLocal() as session:
        event = session.scalar(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(1)
        )
        if event is None:
            return False
        event_id = event.id
        body = event.body
    try:
        connection = connect()
        try:
            channel = connection.channel()
            declare_topology(channel)
            channel.confirm_delivery()
            publish_json(channel, TASK_QUEUE, body)
        finally:
            connection.close()
    except (SQLAlchemyError, pika.exceptions.AMQPError, OSError) as exc:
        with SessionLocal.begin() as session:
            pending = session.get(OutboxEvent, event_id)
            if pending is not None and pending.published_at is None:
                pending.attempts += 1
                pending.last_error = str(exc)[:1000]
        raise
    with SessionLocal.begin() as session:
        pending = session.get(OutboxEvent, event_id)
        if pending is not None and pending.published_at is None:
            pending.published_at = utc_now()
            pending.attempts += 1
            pending.last_error = None
    logger.info("Published task outbox event id=%s", event_id)
    return True


def run() -> None:
    settings = get_settings()
    logger.info("Outbox publisher started; poll interval %.2fs", settings.outbox_poll_seconds)
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
        except (SQLAlchemyError, pika.exceptions.AMQPError, OSError):
            logger.exception("Outbox publish attempt failed; retrying in %.1f seconds", retry_delay)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30.0)


def main() -> None:
    logging.basicConfig(level=get_settings().log_level.upper())
    retry_connection(run, "outbox publisher")


if __name__ == "__main__":
    main()
