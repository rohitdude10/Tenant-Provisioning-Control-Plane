import logging

import pika
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError

from app.core.config import get_settings
from app.db import SessionLocal, check_database
from app.messaging.rabbit import (
    UPDATE_QUEUE,
    UPDATE_RETRY_ATTEMPT_HEADER,
    connect,
    declare_topology,
    decode_message,
    inbound_retry_delays,
    inbound_retry_queue_name,
    publish_json,
    retry_connection,
)
from app.schemas import WorkerUpdate
from app.services import InvalidWorkerUpdate, apply_worker_update

logger = logging.getLogger(__name__)
TRANSIENT_BROKER_ERRORS = (pika.exceptions.AMQPError, OSError, TimeoutError)


def _apply(message: WorkerUpdate) -> bool:
    with SessionLocal() as session:
        return apply_worker_update(session, message)


def _dead_letter(channel, delivery_tag: int, reason: str) -> None:
    logger.error("Dead-lettering worker update delivery=%s: %s", delivery_tag, reason)
    channel.basic_nack(delivery_tag=delivery_tag, requeue=False)


def _retry_or_dead_letter(channel, method, properties, message: WorkerUpdate) -> None:
    headers = dict(getattr(properties, "headers", None) or {})
    retry_count = headers.get(UPDATE_RETRY_ATTEMPT_HEADER, 0)
    if isinstance(retry_count, bool) or not isinstance(retry_count, int) or retry_count < 0:
        _dead_letter(channel, method.delivery_tag, "invalid retry-count header")
        return

    next_attempt = retry_count + 1
    delays = inbound_retry_delays()
    if next_attempt > len(delays):
        _dead_letter(
            channel,
            method.delivery_tag,
            f"database processing failed after {len(delays)} retries",
        )
        return

    headers[UPDATE_RETRY_ATTEMPT_HEADER] = next_attempt
    try:
        publish_json(
            channel,
            inbound_retry_queue_name(delays[next_attempt - 1]),
            message.model_dump(mode="json"),
            message_id=getattr(properties, "message_id", None),
            headers=headers,
        )
    except TRANSIENT_BROKER_ERRORS:
        logger.exception(
            "Could not publish delayed retry for update_id=%s; requeueing original delivery",
            message.update_id,
        )
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.warning(
        "Scheduled worker update id=%s for retry %s/%s in %.2fs",
        message.update_id,
        next_attempt,
        len(delays),
        delays[next_attempt - 1],
    )


def handle_delivery(channel, method, properties, body: bytes) -> None:
    try:
        update_message = decode_message(body, WorkerUpdate.model_validate)
    except ValueError as exc:
        _dead_letter(channel, method.delivery_tag, f"invalid update schema: {exc}")
        return

    try:
        applied = _apply(update_message)
    except InvalidWorkerUpdate as exc:
        _dead_letter(channel, method.delivery_tag, str(exc))
        return
    except (DataError, IntegrityError) as exc:
        _dead_letter(channel, method.delivery_tag, f"database constraint error: {exc}")
        return
    except SQLAlchemyError:
        logger.exception("Database failure applying worker update id=%s", update_message.update_id)
        _retry_or_dead_letter(channel, method, properties, update_message)
        return
    except Exception as exc:
        _dead_letter(
            channel,
            method.delivery_tag,
            f"unexpected permanent processing error: {type(exc).__name__}",
        )
        logger.exception("Unexpected error applying worker update")
        return

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(
        "Processed worker update id=%s task_id=%s applied=%s",
        update_message.update_id,
        update_message.task_id,
        applied,
    )


def run() -> None:
    check_database()
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=UPDATE_QUEUE, on_message_callback=handle_delivery)
        logger.info("Inbound consumer listening on %s", UPDATE_QUEUE)
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()


def main() -> None:
    logging.basicConfig(level=get_settings().log_level.upper())
    retry_connection(run, "inbound consumer")


if __name__ == "__main__":
    main()
