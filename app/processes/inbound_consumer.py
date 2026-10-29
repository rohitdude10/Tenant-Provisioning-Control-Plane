import logging
import time

from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError

from app.core.config import get_settings
from app.db import SessionLocal, check_database
from app.messaging.rabbit import (
    UPDATE_QUEUE,
    connect,
    declare_topology,
    decode_message,
    retry_connection,
)
from app.schemas import WorkerUpdate
from app.services import apply_worker_update

logger = logging.getLogger(__name__)


def _apply(message: WorkerUpdate) -> bool:
    with SessionLocal() as session:
        return apply_worker_update(session, message)


def run() -> None:
    settings = get_settings()
    check_database()
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.basic_qos(prefetch_count=1)

        def handle_delivery(channel, method, properties, body: bytes) -> None:
            try:
                update_message = decode_message(body, WorkerUpdate.model_validate)
                applied = _apply(update_message)
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(
                    "Processed worker update id=%s task_id=%s applied=%s",
                    update_message.update_id,
                    update_message.task_id,
                    applied,
                )
            except ValueError as exc:
                logger.error("Rejecting poison worker update: %s", exc)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except (DataError, IntegrityError):
                logger.exception("Rejecting worker update that violates database constraints")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except SQLAlchemyError:
                logger.exception("Database failure applying worker update; retrying after backoff")
                time.sleep(settings.inbound_retry_seconds)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                logger.exception(
                    "Unexpected worker update failure; sending message to dead-letter queue"
                )
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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
