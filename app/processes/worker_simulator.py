import argparse
import logging
import secrets
import threading
import time

import pika

from app.core.config import get_settings
from app.core.time import utc_now
from app.messaging.rabbit import (
    TASK_QUEUE,
    UPDATE_QUEUE,
    connect,
    declare_topology,
    decode_message,
    publish_json,
    retry_connection,
)
from app.processes.shutdown import shutdown_event, watch_shutdown
from app.schemas import TaskEvent, WorkerUpdate

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Simulate tenant provisioning task workers.")
    parser.add_argument("--fail-rate", type=float, default=settings.worker_fail_rate)
    parser.add_argument("--min-delay-ms", type=int, default=settings.worker_min_delay_ms)
    parser.add_argument("--max-delay-ms", type=int, default=settings.worker_max_delay_ms)
    args = parser.parse_args()
    if not 0 <= args.fail_rate <= 1:
        parser.error("--fail-rate must be between 0 and 1")
    if args.min_delay_ms < 0 or args.max_delay_ms < args.min_delay_ms:
        parser.error("delay values must be non-negative and max must be >= min")
    return args


def _publish_update(channel, task: TaskEvent, status: str) -> None:
    update = WorkerUpdate(
        update_id=f"{task.id}:{status}",
        task_id=task.id,
        status=status,
        occurred_at=utc_now(),
    )
    publish_json(
        channel,
        UPDATE_QUEUE,
        update.model_dump(mode="json"),
        message_id=update.update_id,
    )


def _process_task(channel, task: TaskEvent, args: argparse.Namespace) -> str:
    _publish_update(channel, task, "in_progress")
    delay_ms = args.min_delay_ms + secrets.randbelow(args.max_delay_ms - args.min_delay_ms + 1)
    time.sleep(delay_ms / 1000)
    failure_threshold = int(args.fail_rate * 1_000_000)
    outcome = "failed" if secrets.randbelow(1_000_000) < failure_threshold else "done"
    _publish_update(channel, task, outcome)
    return outcome


def handle_delivery(channel, method, properties, body: bytes, args: argparse.Namespace) -> None:
    try:
        task = decode_message(body, TaskEvent.model_validate)
    except ValueError as exc:
        logger.error("Rejecting malformed task message: %s", exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    message_id = getattr(properties, "message_id", None)
    if message_id is not None and message_id != str(task.id):
        logger.error(
            "Rejecting task message with mismatched broker message ID task_id=%s message_id=%s",
            task.id,
            message_id,
        )
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    if task.status != "accepted":
        logger.warning("Ignoring non-accepted task event id=%s status=%s", task.id, task.status)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return
    try:
        outcome = _process_task(channel, task, args)
    except (pika.exceptions.AMQPError, OSError, TimeoutError):
        logger.exception("Worker could not publish task progress; task will be retried")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return
    channel.basic_ack(delivery_tag=method.delivery_tag)
    logger.info("Completed task id=%s outcome=%s", task.id, outcome)


def run(args: argparse.Namespace, stop_event: threading.Event | None = None) -> None:
    if stop_event is None:
        stop_event = shutdown_event()
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        channel.basic_qos(prefetch_count=1)

        def stop_consuming() -> None:
            stop_event.set()
            if connection.is_open and channel.is_open:
                connection.add_callback_threadsafe(channel.stop_consuming)

        watch_shutdown(stop_consuming)
        channel.basic_consume(
            queue=TASK_QUEUE,
            on_message_callback=lambda callback_channel, method, properties, body: handle_delivery(
                callback_channel, method, properties, body, args
            ),
        )
        logger.info(
            "Worker listening on %s (fail_rate=%.3f delay_ms=%d..%d)",
            TASK_QUEUE,
            args.fail_rate,
            args.min_delay_ms,
            args.max_delay_ms,
        )
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()


def main() -> None:
    logging.basicConfig(level=get_settings().log_level.upper())
    args = parse_args()
    stop_event = shutdown_event()
    retry_connection(lambda: run(args, stop_event), "worker simulator", stop_event)


if __name__ == "__main__":
    main()
