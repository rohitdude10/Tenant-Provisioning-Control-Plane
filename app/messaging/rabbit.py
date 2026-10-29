import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings

TASK_QUEUE = "tenant.provisioning.tasks"
UPDATE_QUEUE = "tenant.provisioning.updates"
TASK_DEAD_LETTER_EXCHANGE = "tenant.provisioning.tasks.dlx"
TASK_DEAD_LETTER_QUEUE = "tenant.provisioning.tasks.dead"
UPDATE_DEAD_LETTER_EXCHANGE = "tenant.provisioning.updates.dlx"
UPDATE_DEAD_LETTER_QUEUE = "tenant.provisioning.updates.dead"
UPDATE_RETRY_QUEUE_PREFIX = f"{UPDATE_QUEUE}.retry"
UPDATE_RETRY_ATTEMPT_HEADER = "x-control-plane-retry-attempt"
logger = logging.getLogger(__name__)


def inbound_retry_delays() -> list[float]:
    settings = get_settings()
    delays = []
    for attempt in range(1, settings.inbound_retry_max_attempts + 1):
        delay = min(
            settings.inbound_retry_seconds * (2 ** (attempt - 1)),
            settings.inbound_retry_max_seconds,
        )
        delays.append(delay)
    return delays


def inbound_retry_queue_name(delay_seconds: float) -> str:
    delay_ms = max(1, round(delay_seconds * 1000))
    return f"{UPDATE_RETRY_QUEUE_PREFIX}.{delay_ms}ms"


def connect() -> pika.BlockingConnection:
    settings = get_settings()
    parameters = pika.URLParameters(settings.rabbitmq_url)
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 30
    return pika.BlockingConnection(parameters)


def declare_topology(channel: BlockingChannel) -> None:
    channel.exchange_declare(
        exchange=TASK_DEAD_LETTER_EXCHANGE, exchange_type="direct", durable=True
    )
    channel.queue_declare(queue=TASK_DEAD_LETTER_QUEUE, durable=True)
    channel.queue_bind(
        queue=TASK_DEAD_LETTER_QUEUE,
        exchange=TASK_DEAD_LETTER_EXCHANGE,
        routing_key=TASK_QUEUE,
    )
    channel.exchange_declare(
        exchange=UPDATE_DEAD_LETTER_EXCHANGE, exchange_type="direct", durable=True
    )
    channel.queue_declare(queue=UPDATE_DEAD_LETTER_QUEUE, durable=True)
    channel.queue_bind(
        queue=UPDATE_DEAD_LETTER_QUEUE,
        exchange=UPDATE_DEAD_LETTER_EXCHANGE,
        routing_key=UPDATE_QUEUE,
    )
    channel.queue_declare(
        queue=TASK_QUEUE,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": TASK_DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": TASK_QUEUE,
        },
    )
    channel.queue_declare(
        queue=UPDATE_QUEUE,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": UPDATE_DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": UPDATE_QUEUE,
        },
    )
    for delay in inbound_retry_delays():
        channel.queue_declare(
            queue=inbound_retry_queue_name(delay),
            durable=True,
            arguments={
                "x-message-ttl": max(1, round(delay * 1000)),
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": UPDATE_QUEUE,
            },
        )


def publish_json(
    channel: BlockingChannel,
    routing_key: str,
    body: dict[str, Any],
    *,
    message_id: str | None = None,
    headers: dict[str, Any] | None = None,
) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=routing_key,
        body=json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=message_id,
            headers=headers,
        ),
        mandatory=True,
    )


def decode_message(body: bytes, parser: Callable[[dict[str, Any]], Any]) -> Any:
    try:
        decoded = json.loads(body)
        if not isinstance(decoded, dict):
            raise ValueError("message body must be a JSON object")
        return parser(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
        raise ValueError(f"invalid JSON message: {exc}") from exc


def retry_connection(
    run: Callable[[], None],
    process_name: str,
    stop_event: threading.Event | None = None,
) -> None:
    delay = 1.0
    while stop_event is None or not stop_event.is_set():
        try:
            run()
            delay = 1.0
        except KeyboardInterrupt:
            logger.info("%s stopped", process_name)
            return
        except (pika.exceptions.AMQPError, OSError, SQLAlchemyError) as exc:
            logger.error(
                "%s connection failed: %s; retrying in %.1f seconds", process_name, exc, delay
            )
            if stop_event is not None:
                stop_event.wait(delay)
            else:
                time.sleep(delay)
            delay = min(delay * 2, 30.0)
    logger.info("%s stopped", process_name)
