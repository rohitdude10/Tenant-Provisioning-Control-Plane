import argparse
import logging
from uuid import UUID

from app.core.time import utc_now
from app.messaging.rabbit import UPDATE_QUEUE, connect, declare_topology, publish_json
from app.schemas import WorkerUpdate


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish a test worker update to local RabbitMQ.")
    parser.add_argument("--task-id", required=True, type=UUID)
    parser.add_argument("--status", required=True, choices=("in_progress", "done", "failed"))
    parser.add_argument("--update-id", required=True, help="Unique, stable idempotency key.")
    args = parser.parse_args()
    message = WorkerUpdate(
        update_id=args.update_id,
        task_id=args.task_id,
        status=args.status,
        occurred_at=utc_now(),
    )
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        publish_json(
            channel,
            UPDATE_QUEUE,
            message.model_dump(mode="json"),
            message_id=message.update_id,
        )
    finally:
        connection.close()
    logging.info("Published update %s for task %s", args.update_id, args.task_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
