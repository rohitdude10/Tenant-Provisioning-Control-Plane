import logging
import sys

import pika
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.db import check_database
from app.messaging.rabbit import connect, declare_topology


def check_dependencies() -> None:
    try:
        check_database()
    except SQLAlchemyError as exc:
        raise RuntimeError(
            "Cannot connect to PostgreSQL. Start its Windows service and check "
            "DATABASE_URL in .env."
        ) from exc
    try:
        connection = connect()
        try:
            declare_topology(connection.channel())
        finally:
            connection.close()
    except (pika.exceptions.AMQPError, OSError) as exc:
        raise RuntimeError(
            "Cannot connect to RabbitMQ. Start its Windows service and check RABBITMQ_URL in .env."
        ) from exc
    print("PostgreSQL and RabbitMQ are reachable; durable queues are ready.")


def main() -> None:
    logging.basicConfig(level=get_settings().log_level.upper())
    if len(sys.argv) != 2 or sys.argv[1] != "check":
        print("Usage: python -m app.cli check", file=sys.stderr)
        raise SystemExit(2)
    try:
        check_dependencies()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
