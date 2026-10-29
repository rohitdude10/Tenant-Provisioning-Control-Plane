import os
from pathlib import Path

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if TEST_DATABASE_URL:
    if not TEST_DATABASE_URL.startswith(("postgresql+psycopg://", "postgresql://")):
        raise RuntimeError("TEST_DATABASE_URL must point to PostgreSQL; SQLite is not supported")
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

TEST_RABBITMQ_URL = os.getenv("TEST_RABBITMQ_URL")
if TEST_RABBITMQ_URL:
    if not TEST_RABBITMQ_URL.startswith(("amqp://", "amqps://")):
        raise RuntimeError("TEST_RABBITMQ_URL must use RabbitMQ AMQP")
    test_vhost = TEST_RABBITMQ_URL.rsplit("/", 1)[-1].split("?", 1)[0]
    if not test_vhost.endswith("_test"):
        raise RuntimeError("TEST_RABBITMQ_URL virtual host must end with '_test'")
    os.environ["RABBITMQ_URL"] = TEST_RABBITMQ_URL


@pytest.fixture(scope="session", autouse=True)
def migrate_integration_database():
    if TEST_DATABASE_URL:
        from alembic.config import Config

        from alembic import command

        project_root = Path(__file__).resolve().parents[1]
        config = Config(str(project_root / "alembic.ini"))
        config.set_main_option("script_location", str(project_root / "alembic"))
        command.upgrade(config, "head")
    yield
