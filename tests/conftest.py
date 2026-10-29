import os

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
def create_integration_schema():
    if TEST_DATABASE_URL:
        from app import models  # noqa: F401
        from app.db import Base, engine

        Base.metadata.create_all(engine)
    yield
