import json
import os
from uuid import uuid4

import pytest

from app.messaging.rabbit import connect, publish_json

pytestmark = pytest.mark.integration
if not os.getenv("TEST_RABBITMQ_URL"):
    pytestmark = pytest.mark.skip(reason="Set TEST_RABBITMQ_URL to run RabbitMQ integration tests")


def test_persistent_message_round_trips_through_local_rabbitmq():
    connection = connect()
    try:
        channel = connection.channel()
        channel.confirm_delivery()
        queue = channel.queue_declare(
            queue=f"test.probe.{uuid4().hex}", exclusive=True
        ).method.queue
        message = {"probe_id": uuid4().hex, "status": "accepted"}
        publish_json(channel, queue, message)
        method, properties, body = channel.basic_get(queue=queue, auto_ack=True)
        assert method is not None
        assert properties is not None
        assert properties.content_type == "application/json"
        assert json.loads(body) == message
    finally:
        connection.close()
