import os

import pytest
from confluent_kafka.admin import AdminClient


@pytest.fixture()
def bootstrap_server() -> str:
    return os.environ.get("KAFKA_SERVER", "localhost:9092")


@pytest.fixture()
def admin_client(bootstrap_server) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_server})
