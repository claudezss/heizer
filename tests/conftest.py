import os

import pytest
from confluent_kafka.admin import AdminClient

from heizer import Topic, delete_topics, list_topics

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_SERVER", "localhost:9092")


@pytest.fixture()
def bootstrap_server() -> str:
    return BOOTSTRAP_SERVERS


@pytest.fixture()
def admin_client(bootstrap_server) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_server})


@pytest.fixture(autouse=True, scope="session")
def clean_topics():
    yield
    config = {"bootstrap.servers": BOOTSTRAP_SERVERS}
    topics = list_topics(config)
    delete_topics(config, [Topic(t) for t in topics])
