import asyncio
import uuid

import pytest

from heizer import Producer, ProducerConfig


@pytest.fixture
def message_count() -> int:
    return 10_000


@pytest.fixture
def messages(message_count):
    return [
        {
            "key": "test_key",
            "value": ["test"] * 1000,
            "headers": {"k": "v"},
        }
    ] * message_count


def test_produce_message(messages, bootstrap_server, caplog):
    pd = Producer(config=ProducerConfig(bootstrap_servers=bootstrap_server), name="test_producer")

    topic = f"test_producer_topic_{uuid.uuid4()}"

    for msg in messages:
        pd.produce(topic=topic, auto_flush=False, **msg)
    pd.flush()


def test_async_produce_message(messages, bootstrap_server, caplog):
    pd = Producer(config=ProducerConfig(bootstrap_servers=bootstrap_server), name="test_producer")
    topic = f"test_async_producer_topic_{uuid.uuid4()}"
    jobs = []

    async def produce():
        for msg in messages:
            jobs.append(asyncio.ensure_future(pd.async_produce(topic=topic, auto_flush=False, **msg)))
        await asyncio.gather(*jobs)
        pd.flush()

    asyncio.run(produce())
