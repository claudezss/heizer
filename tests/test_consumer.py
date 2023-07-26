import json
import logging
import os
from typing import Any, Dict, cast

import pytest
from pydantic import BaseModel

from heizer import HeizerConfig, HeizerMessage, HeizerTopic, consumer, create_new_topic, get_admin_client, producer

Producer_Config = HeizerConfig(
    {
        "bootstrap.servers": os.environ.get("KAFKA_SERVER", "localhost:9092"),
    }
)


@pytest.fixture
def group_id():
    return "test_group"


@pytest.fixture
def consumer_config(group_id):
    return HeizerConfig(
        {
            "bootstrap.servers": os.environ.get("KAFKA_SERVER", "localhost:9092"),
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )


@pytest.mark.parametrize("group_id", ["test_consumer_stopper"])
def test_consumer_stopper(group_id, consumer_config) -> None:
    toppics = [HeizerTopic(name="heizer.test.result", partitions=[0, 1, 2], replication_factor=2)]
    admin = get_admin_client(consumer_config)
    create_new_topic(admin, toppics)

    @producer(
        topics=toppics,
        config=Producer_Config,
        key_alias="myKey",
        headers_alias="myHeaders",
    )
    def produce_data(status: str, result_value: str) -> Dict[str, Any]:
        return {
            "key1": 1,
            "key2": "2",
            "key3": True,
            "status": status,
            "result": result_value,
            "myKey": "id1",
            "myHeaders": {"header1": "value1", "header2": "value2"},
        }

    def stopper(msg: HeizerMessage) -> bool:
        data = json.loads(msg.value)
        if data["status"] == "success":
            return True
        return False

    @consumer(
        topics=[HeizerTopic(name="heizer.test.result")],
        config=consumer_config,
        stopper=stopper,
    )
    def consume_data(msg, *args, **kwargs) -> str:
        data = json.loads(msg.value)

        assert msg.key == "id1"
        assert msg.headers == {"header1": "value1", "header2": "value2"}

        assert "myKey" not in data
        assert "myHeaders" not in data

        return cast(str, data["result"])

    produce_data("start", "waiting")
    produce_data("loading", "waiting")
    produce_data("success", "finished")
    produce_data("postprocess", "postprocess")

    result = consume_data()  # type: ignore

    assert result == "finished"


@pytest.mark.parametrize("group_id", ["test_consumer_call_once"])
def test_consumer_call_once(consumer_config) -> None:
    topic_name = "heizer.test.test_consumer_call_once"

    @producer(
        topics=[HeizerTopic(name=topic_name)],
        config=Producer_Config,
        key_alias="non_existing_key",
        headers_alias="non_existing_headers",
    )
    def produce_data(status: str, result: str) -> Dict[str, Any]:
        return {
            "key1": 1,
            "key2": "2",
            "key3": True,
            "status": status,
            "result": result,
        }

    @consumer(
        topics=[HeizerTopic(name=topic_name)],
        config=consumer_config,
        call_once=True,
    )
    def consume_data(msg, *args, **kwargs) -> str:
        data = json.loads(msg.value)
        return cast(str, data["result"])

    produce_data("start", "waiting")
    produce_data("loading", "waiting")
    produce_data("success", "finished")

    result = consume_data()

    assert result == "waiting"


@pytest.mark.parametrize("group_id", ["test_consumer_deserializer"])
def test_consumer_deserializer(caplog, consumer_config) -> None:
    caplog.set_level(logging.DEBUG)
    topic_name = "heizer.test.test_consumer_deserializer"

    class TestModel(BaseModel):
        name: str
        age: int

    deserializer = TestModel

    @producer(
        topics=[HeizerTopic(name=topic_name)],
        config=Producer_Config,
    )
    def produce_data() -> Dict[str, Any]:
        return {
            "name": "mike",
            "age": 20,
        }

    @consumer(
        topics=[HeizerTopic(name=topic_name)],
        config=consumer_config,
        call_once=True,
        deserializer=deserializer,
    )
    def consume_data(message: HeizerMessage, *args, **kwargs):
        return message.formatted_value

    produce_data()

    result = consume_data()

    assert isinstance(result, TestModel)

    assert result.name == "mike"
    assert result.age == 20
