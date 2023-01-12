import json
import os
from typing import Any, Dict, cast

from heizer import HeizerConfig, HeizerMessage, HeizerTopic, consumer, producer

Producer_Config = HeizerConfig(
    {
        "bootstrap.servers": os.environ.get("KAFKA_SERVER", "localhost:9092"),
    }
)
Consumer_Config = HeizerConfig(
    {
        "bootstrap.servers": os.environ.get("KAFKA_SERVER", "localhost:9092"),
        "group.id": "default",
        "auto.offset.reset": "earliest",
    }
)


def test_consumer_stopper() -> None:
    @producer(
        topics=[HeizerTopic(name="heizer.test.result")],
        config=Producer_Config,
    )
    def produce_data(status: str, result_value: str) -> Dict[str, Any]:
        return {
            "key1": 1,
            "key2": "2",
            "key3": True,
            "status": status,
            "result": result_value,
        }

    def stop(msg: HeizerMessage) -> bool:
        data = json.loads(msg.value().decode("utf-8"))
        if data["status"] == "success":
            return True
        return False

    @consumer(
        topics=[HeizerTopic(name="heizer.test.result")],
        config=Consumer_Config,
        stopper=stop,
    )
    def consume_data(message: HeizerMessage) -> str:
        data = json.loads(message.value().decode("utf-8"))
        return cast(str, data["result"])

    produce_data("start", "waiting")
    produce_data("loading", "waiting")
    produce_data("success", "finished")
    produce_data("postprocess", "postprocess")

    result = consume_data()  # type: ignore

    assert result == "finished"


def test_consumer_call_once() -> None:
    topic_name = "heizer.test.test_consumer_call_once"

    @producer(
        topics=[HeizerTopic(name=topic_name)],
        config=Producer_Config,
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
        config=Consumer_Config,
        call_once=True,
    )
    def consume_data(message: HeizerMessage) -> str:
        data = json.loads(message.value().decode("utf-8"))
        return cast(str, data["result"])

    produce_data("start", "waiting")
    produce_data("loading", "waiting")
    produce_data("success", "finished")

    result = consume_data()

    assert result == "waiting"
