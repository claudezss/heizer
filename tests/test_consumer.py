import json
import os
from uuid import uuid4

from heizer import HeizerConfig, HeizerTopic, consumer, producer

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


def test_consumer_stopper():
    @producer(
        topics=[HeizerTopic(name="heizer.test.result")],
        status_topics=[HeizerTopic(name="heizer.test.status")],
        error_topics=[HeizerTopic(name="heizer.test.error")],
        config=Producer_Config,
    )
    def produce_data(status: str, result: str):
        return {
            "key1": 1,
            "key2": "2",
            "key3": True,
            "status": status,
            "result": result,
        }

    def stop(msg):
        data = json.loads(msg.value().decode("utf-8"))
        if data["status"] == "success":
            return True
        return False

    @consumer(
        topics=[HeizerTopic(name="heizer.test.result")],
        config=Consumer_Config,
        stopper=stop,
    )
    def consume_data(msg):
        data = json.loads(msg.value().decode("utf-8"))
        return data["result"]

    produce_data("start", "waiting")
    produce_data("loading", "waiting")
    produce_data("success", "finished")
    produce_data("postprocess", "postprocess")

    result = consume_data()

    assert result == "finished"


def test_consumer_call_once():
    uuid = str(uuid4())

    @producer(
        topics=[HeizerTopic(name=f"heizer.test-{uuid}.result")],
        config=Producer_Config,
    )
    def produce_data(status: str, result: str):
        return {
            "key1": 1,
            "key2": "2",
            "key3": True,
            "status": status,
            "result": result,
        }

    @consumer(
        topics=[HeizerTopic(name=f"heizer.test-{uuid}.result")],
        config=Consumer_Config,
        call_once=True,
    )
    def consume_data(msg):
        data = json.loads(msg.value().decode("utf-8"))
        return data["result"]

    produce_data("start", "waiting")
    produce_data("loading", "waiting")
    produce_data("success", "finished")

    result = consume_data()

    assert result == "waiting"
