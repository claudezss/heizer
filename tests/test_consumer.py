import json
import os

from heizer import HeizerConfig, HeizerTopic, consumer, producer


def test_consumer():
    config = HeizerConfig(
        {
            "bootstrap.servers": os.environ.get(
                "KAFKA_SERVER", "localhost:9092"
            ),
            "group.id": "default",
            "auto.offset.reset": "earliest",
        }
    )

    def stop(msg):
        data = json.loads(msg.value().decode("utf-8"))
        if data["status"] == "success":
            return True
        return False

    @producer(
        topics=[HeizerTopic(name="heizer.test.result")],
        status_topics=[HeizerTopic(name="heizer.test.status")],
        error_topics=[HeizerTopic(name="heizer.test.error")],
        config=config,
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
        topics=[HeizerTopic(name="heizer.test.result")],
        config=config,
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
