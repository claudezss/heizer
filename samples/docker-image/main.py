import os
import time

from pydantic import BaseModel

from heizer import HeizerConfig, HeizerMessage, HeizerTopic, consumer, producer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "test_group")
IS_SCHEDULER = os.getenv("IS_SCHEDULER", "false")

consumer_config = HeizerConfig(
    {
        "bootstrap.servers": KAFKA_SERVER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }
)

producer_config = HeizerConfig(
    {
        "bootstrap.servers": KAFKA_SERVER,
    }
)

topics = [HeizerTopic(name="test.topic")]


class MyMessage(BaseModel):
    name: str
    value: int


@producer(
    topics=topics,
    config=producer_config,
    init_topics=False,
)
def producer(data: MyMessage):
    return data.dict()


def scheduler():
    i = 0
    while True:
        producer(MyMessage(name="count", value=i))
        time.sleep(1)
        i += 1


@consumer(
    topics=topics,
    config=consumer_config,
    init_topics=True,
    deserializer=MyMessage,
)
def data_consumer(message: HeizerMessage):
    print(
        f"Headers: {message.headers} \n",
        f"Key: {message.key} \n",
        f"Value: {message.value} \n",
        f"Topic: {message.topic} \n",
        f"Deserialized Data: {message.formatted_value} \n",
    )


if __name__ == "__main__":
    if IS_SCHEDULER == "true":
        scheduler()
    else:
        data_consumer()
