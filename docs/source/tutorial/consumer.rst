Basic Producer and Consumer
---------------------------

.. note::
    You need spin up a Kafka server before running this example.

1. Create producer and consumer configurations

.. ipython:: python

    from heizer import HeizerConfig, HeizerTopic, consumer, producer, HeizerMessage
    import json

    producer_config = HeizerConfig(
        {
            "bootstrap.servers": "localhost:9092",
        }
    )

    consumer_config = HeizerConfig(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "default",
            "auto.offset.reset": "earliest",
        }
    )

2. Create the topic

.. ipython:: python

    topics = [HeizerTopic(name="my.topic1.consumer.example")]

3. Create producer

.. ipython:: python

    @producer(
        topics=topics,
        config=producer_config,
        key_alias="key",
        headers_alias="headers",
    )
    def produce_data(status: str, result: str):
        return {
            "status": status,
            "result": result,
            "key": "my_key",
            "headers": {"my_header": "my_header_value"},
        }

4. Publish messages

.. ipython:: python

    produce_data("start", "1")

    produce_data("loading", "2")

    produce_data("success", "3")

    produce_data("postprocess", "4")

5. Create consumer

.. ipython:: python

    # Heizer expects consumer stopper func return Bool type result
    # For this example, consumer will stop and return value if
    # `status` is `success` in msg
    # If there is no stopper func, consumer will keep running forever

    def stopper(msg: HeizerMessage):
        data = json.loads(msg.value)
        if data["status"] == "success":
            return True
        return False

    @consumer(
        topics=topics,
        config=consumer_config,
        stopper=stopper,
    )
    def consume_data(message: HeizerMessage):
        data = json.loads(message.value)
        print(data)
        print(message.key)
        print(message.headers)
        return data["result"]

    result = consume_data()
    print("Expected Result:", result)
