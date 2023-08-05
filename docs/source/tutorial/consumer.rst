Basic Producer and Consumer
---------------------------

.. note::
    You need spin up a Kafka server before running this example.

1. Create producer and consumer configurations

.. ipython:: python

    from heizer import Topic, consumer, Producer, Message, ProducerConfig, ConsumerConfig, create_new_topics
    import json
    import uuid
    import asyncio

    producer_config =  ProducerConfig(bootstrap_servers="localhost:9092")

    consumer_config = ConsumerConfig(bootstrap_servers="localhost:9092", group_id="default")

2. Create the topic with 2 partitions

.. ipython:: python

    topics = [Topic(name=f"my.topic1.consumer.example.{uuid.uuid4()}", num_partitions=2)]
    create_new_topics(config=producer_config, topics=topics)

3. Create producer

.. ipython:: python

    pd = Producer(config=producer_config)

4. Publish messages synchronously to partition 0

.. ipython:: python

    for status, val in [("start", "1"), ("loading", "2"), ("success", "3"), ("postprocess", "4")]:
        pd.produce(
            topic=topics[0],
            key="my_key",
            value={"status": status, "result": val},
            headers={"k": "v"},
            partition=0,
            auto_flush=False
        )
    pd.flush()

5. Publish messages asynchronously to partition 1 ( it's faster than sync produce in most cases)

.. ipython:: python

    jobs = []
    async def produce():
        for status, val in [("start", "1"), ("loading", "2"), ("success", "3"), ("postprocess", "4")]:
            jobs.append(
                asyncio.ensure_future(
                    pd.async_produce(
                        topic=topics[0],
                        key="my_key",
                        value={"status": status, "result": val},
                        headers={"k": "v"},
                        partition=1,
                        auto_flush=False
                    )
                )
            )
        await asyncio.gather(*jobs)
        pd.flush()

    asyncio.run(produce())

6. Create consumer

.. ipython:: python

    # Heizer expects consumer stopper func return Bool type result
    # For this example, consumer will stop and return value if
    # `status` is `success` in msg
    # If there is no stopper func, consumer will keep running forever

    def stopper(msg: Message, C: consumer, *arg, **kargs):
        print(f"Consumer name: {C.name}")
        data = json.loads(msg.value)
        if data["status"] == "success":
            return True
        return False

    @consumer(
        topics=topics,
        config=consumer_config,
        stopper=stopper,
    )
    def consume_data(message: Message, *arg, **kwargs):
        data = json.loads(message.value)
        print(f"message data: {data}")
        print(f"message key: {message.key}")
        print(f"message headers: {message.headers}")
        return data["result"]

    result = consume_data()
    print("Expected Result (should be 3):", result)


7. More samples:

.. literalinclude :: ./../../../tests/test_consumer.py
       :language: python
