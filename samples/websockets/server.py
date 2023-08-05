import asyncio

import websockets

from heizer import ConsumerConfig, Message, Topic, consumer

consumer_config = ConsumerConfig(
    bootstrap_servers="localhost:9092",
    group_id="websockets_sample",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)

topics = [Topic(name="my.topic1")]


@consumer(topics=topics, config=consumer_config, is_async=True, init_topics=True, name="websocket_sample")
async def handler(message: Message, C: consumer, websocket, *args, **kwargs):
    print(C.name)
    await websocket.send(message.value)


async def main():
    async with websockets.serve(handler, "", 8001):
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    asyncio.run(main())
