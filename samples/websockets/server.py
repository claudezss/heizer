import asyncio

import websockets

from heizer import HeizerConfig, HeizerTopic, consumer

consumer_config = HeizerConfig(
    {
        "bootstrap.servers": "0.0.0.0:9092",
        "group.id": "test",
        "auto.offset.reset": "earliest",
    }
)

topics = [HeizerTopic(name="my.topic1")]


@consumer(topics=topics, config=consumer_config, is_async=True)
async def handler(message, websocket, *args, **kwargs):
    await websocket.send(message.value)


async def main():
    async with websockets.serve(handler, "", 8001):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
