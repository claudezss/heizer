import asyncio
import json
from uuid import uuid4

import confluent_kafka as ck

from heizer._source import get_logger
from heizer._source.topic import Topic
from heizer.config import ProducerConfig
from heizer.types import Any, Callable, Dict, KafkaConfig, Optional, Union

logger = get_logger(__name__)


def _create_producer_config_dict(config: ProducerConfig) -> KafkaConfig:
    config_dict = {"bootstrap.servers": config.bootstrap_servers}

    if config.other_configs:
        config_dict.update(config.other_configs)

    return config_dict


def _default_serializer(value: Union[Dict[Any, Any], str, bytes]) -> bytes:
    """
    :param value:
    :return: bytes

    Default Kafka message serializer, which will encode inputs to bytes

    """
    if isinstance(value, dict) or isinstance(value, list):
        return json.dumps(value).encode("utf-8")
    elif isinstance(value, str):
        return value.encode("utf-8")
    elif isinstance(value, bytes):
        return value
    else:
        raise ValueError(f"Input type is not supported: {type(value).__name__}")


class Producer(object):

    """
    Kafka Message Producer
    """

    __id__: str
    name: str

    # attrs need be initialized
    config: KafkaConfig
    serializer: Callable[..., bytes] = _default_serializer
    call_back: Optional[Callable[..., Any]] = None

    # private properties
    _producer_instance: Optional[ck.Producer] = None

    def __init__(
        self,
        config: Union[ProducerConfig, KafkaConfig],
        serializer: Callable[..., bytes] = _default_serializer,
        call_back: Optional[Callable[..., Any]] = None,
        name: Optional[str] = None,
    ):
        self.config = _create_producer_config_dict(config) if isinstance(config, ProducerConfig) else config
        self.serializer = serializer
        self.call_back = call_back or self.default_delivery_report

        self.__id__ = str(uuid4())
        self.name = name or self.__id__

    def __repr__(self) -> str:
        return self.name

    def default_delivery_report(self, err: str, msg: ck.Message) -> None:
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().

        :param msg:
        :param err:
        :return:
        """
        if err is not None:
            logger.error(f"[{self}] Message delivery failed: {err}")
        else:
            logger.debug(f"[{self}] Message delivered to {msg.topic()} [{msg.partition()}]")

    @property
    def _producer(self) -> ck.Producer:
        if not self._producer_instance:
            self._producer_instance = ck.Producer(self.config)
        return self._producer_instance

    def produce(
        self,
        topic: Union[str, Topic],
        value: Union[bytes, str, Dict[Any, Any]],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = -1,
        auto_flush: bool = True,
    ) -> None:
        """
        This method is used to produce messages to a Kafka topic using the confluent_kafka library.

        :param topic: The topic to publish the message to. Can be either a string representing the topic name,
         or an instance of the Topic class.
        :param value: The message body to be produced. Can be either bytes, a string, or a dictionary.
        :param key: Optional. The key for the message.
        :param headers: Optional. Additional headers for the message.
        :param auto_flush: Optional. Default is True. If True, the producer will automatically flush messages after
         producing them.

        :return: None

        Example usage:

        >>> producer = Producer(config={"xx": "xx"})
        >>> producer.produce(topic="my_topic", value={"k":"v"})

        """
        return asyncio.run(
            self._produce(
                topic=topic, value=value, key=key, headers=headers, partition=partition, auto_flush=auto_flush
            )
        )

    async def async_produce(
        self,
        topic: Union[str, Topic],
        value: Union[bytes, str, Dict[Any, Any]],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = -1,
        auto_flush: bool = True,
    ) -> None:
        """
        Produce a message asynchronously to a Kafka topic.

        :param topic: The topic to produce the message to. Can be either a string or a `Topic` object.
        :param value: The message body to be produced. Can be either bytes, a string, or a dictionary.
        :param key: The key to associate with the message. Optional.
        :param headers: Additional headers to include with the message. Optional.
        :param auto_flush: If set to True, automatically flush the producer after producing the message.
         Default is True.
        :return: None

        This method asynchronously produces a message to a Kafka topic. It accepts the topic, message, key, headers,
        and auto_flush parameters.

        Example usage:

        >>> producer = Producer(config={"xx": "xx"})
        >>> asyncio.run(producer.async_produce(topic="my_topic", value={"k":"v"}))
        """
        return await self._produce(
            topic=topic, value=value, key=key, headers=headers, partition=partition, auto_flush=auto_flush
        )

    async def _produce(
        self,
        topic: Union[str, Topic],
        value: Union[bytes, str, Dict[Any, Any]],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = -1,
        auto_flush: bool = True,
    ) -> None:
        topic = Topic(name=topic) if isinstance(topic, str) else topic

        try:
            self._producer.produce(
                topic=topic.name,
                value=self.serializer(value),
                key=key,
                headers=headers,
                on_delivery=self.call_back,
                partition=partition,
            )
            if auto_flush:
                self.flush()
        except ck.error.KafkaError.QUEUE_FULL:
            logger.warning(f"[{self}] Local queue is full, force to flush")
            self.flush()
        except Exception as e:
            logger.exception(f"[{self}] Failed to produce msg to topic: {topic.name}", exc_info=e)
            raise e

    def flush(self) -> None:
        self._producer.flush()
