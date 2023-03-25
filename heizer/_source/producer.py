import functools
import json
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, cast

from confluent_kafka import Message, Producer

from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig

logger = getLogger(__name__)
PRODUCER_LIST = []

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])


def _default_encoder(value: Union[Dict[Any, Any], str, bytes]) -> bytes:
    """
    :param value:
    :return: bytes

    Default Kafka message encoder, which will encode inputs to bytes

    """
    if isinstance(value, dict):
        return json.dumps(value).encode("utf-8")
    elif isinstance(value, str):
        return value.encode("utf-8")
    elif isinstance(value, bytes):
        return value
    else:
        raise ValueError(f"Input type is not supported: {type(value).__name__}")


class producer(object):
    # args
    config: HeizerConfig = HeizerConfig()
    topics: List[HeizerTopic]
    message_encoder: Callable[..., bytes] = _default_encoder
    call_back: Optional[Callable[..., Any]] = None
    key: Optional[str] = None
    headers: Optional[Dict[str, str]] = None

    # private properties
    _producer_instance: Optional[Producer] = None

    def __init__(
        self,
        topics: List[HeizerTopic],
        config: HeizerConfig = HeizerConfig(),
        message_encoder: Callable[..., bytes] = _default_encoder,
        call_back: Optional[Callable[..., Any]] = None,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.topics = topics
        self.config = config
        self.message_encoder = message_encoder
        self.call_back = call_back
        self.key = key
        self.headers = headers

        PRODUCER_LIST.append(self)

    @property
    def _producer(self) -> Producer:
        if not self._producer_instance:
            self._producer_instance = Producer(self.config.value)
        return self._producer_instance

    def __call__(self, func: F) -> F:
        @functools.wraps(func)
        def decorator(*args: Any, **kwargs: Any) -> Any:
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                raise e

            msg = self.message_encoder(result)

            self._produce_message(message=msg)

            return result

        return cast(F, decorator)

    def _produce_message(self, message: bytes) -> None:
        for topic in self.topics:
            for partition in topic.partitions:
                try:
                    self._producer.poll(0)
                    self._producer.produce(
                        topic=topic.name,
                        value=message,
                        partition=partition,
                        key=self.key,
                        headers=self.headers,
                        on_delivery=self.call_back,
                    )
                    self._producer.flush()
                except Exception as e:
                    logger.error(f"Failed to produce msg. {str(e)}")
                    raise e


def delivery_report(err: str, msg: Message) -> None:
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().

    :param msg:
    :param err:
    :return:
    """
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))
