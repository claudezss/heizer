import functools
import json
from uuid import uuid4

from confluent_kafka import Message, Producer

from heizer._source import get_logger
from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig
from heizer.types import Any, Callable, Dict, List, Optional, TypeVar, Union, cast

logger = get_logger(__name__)

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Dict[str, str]])


def _default_serializer(value: Union[Dict[Any, Any], str, bytes]) -> bytes:
    """
    :param value:
    :return: bytes

    Default Kafka message serializer, which will encode inputs to bytes

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

    """
    A decorator to create a producer
    """

    __id__: str
    name: str

    # args
    config: HeizerConfig = HeizerConfig()
    topics: List[HeizerTopic]
    serializer: Callable[..., bytes] = _default_serializer
    call_back: Optional[Callable[..., Any]] = None

    default_key: Optional[str] = None
    default_headers: Optional[Dict[str, str]] = None

    key_alias: str = "key"
    headers_alias: str = "headers"

    # private properties
    _producer_instance: Optional[Producer] = None

    def __init__(
        self,
        topics: List[HeizerTopic],
        config: HeizerConfig = HeizerConfig(),
        serializer: Callable[..., bytes] = _default_serializer,
        call_back: Optional[Callable[..., Any]] = None,
        default_key: Optional[str] = None,
        default_headers: Optional[Dict[str, str]] = None,
        key_alias: Optional[str] = None,
        headers_alias: Optional[str] = None,
        name: Optional[str] = None,
        init_topics: bool = True,
    ):
        self.topics = topics
        self.config = config
        self.serializer = serializer
        self.call_back = call_back
        self.default_key = default_key
        self.default_headers = default_headers

        if key_alias:
            self.key_alias = key_alias
        if headers_alias:
            self.headers_alias = headers_alias

        self.__id__ = str(uuid4())
        self.name = name or self.__id__

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

            try:
                key = result.pop(self.key_alias, self.default_key)
            except Exception as e:
                logger.debug(f"Failed to get key from result. {str(e)}")
                key = self.default_key

            try:
                headers = result.pop(self.headers_alias, self.default_headers)
            except Exception as e:
                logger.debug(
                    f"Failed to get headers from result. {str(e)}",
                )
                headers = self.default_headers

            headers = cast(Dict[str, str], headers)

            msg = self.serializer(result)

            self._produce_message(message=msg, key=key, headers=headers)

            return result

        return cast(F, decorator)

    def _produce_message(self, message: bytes, key: Optional[str], headers: Optional[Dict[str, str]]) -> None:
        for topic in self.topics:
            for partition in topic.partitions:
                try:
                    self._producer.poll(0)
                    self._producer.produce(
                        topic=topic.name,
                        value=message,
                        partition=partition,
                        key=key,
                        headers=headers,
                        on_delivery=self.call_back,
                    )
                    self._producer.flush()
                except Exception as e:
                    logger.exception(f"Failed to produce msg to topic: {topic.name}", exc_info=e)
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
        logger.error("Message delivery failed: {}".format(err))
    else:
        logger.debug("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))
