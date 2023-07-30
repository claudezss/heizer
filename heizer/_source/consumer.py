import asyncio
import atexit
import functools
import logging
import signal
from dataclasses import dataclass
from uuid import uuid4

from confluent_kafka import Consumer

from heizer._source import get_logger
from heizer._source.admin import create_new_topic
from heizer._source.message import Message
from heizer._source.topic import Topic
from heizer.config import ConsumerConfig
from heizer.types import (
    Any,
    Awaitable,
    Callable,
    Concatenate,
    Coroutine,
    KafkaConfig,
    List,
    Optional,
    ParamSpec,
    Stopper,
    TypeVar,
    Union,
)

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
T = TypeVar("T")

logger = get_logger(__name__)


def _make_consumer_config_dict(config: ConsumerConfig) -> KafkaConfig:
    config_dict = {
        "bootstrap.servers": config.bootstrap_servers,
        "group.id": config.group_id,
        "auto.offset.reset": config.auto_offset_reset,
        "enable.auto.commit": config.enable_auto_commit,
    }

    if config.other_configs:
        config_dict.update(config.other_configs)

    return config_dict


@dataclass
class ConsumerSignal:
    is_running: bool = True

    def stop(self) -> None:
        self.is_running = False


class consumer(object):
    """A decorator to create a consumer"""

    __id__: str
    name: Optional[str]

    topics: List[Topic]
    config: KafkaConfig
    call_once: bool = False
    stopper: Optional[Stopper] = None
    deserializer: Optional[Callable] = None
    consumer_signal: ConsumerSignal
    is_async: bool = False
    poll_timeout: int = 1
    init_topics: bool = False

    # private attr
    _consumer_instance: Consumer

    def __init__(
        self,
        *,
        topics: List[Topic],
        config: Union[KafkaConfig, ConsumerConfig],
        call_once: bool = False,
        stopper: Optional[Stopper] = None,
        deserializer: Optional[Callable] = None,
        is_async: bool = False,
        name: Optional[str] = None,
        poll_timeout: Optional[int] = None,
        init_topics: bool = False,
        consumer_signal: Optional[ConsumerSignal] = None,
    ):
        self.topics = topics
        self.config = _make_consumer_config_dict(config) if isinstance(config, ConsumerConfig) else config
        self.call_once = call_once
        self.stopper = stopper
        self.deserializer = deserializer
        self.__id__ = str(uuid4())
        self.name = name or self.__id__
        self.is_async = is_async
        self.poll_timeout = poll_timeout if poll_timeout is not None else 1
        self.init_topics = init_topics
        self.consumer_signal = consumer_signal or ConsumerSignal()

        if self.init_topics:
            logger.debug(f"[{self}] Initializing topics")
            create_new_topic({"bootstrap.servers": self.config["bootstrap.servers"]}, self.topics)

    def __repr__(self) -> str:
        return self.name or self.__id__

    async def _long_run(self, f: Callable, is_async: bool, *args, **kwargs):
        logger.debug(f"[{self}] Subscribing to topics {[topic.name for topic in self.topics]}")
        self._consumer_instance.subscribe([topic.name for topic in self.topics])

        result = None

        while self.consumer_signal.is_running:
            result = None

            msg = self._consumer_instance.poll(self.poll_timeout)

            if msg is None:
                continue

            if msg.error():
                logger.error(f"[{self}] Consumer error: {msg.error()}")
                continue

            logger.debug(f"[{self}] Received message")

            h_message = Message(msg)

            if self.deserializer is not None:
                logger.debug(f"[{self}] Parsing message")
                try:
                    h_message.formatted_value = self.deserializer(h_message.value)
                except Exception as e:
                    logger.exception(f"[{self}] Failed to deserialize message", exc_info=e)

            logger.debug(f"[{self}] Executing function {f.__name__}")

            try:
                if is_async:
                    result = await f(h_message, self, *args, **kwargs)  # type: ignore
                else:
                    result = f(h_message, self, *args, **kwargs)
            except Exception as e:
                logger.exception(
                    f"[{self}] Failed to execute function {f.__name__}",
                    exc_info=e,
                )
            finally:
                # TODO: add failed message to a retry queue
                logger.debug(f"[{self}] Committing message")
                self._consumer_instance.commit()

            if self.stopper is not None:
                logger.debug(f"[{self}] Executing stopper function")
                try:
                    should_stop = self.stopper(h_message)
                except Exception as e:
                    logger.warning(
                        f"[{self}] Failed to execute stopper function {self.stopper.__name__}.",
                        exc_info=e,
                    )
                    should_stop = False

                if should_stop:
                    self.consumer_signal.stop()
                    break

            if self.call_once is True:
                logger.debug(f"[{self}] Call Once is on, returning result")
                self.consumer_signal.stop()
                break

        return result

    async def _run(  # type: ignore
        self,
        func: Callable[Concatenate[Message, P], Union[T, Awaitable[T]]],
        is_async: bool,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Union[Optional[T]]:
        """Run the consumer"""

        logger.debug(f"[{self}] Creating consumer ")
        self._consumer_instance = Consumer(self.config)

        atexit.register(self._atexit)
        signal.signal(signal.SIGTERM, self._exit)
        signal.signal(signal.SIGINT, self._exit)

        result = None

        try:
            result = await self._long_run(func, is_async, *args, **kwargs)
        except KeyboardInterrupt:
            logger.info(
                f"[{self}] stopped because of keyboard interruption",
            )
        except Exception as e:
            logger.exception(f"[{self}] stopped", exc_info=e)
        finally:
            self.consumer_signal.stop()
            self._consumer_instance.close()
        return result

    def __call__(
        self, func: Callable[Concatenate[Message, P], T]
    ) -> Callable[P, Union[Coroutine[Any, Any, Optional[T]], T, None]]:
        @functools.wraps(func)
        async def async_decorator(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            """Async decorator"""
            logging.debug(f"[{self}] Running async decorator")
            return await self._run(func, self.is_async, *args, **kwargs)

        @functools.wraps(func)
        def decorator(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            """Sync decorator"""
            logging.debug(f"[{self}] Running sync decorator")
            return asyncio.run(self._run(func, self.is_async, *args, **kwargs))

        return async_decorator if self.is_async else decorator

    def _exit(self, sig, frame, *args, **kwargs) -> None:
        self.consumer_signal.stop()
        self._consumer_instance.close()

    def _atexit(self) -> None:
        self._consumer_instance.close()
