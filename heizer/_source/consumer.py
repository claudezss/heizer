import asyncio
import atexit
import functools
import logging
import os
import signal
from dataclasses import dataclass
from uuid import uuid4

import confluent_kafka as ck

from heizer._source import get_logger
from heizer._source.admin import create_new_topics
from heizer._source.enums import ConsumerStatusEnum
from heizer._source.message import Message
from heizer._source.producer import Producer
from heizer._source.status_manager import write_consumer_status
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
    """
    A decorator to create a consumer
    """

    id: str
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

    enable_retry: bool = False
    retry_topic: Optional[Topic] = None
    retry_times: int = 1
    retry_times_header_key: str = "!!-heizer_func_retried_times-!!"

    # private attr
    _consumer_instance: ck.Consumer
    _producer_instance: Optional[Producer] = None  # for retry

    def __init__(
        self,
        *,
        topics: List[Topic],
        config: Union[KafkaConfig, ConsumerConfig],
        call_once: bool = False,
        stopper: Optional[Stopper] = None,
        deserializer: Optional[Callable] = None,
        is_async: bool = False,
        id: Optional[str] = None,
        name: Optional[str] = None,
        poll_timeout: Optional[int] = None,
        init_topics: bool = False,
        consumer_signal: Optional[ConsumerSignal] = None,
        enable_retry: bool = False,
        retry_topic: Optional[Topic] = None,
        retry_times: int = 1,
    ):
        self.topics = topics
        self.config = _make_consumer_config_dict(config) if isinstance(config, ConsumerConfig) else config
        self.call_once = call_once
        self.stopper = stopper
        self.deserializer = deserializer
        self.id = id or str(uuid4())
        self.name = name or self.id
        self.is_async = is_async
        self.poll_timeout = poll_timeout if poll_timeout is not None else 1
        self.init_topics = init_topics
        self.consumer_signal = consumer_signal or ConsumerSignal()
        self.enable_retry = enable_retry
        self.retry_topic = retry_topic
        self.retry_times = retry_times

    def __repr__(self) -> str:
        return self.name or self.id

    @property
    def ck_consumer(self) -> ck.Consumer:
        return self._consumer_instance

    @property
    def ck_producer(self) -> Optional[ck.Producer]:
        return self._producer_instance

    def commit(self, asynchronous: bool = False, *arg, **kwargs):
        self.ck_consumer.commit(asynchronous=asynchronous, *arg, **kwargs)

    async def _long_run(self, f: Callable, is_async: bool, *args, **kwargs):
        target_topics: List[Topic] = [topic for topic in self.topics]

        if self.enable_retry and self.retry_topic:
            target_topics.append(self.retry_topic)

        logger.info(f"[{self}] Subscribing to topics {[topic.name for topic in target_topics]}")
        self.ck_consumer.subscribe([topic.name for topic in target_topics])

        result = None

        while self.consumer_signal.is_running:
            write_consumer_status(
                consumer_id=self.id, consumer_name=self.name, pid=os.getpid(), status=ConsumerStatusEnum.RUNNING
            )

            result = None

            msg = self.ck_consumer.poll(self.poll_timeout)

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

            # If heizer retry times header key exists, remove it when passing to user func
            has_retired: Optional[int] = None
            if h_message.headers and self.retry_times_header_key in h_message.headers:
                has_retired = int(h_message.headers.pop(self.retry_times_header_key))

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
                if self.enable_retry:
                    logger.info(f"[{self}] Start producing retry message for function {f.__name__}")
                    await self.produce_retry_message(message=h_message, has_retired=has_retired, func_name=f.__name__)
            finally:
                logger.debug(f"[{self}] Committing message")
                self.commit()

            if self.stopper is not None:
                logger.debug(f"[{self}] Executing stopper function")
                try:
                    should_stop = self.stopper(h_message, self)
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

        if self.init_topics:
            logger.info(f"[{self}] Initializing topics")
            create_new_topics({"bootstrap.servers": self.config["bootstrap.servers"]}, self.topics)

        logger.info(f"[{self}] Creating consumer")
        self._consumer_instance = ck.Consumer(self.config)

        if self.enable_retry:
            self.retry_topic = self.retry_topic or Topic(name=f"{func.__name__}_heizer_retry")
            self._producer_instance = Producer(config={"bootstrap.servers": self.config["bootstrap.servers"]})

            logger.info(f"[{self}] Creating retry topic {self.retry_topic.name}")
            create_new_topics({"bootstrap.servers": self.config["bootstrap.servers"]}, [self.retry_topic])

        atexit.register(self._atexit)
        signal.signal(signal.SIGTERM, self._exit)
        signal.signal(signal.SIGINT, self._exit)

        result = None

        try:
            result = await self._long_run(func, is_async, *args, **kwargs)
        except KeyboardInterrupt:
            logger.info(
                f"[{self}] Stopped because of keyboard interruption",
            )
        except Exception as e:
            logger.exception(f"[{self}] stopped", exc_info=e)
        finally:
            self.consumer_signal.stop()
            self.ck_consumer.close()
            write_consumer_status(
                consumer_id=self.id, consumer_name=self.name, pid=os.getpid(), status=ConsumerStatusEnum.CLOSED
            )
            logger.info(
                f"[{self}] Closed",
            )
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
        self.ck_consumer.close()

    def _atexit(self) -> None:
        self.ck_consumer.close()

    async def produce_retry_message(self, message: Message, has_retired: Optional[int], func_name: str) -> None:
        """
        Produces a retry message for a given function.

        :param message: The original message that needs to be retried.
        :type message: Message
        :param has_retired: The number of times the function has been retried before. Defaults to None.
        :type has_retired: Optional[int]
        :param func_name: The name of the function.
        :type func_name: str
        :return: None
        """
        if not self._producer_instance:
            logger.error(
                f"[{self}] Confluent producer instance not found,"
                f" failed to produce retry message for function {func_name}"
            )
            return

        if not self.retry_topic:
            logger.error(f"[{self}] Retry topic not found, failed to produce retry message for function {func_name}")
            return

        if has_retired == self.retry_times:
            logger.info(f"[{self}] Function {func_name} reached retry limit ({self.retry_times}), will give up")
            return
        elif has_retired is None:
            has_retired = 1
        else:
            has_retired += 1

        new_headers = message.headers or {}
        new_headers[self.retry_times_header_key] = str(has_retired)

        await self._producer_instance.async_produce(
            topic=self.retry_topic, key=message.key, value=message.value or "", headers=new_headers, auto_flush=True
        )
        logger.info(f"[{self}] Produced retry message for function {func_name}, retry time(s): {has_retired}")
