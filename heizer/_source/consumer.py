import asyncio
import functools
import logging
import signal
from uuid import uuid4

from confluent_kafka import Consumer
from pydantic import BaseModel

from heizer._source import get_logger
from heizer._source.admin import create_new_topic, get_admin_client
from heizer._source.message import HeizerMessage
from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig
from heizer.types import (
    Any,
    Awaitable,
    Callable,
    Concatenate,
    Coroutine,
    List,
    Optional,
    ParamSpec,
    Stopper,
    Type,
    TypeVar,
    Union,
)

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
T = TypeVar("T")

logger = get_logger(__name__)


def signal_handler(signal: Any, frame: Any) -> None:
    global interrupted
    interrupted = True


signal.signal(signal.SIGINT, signal_handler)

interrupted = False


class consumer(object):
    """A decorator to create a consumer"""

    __id__: str
    name: Optional[str]

    topics: List[HeizerTopic]
    config: HeizerConfig = HeizerConfig()
    call_once: bool = False
    stopper: Optional[Stopper] = None
    deserializer: Optional[Type[BaseModel]] = None

    is_async: bool = False
    poll_timeout: int = 1

    init_topics: bool = False

    def __init__(
        self,
        *,
        topics: List[HeizerTopic],
        config: HeizerConfig = HeizerConfig(),
        call_once: bool = False,
        stopper: Optional[Stopper] = None,
        deserializer: Optional[Type[BaseModel]] = None,
        is_async: bool = False,
        name: Optional[str] = None,
        poll_timeout: Optional[int] = None,
        init_topics: bool = False,
    ):
        self.topics = topics
        self.config = config
        self.call_once = call_once
        self.stopper = stopper
        self.deserializer = deserializer
        self.__id__ = str(uuid4())
        self.name = name or self.__id__
        self.is_async = is_async
        self.poll_timeout = poll_timeout if poll_timeout is not None else 1
        self.init_topics = init_topics

    def __repr__(self) -> str:
        return self.name or self.__id__

    async def __run__(  # type: ignore
        self,
        func: Callable[Concatenate[HeizerMessage, P], Union[T, Awaitable[T]]],
        is_async: bool,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Union[Optional[T]]:
        """Run the consumer"""

        logger.debug(f"[{self}] Creating consumer ")
        c = Consumer(self.config.value)

        logger.debug(f"[{self}] Subscribing to topics {[topic.name for topic in self.topics]}")
        c.subscribe([topic.name for topic in self.topics])

        if self.init_topics:
            logger.debug(f"[{self}] Initializing topics")
            admin_client = get_admin_client(self.config)
            create_new_topic(admin_client, self.topics)

        while True:
            if interrupted:
                logger.debug(f"[{self}] Interrupted by keyboard")
                break

            msg = c.poll(self.poll_timeout)

            if msg is None:
                continue

            if msg.error():
                logger.error(f"[{self}] Consumer error: {msg.error()}")
                continue

            logger.debug(f"[{self}] Received message")
            hmessage = HeizerMessage(msg)

            if self.deserializer is not None:
                logger.debug(f"[{self}] Parsing message")
                try:
                    hmessage.formatted_value = self.deserializer.parse_raw(hmessage.value)
                except Exception as e:
                    logger.exception(f"[{self}] Failed to deserialize message", exc_info=e)

            result = None

            logger.debug(f"[{self}] Executing function {func.__name__}")
            try:
                if is_async:
                    result = await func(hmessage, *args, **kwargs)  # type: ignore
                else:
                    result = func(hmessage, *args, **kwargs)
            except Exception as e:
                logger.exception(
                    f"[{self}] Failed to execute function {func.__name__}",
                    exc_info=e,
                )
            finally:
                # TODO: add failed message to a retry queue
                logger.debug(f"[{self}] Committing message")
                c.commit()

            if self.stopper is not None:
                logger.debug(f"[{self}] Executing stopper function")
                try:
                    should_stop = self.stopper(hmessage)
                except Exception as e:
                    logger.warning(
                        f"[{self}] Failed to execute stopper function {self.stopper.__name__}.",
                        exc_info=e,
                    )
                    should_stop = False

                if should_stop:
                    return result

            if self.call_once is True:
                logger.debug(f"[{self}] Call Once is on, returning result")
                return result

    def __call__(
        self, func: Callable[Concatenate[HeizerMessage, P], T]
    ) -> Callable[P, Union[Coroutine[Any, Any, Optional[T]], T, None]]:
        @functools.wraps(func)
        async def async_decorator(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            """Async decorator"""
            logging.debug(f"[{self}] Running async decorator")
            return await self.__run__(func, self.is_async, *args, **kwargs)

        @functools.wraps(func)
        def decorator(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            """Sync decorator"""
            logging.debug(f"[{self}] Running sync decorator")
            return asyncio.run(self.__run__(func, self.is_async, *args, **kwargs))

        return async_decorator if self.is_async else decorator
