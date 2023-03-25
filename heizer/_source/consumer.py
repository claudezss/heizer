import asyncio
import functools
import logging
from typing import Any, Awaitable, Callable, Concatenate, Coroutine, List, Optional, ParamSpec, Type, TypeVar, Union
from uuid import uuid4

from confluent_kafka import Consumer
from pydantic import BaseModel

from heizer._source import get_logger
from heizer._source.message import HeizerMessage
from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig
from heizer.types import Stopper

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
T = TypeVar("T")

logger = get_logger(__name__)


class consumer(object):
    __id__: str

    topics: List[HeizerTopic]
    config: HeizerConfig = HeizerConfig()
    call_once: bool = False
    stopper: Optional[Stopper] = None
    deserializer: Optional[Type[BaseModel]] = None

    is_async: bool = False

    def __init__(
        self,
        *,
        topics: List[HeizerTopic],
        config: HeizerConfig = HeizerConfig(),
        call_once: bool = False,
        stopper: Optional[Stopper] = None,
        deserializer: Optional[Type[BaseModel]] = None,
        is_async: bool = False,
    ):
        self.topics = topics
        self.config = config
        self.call_once = call_once
        self.stopper = stopper
        self.deserializer = deserializer
        self.__id__ = str(uuid4())
        self.is_async = is_async

    async def __run__(
        self,
        func: Callable[Concatenate[HeizerMessage, P], Union[T, Awaitable[T]]],
        c: Consumer,
        is_async: bool,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Union[Optional[T]]:  # ignore type
        """Run the consumer"""

        while True:
            msg = c.poll(1)

            if msg is None:
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            logger.debug("Received message")
            hmessage = HeizerMessage(msg)

            if self.deserializer is not None:
                logger.debug("Parsing message")
                try:
                    hmessage.formatted_value = self.deserializer.parse_raw(hmessage.value)
                except Exception as e:
                    logger.exception("Failed to deserialize message", exc_info=e)

            result = None

            logger.debug("Executing function")
            try:
                if is_async:
                    result = await func(hmessage, *args, **kwargs)  # type: ignore
                else:
                    result = func(hmessage, *args, **kwargs)
            except Exception as e:
                logger.exception(
                    f"Failed to execute function {func.__name__}",
                    exc_info=e,
                )
            finally:
                # TODO: add failed message to a retry queue
                logger.debug("Committing message")
                c.commit()

            if self.stopper is not None:
                logger.debug("Executing stopper function")
                try:
                    should_stop = self.stopper(hmessage)
                except Exception as e:
                    logger.warning(
                        f"Failed to execute stopper function {self.stopper.__name__}.",
                        exc_info=e,
                    )
                    should_stop = False

                if should_stop:
                    return result

            if self.call_once is True:
                logger.debug("Call Once is on, returning result")
                return result

    def __call__(
        self, func: Callable[Concatenate[HeizerMessage, P], T]
    ) -> Callable[P, Union[Coroutine[Any, Any, Optional[T]], T, None]]:
        logger.debug("Creating consumer")
        c = Consumer(self.config.value)

        logger.debug("Subscribing to topics")
        c.subscribe([topic.name for topic in self.topics])

        @functools.wraps(func)
        async def async_decorator(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            """Async decorator"""
            logging.debug("Running async decorator")
            return await self.__run__(func, c, self.is_async, *args, **kwargs)

        @functools.wraps(func)
        def decorator(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            """Sync decorator"""
            logging.debug("Running sync decorator")
            return asyncio.get_event_loop().run_until_complete(self.__run__(func, c, self.is_async, *args, **kwargs))

        return async_decorator if self.is_async else decorator
