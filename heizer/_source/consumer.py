import functools
from logging import getLogger
from typing import Any, Callable, List, Optional, ParamSpec, TypeVar, cast

from confluent_kafka import Consumer

from heizer._source.message import HeizerMessage
from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig
from heizer.types import Stopper

logger = getLogger(__name__)

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
T = TypeVar("T")


class consumer(object):
    topics: List[HeizerTopic]
    config: HeizerConfig = HeizerConfig()
    call_once: bool = False
    stopper: Optional[Stopper] = None

    def __init__(
        self,
        topics: List[HeizerTopic],
        config: HeizerConfig = HeizerConfig(),
        call_once: bool = False,
        stopper: Optional[Stopper] = None,
    ):
        self.topics = topics
        self.config = config
        self.call_once = call_once
        self.stopper = stopper

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        c = Consumer(self.config.value)

        c.subscribe([topic.name for topic in self.topics])

        @functools.wraps(func)
        def decorator(*args: P.args, **kwargs: P.kwargs) -> T:

            while True:
                msg = c.poll(1)

                if msg is None:
                    continue

                if msg.error():
                    logger.error("Consumer error: {}".format(msg.error()))
                    continue

                try:
                    cast(HeizerMessage, msg)
                    result = func(*args, message=msg, **kwargs)
                    c.commit()
                except Exception as e:
                    logger.error(
                        f"Failed to execute function {func.__name__}. {str(e)}"
                    )
                    continue

                if self.stopper is not None:
                    try:
                        should_stop = self.stopper(msg)
                    except Exception:
                        should_stop = False
                    if should_stop:
                        return result

                if self.call_once:
                    return result

        return decorator
