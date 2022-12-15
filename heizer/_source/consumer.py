import functools
from logging import getLogger
from typing import Any, Callable, List, Optional, cast

from confluent_kafka import Consumer

from heizer._source.message import HeizerMessage
from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig
from heizer.types import P, Stopper, T

logger = getLogger(__name__)


class ConsumerCollector(object):
    consumer_funcs: List[Callable[..., Any]]


def consumer(
    topics: List[HeizerTopic],
    config: HeizerConfig = HeizerConfig(),
    call_once: bool = False,
    stopper: Optional[Stopper] = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    :param topics:
    :param config:
    :param call_once:
    :param stopper:
    :return:
    """

    def consumer_decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:

            c = Consumer(config.value)

            c.subscribe([topic.name for topic in topics])

            while True:
                msg = c.poll(1)

                if msg is None:
                    continue

                if msg.error():
                    logger.error("Consumer error: {}".format(msg.error()))
                    continue

                c.commit()

                try:
                    cast(HeizerMessage, msg)
                    result = func(*args, msg=msg, **kwargs)
                except Exception as e:
                    logger.error(
                        f"Failed to execute function {func.__name__}. {str(e)}"
                    )
                    continue

                if stopper is not None:
                    try:
                        should_stop = stopper(msg)
                    except Exception:
                        should_stop = False
                    if should_stop:
                        return result

                if call_once:
                    return result

        return wrapper

    return consumer_decorator
