import functools
from logging import getLogger
from typing import Callable, List

from confluent_kafka import Consumer

from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig

logger = getLogger(__name__)


class ConsumerCollector(object):
    consumer_funcs: List[Callable]


def consumer(
    topics: List[HeizerTopic],
    config: HeizerConfig = HeizerConfig(),
    call_once: bool = False,
    stopper: Callable = None,
):
    def consumer_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

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

                result = None

                try:
                    result = func(msg=msg, *args, **kwargs)
                except Exception as e:
                    logger.error(
                        f"Failed to execute function {func.__name__}. {str(e)}"
                    )

                if stopper:
                    try:
                        should_stop = stopper(msg)
                    except Exception:
                        should_stop = False
                    if should_stop:
                        return result

                if call_once:
                    break

        return wrapper

    return consumer_decorator
