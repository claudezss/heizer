import functools
import json
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Union, cast

from confluent_kafka import Message, Producer

from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig
from heizer.types import F, Stopper

logger = getLogger(__name__)


def _default_encoder(result: Union[Dict[Any, Any], str, bytes]) -> bytes:
    if isinstance(result, dict):
        return json.dumps(result).encode("utf-8")
    elif isinstance(result, str):
        return result.encode("utf-8")
    elif isinstance(result, bytes):
        return result
    else:
        raise ValueError(
            f"result type is not supported: {type(result).__name__}"
        )


def _produce_msgs(
    pd: Producer,
    topics: List[HeizerTopic],
    msg: Union[bytes, str],
    key: Optional[Union[bytes, str]] = None,
    headers: Optional[Dict[str, str]] = None,
    on_delivery: Optional[Stopper] = None,
) -> None:
    for topic in topics:
        for partition in topic.partitions:
            try:
                pd.poll(0)
                pd.produce(
                    topic=topic.name,
                    value=msg,
                    partition=partition,
                    key=key,
                    headers=headers,
                    on_delivery=on_delivery,
                )
                pd.flush()
            except Exception as e:
                logger.error(f"Failed to produce msg. {str(e)}")


def update_func_status(
    pd: Producer,
    func_name: str,
    topics: List[HeizerTopic],
    status: str,
    args: Any,
    kwargs: Dict[Any, Any],
) -> None:
    _produce_msgs(
        pd,
        topics,
        key=func_name,
        msg=f"Function `{func_name}`"
        f" with args: {str(args)}"
        f" kwargs: {str(kwargs)} {status}",
    )


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
        print(
            "Message delivered to {} [{}]".format(msg.topic(), msg.partition())
        )


def producer(
    topics: List[HeizerTopic],
    config: HeizerConfig = HeizerConfig(),
    error_topics: Optional[List[HeizerTopic]] = None,
    msg_encoder: Callable[..., bytes] = _default_encoder,
    error_encoder: Callable[..., bytes] = _default_encoder,
    call_back: Optional[Callable[..., Any]] = None,
    key: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    status_topics: Optional[List[HeizerTopic]] = None,
) -> Callable[[F], F]:
    """
    :param topics:
    :param config:
    :param error_topics:
    :param msg_encoder:
    :param error_encoder:
    :param call_back:
    :param key:
    :param headers:
    :param status_topics:
    :return: None

    """

    def producer_decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # initial producer
            p = Producer(config.value)

            if status_topics:
                update_func_status(
                    p, func.__name__, status_topics, "started", args, kwargs
                )

            try:
                result = func(*args, **kwargs)

            except Exception as e:

                error_msg = str(e)

                logger.error(f"Failed to execute function {func.__name__}")

                if error_topics:
                    _produce_msgs(
                        p,
                        error_topics,
                        msg=error_encoder(error_msg),
                        on_delivery=call_back,
                    )

                if status_topics:
                    update_func_status(
                        p, func.__name__, status_topics, "failed", args, kwargs
                    )
                raise e

            if status_topics:
                update_func_status(
                    p, func.__name__, status_topics, "finished", args, kwargs
                )

            _produce_msgs(
                p,
                topics,
                msg=msg_encoder(result),
                on_delivery=call_back,
                key=key,
                headers=headers,
            )

            return result

        return cast(F, wrapper)

    return producer_decorator
