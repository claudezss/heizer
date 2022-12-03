from typing import Any, Callable, Dict, Generator, ParamSpec, TypeVar, Union

from heizer._source.message import HeizerMessage

KafkaConfig = Dict[str, str]
Stopper = Callable[..., bool]

P = ParamSpec("P")
T = TypeVar("T")

F = TypeVar("F", bound=Callable[..., Any])
ConsumerFunction = TypeVar("ConsumerFunction", bound=Callable[..., object])
_ConsumerFunc = Union[
    Callable[..., HeizerMessage],
    Callable[..., Generator[HeizerMessage, None, None]],
]
