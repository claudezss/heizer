# mypy: ignore-errors

import sys
from typing import Any, Awaitable, Callable, Coroutine, Dict, List, Optional, Tuple, Type, TypeVar, Union, cast

if sys.version_info.minor <= 10:
    from typing_extensions import Concatenate, ParamSpec
else:
    from typing import Concatenate, ParamSpec

KafkaConfig = Dict[str, Any]
Stopper = Callable[..., bool]


__all__ = [
    "Any",
    "Awaitable",
    "Callable",
    "Concatenate",
    "Coroutine",
    "Dict",
    "List",
    "Optional",
    "ParamSpec",
    "Stopper",
    "Type",
    "TypeVar",
    "Union",
    "cast",
    "KafkaConfig",
    "Tuple",
]
