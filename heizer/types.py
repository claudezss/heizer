from typing import Callable, Dict

KafkaConfig = Dict[str, str]
Stopper = Callable[..., bool]
