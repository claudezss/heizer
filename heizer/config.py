from collections import defaultdict
from dataclasses import dataclass, field

from heizer.types import Any, Dict

# confluent kafka configs
# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration


@dataclass
class BaseConfig:
    """
    Base configurations
    """

    bootstrap_servers: str


@dataclass
class ProducerConfig(BaseConfig):
    """
    Configuration class for producer.
    """

    other_configs: Dict[str, Any] = field(default_factory=defaultdict)


@dataclass
class ConsumerConfig(BaseConfig):
    """
    Configuration class for consumer.
    """

    group_id: str
    auto_offset_reset: str = "earliest"
    other_configs: Dict[str, Any] = field(default_factory=defaultdict)
    enable_auto_commit: bool = False
