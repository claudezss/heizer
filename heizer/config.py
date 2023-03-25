import os
from typing import Optional

from heizer.types import KafkaConfig

DEFAULT_KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:9094")
DEFAULT_KAFKA_GROUP = os.environ.get("KAFKA_GROUP", "default")


class HeizerConfig(object):
    # confluent kafka configs
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration
    __kafka_config: KafkaConfig

    def __init__(self, config: Optional[KafkaConfig] = None):
        self.__kafka_config = config or {
            "bootstrap.servers": DEFAULT_KAFKA_BOOTSTRAP_SERVER,
            "group.id": DEFAULT_KAFKA_GROUP,
        }

    @property
    def value(self) -> KafkaConfig:
        """
        Return kafka configurations

        Returns
        -------
        config : dict
            The dict contains kafka configurations
        """
        return self.__kafka_config
