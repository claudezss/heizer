from confluent_kafka.admin import AdminClient

from heizer._source import get_logger
from heizer._source.topic import Topic
from heizer.config import BaseConfig
from heizer.types import KafkaConfig, List, Union

logger = get_logger(__name__)


def get_admin_client(config: Union[BaseConfig, KafkaConfig]) -> AdminClient:
    """
    Create an admin client using the provided configuration.
    """
    if isinstance(config, BaseConfig):
        config_dict = {"bootstrap.servers": config.bootstrap_servers}
    else:
        config_dict = config
    return AdminClient(config_dict)


def create_new_topics(config: Union[BaseConfig, KafkaConfig], topics: List[Topic]) -> None:
    """
    Create new topics using the provided configuration.
    """
    admin_client = get_admin_client(config)
    new_topics = [topic._new_topic for topic in topics]
    fs = admin_client.create_topics(new_topics)

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info("Topic {} created".format(topic))
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                logger.info(f"Topic {topic} already exists")
                continue
            else:
                logger.exception(f"Failed to create topic {topic}", exc_info=e)


def delete_topics(config: Union[BaseConfig, KafkaConfig], topics: List[Topic]) -> None:
    """Delete topics using the provided configuration."""
    admin_client = get_admin_client(config)
    fs = admin_client.delete_topics([tp.name for tp in topics])

    # Wait for each operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info("Topic {} deleted".format(topic))
        except Exception as e:
            logger.exception(f"Failed to delete topic {topic}", exc_info=e)


def list_topics(config: Union[BaseConfig, KafkaConfig]) -> List[str]:
    """List topics using the provided configuration."""
    admin_client = get_admin_client(config)
    return list(admin_client.list_topics().topics.keys())
