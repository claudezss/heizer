from confluent_kafka.admin import AdminClient, NewTopic

from heizer._source import get_logger
from heizer._source.topic import HeizerTopic
from heizer.config import HeizerConfig
from heizer.types import List

logger = get_logger(__name__)


def get_admin_client(config: HeizerConfig) -> AdminClient:
    return AdminClient({"bootstrap.servers": config.value.get("bootstrap.servers")})


def create_new_topic(admin_client: AdminClient, topics: List[HeizerTopic]) -> None:
    new_topics = [NewTopic(topic.name, num_partitions=len(topic._partitions)) for topic in topics]
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
