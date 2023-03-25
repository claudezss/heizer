from typing import List, Optional

from confluent_kafka import TopicPartition


class HeizerTopic(TopicPartition):
    name: str
    _partitions: List[int]
    _topic_partitions: List[TopicPartition]

    def __init__(self, name: str, partitions: Optional[List[int]] = None):
        self._partitions = partitions or [-1]
        self._topic_partitions = []
        self.name = name

        for partition in self._partitions:
            self._topic_partitions.append(TopicPartition(topic=name, partition=partition))

    @property
    def partitions(self) -> List[int]:
        return self._partitions

    @property
    def topic_partitions(self) -> List[TopicPartition]:
        return self._topic_partitions
