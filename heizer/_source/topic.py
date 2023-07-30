from confluent_kafka import TopicPartition, admin

from heizer.types import Any, Dict, List, Optional


class Topic:
    name: str
    partition: int
    offset: Optional[int] = None
    metadata: Optional[str] = None
    leader_epoch: Optional[int] = None
    num_partitions: Optional[int] = None
    replication_factor: Optional[int] = None
    replica_assignment: Optional[List[Any]] = None
    config: Optional[Dict[Any, Any]] = None

    _topic_partition: TopicPartition
    _new_topic: admin.NewTopic

    def __init__(
        self,
        name: str,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
        metadata: Optional[str] = None,
        leader_epoch: Optional[int] = None,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        replica_assignment: Optional[List[Any]] = None,
        config: Optional[Dict[Any, Any]] = None,
    ):
        self.name = name
        self.partition = partition if partition is not None else -1
        self.offset = offset if offset is not None else -1
        self.metadata = metadata
        self.leader_epoch = leader_epoch
        self.num_partitions = num_partitions if num_partitions is not None else 1
        self.replication_factor = replication_factor
        self.replica_assignment = replica_assignment
        self.config = config

        topic_partition_args = {
            "topic": self.name,
            "partition": self.partition,
        }
        if self.offset is not None:
            topic_partition_args["offset"] = self.offset
        if self.metadata is not None:
            topic_partition_args["metadata"] = self.metadata
        if self.leader_epoch is not None:
            topic_partition_args["leader_epoch"] = self.leader_epoch

        self._topic_partition = TopicPartition(**topic_partition_args)

        new_topic_args = {"topic": self.name, "num_partitions": self.num_partitions}

        if self.replication_factor is not None:
            new_topic_args["replication_factor"] = self.replication_factor
        if self.replica_assignment is not None:
            new_topic_args["replica_assignment"] = self.replica_assignment
        if self.config is not None:
            new_topic_args["config"] = self.config

        self._new_topic = admin.NewTopic(**new_topic_args)
