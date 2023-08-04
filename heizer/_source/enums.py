import enum


class ConsumerStatusEnum(str, enum.Enum):
    RUNNING = "running"
    CLOSED = "closed"
