from confluent_kafka import Message

from heizer.types import Any, Dict, List, Optional, Tuple, Union


class HeizerMessage:
    """
    :class: HeizerMessage

    A utility class for parsing Kafka messages using the confluent_kafka library.

    Properties:
        - `message`: A `confluent_kafka.Message` object representing the Kafka message.
        - `topic`: Optional[str] - The topic of the Kafka message.
        - `partition`: int - The partition of the Kafka message.
        - `headers`: Optional[Dict[str, str]] - The headers of the Kafka message.
        - `key`: Optional[str] - The key of the Kafka message.
        - `value`: Optional[str] - The value of the Kafka message.
        - `formatted_value`: Optional[Any] - A formatted version of the value.
    """

    # initialized properties
    message: Message
    topic: Optional[str]
    partition: int
    headers: Optional[Dict[str, str]]
    key: Optional[str]
    value: Optional[str]

    formatted_value: Optional[Any] = None

    def __init__(self, message: Message):
        self.message = message
        self.topic = self._parse_topic(message.topic())
        self.partition = message.partition()
        self.headers = self._parse_headers(message.headers())
        self.key = self._parse_key(message.key())
        self.value = self._parse_value(message.value())

    @staticmethod
    def __bytes_to_str(b: bytes) -> str:
        """Convert bytes to string"""
        return str(b, "utf-8")

    def _parse_topic(self, topic: Union[str, bytes]) -> Optional[str]:
        """Parse topic to string"""
        if topic is None:
            return None
        return self.__bytes_to_str(topic) if isinstance(topic, bytes) else topic

    def _parse_headers(self, headers: Optional[List[Tuple[str, bytes]]]) -> Optional[Dict[str, str]]:
        """Parse headers to dict"""
        parsed_headers = {}
        if headers is None:
            return None
        for k, v in headers:
            parsed_headers.update({k: v if isinstance(v, str) else self.__bytes_to_str(v)})
        return parsed_headers

    def _parse_key(self, key: Union[str, bytes]) -> Optional[str]:
        """Parse key to string"""
        if key is None:
            return None
        return self.__bytes_to_str(key) if isinstance(key, bytes) else key

    def _parse_value(self, value: Union[str, bytes]) -> Optional[str]:
        """Parse value to string"""
        if value is None:
            return None
        return self.__bytes_to_str(value) if isinstance(value, bytes) else value
