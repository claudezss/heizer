from typing import Any, List, Optional, Tuple

from confluent_kafka import Message


class HeizerMessage:
    # initialized properties
    message: Message
    topic: Optional[str]
    partition: int
    headers: Optional[dict[str, str]]
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

    def _parse_topic(self, topic: str | bytes) -> Optional[str]:
        """Parse topic to string"""
        if topic is None:
            return None
        return self.__bytes_to_str(topic) if isinstance(topic, bytes) else topic

    def _parse_headers(self, headers: Optional[List[Tuple[str, bytes]]]) -> Optional[dict[str, str]]:
        """Parse headers to dict"""
        parsed_headers = {}
        if headers is None:
            return None
        for k, v in headers:
            parsed_headers.update({k: v if isinstance(v, str) else self.__bytes_to_str(v)})
        return parsed_headers

    def _parse_key(self, key: str | bytes) -> Optional[str]:
        """Parse key to string"""
        if key is None:
            return None
        return self.__bytes_to_str(key) if isinstance(key, bytes) else key

    def _parse_value(self, value: str | bytes) -> Optional[str]:
        """Parse value to string"""
        if value is None:
            return None
        return self.__bytes_to_str(value) if isinstance(value, bytes) else value
