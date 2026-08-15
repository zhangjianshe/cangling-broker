"""kafka-python shaped helpers so existing senders can target this broker."""

from __future__ import annotations

from .client import SatwayClient


class KafkaProducer:
    """Stand-in for ``kafka.KafkaProducer``.

    Keep calling ``send(topic, value)`` and ``flush()``. ``bootstrap_servers``
    is the cangling-broker (``host:port``). Extra Kafka kwargs are ignored.
    """

    def __init__(
        self,
        bootstrap_servers="",
        token=None,
        value_serializer=None,
        **_ignored,
    ):
        if isinstance(bootstrap_servers, (list, tuple)):
            if not bootstrap_servers:
                raise ValueError("bootstrap_servers is required")
            bootstrap_servers = bootstrap_servers[0]
        self._client = SatwayClient.connect(str(bootstrap_servers), token)
        self._value_serializer = value_serializer

    def send(self, topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
        payload = self._value_serializer(value) if self._value_serializer is not None else value
        return self._client.send(topic, payload)

    def flush(self, timeout=None):
        return None

    def close(self, timeout=None):
        self._client.close()
