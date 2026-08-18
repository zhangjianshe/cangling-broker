from __future__ import annotations

from collections.abc import Callable
import logging
import threading
import time
import uuid

import grpc

from .models import SatwayMessage, SendResult, SubscribeOptions, TopicConfig, auth_token_from_env
from .proto import queue_pb2, queue_pb2_grpc


def _sdk_version() -> str:
    try:
        from importlib.metadata import version

        return "python/" + version("cangling-broker")
    except Exception:
        return "python"

LOG = logging.getLogger("cangling_broker")

INITIAL_BACKOFF_SECS = 0.2
MAX_BACKOFF_SECS = 5.0
RPC_DEADLINE_SECS = 15
RETRYABLE = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.ABORTED,
    grpc.StatusCode.UNKNOWN,
}

Handler = Callable[[SatwayMessage], None]


def _broker_target(broker: str) -> str:
    broker = broker.strip()
    if broker.startswith("http://"):
        return broker[len("http://") :]
    if broker.startswith("https://"):
        return broker[len("https://") :]
    return broker


def _auth_metadata(token: str | None) -> list[tuple[str, str]]:
    metadata = [("x-client-version", _sdk_version())]
    token = (token or "").strip()
    if not token:
        return metadata
    if not token.lower().startswith("bearer "):
        token = "Bearer " + token
    metadata.append(("authorization", token))
    return metadata


def _with_client_version(attributes: dict[str, str] | None) -> dict[str, str]:
    attrs = dict(attributes or {})
    attrs.setdefault("version", _sdk_version())
    return attrs


class SatwayClient:
    """Broker client. Owns the gRPC channel and retries unary RPCs.

    Each :meth:`subscribe` stream reopens on the same ``consumer_id`` after a drop.
    """

    def __init__(self, channel: grpc.Channel, metadata: list[tuple[str, str]]):
        self._channel = channel
        self._stub = queue_pb2_grpc.MessageQueueStub(channel)
        self._metadata = metadata
        self._open = True
        self._consumers: list[Consumer] = []
        self._lock = threading.Lock()

    @classmethod
    def connect(cls, broker: str, token: str | None = None) -> SatwayClient:
        if not broker or not broker.strip():
            raise ValueError("broker is required")
        if token is None:
            token = auth_token_from_env()
        channel = grpc.insecure_channel(_broker_target(broker))
        return cls(channel, _auth_metadata(token))

    def send(
        self,
        topic: str,
        payload: str | bytes,
        idempotency_key: str = "",
        attributes: dict[str, str] | None = None,
    ) -> SendResult:
        if not topic or not topic.strip():
            raise ValueError("topic is required")
        if payload is None or payload == "" or payload == b"":
            raise ValueError("payload is required")
        body = payload.encode("utf-8") if isinstance(payload, str) else payload
        key = idempotency_key.strip() if idempotency_key else str(uuid.uuid4())
        attrs = attributes or {}

        def once() -> SendResult:
            request = queue_pb2.AcceptMessageRequest(
                idempotency_key=key,
                topic=topic,
                payload=body,
                attributes=attrs,
            )
            for response in self._stub.AcceptMessages(
                iter([request]),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ):
                return SendResult(response.message_id, response.duplicate)
            raise RuntimeError("publish stream closed without a response")

        return self._call_with_reconnect("publish", once)

    def register(
        self,
        topic: str,
        name: str = "",
        attributes: dict[str, str] | None = None,
        consumer_id: str = "",
    ) -> str:
        def once() -> str:
            return self._stub.Register(
                queue_pb2.RegisterRequest(
                    topic=topic,
                    consumer_id=consumer_id or "",
                    name=name or "",
                    attributes=_with_client_version(attributes),
                ),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).consumer_id

        return self._call_with_reconnect("register", once)

    def configure_topics(self, topics: list[TopicConfig]) -> list[TopicConfig]:
        if not topics:
            raise ValueError("topics is required")

        def once() -> list[TopicConfig]:
            response = self._stub.ConfigureTopics(
                queue_pb2.ConfigureTopicsRequest(
                    topics=[
                        queue_pb2.TopicConfig(
                            topic=item.topic,
                            delivery=item.delivery,
                            persistence=item.persistence,
                        )
                        for item in topics
                    ]
                ),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )
            return [
                TopicConfig(
                    topic=item.topic,
                    delivery=item.delivery,
                    persistence=item.persistence,
                )
                for item in response.topics
            ]

        return self._call_with_reconnect("configure_topics", once)

    def list_topics(self) -> list[TopicConfig]:
        def once() -> list[TopicConfig]:
            response = self._stub.ListTopics(
                queue_pb2.ListTopicsRequest(),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )
            return [
                TopicConfig(
                    topic=item.topic,
                    delivery=item.delivery,
                    persistence=item.persistence,
                )
                for item in response.topics
            ]

        return self._call_with_reconnect("list_topics", once)

    def unregister(self, consumer_id: str) -> None:
        if not consumer_id:
            return

        def once() -> None:
            self._stub.Unregister(
                queue_pb2.UnregisterRequest(consumer_id=consumer_id),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )

        self._call_with_reconnect("unregister", once)

    def subscribe(
        self,
        topic: str | SubscribeOptions,
        handler: Handler,
        *,
        name: str = "",
        consumer_id: str = "",
        attributes: dict[str, str] | None = None,
    ) -> Consumer:
        if handler is None:
            raise ValueError("handler is required")
        if isinstance(topic, SubscribeOptions):
            options = topic
        else:
            options = SubscribeOptions(
                topic=topic,
                name=name,
                consumer_id=consumer_id,
                attributes=attributes or {},
            )
        cid = options.consumer_id
        if options.name or options.attributes or options.consumer_id:
            cid = self.register(
                options.topic,
                name=options.name,
                attributes=_with_client_version(dict(options.attributes)),
                consumer_id=options.consumer_id,
            )
        consumer = Consumer(self, options, cid, handler)
        with self._lock:
            self._consumers.append(consumer)
        return consumer

    def close(self) -> None:
        with self._lock:
            if not self._open:
                return
            self._open = False
            consumers = list(self._consumers)
        for consumer in consumers:
            consumer.close()
        self._channel.close()

    def __enter__(self) -> SatwayClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _is_open(self) -> bool:
        return self._open

    def _ack(self, message_id: str, lease: str, success: bool, error: str = "") -> None:
        def once() -> None:
            self._stub.AckMessage(
                queue_pb2.AckMessageRequest(
                    message_id=message_id,
                    lease=lease,
                    success=success,
                    error=error or "",
                ),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )

        self._call_with_reconnect("ack", once)

    def _ensure_registered(self, options: SubscribeOptions, consumer_id: str) -> None:
        if not consumer_id:
            return
        self.register(
            options.topic,
            name=options.name,
            attributes=_with_client_version(dict(options.attributes)),
            consumer_id=consumer_id,
        )

    def _subscribe_stream(self, topic: str, consumer_id: str):
        return self._stub.Subscribe(
            queue_pb2.SubscribeRequest(topic=topic, consumer_id=consumer_id or ""),
            metadata=self._metadata,
        )

    def _call_with_reconnect(self, op: str, call):
        backoff = INITIAL_BACKOFF_SECS
        while True:
            if not self._open:
                raise RuntimeError("client closed")
            try:
                return call()
            except grpc.RpcError as error:
                if not self._open:
                    raise RuntimeError("client closed") from error
                if error.code() not in RETRYABLE:
                    raise RuntimeError(f"{op} failed: {error.code()}: {error.details()}") from error
                LOG.warning("%s failed, reconnecting: %s", op, error.details() or error.code())
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECS)


class Consumer:
    def __init__(
        self,
        client: SatwayClient,
        options: SubscribeOptions,
        consumer_id: str,
        handler: Handler,
    ):
        self._client = client
        self._closed = False
        self.consumer_id = consumer_id
        self._thread = threading.Thread(
            target=self._run,
            args=(options, handler),
            name="cangling-subscribe",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> Consumer:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _running(self) -> bool:
        return not self._closed and self._client._is_open()

    def _run(self, options: SubscribeOptions, handler: Handler) -> None:
        backoff = INITIAL_BACKOFF_SECS
        while self._running():
            try:
                self._client._ensure_registered(options, self.consumer_id)
                stream = self._client._subscribe_stream(options.topic, self.consumer_id)
                backoff = INITIAL_BACKOFF_SECS
                for incoming in stream:
                    if not self._running():
                        return
                    message = _to_message(incoming)
                    try:
                        handler(message)
                        self._client._ack(incoming.message_id, incoming.lease, True, "")
                    except Exception as error:
                        LOG.warning("handler failed", exc_info=error)
                        self._client._ack(
                            incoming.message_id,
                            incoming.lease,
                            False,
                            str(error) or "handler failed",
                        )
                if self._running():
                    LOG.info("subscribe stream ended, reconnecting")
            except grpc.RpcError as error:
                if self._running():
                    LOG.warning("subscribe stream closed, reconnecting: %s", error.details() or error.code())
                else:
                    return
            except Exception as error:
                if self._running():
                    LOG.warning("subscribe failed, reconnecting: %s", error)
                else:
                    return
            if self._running():
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECS)


def _to_message(incoming: queue_pb2.SatwayMessage) -> SatwayMessage:
    raw = incoming.payload
    try:
        payload = raw.decode("utf-8")
        encoding = "utf-8"
    except UnicodeDecodeError:
        import base64

        payload = base64.b64encode(raw).decode("ascii")
        encoding = "base64"
    return SatwayMessage(
        id=incoming.message_id,
        topic=incoming.topic,
        payload=payload,
        payload_encoding=encoding,
        attributes=dict(incoming.attributes),
        created_at=incoming.created_at,
        lease=incoming.lease,
    )
