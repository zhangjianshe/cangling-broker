from __future__ import annotations

from collections.abc import Callable
import logging
import os
import queue
import socket
import threading
import time
import uuid

import grpc

from .models import (
    ElectionCampaignResult,
    ElectionLeader,
    ElectionRenewResult,
    ElectionState,
    SatwayMessage,
    SendResult,
    SubscribeOptions,
    TopicConfig,
    auth_token_from_env,
)
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
CONNECT_TIMEOUT_SECS = 15
_CHANNEL_OPTIONS = (
    ("grpc.keepalive_time_ms", 30_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.http2.min_time_between_pings_ms", 10_000),
)
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


def _ascii_header(value: str) -> str:
    return "".join(ch for ch in value if 32 <= ord(ch) < 127).strip()


def _sdk_host() -> str:
    override = _ascii_header(os.environ.get("CL_BROKER_CLIENT_HOST") or "")
    if override:
        return override
    return _ascii_header(socket.gethostname())


def _auth_metadata(token: str | None) -> list[tuple[str, str]]:
    metadata = [("x-client-version", _sdk_version())]
    host = _sdk_host()
    if host:
        metadata.append(("x-client-host", host))
    token = (token or "").strip()
    if not token:
        return metadata
    if not token.lower().startswith("bearer "):
        token = "Bearer " + token
    metadata.append(("authorization", token))
    return metadata


def _with_client_identity(attributes: dict[str, str] | None) -> dict[str, str]:
    attrs = dict(attributes or {})
    attrs.setdefault("version", _sdk_version())
    host = _sdk_host()
    if host:
        attrs.setdefault("host", host)
    return attrs


def _open_channel(channel: grpc.Channel) -> None:
    try:
        grpc.channel_ready_future(channel).result(timeout=CONNECT_TIMEOUT_SECS)
    except Exception as error:
        LOG.warning("broker not ready yet: %s", error)


class SatwayClient:
    """Broker client. Owns the gRPC channel and retries unary RPCs.

    Each :meth:`subscribe` stream reopens on the same ``consumer_id`` after a drop.
    """

    def __init__(self, channel: grpc.Channel, metadata: list[tuple[str, str]]):
        self._channel = channel
        self._stub = queue_pb2_grpc.MessageQueueStub(channel)
        self._cache_stub = queue_pb2_grpc.CacheServiceStub(channel)
        self._election_stub = queue_pb2_grpc.ElectionServiceStub(channel)
        self._metadata = metadata
        self._open = True
        self._consumers: list[Consumer] = []
        self._elections: list[Election] = []
        self._lock = threading.Lock()
        self._acks = _AckBatcher(self)

    @classmethod
    def connect(cls, broker: str, token: str | None = None) -> SatwayClient:
        if not broker or not broker.strip():
            raise ValueError("broker is required")
        if token is None:
            token = auth_token_from_env()
        channel = grpc.insecure_channel(
            _broker_target(broker),
            options=list(_CHANNEL_OPTIONS),
        )
        threading.Thread(
            target=_open_channel,
            args=(channel,),
            name="cangling-connect",
            daemon=True,
        ).start()
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
                    attributes=_with_client_identity(attributes),
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
                attributes=_with_client_identity(dict(options.attributes)),
                consumer_id=options.consumer_id,
            )
        consumer = Consumer(self, options, cid, handler)
        with self._lock:
            self._consumers.append(consumer)
        return consumer

    def close(self) -> None:
        with self._lock:
            elections = list(self._elections)
        for election in elections:
            election.close()
        self._acks.close()
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

    # ========= generic lease election ==========

    def campaign_election(
        self, election: str, candidate_id: str, ttl_seconds: int
    ) -> ElectionCampaignResult:
        """Perform one campaign attempt for an application-defined election."""
        _validate_election(election, candidate_id, ttl_seconds)
        return self._call_with_reconnect(
            "campaign_election",
            lambda: self._campaign_election_once(election, candidate_id, ttl_seconds),
        )

    def renew_election(
        self,
        election: str,
        leader_id: str,
        term: int,
        lease_id: str,
        ttl_seconds: int,
    ) -> ElectionRenewResult:
        """Renew an exact active leader/term/lease tuple."""
        _validate_lease(election, leader_id, term, lease_id)
        _validate_ttl(ttl_seconds)
        return self._call_with_reconnect(
            "renew_election",
            lambda: self._renew_election_once(
                election, leader_id, term, lease_id, ttl_seconds
            ),
        )

    def resign_election(
        self, election: str, leader_id: str, term: int, lease_id: str
    ) -> bool:
        """Relinquish leadership when the exact tuple still owns it."""
        _validate_lease(election, leader_id, term, lease_id)
        return self._call_with_reconnect(
            "resign_election",
            lambda: self._resign_election_once(election, leader_id, term, lease_id),
        )

    def get_election(self, election: str) -> ElectionState:
        """Read the observed leader, including inactive/expired state."""
        _require_election(election)

        def once() -> ElectionState:
            response = self._election_stub.GetLeader(
                queue_pb2.ElectionGetLeaderRequest(election=election.strip()),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )
            return ElectionState(
                found=response.found,
                active=response.active,
                leader=_to_election_leader(response.leader)
                if response.HasField("leader")
                else None,
            )

        return self._call_with_reconnect("get_election", once)

    def start_election(
        self,
        election: str,
        candidate_id: str,
        ttl_seconds: int = 15,
        *,
        on_leader: Callable[[Election, ElectionLeader], None] | None = None,
        on_follower: Callable[[Election, ElectionLeader | None], None] | None = None,
    ) -> Election:
        """Continuously campaign and renew, returning a fail-closed handle."""
        _validate_election(election, candidate_id, ttl_seconds)
        if ttl_seconds < 3:
            raise ValueError("automatic election ttl_seconds must be >= 3")
        handle = Election(
            self,
            election.strip(),
            candidate_id.strip(),
            ttl_seconds,
            on_leader,
            on_follower,
        )
        with self._lock:
            self._elections.append(handle)
        handle._start()
        return handle

    def _campaign_election_once(
        self, election: str, candidate_id: str, ttl_seconds: int
    ) -> ElectionCampaignResult:
        response = self._election_stub.Campaign(
            queue_pb2.ElectionCampaignRequest(
                election=election,
                candidate_id=candidate_id,
                ttl_seconds=ttl_seconds,
            ),
            timeout=RPC_DEADLINE_SECS,
            metadata=self._metadata,
        )
        return ElectionCampaignResult(response.won, _to_election_leader(response.leader))

    def _renew_election_once(
        self,
        election: str,
        leader_id: str,
        term: int,
        lease_id: str,
        ttl_seconds: int,
    ) -> ElectionRenewResult:
        response = self._election_stub.Renew(
            queue_pb2.ElectionRenewRequest(
                election=election,
                leader_id=leader_id,
                term=term,
                lease_id=lease_id,
                ttl_seconds=ttl_seconds,
            ),
            timeout=RPC_DEADLINE_SECS,
            metadata=self._metadata,
        )
        return ElectionRenewResult(
            response.renewed,
            _to_election_leader(response.leader)
            if response.HasField("leader")
            else None,
        )

    def _resign_election_once(
        self, election: str, leader_id: str, term: int, lease_id: str
    ) -> bool:
        return self._election_stub.Resign(
            queue_pb2.ElectionResignRequest(
                election=election,
                leader_id=leader_id,
                term=term,
                lease_id=lease_id,
            ),
            timeout=RPC_DEADLINE_SECS,
            metadata=self._metadata,
        ).resigned

    def _remove_election(self, election: Election) -> None:
        with self._lock:
            if election in self._elections:
                self._elections.remove(election)

    # ========= cache (in-memory) & lock (SQLite-backed) Redis replacement ==========

    def cache_set(
        self,
        key: str,
        value: str | bytes,
        ttl_seconds: int = 0,
        value_type: str = "string",
    ) -> None:
        """Store a value. ``ttl_seconds <= 0`` means no expiry.

        ``value_type`` is an optional hint (``string`` / ``long`` / ``int`` /
        ``double`` / ``bool``) stored alongside the value so typed reads can
        restore the original type.
        """
        if not key or not key.strip():
            raise ValueError("key is required")
        body = value.encode("utf-8") if isinstance(value, str) else bytes(value)

        def once() -> None:
            self._cache_stub.Set(
                queue_pb2.CacheSetRequest(
                    key=key,
                    value=body,
                    ttl_seconds=ttl_seconds,
                    value_type=value_type or "string",
                ),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )

        self._call_with_reconnect("cache_set", once)

    def cache_get(self, key: str) -> bytes | None:
        """Return the raw value, or ``None`` when missing/expired."""
        if not key or not key.strip():
            raise ValueError("key is required")

        def once() -> bytes | None:
            response = self._cache_stub.Get(
                queue_pb2.CacheGetRequest(key=key),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )
            return bytes(response.value) if response.found else None

        return self._call_with_reconnect("cache_get", once)

    def cache_get_entry(self, key: str) -> tuple[bytes, str] | None:
        """Return ``(value, value_type)``, or ``None`` when missing/expired."""
        if not key or not key.strip():
            raise ValueError("key is required")

        def once() -> tuple[bytes, str] | None:
            response = self._cache_stub.Get(
                queue_pb2.CacheGetRequest(key=key),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )
            if not response.found:
                return None
            return (bytes(response.value), response.value_type)

        return self._call_with_reconnect("cache_get_entry", once)

    def cache_get_string(self, key: str) -> str | None:
        """Return the value decoded as UTF-8, or ``None`` when missing."""
        value = self.cache_get(key)
        return value.decode("utf-8") if value is not None else None

    def cache_delete(self, key: str) -> bool:
        if not key or not key.strip():
            raise ValueError("key is required")

        def once() -> bool:
            return self._cache_stub.Delete(
                queue_pb2.CacheDeleteRequest(key=key),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).deleted

        return self._call_with_reconnect("cache_delete", once)

    def cache_incr(self, key: str, delta: int = 1, ttl_seconds: int = 0) -> int:
        if not key or not key.strip():
            raise ValueError("key is required")

        def once() -> int:
            return self._cache_stub.Incr(
                queue_pb2.CacheIncrRequest(key=key, delta=delta, ttl_seconds=ttl_seconds),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).value

        return self._call_with_reconnect("cache_incr", once)

    def cache_expire(self, key: str, ttl_seconds: int) -> bool:
        if not key or not key.strip():
            raise ValueError("key is required")

        def once() -> bool:
            return self._cache_stub.Expire(
                queue_pb2.CacheExpireRequest(key=key, ttl_seconds=ttl_seconds),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).ok

        return self._call_with_reconnect("cache_expire", once)

    def cache_ttl(self, key: str) -> int:
        """Redis semantics: -2 missing, -1 no expiry, else seconds remaining."""
        if not key or not key.strip():
            raise ValueError("key is required")

        def once() -> int:
            return self._cache_stub.Ttl(
                queue_pb2.CacheTtlRequest(key=key),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).ttl_seconds

        return self._call_with_reconnect("cache_ttl", once)

    def acquire_lock(self, lock_key: str, ttl_seconds: int, owner: str = "") -> Lock | None:
        """Try to acquire a distributed lock. Returns a :class:`Lock` or ``None``."""
        if not lock_key or not lock_key.strip():
            raise ValueError("lock_key is required")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        owner = owner or str(uuid.uuid4())

        def once() -> bool:
            return self._cache_stub.AcquireLock(
                queue_pb2.LockAcquireRequest(
                    lock_key=lock_key,
                    owner=owner,
                    ttl_seconds=ttl_seconds,
                ),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).acquired

        acquired = self._call_with_reconnect("acquire_lock", once)
        return Lock(self, lock_key, owner) if acquired else None

    def _lock_renew(self, lock_key: str, owner: str, ttl_seconds: int) -> bool:
        def once() -> bool:
            return self._cache_stub.RenewLock(
                queue_pb2.LockRenewRequest(
                    lock_key=lock_key,
                    owner=owner,
                    ttl_seconds=ttl_seconds,
                ),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).renewed

        return self._call_with_reconnect("renew_lock", once)

    def _lock_release(self, lock_key: str, owner: str) -> bool:
        def once() -> bool:
            return self._cache_stub.ReleaseLock(
                queue_pb2.LockReleaseRequest(lock_key=lock_key, owner=owner),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).released

        return self._call_with_reconnect("release_lock", once)

    def lock_is_locked(self, lock_key: str) -> bool:
        if not lock_key or not lock_key.strip():
            raise ValueError("lock_key is required")

        def once() -> bool:
            return self._cache_stub.IsLocked(
                queue_pb2.LockIsLockedRequest(lock_key=lock_key),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            ).locked

        return self._call_with_reconnect("is_locked", once)

    def _is_open(self) -> bool:
        return self._open

    def _ack(self, message_id: str, lease: str, success: bool, error: str = "") -> None:
        self._acks.submit(message_id, lease, success, error)

    def _ack_batch(self, acknowledgements: list[queue_pb2.AckMessageRequest]) -> None:
        def once() -> None:
            self._stub.AckMessages(
                queue_pb2.AckMessagesRequest(acknowledgements=acknowledgements),
                timeout=RPC_DEADLINE_SECS,
                metadata=self._metadata,
            )

        self._call_with_reconnect("ack batch", once)

    def _ensure_registered(self, options: SubscribeOptions, consumer_id: str) -> None:
        if not consumer_id:
            return
        self.register(
            options.topic,
            name=options.name,
            attributes=_with_client_identity(dict(options.attributes)),
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


def _to_election_leader(value) -> ElectionLeader:
    return ElectionLeader(
        election=value.election,
        leader_id=value.leader_id,
        term=value.term,
        lease_id=value.lease_id,
        expire_at=value.expire_at,
        remaining_ttl_ms=value.remaining_ttl_ms,
    )


def _require_election(election: str) -> None:
    if not election or not election.strip():
        raise ValueError("election is required")


def _validate_ttl(ttl_seconds: int) -> None:
    if ttl_seconds < 1 or ttl_seconds > 3600:
        raise ValueError("ttl_seconds must be between 1 and 3600")


def _validate_election(election: str, candidate_id: str, ttl_seconds: int) -> None:
    _require_election(election)
    if not candidate_id or not candidate_id.strip():
        raise ValueError("candidate_id is required")
    _validate_ttl(ttl_seconds)


def _validate_lease(election: str, leader_id: str, term: int, lease_id: str) -> None:
    _require_election(election)
    if not leader_id or not leader_id.strip():
        raise ValueError("leader_id is required")
    if term <= 0:
        raise ValueError("term must be > 0")
    if not lease_id or not lease_id.strip():
        raise ValueError("lease_id is required")


class Election:
    """Auto-renewing, fail-closed handle for one named election."""

    def __init__(
        self,
        client: SatwayClient,
        election: str,
        candidate_id: str,
        ttl_seconds: int,
        on_leader: Callable[[Election, ElectionLeader], None] | None,
        on_follower: Callable[[Election, ElectionLeader | None], None] | None,
    ):
        self._client = client
        self.election = election
        self.candidate_id = candidate_id
        self.ttl_seconds = ttl_seconds
        self._on_leader = on_leader
        self._on_follower = on_follower
        self._state_lock = threading.Lock()
        self._rpc_lock = threading.Lock()
        self._active_rpc = None
        self._leader: ElectionLeader | None = None
        self._leading = False
        self._valid_until = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="cangling-election-" + election.replace("/", "-"),
            daemon=True,
        )

    def _start(self) -> None:
        self._thread.start()

    @property
    def leader(self) -> ElectionLeader | None:
        with self._state_lock:
            return self._leader

    @property
    def is_leader(self) -> bool:
        with self._state_lock:
            return self._leading and time.monotonic() < self._valid_until

    @property
    def term(self) -> int:
        leader = self.leader
        return leader.term if self.is_leader and leader is not None else 0

    def _run(self) -> None:
        refresh = max(1.0, self.ttl_seconds / 3.0)
        while not self._stop.is_set() and self._client._is_open():
            try:
                leader = self.leader
                if self.is_leader and leader is not None:
                    response = self._rpc(
                        self._client._election_stub.Renew,
                        queue_pb2.ElectionRenewRequest(
                            election=self.election,
                            leader_id=self.candidate_id,
                            term=leader.term,
                            lease_id=leader.lease_id,
                            ttl_seconds=self.ttl_seconds,
                        ),
                    )
                    result = ElectionRenewResult(
                        response.renewed,
                        _to_election_leader(response.leader)
                        if response.HasField("leader")
                        else None,
                    )
                    if self._stop.is_set():
                        break
                    if result.renewed and result.leader is not None:
                        self._become_leader(result.leader)
                    else:
                        self._become_follower(result.leader)
                else:
                    response = self._rpc(
                        self._client._election_stub.Campaign,
                        queue_pb2.ElectionCampaignRequest(
                            election=self.election,
                            candidate_id=self.candidate_id,
                            ttl_seconds=self.ttl_seconds,
                        ),
                    )
                    result = ElectionCampaignResult(
                        response.won, _to_election_leader(response.leader)
                    )
                    if self._stop.is_set():
                        if result.won:
                            self._resign_quietly(result.leader)
                        break
                    if result.won:
                        self._become_leader(result.leader)
                    else:
                        self._become_follower(result.leader)
                self._stop.wait(refresh)
            except Exception as error:
                if self._stop.is_set():
                    break
                if not self.is_leader:
                    self._become_follower(self.leader)
                LOG.warning("election request failed for %s: %s", self.election, error)
                self._stop.wait(min(1.0, refresh))
        self._become_follower(self.leader)

    def _rpc(self, method, request):
        future = method.future(
            request,
            timeout=RPC_DEADLINE_SECS,
            metadata=self._client._metadata,
        )
        with self._rpc_lock:
            self._active_rpc = future
            stopped = self._stop.is_set()
        if stopped:
            future.cancel()
        try:
            return future.result()
        finally:
            with self._rpc_lock:
                if self._active_rpc is future:
                    self._active_rpc = None

    def _become_leader(self, leader: ElectionLeader) -> None:
        safety = min(1.0, self.ttl_seconds / 3.0)
        valid_for = leader.remaining_ttl_ms / 1000.0 - safety
        if valid_for <= 0:
            self._become_follower(leader)
            return
        with self._state_lock:
            transition = not (
                self._leading and time.monotonic() < self._valid_until
            )
            self._leader = leader
            self._valid_until = time.monotonic() + valid_for
            self._leading = True
        if transition and self._on_leader is not None:
            try:
                self._on_leader(self, leader)
            except Exception:
                LOG.exception("on_leader callback failed")

    def _become_follower(self, leader: ElectionLeader | None) -> None:
        with self._state_lock:
            transition = self._leading
            self._leader = leader
            self._leading = False
            self._valid_until = 0.0
        if transition and self._on_follower is not None:
            try:
                self._on_follower(self, leader)
            except Exception:
                LOG.exception("on_follower callback failed")

    def _resign_quietly(self, leader: ElectionLeader | None) -> None:
        if leader is None:
            return
        try:
            self._client._resign_election_once(
                self.election,
                self.candidate_id,
                leader.term,
                leader.lease_id,
            )
        except Exception:
            LOG.debug("could not resign election %s", self.election, exc_info=True)

    def close(self) -> None:
        if self._stop.is_set():
            return
        was_leader = self.is_leader
        leader = self.leader
        self._stop.set()
        with self._rpc_lock:
            active_rpc = self._active_rpc
        if active_rpc is not None:
            active_rpc.cancel()
        if was_leader and leader is not None:
            self._resign_quietly(leader)
        self._become_follower(leader)
        self._client._remove_election(self)
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=1.0)

    def __enter__(self) -> Election:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class _AckBatcher:
    """Combines consumer acknowledgements into one RPC without delaying handlers."""

    _STOP = object()

    def __init__(self, client: SatwayClient):
        self._client = client
        self._queue: queue.Queue = queue.Queue(maxsize=8192)
        self._thread = threading.Thread(target=self._run, name="cangling-ack", daemon=True)
        self._thread.start()

    def submit(self, message_id: str, lease: str, success: bool, error: str) -> None:
        self._queue.put(queue_pb2.AckMessageRequest(
            message_id=message_id,
            lease=lease,
            success=success,
            error=error or "",
        ))

    def close(self) -> None:
        self._queue.put(self._STOP)
        self._thread.join(timeout=RPC_DEADLINE_SECS + 1)

    def _run(self) -> None:
        stopping = False
        while not stopping:
            first = self._queue.get()
            if first is self._STOP:
                return
            batch = [first]
            deadline = time.monotonic() + 0.002
            while len(batch) < 64:
                timeout = max(0.0, deadline - time.monotonic())
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    break
                if item is self._STOP:
                    stopping = True
                    break
                batch.append(item)
            try:
                self._client._ack_batch(batch)
            except Exception:
                LOG.exception("ack batch failed")

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


class Lock:
    """A held distributed lock returned by :meth:`SatwayClient.acquire_lock`.

    Renew the lease with :meth:`renew` and release it with :meth:`release`
    (also called by ``close()`` / context-manager exit). Releasing only
    succeeds while this lock's ``owner`` still holds the key.
    """

    def __init__(self, client: SatwayClient, lock_key: str, owner: str):
        self._client = client
        self.lock_key = lock_key
        self.owner = owner

    def renew(self, ttl_seconds: int) -> bool:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        return self._client._lock_renew(self.lock_key, self.owner, ttl_seconds)

    def release(self) -> bool:
        return self._client._lock_release(self.lock_key, self.owner)

    def close(self) -> bool:
        return self.release()

    def __enter__(self) -> Lock:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
