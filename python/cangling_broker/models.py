from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Mapping


def auth_token_from_env() -> str:
    return (os.environ.get("CL_BROKER_AUTH_TOKEN") or "").strip()


@dataclass(frozen=True)
class SendResult:
    message_id: str
    duplicate: bool = False


@dataclass
class SatwayMessage:
    id: str
    topic: str
    payload: str
    payload_encoding: str = "utf-8"
    attributes: Mapping[str, str] = field(default_factory=dict)
    created_at: str = ""
    lease: str = ""


@dataclass(frozen=True)
class TopicConfig:
    topic: str
    delivery: str = "single"

    def __post_init__(self) -> None:
        if not self.topic or not self.topic.strip():
            raise ValueError("topic is required")
        delivery = (self.delivery or "single").strip().lower()
        if delivery not in {"single", "broadcast", "queue", "competing", "fanout", "pubsub"}:
            raise ValueError("delivery must be single or broadcast")
        if delivery in {"queue", "competing"}:
            delivery = "single"
        if delivery in {"fanout", "pubsub"}:
            delivery = "broadcast"
        object.__setattr__(self, "topic", self.topic.strip())
        object.__setattr__(self, "delivery", delivery)


@dataclass(frozen=True)
class SubscribeOptions:
    topic: str
    consumer_id: str = ""
    name: str = ""
    attributes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.topic or not self.topic.strip():
            raise ValueError("topic is required")
        object.__setattr__(self, "topic", self.topic.strip())
        object.__setattr__(self, "consumer_id", self.consumer_id or "")
        object.__setattr__(self, "name", self.name or "")
        object.__setattr__(self, "attributes", dict(self.attributes or {}))
