from .client import Election, SatwayClient
from .compat import KafkaProducer
from .models import (
    ElectionCampaignResult,
    ElectionLeader,
    ElectionRenewResult,
    ElectionState,
    SatwayMessage,
    SendResult,
    SubscribeOptions,
    TopicConfig,
)

try:
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("cangling-broker")
except Exception:
    __version__ = "0.0.0"

__all__ = [
    "KafkaProducer",
    "Election",
    "ElectionCampaignResult",
    "ElectionLeader",
    "ElectionRenewResult",
    "ElectionState",
    "SatwayClient",
    "SatwayMessage",
    "SendResult",
    "SubscribeOptions",
    "TopicConfig",
    "__version__",
]
