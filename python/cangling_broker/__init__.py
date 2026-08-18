from .client import SatwayClient
from .compat import KafkaProducer
from .models import SatwayMessage, SendResult, SubscribeOptions, TopicConfig

try:
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("cangling-broker")
except Exception:
    __version__ = "0.0.0"

__all__ = [
    "KafkaProducer",
    "SatwayClient",
    "SatwayMessage",
    "SendResult",
    "SubscribeOptions",
    "TopicConfig",
    "__version__",
]
