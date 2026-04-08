"""kubemq-faststream — KubeMQ broker adapter for FastStream."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from kubemq_faststream.schemas import (
    AckPolicy,
    FeatureNotSupportedException,
    KubeMQPattern,
    StartPosition,
)

try:
    __version__ = version("kubemq-faststream")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"


def _ensure_kubemq() -> None:
    try:
        import kubemq  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "kubemq-faststream requires the 'kubemq' package (KubeMQ Python SDK). "
            "Install with: pip install 'kubemq>=4.1.1'"
        ) from exc


def __getattr__(name: str):
    """Lazy imports for heavy modules — only check SDK when actually used."""
    _ensure_kubemq()

    if name == "KubeMQBroker":
        from kubemq_faststream.broker import KubeMQBroker

        return KubeMQBroker
    if name == "KubeMQMessage":
        from kubemq_faststream.message import KubeMQMessage

        return KubeMQMessage
    if name == "KubeMQPublishCommand":
        from kubemq_faststream.response import KubeMQPublishCommand

        return KubeMQPublishCommand
    if name == "KubeMQRouter":
        from kubemq_faststream.router import KubeMQRouter

        return KubeMQRouter
    if name == "KubeMQPublisher":
        from kubemq_faststream.publisher import KubeMQPublisher

        return KubeMQPublisher
    if name == "TestKubeMQBroker":
        from kubemq_faststream.testing import TestKubeMQBroker

        return TestKubeMQBroker
    raise AttributeError(f"module 'kubemq_faststream' has no attribute {name!r}")


__all__ = [
    "AckPolicy",
    "FeatureNotSupportedException",
    "KubeMQBroker",
    "KubeMQMessage",
    "KubeMQPattern",
    "KubeMQPublishCommand",
    "KubeMQPublisher",
    "KubeMQRouter",
    "StartPosition",
    "TestKubeMQBroker",
    "__version__",
]
