"""Publisher-specific configuration dataclasses."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from faststream._internal.configs.endpoint import PublisherUsecaseConfig
from faststream._internal.configs.specification import PublisherSpecificationConfig

from kubemq_faststream.schemas import KubeMQPattern

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware

    from kubemq_faststream.config import KubeMQBrokerConfig


@dataclass(kw_only=True)
class KubeMQPublisherSpecificationConfig(PublisherSpecificationConfig):
    """Specification metadata for KubeMQ publishers (AsyncAPI schema)."""

    channel: str = ""
    pattern: KubeMQPattern = KubeMQPattern.EVENTS


@dataclass(kw_only=True)
class KubeMQPublisherConfig(PublisherUsecaseConfig):
    """Per-publisher configuration for KubeMQ patterns.

    The `_outer_config` references the broker's KubeMQBrokerConfig.
    """

    _outer_config: KubeMQBrokerConfig = None  # type: ignore[assignment]
    channel: str = ""
    pattern: KubeMQPattern = KubeMQPattern.EVENTS
    middlewares: Sequence[PublisherMiddleware[Any]] = ()
