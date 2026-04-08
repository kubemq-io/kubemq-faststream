"""Subscriber-specific configuration dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs.endpoint import SubscriberUsecaseConfig
from faststream._internal.configs.specification import SpecificationConfig
from faststream.middlewares import AckPolicy

from kubemq_faststream.schemas import KubeMQPattern

if TYPE_CHECKING:
    from kubemq_faststream.config import KubeMQBrokerConfig


@dataclass(kw_only=True)
class KubeMQSubscriberSpecificationConfig(SpecificationConfig):
    """Specification metadata for KubeMQ subscribers (AsyncAPI schema)."""

    channel: str = ""
    pattern: KubeMQPattern = KubeMQPattern.EVENTS


@dataclass(kw_only=True)
class KubeMQSubscriberConfig(SubscriberUsecaseConfig):
    """Per-subscriber configuration for KubeMQ patterns.

    Note: `parser` and `decoder` are `init=False` fields inherited from
    SubscriberUsecaseConfig. They are set on the instance by
    KubeMQSubscriber.__init__() before calling super().__init__().
    The `_outer_config` references the broker's KubeMQBrokerConfig.
    """

    _outer_config: KubeMQBrokerConfig = field(default=None)  # type: ignore[assignment]

    channel: str = ""
    pattern: KubeMQPattern = KubeMQPattern.EVENTS
    group: str | None = None
    max_messages: int = 1
    wait_timeout: int = 60

    @property
    def ack_policy(self) -> AckPolicy:
        """Return the configured AckPolicy (override base property)."""
        from faststream._internal.constants import EMPTY

        if self._ack_policy is EMPTY:
            return AckPolicy.ACK
        return self._ack_policy
