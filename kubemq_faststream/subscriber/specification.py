"""Subscriber specification for AsyncAPI schema generation."""

from __future__ import annotations

from typing import Any

from faststream._internal.endpoint.subscriber.specification import SubscriberSpecification
from faststream.specification.schema import SubscriberSpec


class KubeMQSubscriberSpecification(
    SubscriberSpecification[Any, Any],
):
    """AsyncAPI specification for a KubeMQ subscriber endpoint."""

    @property
    def name(self) -> str:
        """Return the specification name for this subscriber."""
        return self.config.title_ or self.call_name

    def get_schema(self) -> dict[str, SubscriberSpec]:
        """Return the AsyncAPI schema for this subscriber."""
        return {}
