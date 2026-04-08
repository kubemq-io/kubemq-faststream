"""Publisher specification for AsyncAPI schema generation."""

from __future__ import annotations

from typing import Any

from faststream._internal.endpoint.publisher.specification import PublisherSpecification


class KubeMQPublisherSpecification(
    PublisherSpecification[Any, Any],
):
    """AsyncAPI specification for a KubeMQ publisher endpoint."""

    pass
