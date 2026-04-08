"""Base subscriber class for all KubeMQ patterns."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterable
from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase

from kubemq_faststream.message import KubeMQRawMessage
from kubemq_faststream.parser import KubeMQParser
from kubemq_faststream.subscriber.config import (
    KubeMQSubscriberConfig,
)

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream.message import StreamMessage

    from kubemq_faststream.config import KubeMQConnection

logger = logging.getLogger("kubemq_faststream")


class KubeMQSubscriber(TasksMixin, SubscriberUsecase[KubeMQRawMessage]):
    """Base class for KubeMQ pattern subscribers.

    Extends FastStream's SubscriberUsecase to participate in the standard
    parser -> decoder -> middleware -> handler pipeline.

    Uses TasksMixin for asyncio.Task lifecycle management (add_task / stop).
    """

    _connection: KubeMQConnection | None

    def __init__(
        self,
        config: KubeMQSubscriberConfig,
        specification: SubscriberSpecification[Any, Any],
        calls: CallsCollection[KubeMQRawMessage],
    ) -> None:
        # Parser and decoder are init=False fields on SubscriberUsecaseConfig.
        # Set them on the config instance BEFORE super().__init__() so the
        # pipeline builds correctly.
        parser = KubeMQParser()
        config.parser = parser.parse_message
        config.decoder = parser.decode_message

        super().__init__(config, specification, calls)

        self.channel = config.channel
        self.pattern = config.pattern
        self.group = config.group
        self._connection = None
        self._kubemq_parser = parser

    def set_connection(self, connection: KubeMQConnection) -> None:
        """Inject the KubeMQ connection before start()."""
        self._connection = connection

    async def start(self) -> None:
        """Start the subscriber: build pipeline, then launch consume task."""
        await super().start()
        self._post_start()
        if self.calls:
            self.add_task(self._consume)

    async def _consume(self) -> None:
        """Override in concrete subscribers."""
        raise NotImplementedError

    def _make_response_publisher(
        self,
        message: StreamMessage[KubeMQRawMessage],
    ) -> Iterable[PublisherProto]:
        """KubeMQ handles responses in the subscriber _consume loop, not here."""
        return ()

    async def get_one(self, *, timeout: float = 5.0) -> StreamMessage[KubeMQRawMessage] | None:
        """Not supported for KubeMQ subscribers (use handler pattern)."""
        raise NotImplementedError(
            "get_one() is not supported for KubeMQ subscribers. "
            "Use the @broker.subscriber() decorator pattern instead."
        )

    async def __aiter__(self) -> AsyncIterator[StreamMessage[KubeMQRawMessage]]:  # type: ignore[override]
        """Not supported for KubeMQ subscribers (use handler pattern)."""
        raise NotImplementedError(
            "Async iteration is not supported for KubeMQ subscribers. "
            "Use the @broker.subscriber() decorator pattern instead."
        )
        yield  # Make this an async generator to satisfy the type
