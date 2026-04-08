"""Events subscriber -- fire-and-forget broadcast."""

from __future__ import annotations

import logging
from typing import Any

import anyio

from kubemq_faststream.subscriber.usecase import KubeMQSubscriber

logger = logging.getLogger("kubemq_faststream")


class EventsSubscriber(KubeMQSubscriber):
    """Subscribe to KubeMQ Events (fire-and-forget, fan-out or grouped)."""

    def __init__(
        self,
        config: Any,
        specification: Any,
        calls: Any,
    ) -> None:
        super().__init__(config, specification, calls)

    async def _consume(self) -> None:
        """Consume loop: iterate SDK event stream, parse, and route through pipeline."""
        from kubemq.pubsub.events_subscription import EventsSubscription

        from kubemq_faststream.parser import KubeMQParser

        subscription = EventsSubscription(
            channel=self.channel,
            group=self.group or "",
            on_receive_event_callback=lambda _: None,
        )
        async for event in self._connection.pubsub.subscribe_to_events(subscription):  # type: ignore[union-attr]
            if not self.running:
                break
            try:
                raw = KubeMQParser.from_event_received(event)
                await self.consume(raw)
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                logger.exception("Event handler error on channel %s", self.channel)
