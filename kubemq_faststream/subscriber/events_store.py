"""EventsStore subscriber -- persistent pub/sub with replay."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import anyio

from kubemq_faststream.schemas import StartPosition
from kubemq_faststream.subscriber.usecase import KubeMQSubscriber

logger = logging.getLogger("kubemq_faststream")


class EventsStoreSubscriber(KubeMQSubscriber):
    """Subscribe to KubeMQ EventsStore with configurable start position."""

    start_position: StartPosition
    start_value: int | float | None

    def __init__(
        self,
        config: Any,
        specification: Any,
        calls: Any,
        *,
        start_position: StartPosition = StartPosition.START_FROM_NEW,
        start_value: int | float | None = None,
    ) -> None:
        super().__init__(config, specification, calls)
        self.start_position = start_position
        self.start_value = start_value
        self._validate_start_value()

    def _validate_start_value(self) -> None:
        """Validate start_value for positions that require it."""
        requires_value = {
            StartPosition.START_AT_SEQUENCE,
            StartPosition.START_AT_TIME,
            StartPosition.START_AT_TIME_DELTA,
        }
        if self.start_position in requires_value and (
            self.start_value is None or self.start_value <= 0
        ):
            raise ValueError(
                f"start_value must be a positive number for "
                f"{self.start_position.value}, got {self.start_value!r}"
            )

    def _map_start_position(self) -> Any:
        """Map adapter StartPosition to SDK EventStoreStartPosition."""
        from kubemq.pubsub.events_store_subscription import EventStoreStartPosition

        _MAPPING = {
            StartPosition.START_FROM_NEW: EventStoreStartPosition.StartFromNew,
            StartPosition.START_FROM_FIRST: EventStoreStartPosition.StartFromFirst,
            StartPosition.START_FROM_LAST: EventStoreStartPosition.StartFromLast,
            StartPosition.START_AT_SEQUENCE: EventStoreStartPosition.StartAtSequence,
            StartPosition.START_AT_TIME: EventStoreStartPosition.StartAtTime,
            StartPosition.START_AT_TIME_DELTA: EventStoreStartPosition.StartAtTimeDelta,
        }
        return _MAPPING[self.start_position]

    async def _consume(self) -> None:
        """Consume loop: build subscription, iterate, parse, and process."""
        from kubemq.pubsub.events_store_subscription import EventsStoreSubscription

        from kubemq_faststream.parser import KubeMQParser

        sub_kwargs: dict[str, Any] = {
            "channel": self.channel,
            "group": self.group or "",
            "events_store_type": self._map_start_position(),
            "on_receive_event_callback": lambda _: None,
        }

        match self.start_position:
            case StartPosition.START_AT_SEQUENCE:
                sub_kwargs["events_store_sequence_value"] = int(self.start_value or 0)
            case StartPosition.START_AT_TIME:
                sub_kwargs["events_store_start_time"] = datetime.fromtimestamp(
                    float(self.start_value or 0), tz=UTC
                )
            case StartPosition.START_AT_TIME_DELTA:
                sub_kwargs["events_store_time_delta_seconds"] = int(self.start_value or 0)

        subscription = EventsStoreSubscription(**sub_kwargs)
        async for event in self._connection.pubsub.subscribe_to_events_store(subscription):  # type: ignore[union-attr]
            if not self.running:
                break
            try:
                raw = KubeMQParser.from_event_store_received(event)
                await self.consume(raw)
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                logger.exception("EventStore handler error on channel %s", self.channel)
