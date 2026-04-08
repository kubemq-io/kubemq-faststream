"""Typed subscriber/publisher decorator registration for KubeMQ."""

from __future__ import annotations

from typing import TYPE_CHECKING

from faststream._internal.broker.registrator import Registrator
from faststream.middlewares import AckPolicy

from kubemq_faststream.config import KubeMQBrokerConfig
from kubemq_faststream.message import KubeMQRawMessage
from kubemq_faststream.schemas import KubeMQPattern, StartPosition
from kubemq_faststream.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from kubemq_faststream.subscriber.usecase import KubeMQSubscriber


class KubeMQRegistrator(Registrator[KubeMQRawMessage, KubeMQBrokerConfig]):
    """Provides typed @broker.subscriber() and @broker.publisher() decorators.

    Extends FastStream's Registrator to route subscriber/publisher registration
    through KubeMQ-specific factory and pattern resolution.

    Note: subscriber() and publisher() here are NOT overrides of the base
    Registrator methods (which take an already-constructed subscriber/publisher).
    These are NEW methods with different signatures that internally call
    super().subscriber(subscriber) / super().publisher(publisher) to register
    the constructed objects with the base class.
    """

    def subscriber(  # type: ignore[override]
        self,
        *,
        events: str | None = None,
        events_store: str | None = None,
        queues: str | None = None,
        commands: str | None = None,
        queries: str | None = None,
        group: str | None = None,
        start_position: StartPosition = StartPosition.START_FROM_NEW,
        start_value: int | float | None = None,
        batch: bool = False,
        max_messages: int = 1,
        wait_timeout: int = 60,
        ack_policy: AckPolicy = AckPolicy.ACK,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> KubeMQSubscriber:
        """Register a subscriber for a KubeMQ channel.

        Exactly one of events, events_store, queues, commands, or queries
        must be specified.
        """
        channel, pattern = _resolve_pattern(
            events=events,
            events_store=events_store,
            queues=queues,
            commands=commands,
            queries=queries,
        )
        subscriber = create_subscriber(
            channel=channel,
            pattern=pattern,
            group=group,
            start_position=start_position,
            start_value=start_value,
            batch=batch,
            max_messages=max_messages,
            wait_timeout=wait_timeout,
            ack_policy=ack_policy,
            no_reply=no_reply,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
            config=self.config,  # type: ignore[arg-type]  # pass broker config as _outer_config
        )
        subscriber = super().subscriber(subscriber)  # type: ignore[assignment]  # register in base Registrator
        return subscriber

    def publisher(  # type: ignore[override]
        self,
        *,
        events: str | None = None,
        events_store: str | None = None,
        queues: str | None = None,
        commands: str | None = None,
        queries: str | None = None,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ):
        """Register a publisher for a KubeMQ channel."""
        from kubemq_faststream.publisher import KubeMQPublisher
        from kubemq_faststream.publisher.config import (
            KubeMQPublisherConfig,
            KubeMQPublisherSpecificationConfig,
        )
        from kubemq_faststream.publisher.specification import (
            KubeMQPublisherSpecification,
        )

        channel, pattern = _resolve_pattern(
            events=events,
            events_store=events_store,
            queues=queues,
            commands=commands,
            queries=queries,
        )
        publisher_config = KubeMQPublisherConfig(
            _outer_config=self.config,  # type: ignore[arg-type]
            channel=channel,
            pattern=pattern,
            middlewares=(),
        )
        publisher_specification = KubeMQPublisherSpecification(
            _outer_config=self.config,  # type: ignore[arg-type]
            specification_config=KubeMQPublisherSpecificationConfig(
                channel=channel,
                pattern=pattern,
                title_=title,
                description_=description,
                schema_=None,
                include_in_schema=include_in_schema,
            ),
        )
        publisher = KubeMQPublisher(
            config=publisher_config,
            specification=publisher_specification,
        )
        publisher = super().publisher(publisher)  # type: ignore[assignment]  # register in base Registrator
        return publisher


def _resolve_pattern(
    *,
    events: str | None = None,
    events_store: str | None = None,
    queues: str | None = None,
    commands: str | None = None,
    queries: str | None = None,
) -> tuple[str, KubeMQPattern]:
    """Resolve which pattern keyword was provided and return (channel, pattern)."""
    from faststream.exceptions import SetupError

    pairs = [
        (events, KubeMQPattern.EVENTS),
        (events_store, KubeMQPattern.EVENTS_STORE),
        (queues, KubeMQPattern.QUEUES),
        (commands, KubeMQPattern.COMMANDS),
        (queries, KubeMQPattern.QUERIES),
    ]
    specified = [(ch, pat) for ch, pat in pairs if ch is not None]
    if len(specified) == 0:
        raise SetupError("Specify one of: events, events_store, queues, commands, queries")
    if len(specified) > 1:
        raise SetupError("Specify only ONE of: events, events_store, queues, commands, queries")
    channel, pattern = specified[0]
    if not channel:
        raise SetupError(f"Channel name for {pattern.value!r} cannot be empty")
    return channel, pattern
