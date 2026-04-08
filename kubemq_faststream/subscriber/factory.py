"""Subscriber factory -- dispatches by pattern keyword.

Follows the Kafka subscriber factory pattern: constructs
config, specification, and calls, then passes them to the
concrete subscriber class.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.middlewares import AckPolicy

from kubemq_faststream.schemas import KubeMQPattern, StartPosition
from kubemq_faststream.subscriber.commands import CommandsSubscriber
from kubemq_faststream.subscriber.config import (
    KubeMQSubscriberConfig,
    KubeMQSubscriberSpecificationConfig,
)
from kubemq_faststream.subscriber.events import EventsSubscriber
from kubemq_faststream.subscriber.events_store import EventsStoreSubscriber
from kubemq_faststream.subscriber.queries import QueriesSubscriber
from kubemq_faststream.subscriber.queues import BatchQueuesSubscriber, QueuesSubscriber
from kubemq_faststream.subscriber.specification import KubeMQSubscriberSpecification
from kubemq_faststream.subscriber.usecase import KubeMQSubscriber

if TYPE_CHECKING:
    from kubemq_faststream.config import KubeMQBrokerConfig


def create_subscriber(
    *,
    channel: str,
    pattern: KubeMQPattern,
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
    config: KubeMQBrokerConfig,
) -> KubeMQSubscriber:
    """Create the correct subscriber class based on pattern.

    Constructs SubscriberConfig, SubscriberSpecification, and CallsCollection
    then passes them to the concrete subscriber (following KafkaBroker pattern).
    """
    subscriber_config = KubeMQSubscriberConfig(
        _outer_config=config,
        channel=channel,
        pattern=pattern,
        group=group,
        no_reply=no_reply,
        _ack_policy=ack_policy,
        max_messages=max_messages,
        wait_timeout=wait_timeout,
    )

    calls: CallsCollection[Any] = CallsCollection()

    specification = KubeMQSubscriberSpecification(
        _outer_config=config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=pattern,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        ),
    )

    if pattern == KubeMQPattern.EVENTS:
        return EventsSubscriber(subscriber_config, specification, calls)
    if pattern == KubeMQPattern.EVENTS_STORE:
        return EventsStoreSubscriber(
            subscriber_config,
            specification,
            calls,
            start_position=start_position,
            start_value=start_value,
        )
    if pattern == KubeMQPattern.QUEUES:
        if batch:
            return BatchQueuesSubscriber(subscriber_config, specification, calls)
        return QueuesSubscriber(subscriber_config, specification, calls)
    if pattern == KubeMQPattern.COMMANDS:
        return CommandsSubscriber(subscriber_config, specification, calls)
    if pattern == KubeMQPattern.QUERIES:
        return QueriesSubscriber(subscriber_config, specification, calls)
    raise ValueError(f"Unknown pattern: {pattern}")
