"""Tests for subscriber factory -- pattern dispatch."""

from __future__ import annotations

import pytest
from faststream.middlewares import AckPolicy

from kubemq_faststream.config import KubeMQBrokerConfig
from kubemq_faststream.schemas import KubeMQPattern, StartPosition
from kubemq_faststream.subscriber.commands import CommandsSubscriber
from kubemq_faststream.subscriber.events import EventsSubscriber
from kubemq_faststream.subscriber.events_store import EventsStoreSubscriber
from kubemq_faststream.subscriber.factory import create_subscriber
from kubemq_faststream.subscriber.queries import QueriesSubscriber
from kubemq_faststream.subscriber.queues import BatchQueuesSubscriber, QueuesSubscriber


def _make_config():
    return KubeMQBrokerConfig()


def test_create_events_subscriber():
    """EVENTS pattern creates EventsSubscriber."""
    sub = create_subscriber(
        channel="ev-ch",
        pattern=KubeMQPattern.EVENTS,
        config=_make_config(),
    )
    assert isinstance(sub, EventsSubscriber)
    assert sub.channel == "ev-ch"


def test_create_events_store_subscriber():
    """EVENTS_STORE pattern creates EventsStoreSubscriber."""
    sub = create_subscriber(
        channel="es-ch",
        pattern=KubeMQPattern.EVENTS_STORE,
        start_position=StartPosition.START_FROM_NEW,
        config=_make_config(),
    )
    assert isinstance(sub, EventsStoreSubscriber)
    assert sub.channel == "es-ch"


def test_create_events_store_with_sequence():
    """EVENTS_STORE with START_AT_SEQUENCE creates subscriber with start_value."""
    sub = create_subscriber(
        channel="es-ch",
        pattern=KubeMQPattern.EVENTS_STORE,
        start_position=StartPosition.START_AT_SEQUENCE,
        start_value=42,
        config=_make_config(),
    )
    assert isinstance(sub, EventsStoreSubscriber)
    assert sub.start_value == 42


def test_create_queues_subscriber():
    """QUEUES pattern creates QueuesSubscriber."""
    sub = create_subscriber(
        channel="q-ch",
        pattern=KubeMQPattern.QUEUES,
        config=_make_config(),
    )
    assert isinstance(sub, QueuesSubscriber)
    assert sub.channel == "q-ch"


def test_create_batch_queues_subscriber():
    """QUEUES pattern with batch=True creates BatchQueuesSubscriber."""
    sub = create_subscriber(
        channel="q-ch",
        pattern=KubeMQPattern.QUEUES,
        batch=True,
        max_messages=10,
        config=_make_config(),
    )
    assert isinstance(sub, BatchQueuesSubscriber)
    assert sub.max_messages == 10


def test_create_commands_subscriber():
    """COMMANDS pattern creates CommandsSubscriber."""
    sub = create_subscriber(
        channel="cmd-ch",
        pattern=KubeMQPattern.COMMANDS,
        config=_make_config(),
    )
    assert isinstance(sub, CommandsSubscriber)
    assert sub.channel == "cmd-ch"


def test_create_queries_subscriber():
    """QUERIES pattern creates QueriesSubscriber."""
    sub = create_subscriber(
        channel="qry-ch",
        pattern=KubeMQPattern.QUERIES,
        config=_make_config(),
    )
    assert isinstance(sub, QueriesSubscriber)
    assert sub.channel == "qry-ch"


def test_create_subscriber_unknown_pattern():
    """Unknown pattern raises ValueError."""
    with pytest.raises(ValueError, match="Unknown pattern"):
        create_subscriber(
            channel="ch",
            pattern="nonexistent",  # type: ignore[arg-type]
            config=_make_config(),
        )


def test_create_subscriber_with_group():
    """Subscriber factory passes group to config."""
    sub = create_subscriber(
        channel="ch",
        pattern=KubeMQPattern.EVENTS,
        group="my-group",
        config=_make_config(),
    )
    assert sub.group == "my-group"


def test_create_subscriber_with_no_reply():
    """Subscriber factory passes no_reply to config."""
    sub = create_subscriber(
        channel="ch",
        pattern=KubeMQPattern.COMMANDS,
        no_reply=True,
        config=_make_config(),
    )
    assert sub._no_reply is True


def test_create_queues_with_ack_policy():
    """Subscriber factory passes ack_policy to subscriber."""
    sub = create_subscriber(
        channel="q-ch",
        pattern=KubeMQPattern.QUEUES,
        ack_policy=AckPolicy.ACK_FIRST,
        config=_make_config(),
    )
    assert sub.ack_policy == AckPolicy.ACK_FIRST


def test_create_queues_with_wait_timeout():
    """Subscriber factory passes wait_timeout to subscriber."""
    sub = create_subscriber(
        channel="q-ch",
        pattern=KubeMQPattern.QUEUES,
        wait_timeout=120,
        config=_make_config(),
    )
    assert sub.wait_timeout == 120
