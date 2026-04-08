"""Events subscriber consume loop, error handling tests.

Tests: U-23, U-24.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq_faststream.message import KubeMQRawMessage
from kubemq_faststream.schemas import KubeMQPattern
from kubemq_faststream.subscriber.events import EventsSubscriber


def _make_mock_event():
    """Create a mock EventReceived."""
    mock = MagicMock()
    mock.body = b"event-body"
    mock.channel = "test-events"
    mock.tags = {"t1": "v1"}
    mock.id = "evt-1"
    mock.metadata = "meta"
    mock.timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return mock


def _make_events_subscriber(channel="test-events", group=None):
    """Create an EventsSubscriber with mock config."""
    from kubemq_faststream.config import KubeMQBrokerConfig
    from kubemq_faststream.subscriber.config import (
        KubeMQSubscriberConfig,
        KubeMQSubscriberSpecificationConfig,
    )
    from kubemq_faststream.subscriber.specification import KubeMQSubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.middlewares import AckPolicy

    broker_config = KubeMQBrokerConfig()
    sub_config = KubeMQSubscriberConfig(
        _outer_config=broker_config,
        channel=channel,
        pattern=KubeMQPattern.EVENTS,
        group=group,
        _ack_policy=AckPolicy.ACK,
    )
    calls = CallsCollection()
    spec = KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=KubeMQPattern.EVENTS,
            title_=None,
            description_=None,
        ),
    )
    return EventsSubscriber(sub_config, spec, calls)


# U-23: test_events_consume_processes_message
async def test_events_consume_processes_message():
    """Events _consume() parses SDK events and routes through consume pipeline."""
    sub = _make_events_subscriber()
    mock_event = _make_mock_event()

    # Create an async generator that yields one event then stops
    async def mock_subscribe(subscription):
        yield mock_event

    # Set up connection mock
    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events = mock_subscribe
    sub._connection = mock_conn
    sub.running = True

    # Mock consume to track calls
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_awaited_once()
    call_arg = sub.consume.call_args[0][0]
    assert call_arg.body == b"event-body"


# U-24: test_events_consume_error_continues
async def test_events_consume_error_continues(caplog):
    """Events _consume() logs error and continues on handler exception (H-4)."""
    sub = _make_events_subscriber()
    mock_event1 = _make_mock_event()
    mock_event2 = _make_mock_event()
    mock_event2.id = "evt-2"

    call_count = 0

    async def mock_subscribe(subscription):
        yield mock_event1
        yield mock_event2

    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events = mock_subscribe
    sub._connection = mock_conn
    sub.running = True

    # First call raises, second succeeds
    sub.consume = AsyncMock(side_effect=[RuntimeError("handler boom"), None])

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    # consume should have been called twice (error did not kill the loop)
    assert sub.consume.await_count == 2
    assert "Event handler error" in caplog.text


async def test_events_consume_stops_when_not_running():
    """Events _consume() breaks when running becomes False."""
    sub = _make_events_subscriber()
    mock_event = _make_mock_event()

    async def mock_subscribe(subscription):
        sub.running = False  # Stop after yielding
        yield mock_event

    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events = mock_subscribe
    sub._connection = mock_conn
    sub.running = True

    sub.consume = AsyncMock()

    await sub._consume()
    # Should have stopped without processing the event
    sub.consume.assert_not_awaited()
