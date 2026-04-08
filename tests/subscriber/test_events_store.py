"""EventsStore subscriber consume, start positions tests."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq_faststream.schemas import KubeMQPattern, StartPosition
from kubemq_faststream.subscriber.events_store import EventsStoreSubscriber


def _make_mock_event_store():
    """Create a mock EventStoreReceived."""
    mock = MagicMock()
    mock.body = b"es-body"
    mock.channel = "es-channel"
    mock.tags = {"tag": "val"}
    mock.id = "es-id-1"
    mock.metadata = "es-meta"
    mock.timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
    mock.sequence = 42
    return mock


def _make_es_subscriber(
    channel="es-channel",
    start_position=StartPosition.START_FROM_NEW,
    start_value=None,
):
    """Create an EventsStoreSubscriber with mock config."""
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
        pattern=KubeMQPattern.EVENTS_STORE,
        _ack_policy=AckPolicy.ACK,
    )
    calls = CallsCollection()
    spec = KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=KubeMQPattern.EVENTS_STORE,
            title_=None,
            description_=None,
        ),
    )
    return EventsStoreSubscriber(
        sub_config, spec, calls,
        start_position=start_position,
        start_value=start_value,
    )


async def test_events_store_consume_processes_message():
    """EventsStore _consume() parses SDK events and routes through consume."""
    sub = _make_es_subscriber()
    mock_event = _make_mock_event_store()

    async def mock_subscribe(subscription):
        yield mock_event

    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events_store = mock_subscribe
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_awaited_once()
    call_arg = sub.consume.call_args[0][0]
    assert call_arg.body == b"es-body"


async def test_events_store_error_continues(caplog):
    """EventsStore _consume() logs error and continues on handler exception."""
    sub = _make_es_subscriber()
    mock_event1 = _make_mock_event_store()
    mock_event2 = _make_mock_event_store()
    mock_event2.id = "es-id-2"

    async def mock_subscribe(subscription):
        yield mock_event1
        yield mock_event2

    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events_store = mock_subscribe
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=[RuntimeError("fail"), None])

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert sub.consume.await_count == 2
    assert "EventStore handler error" in caplog.text


def test_start_position_new():
    """START_FROM_NEW does not require start_value."""
    sub = _make_es_subscriber(start_position=StartPosition.START_FROM_NEW)
    assert sub.start_position == StartPosition.START_FROM_NEW


def test_start_position_first():
    """START_FROM_FIRST does not require start_value."""
    sub = _make_es_subscriber(start_position=StartPosition.START_FROM_FIRST)
    assert sub.start_position == StartPosition.START_FROM_FIRST


def test_start_position_at_sequence_requires_value():
    """START_AT_SEQUENCE requires a positive start_value."""
    with pytest.raises(ValueError, match="start_value must be a positive"):
        _make_es_subscriber(
            start_position=StartPosition.START_AT_SEQUENCE,
            start_value=None,
        )


def test_start_position_at_sequence_valid():
    """START_AT_SEQUENCE with positive value is accepted."""
    sub = _make_es_subscriber(
        start_position=StartPosition.START_AT_SEQUENCE,
        start_value=10,
    )
    assert sub.start_value == 10


def test_start_position_at_time_requires_value():
    """START_AT_TIME requires a positive start_value."""
    with pytest.raises(ValueError, match="start_value must be a positive"):
        _make_es_subscriber(
            start_position=StartPosition.START_AT_TIME,
            start_value=0,
        )


def test_start_position_at_time_delta_requires_value():
    """START_AT_TIME_DELTA requires a positive start_value."""
    with pytest.raises(ValueError, match="start_value must be a positive"):
        _make_es_subscriber(
            start_position=StartPosition.START_AT_TIME_DELTA,
            start_value=-1,
        )


def test_map_start_position():
    """_map_start_position maps adapter enum to SDK enum."""
    sub = _make_es_subscriber(start_position=StartPosition.START_FROM_NEW)
    from kubemq.pubsub.events_store_subscription import EventStoreStartPosition
    result = sub._map_start_position()
    assert result == EventStoreStartPosition.StartFromNew
