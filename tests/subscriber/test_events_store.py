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


async def test_events_store_consume_start_at_sequence_with_value():
    """_consume() passes events_store_sequence_value for START_AT_SEQUENCE."""
    from unittest.mock import patch, call

    sub = _make_es_subscriber(
        start_position=StartPosition.START_AT_SEQUENCE,
        start_value=42,
    )
    mock_event = _make_mock_event_store()

    async def mock_subscribe(subscription):
        yield mock_event

    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events_store = mock_subscribe
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with patch(
        "kubemq.pubsub.events_store_subscription.EventsStoreSubscription"
    ) as MockSub:
        sentinel = MagicMock()
        MockSub.return_value = sentinel

        await sub._consume()

        MockSub.assert_called_once()
        kwargs = MockSub.call_args[1]
        assert kwargs["events_store_sequence_value"] == 42


async def test_events_store_consume_start_at_time_with_value():
    """_consume() passes events_store_start_time for START_AT_TIME."""
    from unittest.mock import patch

    sub = _make_es_subscriber(
        start_position=StartPosition.START_AT_TIME,
        start_value=1700000000.0,
    )
    mock_event = _make_mock_event_store()

    mock_conn = MagicMock()

    async def mock_subscribe(subscription):
        yield mock_event

    mock_conn.pubsub.subscribe_to_events_store = mock_subscribe
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with patch(
        "kubemq.pubsub.events_store_subscription.EventsStoreSubscription"
    ) as MockSub:
        MockSub.return_value = MagicMock()

        await sub._consume()

        MockSub.assert_called_once()
        kwargs = MockSub.call_args[1]
        assert "events_store_start_time" in kwargs
        start_time = kwargs["events_store_start_time"]
        assert isinstance(start_time, datetime)
        # 1700000000.0 -> 2023-11-14T22:13:20 UTC
        assert start_time == datetime.fromtimestamp(1700000000.0, tz=timezone.utc)


async def test_events_store_consume_start_at_time_delta_with_value():
    """_consume() passes events_store_time_delta_seconds for START_AT_TIME_DELTA."""
    from unittest.mock import patch

    sub = _make_es_subscriber(
        start_position=StartPosition.START_AT_TIME_DELTA,
        start_value=3600,
    )
    mock_event = _make_mock_event_store()

    mock_conn = MagicMock()

    async def mock_subscribe(subscription):
        yield mock_event

    mock_conn.pubsub.subscribe_to_events_store = mock_subscribe
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with patch(
        "kubemq.pubsub.events_store_subscription.EventsStoreSubscription"
    ) as MockSub:
        MockSub.return_value = MagicMock()

        await sub._consume()

        MockSub.assert_called_once()
        kwargs = MockSub.call_args[1]
        assert kwargs["events_store_time_delta_seconds"] == 3600


# ---- Lifecycle edge tests ----


async def test_events_store_consume_stops_when_not_running():
    """_consume() breaks out of the loop when running is set to False."""
    sub = _make_es_subscriber()
    mock_event1 = _make_mock_event_store()
    mock_event1.id = "es-id-1"
    mock_event2 = _make_mock_event_store()
    mock_event2.id = "es-id-2"

    async def mock_subscribe(subscription):
        yield mock_event1
        yield mock_event2

    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events_store = mock_subscribe
    sub._connection = mock_conn
    sub.running = True

    call_count = 0

    async def consume_side_effect(msg):
        nonlocal call_count
        call_count += 1
        if call_count >= 1:
            sub.running = False

    sub.consume = AsyncMock(side_effect=consume_side_effect)

    await sub._consume()

    # Only the first event should have been consumed before running was set False
    assert call_count == 1


async def test_events_store_consume_propagates_cancellation():
    """_consume() re-raises CancelledError (anyio cancellation)."""
    import asyncio

    sub = _make_es_subscriber()
    mock_event = _make_mock_event_store()

    async def mock_subscribe(subscription):
        yield mock_event

    mock_conn = MagicMock()
    mock_conn.pubsub.subscribe_to_events_store = mock_subscribe
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await sub._consume()
