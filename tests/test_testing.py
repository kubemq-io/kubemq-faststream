"""TestKubeMQBroker, FakeProducer routing tests.

Tests: U-42 through U-44.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.schemas import FeatureNotSupportedException, KubeMQPattern
from kubemq_faststream.testing import (
    FakeProducer,
    TestKubeMQBroker,
    _build_mock_connection,
    _is_handler_matches,
    _match_wildcard,
    build_message,
)


# U-42: test_fake_producer_routes_to_subscriber
async def test_fake_producer_routes_to_subscriber():
    """FakeProducer.publish() routes to matching subscriber's process_message."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    sub = broker.subscriber(events="test-events")

    # Mock process_message
    sub.process_message = AsyncMock(return_value="processed")

    fake = FakeProducer(broker)

    from kubemq_faststream.response import KubeMQPublishCommand

    cmd = KubeMQPublishCommand(
        b"test-body",
        destination="test-events",
        pattern=KubeMQPattern.EVENTS,
        headers={},
    )
    result = await fake.publish(cmd)

    sub.process_message.assert_awaited_once()
    assert result == "processed"


# U-43: test_fake_producer_error_simulation
async def test_fake_producer_error_simulation():
    """FakeProducer request() rejects non-CQ patterns."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    fake = FakeProducer(broker)

    from kubemq_faststream.response import KubeMQPublishCommand

    cmd = KubeMQPublishCommand(
        b"test",
        destination="ch",
        pattern=KubeMQPattern.EVENTS,
    )
    with pytest.raises(FeatureNotSupportedException, match="Request not supported"):
        await fake.request(cmd)


async def test_fake_producer_request_cq():
    """FakeProducer request() works for commands pattern."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    sub = broker.subscriber(commands="cmd-ch")
    sub.process_message = AsyncMock(return_value="cmd-result")

    fake = FakeProducer(broker)

    from kubemq_faststream.response import KubeMQPublishCommand

    cmd = KubeMQPublishCommand(
        b"cmd-body",
        destination="cmd-ch",
        pattern=KubeMQPattern.COMMANDS,
    )
    result = await fake.request(cmd)
    assert result == "cmd-result"


# U-44: test_test_broker_context_manager
async def test_test_broker_context_manager():
    """TestKubeMQBroker context manager sets up fake connection and producer."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    sub = broker.subscriber(events="test-ch")

    async with TestKubeMQBroker(broker) as br:
        # Broker should be usable inside context
        assert br is broker
        # Producer should be replaced with FakeProducer
        assert isinstance(br.config.broker_config.producer, FakeProducer)


async def test_test_broker_fake_connect():
    """TestKubeMQBroker._fake_connect returns a mock connection."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    conn = await TestKubeMQBroker._fake_connect(broker)

    assert hasattr(conn, "pubsub")
    assert hasattr(conn, "queues")
    assert hasattr(conn, "cq")


def test_build_message_from_dict():
    """build_message serializes a dict body to JSON bytes."""
    raw = build_message(
        body={"key": "value"},
        channel="test-ch",
        pattern=KubeMQPattern.EVENTS,
    )
    assert b"key" in raw.body
    assert raw.channel == "test-ch"
    assert raw.pattern == KubeMQPattern.EVENTS


def test_build_message_from_bytes():
    """build_message accepts bytes directly."""
    raw = build_message(
        body=b"raw-bytes",
        channel="ch",
        pattern=KubeMQPattern.QUEUES,
    )
    assert raw.body == b"raw-bytes"


def test_build_message_from_str():
    """build_message encodes str to UTF-8 bytes."""
    raw = build_message(
        body="hello",
        channel="ch",
        pattern=KubeMQPattern.EVENTS,
    )
    assert raw.body == b"hello"


def test_build_message_unsupported_type():
    """build_message raises TypeError for non-serializable types."""
    with pytest.raises(TypeError, match="Cannot serialize"):
        build_message(
            body=object(),
            channel="ch",
            pattern=KubeMQPattern.EVENTS,
        )


def test_is_handler_matches():
    """_is_handler_matches checks channel and pattern."""
    handler = MagicMock()
    handler.channel = "test-ch"
    handler.pattern = KubeMQPattern.EVENTS

    assert _is_handler_matches(handler, "test-ch", KubeMQPattern.EVENTS) is True
    assert _is_handler_matches(handler, "other-ch", KubeMQPattern.EVENTS) is False
    assert _is_handler_matches(handler, "test-ch", KubeMQPattern.QUEUES) is False


def test_match_wildcard_star():
    """_match_wildcard matches single-level wildcard (*)."""
    assert _match_wildcard("orders.*", "orders.new") is True
    assert _match_wildcard("orders.*", "orders.new.sub") is False
    assert _match_wildcard("orders.*", "other.new") is False


def test_match_wildcard_chevron():
    """_match_wildcard matches multi-level wildcard (>)."""
    assert _match_wildcard("orders.>", "orders.new") is True
    assert _match_wildcard("orders.>", "orders.new.sub") is True
    assert _match_wildcard("orders.>", "other.new") is False


def test_match_wildcard_exact():
    """_match_wildcard matches exact channel."""
    assert _match_wildcard("orders.new", "orders.new") is True
    assert _match_wildcard("orders.new", "orders.old") is False
