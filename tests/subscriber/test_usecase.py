"""Tests for KubeMQSubscriber base class (usecase.py).

Covers: set_connection, start, get_one, __aiter__, _make_response_publisher.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.middlewares import AckPolicy

from kubemq_faststream.config import KubeMQBrokerConfig
from kubemq_faststream.schemas import KubeMQPattern
from kubemq_faststream.subscriber.config import (
    KubeMQSubscriberConfig,
    KubeMQSubscriberSpecificationConfig,
)
from kubemq_faststream.subscriber.specification import KubeMQSubscriberSpecification
from kubemq_faststream.subscriber.usecase import KubeMQSubscriber


class ConcreteSubscriber(KubeMQSubscriber):
    """Concrete implementation for testing the base class."""

    async def _consume(self) -> None:
        """No-op consume for testing."""
        pass


def _make_base_subscriber(channel="test-ch", pattern=KubeMQPattern.EVENTS):
    """Create a ConcreteSubscriber with mock config."""
    broker_config = KubeMQBrokerConfig()
    sub_config = KubeMQSubscriberConfig(
        _outer_config=broker_config,
        channel=channel,
        pattern=pattern,
        _ack_policy=AckPolicy.ACK,
    )
    calls = CallsCollection()
    spec = KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=pattern,
            title_=None,
            description_=None,
        ),
    )
    return ConcreteSubscriber(sub_config, spec, calls)


def test_set_connection():
    """set_connection stores the connection on the subscriber."""
    sub = _make_base_subscriber()
    assert sub._connection is None

    mock_conn = MagicMock()
    sub.set_connection(mock_conn)
    assert sub._connection is mock_conn


def test_initial_state():
    """Subscriber initializes with correct channel, pattern, group."""
    sub = _make_base_subscriber(channel="my-channel", pattern=KubeMQPattern.QUEUES)
    assert sub.channel == "my-channel"
    assert sub.pattern == KubeMQPattern.QUEUES
    assert sub.group is None
    assert sub._connection is None


async def test_get_one_raises_not_implemented():
    """get_one() raises NotImplementedError."""
    sub = _make_base_subscriber()
    with pytest.raises(NotImplementedError, match="get_one.*not supported"):
        await sub.get_one()


async def test_get_one_with_timeout_raises():
    """get_one(timeout=10) raises NotImplementedError."""
    sub = _make_base_subscriber()
    with pytest.raises(NotImplementedError, match="get_one.*not supported"):
        await sub.get_one(timeout=10.0)


async def test_aiter_raises_not_implemented():
    """__aiter__() raises NotImplementedError."""
    sub = _make_base_subscriber()
    with pytest.raises(NotImplementedError, match="Async iteration.*not supported"):
        async for _ in sub:
            pass


def test_make_response_publisher_returns_empty():
    """_make_response_publisher returns empty iterable."""
    sub = _make_base_subscriber()
    mock_msg = MagicMock()
    result = sub._make_response_publisher(mock_msg)
    assert list(result) == []


def test_parser_initialized():
    """Subscriber has a KubeMQ parser initialized."""
    sub = _make_base_subscriber()
    from kubemq_faststream.parser import KubeMQParser

    assert isinstance(sub._kubemq_parser, KubeMQParser)
