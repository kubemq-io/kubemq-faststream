"""Publisher decorator wiring, handler output routing tests.

Tests: U-37, U-38.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.publisher import KubeMQPublisher
from kubemq_faststream.publisher.config import (
    KubeMQPublisherConfig,
    KubeMQPublisherSpecificationConfig,
)
from kubemq_faststream.publisher.specification import KubeMQPublisherSpecification
from kubemq_faststream.response import KubeMQPublishCommand
from kubemq_faststream.schemas import KubeMQPattern


def _make_publisher(
    channel: str = "out-ch",
    pattern: KubeMQPattern = KubeMQPattern.EVENTS,
) -> KubeMQPublisher:
    """Create a KubeMQPublisher with mock config."""
    from kubemq_faststream.config import KubeMQBrokerConfig

    broker_config = KubeMQBrokerConfig()
    pub_config = KubeMQPublisherConfig(
        _outer_config=broker_config,
        channel=channel,
        pattern=pattern,
        middlewares=(),
    )
    pub_spec = KubeMQPublisherSpecification(
        _outer_config=broker_config,
        specification_config=KubeMQPublisherSpecificationConfig(
            channel=channel,
            pattern=pattern,
            title_=None,
            description_=None,
            schema_=None,
        ),
    )
    return KubeMQPublisher(config=pub_config, specification=pub_spec)


# U-37: test_publisher_decorator_wires_handler
def test_publisher_decorator_wires_handler():
    """broker.publisher() creates a KubeMQPublisher with correct channel/pattern."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    pub = broker.publisher(queues="output-queue")

    assert isinstance(pub, KubeMQPublisher)
    assert pub.channel == "output-queue"
    assert pub.pattern == KubeMQPattern.QUEUES


def test_publisher_stores_channel_and_pattern():
    """Publisher __init__ copies channel and pattern from config."""
    pub = _make_publisher(channel="test-ch", pattern=KubeMQPattern.QUERIES)
    assert pub.channel == "test-ch"
    assert pub.pattern == KubeMQPattern.QUERIES


# U-38: test_publisher_routes_return_value
async def test_publisher_routes_return_value():
    """Publisher._publish() wraps the command with from_cmd() for handler output routing (H-2)."""
    pub = _make_publisher(channel="out-ch", pattern=KubeMQPattern.EVENTS)

    # Mock _basic_publish
    pub._basic_publish = AsyncMock()
    pub._producer = MagicMock()

    # Simulate FastStream calling _publish with a generic PublishCommand
    from faststream.response import PublishCommand, PublishType

    generic_cmd = PublishCommand(
        b"handler-result",
        destination="out-ch",
        _publish_type=PublishType.PUBLISH,
    )
    await pub._publish(generic_cmd)

    # _basic_publish should have been called with a KubeMQPublishCommand
    pub._basic_publish.assert_awaited_once()
    call_args = pub._basic_publish.call_args
    cmd = call_args[0][0]
    assert isinstance(cmd, KubeMQPublishCommand)
    assert cmd.pattern == KubeMQPattern.EVENTS


async def test_publisher_publish_creates_kubemq_command():
    """publisher.publish() creates a KubeMQPublishCommand with the right pattern."""
    pub = _make_publisher(channel="direct-ch", pattern=KubeMQPattern.QUEUES)
    pub._basic_publish = AsyncMock()
    pub._producer = MagicMock()

    await pub.publish(b"direct-message")

    pub._basic_publish.assert_awaited_once()
    call_args = pub._basic_publish.call_args
    cmd = call_args[0][0]
    assert isinstance(cmd, KubeMQPublishCommand)
    assert cmd.destination == "direct-ch"
    assert cmd.pattern == KubeMQPattern.QUEUES


async def test_publisher_publish_custom_channel():
    """publisher.publish() can override the channel."""
    pub = _make_publisher(channel="default-ch", pattern=KubeMQPattern.EVENTS)
    pub._basic_publish = AsyncMock()
    pub._producer = MagicMock()

    await pub.publish(b"msg", channel="override-ch")

    call_args = pub._basic_publish.call_args
    cmd = call_args[0][0]
    assert cmd.destination == "override-ch"
