"""Registrator subscriber() and publisher() registration tests.

Tests: subscriber/publisher registration via _resolve_pattern.
"""

from __future__ import annotations

import pytest
from faststream.exceptions import SetupError

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.registrator import _resolve_pattern
from kubemq_faststream.schemas import KubeMQPattern


def test_resolve_pattern_events():
    """events keyword resolves to EVENTS pattern."""
    channel, pattern = _resolve_pattern(events="my-channel")
    assert channel == "my-channel"
    assert pattern == KubeMQPattern.EVENTS


def test_resolve_pattern_events_store():
    """events_store keyword resolves to EVENTS_STORE pattern."""
    channel, pattern = _resolve_pattern(events_store="my-channel")
    assert channel == "my-channel"
    assert pattern == KubeMQPattern.EVENTS_STORE


def test_resolve_pattern_queues():
    """queues keyword resolves to QUEUES pattern."""
    channel, pattern = _resolve_pattern(queues="my-channel")
    assert channel == "my-channel"
    assert pattern == KubeMQPattern.QUEUES


def test_resolve_pattern_commands():
    """commands keyword resolves to COMMANDS pattern."""
    channel, pattern = _resolve_pattern(commands="my-channel")
    assert channel == "my-channel"
    assert pattern == KubeMQPattern.COMMANDS


def test_resolve_pattern_queries():
    """queries keyword resolves to QUERIES pattern."""
    channel, pattern = _resolve_pattern(queries="my-channel")
    assert channel == "my-channel"
    assert pattern == KubeMQPattern.QUERIES


def test_resolve_pattern_none_specified():
    """No pattern keyword raises SetupError."""
    with pytest.raises(SetupError, match="Specify one of"):
        _resolve_pattern()


def test_resolve_pattern_multiple_specified():
    """Multiple pattern keywords raises SetupError."""
    with pytest.raises(SetupError, match="Specify only ONE"):
        _resolve_pattern(events="ch1", queues="ch2")


def test_resolve_pattern_empty_channel():
    """Empty channel name raises SetupError."""
    with pytest.raises(SetupError, match="Channel name for"):
        _resolve_pattern(events="")


def test_subscriber_registration():
    """broker.subscriber() returns a subscriber with correct channel and pattern."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    sub = broker.subscriber(events="test-events")
    assert sub.channel == "test-events"
    assert sub.pattern == KubeMQPattern.EVENTS


def test_subscriber_queues_registration():
    """broker.subscriber(queues=...) returns a QueuesSubscriber."""
    from kubemq_faststream.subscriber.queues import QueuesSubscriber

    broker = KubeMQBroker("kubemq://localhost:50000")
    sub = broker.subscriber(queues="test-queue")
    assert isinstance(sub, QueuesSubscriber)
    assert sub.channel == "test-queue"
    assert sub.pattern == KubeMQPattern.QUEUES


def test_publisher_registration():
    """broker.publisher() returns a publisher with correct channel and pattern."""
    from kubemq_faststream.publisher import KubeMQPublisher

    broker = KubeMQBroker("kubemq://localhost:50000")
    pub = broker.publisher(events="test-output")
    assert isinstance(pub, KubeMQPublisher)
    assert pub.channel == "test-output"
    assert pub.pattern == KubeMQPattern.EVENTS
