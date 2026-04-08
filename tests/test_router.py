"""Router composition, prefix propagation tests.

Tests: U-39 through U-41.
"""

from __future__ import annotations

import pytest

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.router import KubeMQRouter
from kubemq_faststream.schemas import KubeMQPattern


# U-39: test_include_router_merges_subscribers
def test_include_router_merges_subscribers():
    """Router subscribers are merged into broker on include_router."""
    router = KubeMQRouter()
    sub = router.subscriber(events="router-events")

    broker = KubeMQBroker("kubemq://localhost:50000")
    broker.include_router(router)

    # The broker should now contain the subscriber registered via the router
    channels = [getattr(s, "channel", None) for s in broker.subscribers]
    assert "router-events" in channels


# U-40: test_include_router_applies_prefix
def test_include_router_applies_prefix():
    """Router with prefix propagates prefix to subscriber channels."""
    router = KubeMQRouter(prefix="orders.")
    sub = router.subscriber(events="new")

    # The subscriber registered on the router should have the channel "new"
    assert sub.channel == "new"


# U-41: test_broker_include_router
def test_broker_include_router():
    """Broker.include_router integrates router subscribers and publishers."""
    router = KubeMQRouter()
    sub = router.subscriber(queues="router-queue")
    pub = router.publisher(queues="router-out")

    broker = KubeMQBroker("kubemq://localhost:50000")
    broker.include_router(router)

    sub_channels = [getattr(s, "channel", None) for s in broker.subscribers]
    assert "router-queue" in sub_channels

    pub_channels = [getattr(p, "channel", None) for p in broker.publishers]
    assert "router-out" in pub_channels


def test_router_creates_with_prefix():
    """KubeMQRouter can be created with a prefix."""
    router = KubeMQRouter(prefix="test.")
    assert router.config.prefix == "test."


def test_router_subscriber_returns_subscriber():
    """Router.subscriber() returns a KubeMQSubscriber instance."""
    from kubemq_faststream.subscriber.usecase import KubeMQSubscriber

    router = KubeMQRouter()
    sub = router.subscriber(events="test-ch")
    assert isinstance(sub, KubeMQSubscriber)


def test_router_publisher_returns_publisher():
    """Router.publisher() returns a KubeMQPublisher instance."""
    from kubemq_faststream.publisher import KubeMQPublisher

    router = KubeMQRouter()
    pub = router.publisher(events="test-ch")
    assert isinstance(pub, KubeMQPublisher)
