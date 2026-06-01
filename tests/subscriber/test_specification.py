"""Subscriber specification tests -- name fallback, title, get_schema.

Tests: specification name property and get_schema return value.
"""

from __future__ import annotations

from unittest.mock import PropertyMock, patch

from faststream._internal.endpoint.subscriber.call_item import CallsCollection

from kubemq_faststream.config import KubeMQBrokerConfig
from kubemq_faststream.schemas import KubeMQPattern
from kubemq_faststream.subscriber.config import KubeMQSubscriberSpecificationConfig
from kubemq_faststream.subscriber.specification import KubeMQSubscriberSpecification


def _make_spec(
    channel: str = "test-ch",
    pattern: KubeMQPattern = KubeMQPattern.EVENTS,
    title_: str | None = None,
    description_: str | None = None,
) -> KubeMQSubscriberSpecification:
    """Create a KubeMQSubscriberSpecification with the given config."""
    broker_config = KubeMQBrokerConfig()
    calls = CallsCollection()
    return KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=pattern,
            title_=title_,
            description_=description_,
        ),
    )


def test_subscriber_specification_name_fallback_to_call_name():
    """When title_ is None, spec.name falls back to call_name."""
    spec = _make_spec(title_=None)
    with patch.object(
        type(spec), "call_name", new_callable=PropertyMock, return_value="fallback-name"
    ):
        assert spec.name == "fallback-name"


def test_subscriber_specification_name_uses_title():
    """When title_ is set, spec.name returns the title."""
    spec = _make_spec(title_="My Title")
    assert spec.name == "My Title"


def test_subscriber_specification_get_schema_returns_empty_dict():
    """get_schema() returns an empty dict."""
    spec = _make_spec()
    assert spec.get_schema() == {}
