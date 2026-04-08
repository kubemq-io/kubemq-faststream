"""Query consume, response, cache fields tests.

Tests: U-34, U-35.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from faststream.middlewares import AckPolicy

from kubemq_faststream.schemas import KubeMQPattern
from kubemq_faststream.subscriber.queries import QueriesSubscriber


def _make_mock_query_received(**overrides):
    """Create a mock QueryReceived."""
    mock = MagicMock()
    mock.body = overrides.get("body", b"query-body")
    mock.channel = overrides.get("channel", "query-channel")
    mock.tags = overrides.get("tags", {"qt": "qv"})
    mock.id = overrides.get("id", "qry-1")
    mock.reply_channel = overrides.get("reply_channel", "qry-reply-ch")
    mock.metadata = overrides.get("metadata", "query-meta")
    mock.timestamp = overrides.get(
        "timestamp", datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    mock.cache_key = overrides.get("cache_key", "ckey")
    mock.cache_ttl = overrides.get("cache_ttl", 300)
    return mock


def _make_queries_subscriber(channel="query-channel", no_reply=False):
    """Create a QueriesSubscriber with mock config."""
    from kubemq_faststream.config import KubeMQBrokerConfig
    from kubemq_faststream.subscriber.config import (
        KubeMQSubscriberConfig,
        KubeMQSubscriberSpecificationConfig,
    )
    from kubemq_faststream.subscriber.specification import KubeMQSubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection

    broker_config = KubeMQBrokerConfig()
    sub_config = KubeMQSubscriberConfig(
        _outer_config=broker_config,
        channel=channel,
        pattern=KubeMQPattern.QUERIES,
        no_reply=no_reply,
        _ack_policy=AckPolicy.ACK,
    )
    calls = CallsCollection()
    spec = KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=KubeMQPattern.QUERIES,
            title_=None,
            description_=None,
        ),
    )
    return QueriesSubscriber(sub_config, spec, calls)


def _make_subscribe_fn(sub, *items):
    """Create a mock subscribe function that yields items then stops the subscriber."""
    async def mock_subscribe(subscription):
        for item in items:
            yield item
        sub.running = False
    return mock_subscribe


# U-34: test_queries_success_with_body
async def test_queries_success_with_body():
    """Query handler success sends QueryResponse with encoded body."""
    sub = _make_queries_subscriber()
    mock_query = _make_mock_query_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(return_value={"result": "ok"})

    await sub._consume()

    sub.consume.assert_awaited_once()
    mock_cq.send_response.assert_awaited_once()

    response = mock_cq.send_response.call_args[0][0]
    assert response.is_executed is True
    assert response.request_id == "qry-1"
    # Body should be bytes (encoded)
    assert isinstance(response.body, bytes)


# U-35: test_queries_cache_fields_propagated
async def test_queries_cache_fields_propagated():
    """Cache hit from incoming query is propagated to response."""
    sub = _make_queries_subscriber()
    mock_query = _make_mock_query_received()
    mock_query.cache_hit = True

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(return_value="cached-result")

    await sub._consume()

    response = mock_cq.send_response.call_args[0][0]
    assert response.cache_hit is True
    assert response.metadata == "query-meta"
    assert response.tags == {"qt": "qv"}


async def test_queries_error_sanitized(caplog):
    """Handler error sends sanitized error response."""
    sub = _make_queries_subscriber()
    mock_query = _make_mock_query_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=RuntimeError("secret error"))

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    response = mock_cq.send_response.call_args[0][0]
    assert response.is_executed is False
    assert response.error == "handler execution failed"
    assert "Query handler error" in caplog.text


async def test_queries_no_reply_suppresses_response():
    """With no_reply=True, no response is sent."""
    sub = _make_queries_subscriber(no_reply=True)
    mock_query = _make_mock_query_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(return_value="ignored")

    await sub._consume()

    sub.consume.assert_awaited_once()
    mock_cq.send_response.assert_not_awaited()


async def test_queries_none_result():
    """Handler returning None sends empty body in response."""
    sub = _make_queries_subscriber()
    mock_query = _make_mock_query_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(return_value=None)

    await sub._consume()

    response = mock_cq.send_response.call_args[0][0]
    assert response.body == b""
    assert response.is_executed is True


# ---- New coverage tests: consume loop edge cases ----


async def test_queries_send_error_response_failure_logged(caplog):
    """When send_response fails for error response, it is logged."""
    sub = _make_queries_subscriber()
    mock_query = _make_mock_query_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query)
    mock_cq.send_response = AsyncMock(
        side_effect=RuntimeError("send_response failed")
    )

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=RuntimeError("handler error"))

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Failed to send error response" in caplog.text


async def test_queries_subscription_interrupted_resubscribes(caplog):
    """When subscribe_to_queries raises, logs and retries (once)."""
    sub = _make_queries_subscriber()

    call_count = 0

    async def failing_subscribe(subscription):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("stream interrupted")
        # On second call, stop running
        sub.running = False
        # yield nothing
        return
        yield  # make this an async generator

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = failing_subscribe
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "CQ subscription interrupted" in caplog.text


async def test_queries_no_reply_error_suppresses_error_response():
    """With no_reply=True, no error response is sent on handler failure."""
    sub = _make_queries_subscriber(no_reply=True)
    mock_query = _make_mock_query_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=RuntimeError("fail"))

    await sub._consume()

    mock_cq.send_response.assert_not_awaited()


async def test_queries_multiple_messages():
    """Query subscriber processes multiple messages in sequence."""
    sub = _make_queries_subscriber()
    mock_query1 = _make_mock_query_received(id="qry-1")
    mock_query2 = _make_mock_query_received(id="qry-2")

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = _make_subscribe_fn(sub, mock_query1, mock_query2)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(return_value="ok")

    await sub._consume()

    assert sub.consume.await_count == 2
    assert mock_cq.send_response.await_count == 2


async def test_queries_stops_when_not_running():
    """Query subscriber breaks when running becomes False."""
    sub = _make_queries_subscriber()
    mock_query = _make_mock_query_received()

    async def mock_subscribe(subscription):
        sub.running = False
        yield mock_query

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_queries = mock_subscribe
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_not_awaited()
