"""Command consume, response, error sanitization tests.

Tests: U-31 through U-33, U-36.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from faststream.middlewares import AckPolicy

from kubemq_faststream.schemas import KubeMQPattern
from kubemq_faststream.subscriber.commands import CommandsSubscriber


def _make_mock_command_received():
    """Create a mock CommandReceived."""
    mock = MagicMock()
    mock.body = b"cmd-body"
    mock.channel = "cmd-channel"
    mock.tags = {"ct": "cv"}
    mock.id = "cmd-1"
    mock.reply_channel = "reply-ch"
    mock.metadata = "cmd-meta"
    mock.timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return mock


def _make_commands_subscriber(channel="cmd-channel", no_reply=False):
    """Create a CommandsSubscriber with mock config."""
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
        pattern=KubeMQPattern.COMMANDS,
        no_reply=no_reply,
        _ack_policy=AckPolicy.ACK,
    )
    calls = CallsCollection()
    spec = KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=KubeMQPattern.COMMANDS,
            title_=None,
            description_=None,
        ),
    )
    return CommandsSubscriber(sub_config, spec, calls)


def _make_subscribe_fn(sub, *items):
    """Create a mock subscribe function that yields items then stops the subscriber."""
    async def mock_subscribe(subscription):
        for item in items:
            yield item
        sub.running = False
    return mock_subscribe


# U-31: test_commands_success_response
async def test_commands_success_response():
    """Command handler success sends CommandResponse with is_executed=True."""
    sub = _make_commands_subscriber()
    mock_cmd = _make_mock_command_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = _make_subscribe_fn(sub, mock_cmd)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_awaited_once()
    mock_cq.send_response.assert_awaited_once()

    # Check the response was a success
    response = mock_cq.send_response.call_args[0][0]
    assert response.is_executed is True
    assert response.request_id == "cmd-1"


# U-32: test_commands_failure_sanitized
async def test_commands_failure_sanitized(caplog):
    """Handler error sends sanitized 'handler execution failed' response (H-6)."""
    sub = _make_commands_subscriber()
    mock_cmd = _make_mock_command_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = _make_subscribe_fn(sub, mock_cmd)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=RuntimeError("internal secret error"))

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    # Error response should use sanitized message
    assert mock_cq.send_response.await_count == 1
    response = mock_cq.send_response.call_args[0][0]
    assert response.is_executed is False
    assert response.error == "handler execution failed"
    assert "Command handler error" in caplog.text


# U-33: test_commands_send_response_failure_logged
async def test_commands_send_response_failure_logged(caplog):
    """When send_response fails, error is logged but does not crash."""
    sub = _make_commands_subscriber()
    mock_cmd = _make_mock_command_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = _make_subscribe_fn(sub, mock_cmd)
    # Handler fails, then send_response also fails
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


# U-36: test_no_reply_suppresses_response
async def test_no_reply_suppresses_response():
    """With no_reply=True, no response is sent after handler execution (M-5)."""
    sub = _make_commands_subscriber(no_reply=True)
    mock_cmd = _make_mock_command_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = _make_subscribe_fn(sub, mock_cmd)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_awaited_once()
    mock_cq.send_response.assert_not_awaited()


async def test_no_reply_suppresses_error_response():
    """With no_reply=True, no error response is sent on handler failure."""
    sub = _make_commands_subscriber(no_reply=True)
    mock_cmd = _make_mock_command_received()

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = _make_subscribe_fn(sub, mock_cmd)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=RuntimeError("fail"))

    await sub._consume()

    mock_cq.send_response.assert_not_awaited()


# ---- New coverage tests: consume loop edge cases ----


async def test_commands_subscription_interrupted_resubscribes(caplog):
    """When subscribe_to_commands raises, logs and resubscribes."""
    sub = _make_commands_subscriber()

    call_count = 0

    async def failing_subscribe(subscription):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("stream interrupted")
        sub.running = False
        return
        yield  # make this an async generator

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = failing_subscribe
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "CQ subscription interrupted" in caplog.text


async def test_commands_stops_when_not_running():
    """Command subscriber breaks when running becomes False."""
    sub = _make_commands_subscriber()
    mock_cmd = _make_mock_command_received()

    async def mock_subscribe(subscription):
        sub.running = False
        yield mock_cmd

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = mock_subscribe
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_not_awaited()


async def test_commands_multiple_messages():
    """Command subscriber processes multiple messages in sequence."""
    sub = _make_commands_subscriber()
    cmd1 = _make_mock_command_received()
    cmd1.id = "cmd-1"
    cmd2 = _make_mock_command_received()
    cmd2.id = "cmd-2"

    mock_cq = MagicMock()
    mock_cq.config.client_id = "test-client"
    mock_cq.subscribe_to_commands = _make_subscribe_fn(sub, cmd1, cmd2)
    mock_cq.send_response = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cq = mock_cq
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    assert sub.consume.await_count == 2
    assert mock_cq.send_response.await_count == 2
