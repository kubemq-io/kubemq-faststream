"""Queue downstream, ack policy mapping tests.

Tests: U-25 through U-30.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import anyio
import pytest
from faststream.middlewares import AckPolicy

from kubemq_faststream.schemas import KubeMQPattern
from kubemq_faststream.subscriber.queues import QueuesSubscriber


def _make_mock_queue_msg():
    """Create a mock QueueMessageReceived."""
    mock = MagicMock()
    mock.body = b"queue-body"
    mock.channel = "test-queue"
    mock.tags = {"qt": "qv"}
    mock.id = "q-id-1"
    mock.metadata = "q-meta"
    mock.timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)
    mock.sequence = 1
    mock.async_ack = AsyncMock()
    mock.async_re_queue = AsyncMock()
    mock.async_nack = AsyncMock()
    return mock


def _make_mock_poll_response(messages=None, is_error=False, error=""):
    """Create a mock poll response."""
    mock = MagicMock()
    mock.is_error = is_error
    mock.error = error
    mock.messages = messages or []
    return mock


def _make_queues_subscriber(
    channel="test-queue",
    ack_policy=AckPolicy.ACK,
    max_messages=1,
    wait_timeout=60,
):
    """Create a QueuesSubscriber with mock config."""
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
        pattern=KubeMQPattern.QUEUES,
        _ack_policy=ack_policy,
        max_messages=max_messages,
        wait_timeout=wait_timeout,
    )
    calls = CallsCollection()
    spec = KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=KubeMQPattern.QUEUES,
            title_=None,
            description_=None,
        ),
    )
    return QueuesSubscriber(sub_config, spec, calls)


# U-25: test_queues_ack_policy_ack
async def test_queues_ack_policy_ack():
    """ACK policy: message is processed, ack handled by pipeline."""
    sub = _make_queues_subscriber(ack_policy=AckPolicy.ACK)
    queue_msg = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[queue_msg])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        # Stop the loop on second call
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_awaited_once()


# U-26: test_queues_ack_policy_reject_on_error
def test_queues_ack_policy_reject_on_error():
    """REJECT_ON_ERROR ack policy is accepted by subscriber."""
    sub = _make_queues_subscriber(ack_policy=AckPolicy.REJECT_ON_ERROR)
    assert sub.ack_policy == AckPolicy.REJECT_ON_ERROR


# U-27: test_queues_ack_policy_nack_on_error
def test_queues_ack_policy_nack_on_error():
    """NACK_ON_ERROR ack policy is accepted by subscriber."""
    sub = _make_queues_subscriber(ack_policy=AckPolicy.NACK_ON_ERROR)
    assert sub.ack_policy == AckPolicy.NACK_ON_ERROR


# U-28: test_queues_ack_policy_ack_first
async def test_queues_ack_policy_ack_first():
    """ACK_FIRST policy passes auto_ack=True to SDK receive."""
    sub = _make_queues_subscriber(ack_policy=AckPolicy.ACK_FIRST)
    queue_msg = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[queue_msg])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Verify auto_ack is True for ACK_FIRST
            assert kwargs.get("auto_ack") is True
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_awaited_once()


# U-29: test_queues_ack_policy_manual
def test_queues_ack_policy_manual():
    """MANUAL ack policy is accepted by subscriber."""
    sub = _make_queues_subscriber(ack_policy=AckPolicy.MANUAL)
    assert sub.ack_policy == AckPolicy.MANUAL


# U-30: test_queues_poll_error_logged
async def test_queues_poll_error_logged(caplog):
    """Queue poll error is logged and loop continues."""
    sub = _make_queues_subscriber()

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _make_mock_poll_response(is_error=True, error="poll failed")
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Queue poll error" in caplog.text
    sub.consume.assert_not_awaited()


async def test_queues_consume_sets_queue_msg_on_kubemq_message():
    """_consume() passes raw KubeMQRawMessage with queue_msg for ack/nack."""
    sub = _make_queues_subscriber()
    queue_msg = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[queue_msg])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True

    parsed_msgs = []

    async def capture_consume(msg):
        parsed_msgs.append(msg)

    sub.consume = AsyncMock(side_effect=capture_consume)

    await sub._consume()

    assert len(parsed_msgs) == 1
    assert parsed_msgs[0].queue_msg is queue_msg


def test_queues_invalid_max_messages():
    """max_messages <= 0 raises ValueError."""
    with pytest.raises(ValueError, match="max_messages must be > 0"):
        _make_queues_subscriber(max_messages=0)


def test_queues_invalid_wait_timeout():
    """wait_timeout < 0 raises ValueError."""
    with pytest.raises(ValueError, match="wait_timeout must be >= 0"):
        _make_queues_subscriber(wait_timeout=-1)


# ---- New coverage tests: consume loop scenarios, batch, error paths ----

from kubemq_faststream.subscriber.queues import BatchQueuesSubscriber


def _make_batch_queues_subscriber(
    channel="test-queue",
    max_messages=5,
    wait_timeout=60,
):
    """Create a BatchQueuesSubscriber with mock config."""
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
        pattern=KubeMQPattern.QUEUES,
        _ack_policy=AckPolicy.ACK,
        max_messages=max_messages,
        wait_timeout=wait_timeout,
    )
    calls = CallsCollection()
    spec = KubeMQSubscriberSpecification(
        _outer_config=broker_config,
        calls=calls,
        specification_config=KubeMQSubscriberSpecificationConfig(
            channel=channel,
            pattern=KubeMQPattern.QUEUES,
            title_=None,
            description_=None,
        ),
    )
    return BatchQueuesSubscriber(sub_config, spec, calls)


async def test_queues_consume_handler_error_continues(caplog):
    """Queue handler error is logged and loop continues to next poll."""
    sub = _make_queues_subscriber()
    queue_msg = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[queue_msg])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=RuntimeError("handler crash"))

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Queue handler error" in caplog.text


async def test_queues_consume_empty_response_continues():
    """Empty response (no messages) continues the loop."""
    sub = _make_queues_subscriber()

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return _make_mock_poll_response(messages=[])
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_not_awaited()
    assert call_count == 3


async def test_queues_consume_multiple_messages():
    """Multiple messages in a single poll are each processed."""
    sub = _make_queues_subscriber(max_messages=3)
    msg1 = _make_mock_queue_msg()
    msg2 = _make_mock_queue_msg()
    msg2.id = "q-id-2"
    msg3 = _make_mock_queue_msg()
    msg3.id = "q-id-3"
    response = _make_mock_poll_response(messages=[msg1, msg2, msg3])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    assert sub.consume.await_count == 3


async def test_queues_consume_stops_mid_batch():
    """When running becomes False during message processing, loop breaks."""
    sub = _make_queues_subscriber(max_messages=3)
    msg1 = _make_mock_queue_msg()
    msg2 = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[msg1, msg2])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        return response

    async def consume_and_stop(msg):
        sub.running = False  # stop after first message

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=consume_and_stop)

    await sub._consume()

    # Should have only processed the first message
    assert sub.consume.await_count == 1


async def test_queues_consume_exception_in_receive(caplog):
    """Exception in receive_queue_messages is caught and logged."""
    sub = _make_queues_subscriber()

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("connection lost")
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Queue consume error" in caplog.text


# ---- Batch Queues Subscriber tests ----


async def test_batch_queues_consume_success_acks_all():
    """BatchQueuesSubscriber acks all messages on handler success."""
    sub = _make_batch_queues_subscriber()
    msg1 = _make_mock_queue_msg()
    msg2 = _make_mock_queue_msg()
    msg2.id = "q-id-2"
    response = _make_mock_poll_response(messages=[msg1, msg2])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    # Both messages should be consumed
    assert sub.consume.await_count == 2
    # Both messages should be acked
    msg1.async_ack.assert_awaited_once()
    msg2.async_ack.assert_awaited_once()


async def test_batch_queues_consume_handler_error_requeues_all(caplog):
    """BatchQueuesSubscriber requeues all messages on handler error."""
    sub = _make_batch_queues_subscriber()
    msg1 = _make_mock_queue_msg()
    msg2 = _make_mock_queue_msg()
    msg2.id = "q-id-2"
    response = _make_mock_poll_response(messages=[msg1, msg2])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    # First consume call succeeds, second raises -> whole batch requeued
    sub.consume = AsyncMock(side_effect=[None, RuntimeError("handler fail")])

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Batch handler error" in caplog.text
    msg1.async_re_queue.assert_awaited_once_with("test-queue")
    msg2.async_re_queue.assert_awaited_once_with("test-queue")
    # ack should not have been called (error before ack phase)
    msg1.async_ack.assert_not_awaited()
    msg2.async_ack.assert_not_awaited()


async def test_batch_queues_consume_poll_error(caplog):
    """BatchQueuesSubscriber logs poll error and continues."""
    sub = _make_batch_queues_subscriber()

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return _make_mock_poll_response(is_error=True, error="batch poll error")
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Batch queue poll error" in caplog.text
    sub.consume.assert_not_awaited()


async def test_batch_queues_consume_empty_response():
    """BatchQueuesSubscriber continues on empty response."""
    sub = _make_batch_queues_subscriber()

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return _make_mock_poll_response(messages=[])
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()

    sub.consume.assert_not_awaited()


async def test_batch_queues_consume_ack_failure_logged(caplog):
    """BatchQueuesSubscriber logs individual ack failures."""
    sub = _make_batch_queues_subscriber()
    msg1 = _make_mock_queue_msg()
    msg1.async_ack = AsyncMock(side_effect=RuntimeError("ack failed"))
    msg2 = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[msg1, msg2])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Failed to ack queue message" in caplog.text
    # msg2's ack should still be called even if msg1's ack failed
    msg2.async_ack.assert_awaited_once()


async def test_batch_queues_consume_requeue_failure_logged(caplog):
    """BatchQueuesSubscriber logs individual requeue failures."""
    sub = _make_batch_queues_subscriber()
    msg1 = _make_mock_queue_msg()
    msg1.async_re_queue = AsyncMock(side_effect=RuntimeError("requeue failed"))
    msg2 = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[msg1, msg2])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock(side_effect=RuntimeError("handler fail"))

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Failed to requeue message" in caplog.text
    # msg2 should still be requeued even if msg1's requeue failed
    msg2.async_re_queue.assert_awaited_once()


async def test_batch_queues_consume_exception_in_receive(caplog):
    """Exception in receive_queue_messages is caught and logged in batch."""
    sub = _make_batch_queues_subscriber()

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("connection lost")
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    with caplog.at_level(logging.ERROR, logger="kubemq_faststream"):
        await sub._consume()

    assert "Batch queue consume error" in caplog.text


async def test_batch_queues_auto_ack_false():
    """BatchQueuesSubscriber always passes auto_ack=False."""
    sub = _make_batch_queues_subscriber()
    msg1 = _make_mock_queue_msg()
    response = _make_mock_poll_response(messages=[msg1])

    call_count = 0

    async def mock_receive(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            assert kwargs.get("auto_ack") is False
            return response
        sub.running = False
        return _make_mock_poll_response()

    mock_conn = MagicMock()
    mock_conn.queues.receive_queue_messages = AsyncMock(side_effect=mock_receive)
    sub._connection = mock_conn
    sub.running = True
    sub.consume = AsyncMock()

    await sub._consume()


def test_batch_queues_invalid_max_messages():
    """BatchQueuesSubscriber with max_messages <= 0 raises ValueError."""
    with pytest.raises(ValueError, match="max_messages must be > 0"):
        _make_batch_queues_subscriber(max_messages=0)


def test_batch_queues_invalid_wait_timeout():
    """BatchQueuesSubscriber with wait_timeout < 0 raises ValueError."""
    with pytest.raises(ValueError, match="wait_timeout must be >= 0"):
        _make_batch_queues_subscriber(wait_timeout=-1)
