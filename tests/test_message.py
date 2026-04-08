"""KubeMQMessage, headers copy, ack idempotent tests.

Tests: U-45 through U-47.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq_faststream.message import KubeMQMessage, KubeMQRawMessage
from kubemq_faststream.schemas import KubeMQPattern


def _make_raw_message(
    pattern: KubeMQPattern = KubeMQPattern.QUEUES,
    headers: dict[str, str] | None = None,
) -> KubeMQRawMessage:
    """Create a KubeMQRawMessage for testing."""
    return KubeMQRawMessage(
        body=b"test-body",
        channel="test-channel",
        pattern=pattern,
        headers=headers or {},
        correlation_id="corr-1",
        reply_to="",
        message_id="msg-1",
        content_type="application/json",
        metadata="",
        timestamp=datetime.now(tz=timezone.utc),
    )


def _make_kubemq_message(
    pattern: KubeMQPattern = KubeMQPattern.QUEUES,
    headers: dict[str, str] | None = None,
    queue_msg: MagicMock | None = None,
) -> KubeMQMessage:
    """Create a KubeMQMessage wrapping a raw message."""
    raw = _make_raw_message(pattern=pattern, headers=headers)
    return KubeMQMessage(
        raw_message=raw,
        body=raw.body,
        headers=dict(raw.headers),
        correlation_id=raw.correlation_id,
        reply_to=raw.reply_to,
        message_id=raw.message_id,
        content_type=raw.content_type,
        queue_msg=queue_msg,
    )


# U-45: test_raw_message_headers_not_shared
def test_raw_message_headers_not_shared():
    """Headers dict is copied, not shared between raw message and parsed message (M-6)."""
    original_headers = {"key": "value"}
    raw = _make_raw_message(headers=original_headers)
    msg = KubeMQMessage(
        raw_message=raw,
        body=raw.body,
        headers=dict(raw.headers),
        correlation_id=raw.correlation_id,
        reply_to=raw.reply_to,
        message_id=raw.message_id,
        content_type=raw.content_type,
    )
    # Mutating the message headers should not affect the raw message
    msg.headers["new_key"] = "new_value"
    assert "new_key" not in raw.headers


# U-46: test_queue_message_ack_idempotent
async def test_queue_message_ack_idempotent():
    """ack() on a queue message calls SDK ack once, second call is no-op."""
    mock_queue_msg = MagicMock()
    mock_queue_msg.async_ack = AsyncMock()
    mock_queue_msg.async_re_queue = AsyncMock()
    mock_queue_msg.async_nack = AsyncMock()

    msg = _make_kubemq_message(
        pattern=KubeMQPattern.QUEUES,
        queue_msg=mock_queue_msg,
    )

    # First ack should call async_ack
    await msg.ack()
    mock_queue_msg.async_ack.assert_awaited_once()
    assert msg.committed is not None

    # Second ack should be no-op (idempotent)
    mock_queue_msg.async_ack.reset_mock()
    await msg.ack()
    mock_queue_msg.async_ack.assert_not_awaited()


async def test_queue_message_nack():
    """nack() on a queue message calls SDK requeue, then is idempotent."""
    mock_queue_msg = MagicMock()
    mock_queue_msg.async_ack = AsyncMock()
    mock_queue_msg.async_re_queue = AsyncMock()
    mock_queue_msg.async_nack = AsyncMock()

    msg = _make_kubemq_message(
        pattern=KubeMQPattern.QUEUES,
        queue_msg=mock_queue_msg,
    )

    await msg.nack()
    mock_queue_msg.async_re_queue.assert_awaited_once_with("test-channel")
    assert msg.committed is not None

    # Second nack should be no-op
    mock_queue_msg.async_re_queue.reset_mock()
    await msg.nack()
    mock_queue_msg.async_re_queue.assert_not_awaited()


async def test_queue_message_reject():
    """reject() on a queue message calls SDK async_nack, then is idempotent."""
    mock_queue_msg = MagicMock()
    mock_queue_msg.async_ack = AsyncMock()
    mock_queue_msg.async_re_queue = AsyncMock()
    mock_queue_msg.async_nack = AsyncMock()

    msg = _make_kubemq_message(
        pattern=KubeMQPattern.QUEUES,
        queue_msg=mock_queue_msg,
    )

    await msg.reject()
    mock_queue_msg.async_nack.assert_awaited_once()
    assert msg.committed is not None

    # Second reject should be no-op
    mock_queue_msg.async_nack.reset_mock()
    await msg.reject()
    mock_queue_msg.async_nack.assert_not_awaited()


# U-47: test_non_queue_ack_noop
async def test_non_queue_ack_noop():
    """ack() on a non-queue message (events) is a no-op on the SDK side."""
    msg = _make_kubemq_message(pattern=KubeMQPattern.EVENTS)

    # Should not raise, just call super().ack()
    await msg.ack()
    assert msg.committed is not None


async def test_non_queue_nack_noop():
    """nack() on a non-queue message is a no-op on the SDK side."""
    msg = _make_kubemq_message(pattern=KubeMQPattern.EVENTS)
    await msg.nack()
    assert msg.committed is not None


async def test_non_queue_reject_noop():
    """reject() on a non-queue message is a no-op on the SDK side."""
    msg = _make_kubemq_message(pattern=KubeMQPattern.EVENTS)
    await msg.reject()
    assert msg.committed is not None
