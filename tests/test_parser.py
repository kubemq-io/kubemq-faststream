"""Parser conversion methods tests.

Tests KubeMQParser.from_*_received() and parse_message/decode_message.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from kubemq_faststream.message import KubeMQMessage, KubeMQRawMessage
from kubemq_faststream.parser import KubeMQParser
from kubemq_faststream.schemas import KubeMQPattern


def _make_mock_event(**overrides):
    """Create a mock EventReceived."""
    mock = MagicMock()
    mock.body = overrides.get("body", b"event-body")
    mock.channel = overrides.get("channel", "events-ch")
    mock.tags = overrides.get("tags", {"tag1": "val1"})
    mock.id = overrides.get("id", "evt-id-1")
    mock.metadata = overrides.get("metadata", "evt-meta")
    mock.timestamp = overrides.get("timestamp", datetime(2025, 1, 1, tzinfo=timezone.utc))
    return mock


def _make_mock_event_store(**overrides):
    """Create a mock EventStoreReceived."""
    mock = _make_mock_event(**overrides)
    mock.sequence = overrides.get("sequence", 42)
    return mock


def _make_mock_queue_msg(**overrides):
    """Create a mock QueueMessageReceived."""
    mock = MagicMock()
    mock.body = overrides.get("body", b"queue-body")
    mock.channel = overrides.get("channel", "queue-ch")
    mock.tags = overrides.get("tags", {"qtag": "qval"})
    mock.id = overrides.get("id", "q-id-1")
    mock.metadata = overrides.get("metadata", "q-meta")
    mock.timestamp = overrides.get(
        "timestamp", datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    mock.sequence = overrides.get("sequence", 5)
    return mock


def _make_mock_command_received(**overrides):
    """Create a mock CommandReceived."""
    mock = MagicMock()
    mock.body = overrides.get("body", b"cmd-body")
    mock.channel = overrides.get("channel", "cmd-ch")
    mock.tags = overrides.get("tags", {"ctag": "cval"})
    mock.id = overrides.get("id", "cmd-id-1")
    mock.reply_channel = overrides.get("reply_channel", "reply-ch")
    mock.metadata = overrides.get("metadata", "cmd-meta")
    mock.timestamp = overrides.get("timestamp", datetime(2025, 1, 1, tzinfo=timezone.utc))
    return mock


def _make_mock_query_received(**overrides):
    """Create a mock QueryReceived."""
    mock = MagicMock()
    mock.body = overrides.get("body", b"query-body")
    mock.channel = overrides.get("channel", "query-ch")
    mock.tags = overrides.get("tags", {"qtag": "qval"})
    mock.id = overrides.get("id", "qry-id-1")
    mock.reply_channel = overrides.get("reply_channel", "query-reply-ch")
    mock.metadata = overrides.get("metadata", "query-meta")
    mock.timestamp = overrides.get("timestamp", datetime(2025, 1, 1, tzinfo=timezone.utc))
    return mock


def test_from_event_received():
    """from_event_received converts an SDK EventReceived to KubeMQRawMessage."""
    mock_event = _make_mock_event()
    raw = KubeMQParser.from_event_received(mock_event)

    assert isinstance(raw, KubeMQRawMessage)
    assert raw.body == b"event-body"
    assert raw.channel == "events-ch"
    assert raw.pattern == KubeMQPattern.EVENTS
    assert raw.headers == {"tag1": "val1"}
    assert raw.correlation_id == "evt-id-1"
    assert raw.message_id == "evt-id-1"
    assert raw.metadata == "evt-meta"


def test_from_event_received_none_fields():
    """Handles None fields from SDK event gracefully."""
    mock_event = _make_mock_event(body=None, tags=None, id=None, metadata=None, timestamp=None)
    raw = KubeMQParser.from_event_received(mock_event)

    assert raw.body == b""
    assert raw.headers == {}
    assert raw.correlation_id == ""
    assert raw.message_id == ""
    assert raw.metadata == ""
    assert raw.timestamp is not None  # defaults to now


def test_from_event_store_received():
    """from_event_store_received includes sequence number."""
    mock_event = _make_mock_event_store()
    raw = KubeMQParser.from_event_store_received(mock_event)

    assert raw.pattern == KubeMQPattern.EVENTS_STORE
    assert raw.sequence == 42
    assert raw.body == b"event-body"


def test_from_queue_message_received():
    """from_queue_message_received converts a QueueMessageReceived."""
    mock_msg = _make_mock_queue_msg()
    raw = KubeMQParser.from_queue_message_received(mock_msg)

    assert raw.pattern == KubeMQPattern.QUEUES
    assert raw.body == b"queue-body"
    assert raw.channel == "queue-ch"
    assert raw.headers == {"qtag": "qval"}
    assert raw.sequence == 5


def test_from_queue_message_naive_timestamp():
    """Naive datetime timestamp gets UTC timezone."""
    naive_ts = datetime(2025, 6, 1)
    mock_msg = _make_mock_queue_msg(timestamp=naive_ts)
    raw = KubeMQParser.from_queue_message_received(mock_msg)
    assert raw.timestamp.tzinfo is not None


def test_from_command_received():
    """from_command_received converts a CommandReceived."""
    mock_cmd = _make_mock_command_received()
    raw = KubeMQParser.from_command_received(mock_cmd)

    assert raw.pattern == KubeMQPattern.COMMANDS
    assert raw.body == b"cmd-body"
    assert raw.channel == "cmd-ch"
    assert raw.reply_to == "reply-ch"
    assert raw.headers == {"ctag": "cval"}


def test_from_query_received():
    """from_query_received converts a QueryReceived."""
    mock_query = _make_mock_query_received()
    raw = KubeMQParser.from_query_received(mock_query)

    assert raw.pattern == KubeMQPattern.QUERIES
    assert raw.body == b"query-body"
    assert raw.channel == "query-ch"
    assert raw.reply_to == "query-reply-ch"


async def test_parse_message():
    """parse_message creates a KubeMQMessage from a KubeMQRawMessage."""
    raw = KubeMQRawMessage(
        body=b"test",
        channel="ch",
        pattern=KubeMQPattern.EVENTS,
        headers={"h1": "v1"},
        correlation_id="c1",
        reply_to="",
        message_id="m1",
        content_type="text/plain",
        metadata="",
        timestamp=datetime.now(tz=timezone.utc),
    )
    parser = KubeMQParser()
    msg = await parser.parse_message(raw)

    assert isinstance(msg, KubeMQMessage)
    assert msg.body == b"test"
    assert msg.headers == {"h1": "v1"}
    assert msg.correlation_id == "c1"


async def test_parse_message_copies_headers():
    """parse_message copies headers dict to prevent mutation (M-6)."""
    original_headers = {"h1": "v1"}
    raw = KubeMQRawMessage(
        body=b"test",
        channel="ch",
        pattern=KubeMQPattern.EVENTS,
        headers=original_headers,
        correlation_id="c1",
        reply_to="",
        message_id="m1",
        content_type="",
        metadata="",
        timestamp=datetime.now(tz=timezone.utc),
    )
    parser = KubeMQParser()
    msg = await parser.parse_message(raw)

    # Modifying parsed headers should not affect raw
    msg.headers["h2"] = "v2"
    assert "h2" not in raw.headers


async def test_decode_message():
    """decode_message returns raw body bytes."""
    raw = KubeMQRawMessage(
        body=b"decode-test",
        channel="ch",
        pattern=KubeMQPattern.EVENTS,
        headers={},
        correlation_id="",
        reply_to="",
        message_id="",
        content_type="",
        metadata="",
        timestamp=datetime.now(tz=timezone.utc),
    )
    parser = KubeMQParser()
    msg = await parser.parse_message(raw)
    decoded = await parser.decode_message(msg)
    assert decoded == b"decode-test"
