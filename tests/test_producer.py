"""Producer routing, encode_message tests.

Tests producer pattern routing and encode_message (M-10).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from kubemq_faststream.config import KubeMQConnection
from kubemq_faststream.producer import KubeMQProducer
from kubemq_faststream.response import KubeMQPublishCommand
from kubemq_faststream.schemas import FeatureNotSupportedException, KubeMQPattern


def _make_mock_connection():
    """Create a mock KubeMQConnection."""
    conn = MagicMock(spec=KubeMQConnection)
    conn.pubsub = MagicMock()
    conn.pubsub.publish_event = AsyncMock()
    conn.pubsub.send_event_store = AsyncMock()
    conn.queues = MagicMock()
    conn.queues.send_queue_message = AsyncMock()
    conn.queues.send_queue_messages_batch = AsyncMock()
    conn.cq = MagicMock()
    conn.cq.send_command = AsyncMock()
    conn.cq.send_query = AsyncMock()
    return conn


async def test_publish_events_routes_to_pubsub():
    """Publishing EVENTS routes to pubsub.publish_event."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="events-ch",
        pattern=KubeMQPattern.EVENTS,
    )
    await producer.publish(cmd)
    conn.pubsub.publish_event.assert_awaited_once()


async def test_publish_events_store_routes_to_pubsub():
    """Publishing EVENTS_STORE routes to pubsub.send_event_store."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="es-ch",
        pattern=KubeMQPattern.EVENTS_STORE,
    )
    await producer.publish(cmd)
    conn.pubsub.send_event_store.assert_awaited_once()


async def test_publish_queues_routes_to_queues():
    """Publishing QUEUES routes to queues.send_queue_message."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="q-ch",
        pattern=KubeMQPattern.QUEUES,
    )
    await producer.publish(cmd)
    conn.queues.send_queue_message.assert_awaited_once()


async def test_publish_commands_routes_to_cq():
    """Publishing COMMANDS routes to cq.send_command."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="cmd-ch",
        pattern=KubeMQPattern.COMMANDS,
        timeout=10,
    )
    await producer.publish(cmd)
    conn.cq.send_command.assert_awaited_once()


async def test_publish_queries_routes_to_cq():
    """Publishing QUERIES routes to cq.send_query."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="qry-ch",
        pattern=KubeMQPattern.QUERIES,
        timeout=10,
        cache_key="ck",
        cache_ttl=60,
    )
    await producer.publish(cmd)
    conn.cq.send_query.assert_awaited_once()


async def test_publish_not_connected_raises():
    """publish() without connection raises RuntimeError."""
    producer = KubeMQProducer(connection=None)
    cmd = KubeMQPublishCommand(
        b"hello",
        destination="ch",
        pattern=KubeMQPattern.EVENTS,
    )
    with pytest.raises(RuntimeError, match="Producer not connected"):
        await producer.publish(cmd)


async def test_request_requires_cq_pattern():
    """request() only supports commands/queries patterns."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="ch",
        pattern=KubeMQPattern.EVENTS,
    )
    with pytest.raises(FeatureNotSupportedException, match="request.*not supported"):
        await producer.request(cmd)


async def test_request_commands_delegates_to_publish():
    """request() for COMMANDS delegates to publish()."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="cmd-ch",
        pattern=KubeMQPattern.COMMANDS,
        timeout=10,
    )
    await producer.request(cmd)
    conn.cq.send_command.assert_awaited_once()


async def test_publish_batch_queues():
    """publish_batch() sends batch to queues client."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"",
        destination="batch-ch",
        pattern=KubeMQPattern.QUEUES,
        batch_bodies=(b"a", b"b"),
    )
    await producer.publish_batch(cmd)
    conn.queues.send_queue_messages_batch.assert_awaited_once()


async def test_publish_batch_non_queues_raises():
    """publish_batch() raises for non-queue patterns."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"",
        destination="ch",
        pattern=KubeMQPattern.EVENTS,
    )
    with pytest.raises(FeatureNotSupportedException, match="publish_batch.*only supported"):
        await producer.publish_batch(cmd)


async def test_publish_batch_empty_bodies_returns_none():
    """publish_batch() with empty batch bodies returns None."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"",
        destination="ch",
        pattern=KubeMQPattern.QUEUES,
        batch_bodies=(),
    )
    result = await producer.publish_batch(cmd)
    assert result is None
    conn.queues.send_queue_messages_batch.assert_not_awaited()


async def test_encode_message_applied():
    """Producer uses encode_message for body serialization (M-10)."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    # Publishing a dict should encode it before sending
    cmd = KubeMQPublishCommand(
        {"key": "value"},
        destination="events-ch",
        pattern=KubeMQPattern.EVENTS,
    )
    await producer.publish(cmd)

    # The EventMessage should have been created with bytes body
    call_args = conn.pubsub.publish_event.call_args
    event_msg = call_args[0][0]  # first positional arg
    assert isinstance(event_msg.body, bytes)


async def test_publish_events_with_message_id():
    """Publishing EVENTS with message_id sets id on EventMessage."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="events-ch",
        pattern=KubeMQPattern.EVENTS,
        message_id="evt-id-1",
    )
    await producer.publish(cmd)
    event_msg = conn.pubsub.publish_event.call_args[0][0]
    assert event_msg.id == "evt-id-1"


async def test_publish_events_store_with_message_id():
    """Publishing EVENTS_STORE with message_id sets id on EventStoreMessage."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="es-ch",
        pattern=KubeMQPattern.EVENTS_STORE,
        message_id="es-id-1",
    )
    await producer.publish(cmd)
    event_store_msg = conn.pubsub.send_event_store.call_args[0][0]
    assert event_store_msg.id == "es-id-1"


async def test_publish_queues_with_message_id():
    """Publishing QUEUES with message_id sets id on QueueMessage."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="q-ch",
        pattern=KubeMQPattern.QUEUES,
        message_id="q-id-1",
    )
    await producer.publish(cmd)
    queue_msg = conn.queues.send_queue_message.call_args[0][0]
    assert queue_msg.id == "q-id-1"


async def test_publish_commands_with_message_id():
    """Publishing COMMANDS with message_id sets id on CommandMessage."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="cmd-ch",
        pattern=KubeMQPattern.COMMANDS,
        timeout=10,
        message_id="cmd-id-1",
    )
    await producer.publish(cmd)
    command_msg = conn.cq.send_command.call_args[0][0]
    assert command_msg.id == "cmd-id-1"


async def test_publish_queries_with_message_id():
    """Publishing QUERIES with message_id sets id on QueryMessage."""
    conn = _make_mock_connection()
    producer = KubeMQProducer(connection=conn)

    cmd = KubeMQPublishCommand(
        b"hello",
        destination="qry-ch",
        pattern=KubeMQPattern.QUERIES,
        timeout=10,
        cache_key="ck",
        cache_ttl=60,
        message_id="qry-id-1",
    )
    await producer.publish(cmd)
    query_msg = conn.cq.send_query.call_args[0][0]
    assert query_msg.id == "qry-id-1"


async def test_publish_batch_not_connected_raises():
    """publish_batch() without connection raises RuntimeError."""
    producer = KubeMQProducer(connection=None)
    cmd = KubeMQPublishCommand(
        b"",
        destination="batch-ch",
        pattern=KubeMQPattern.QUEUES,
        batch_bodies=(b"a",),
    )
    with pytest.raises(RuntimeError, match="Producer not connected"):
        await producer.publish_batch(cmd)


# ---- Producer identity stubs ----


async def test_producer_parse_identity():
    """_parse_identity returns the message as-is."""
    result = await KubeMQProducer._parse_identity("test")
    assert result == "test"


async def test_producer_decode_identity():
    """_decode_identity returns the message as-is."""
    result = await KubeMQProducer._decode_identity({"key": "val"})
    assert result == {"key": "val"}
