"""Broker lifecycle tests: connect, start, stop, ping.

Tests: U-1 through U-10.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.config import KubeMQConnection


def _make_mock_client(*, fail_connect: bool = False):
    """Create a mock SDK client with connect/close as AsyncMock."""
    client = MagicMock()
    if fail_connect:
        client.connect = AsyncMock(side_effect=RuntimeError("connect failed"))
    else:
        client.connect = AsyncMock()
    client.close = AsyncMock()
    client.config = MagicMock()
    client.config.client_id = "test-client"
    return client


def _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq):
    """Return context manager tuple that patches the 3 SDK client constructors.

    The imports happen inside _connect() as local imports from the kubemq package,
    so we patch at the SDK module level.
    """
    return (
        patch("kubemq.pubsub.AsyncPubSubClient", return_value=mock_pubsub),
        patch("kubemq.queues.AsyncQueuesClient", return_value=mock_queues),
        patch("kubemq.cq.AsyncCQClient", return_value=mock_cq),
    )


# U-1: test_connect_creates_three_clients
async def test_connect_creates_three_clients():
    """_connect() creates pubsub, queues, and cq clients."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        conn = await broker._connect()

    assert conn.pubsub is mock_pubsub
    assert conn.queues is mock_queues
    assert conn.cq is mock_cq
    mock_pubsub.connect.assert_awaited_once()
    mock_queues.connect.assert_awaited_once()
    mock_cq.connect.assert_awaited_once()


# U-2: test_connect_idempotent
async def test_connect_idempotent():
    """Second _connect() returns cached connection without reconnecting."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        conn1 = await broker._connect()
        conn2 = await broker._connect()

    assert conn1 is conn2
    # connect() should only be called once per client
    assert mock_pubsub.connect.await_count == 1


# U-3: test_connect_serialized
async def test_connect_serialized():
    """Concurrent _connect() calls are serialized via _connect_lock (H-10)."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    connect_count = 0

    async def slow_connect():
        nonlocal connect_count
        connect_count += 1
        await asyncio.sleep(0.01)

    mock_pubsub = _make_mock_client()
    mock_pubsub.connect = AsyncMock(side_effect=slow_connect)
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        results = await asyncio.gather(
            broker._connect(),
            broker._connect(),
            broker._connect(),
        )

    # All three results should be the same connection object
    assert results[0] is results[1]
    assert results[1] is results[2]
    # pubsub.connect should only be called once due to lock
    assert connect_count == 1


# U-4: test_connect_partial_failure_cleanup
async def test_connect_partial_failure_cleanup():
    """If cq.connect() fails, already-connected clients are closed."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client(fail_connect=True)

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        with pytest.raises(RuntimeError, match="connect failed"):
            await broker._connect()

    # pubsub and queues should have been cleaned up
    mock_pubsub.close.assert_awaited_once()
    mock_queues.close.assert_awaited_once()
    # cq never connected, so close should not be called
    mock_cq.close.assert_not_awaited()


# U-5: test_start_calls_super_start
async def test_start_calls_super_start():
    """start() invokes BrokerUsecase.start() through the standard lifecycle."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_pubsub.ping = AsyncMock(return_value=True)
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    # After connect, broker should have a connection
    assert broker._connection is not None


# U-6: test_stop_closes_connection
async def test_stop_closes_connection():
    """After connect, closing the connection calls close on all 3 clients."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        conn = await broker._connect()

    await conn.close()
    mock_pubsub.close.assert_awaited_once()
    mock_queues.close.assert_awaited_once()
    mock_cq.close.assert_awaited_once()


# U-7: test_stop_collects_errors
async def test_stop_collects_errors():
    """Connection.close() collects errors in ExceptionGroup."""
    mock_pubsub = _make_mock_client()
    mock_pubsub.close = AsyncMock(side_effect=RuntimeError("pubsub close error"))
    mock_queues = _make_mock_client()
    mock_queues.close = AsyncMock(side_effect=RuntimeError("queues close error"))
    mock_cq = _make_mock_client()

    conn = KubeMQConnection(pubsub=mock_pubsub, queues=mock_queues, cq=mock_cq)

    with pytest.raises(ExceptionGroup) as exc_info:
        await conn.close()

    assert len(exc_info.value.exceptions) == 2
    assert "pubsub close error" in str(exc_info.value.exceptions[0])
    assert "queues close error" in str(exc_info.value.exceptions[1])


# U-8: test_ping_healthy
async def test_ping_healthy():
    """ping() returns True when SDK ping succeeds."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_pubsub.ping = AsyncMock(return_value={"host": "localhost"})
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    conn = KubeMQConnection(pubsub=mock_pubsub, queues=mock_queues, cq=mock_cq)
    broker._connection = conn

    result = await broker.ping(timeout=5.0)
    assert result is True


# U-9: test_ping_timeout
async def test_ping_timeout():
    """ping() returns False when SDK ping always raises within timeout."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_pubsub.ping = AsyncMock(side_effect=RuntimeError("unreachable"))
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    conn = KubeMQConnection(pubsub=mock_pubsub, queues=mock_queues, cq=mock_cq)
    broker._connection = conn

    result = await broker.ping(timeout=0.1)
    assert result is False


# U-10: test_ping_no_connection
async def test_ping_no_connection():
    """ping() returns False when broker is not connected."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    assert broker._connection is None

    result = await broker.ping(timeout=1.0)
    assert result is False


# ---- New coverage tests: publish, request, publish_batch ----


async def test_publish_events():
    """publish() with events= routes through producer.publish()."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    # Now the broker has a producer; mock it
    broker.config.producer.publish = AsyncMock(return_value="ok")

    result = await broker.publish(b"hello", events="test-events")
    assert result == "ok"
    broker.config.producer.publish.assert_awaited_once()
    cmd = broker.config.producer.publish.call_args[0][0]
    assert cmd.destination == "test-events"
    from kubemq_faststream.schemas import KubeMQPattern

    assert cmd.pattern == KubeMQPattern.EVENTS


async def test_publish_events_store():
    """publish() with events_store= routes correctly."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.publish = AsyncMock(return_value="ok")

    result = await broker.publish(b"hello", events_store="test-es")
    assert result == "ok"
    cmd = broker.config.producer.publish.call_args[0][0]
    assert cmd.destination == "test-es"
    from kubemq_faststream.schemas import KubeMQPattern

    assert cmd.pattern == KubeMQPattern.EVENTS_STORE


async def test_publish_queues():
    """publish() with queues= routes correctly."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.publish = AsyncMock(return_value="ok")

    result = await broker.publish(b"hello", queues="test-queues")
    assert result == "ok"
    cmd = broker.config.producer.publish.call_args[0][0]
    assert cmd.destination == "test-queues"
    from kubemq_faststream.schemas import KubeMQPattern

    assert cmd.pattern == KubeMQPattern.QUEUES


async def test_publish_not_connected_raises():
    """publish() raises RuntimeError when broker not connected."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    with pytest.raises(RuntimeError, match="Broker not connected"):
        await broker.publish(b"hello", events="test")


async def test_publish_with_headers_and_metadata():
    """publish() forwards headers, metadata, correlation_id to command."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.publish = AsyncMock(return_value="ok")

    await broker.publish(
        b"hello",
        events="ch",
        headers={"h1": "v1"},
        metadata="meta1",
        correlation_id="corr-1",
    )
    cmd = broker.config.producer.publish.call_args[0][0]
    assert cmd.headers == {"h1": "v1"}
    assert cmd.metadata == "meta1"
    assert cmd.correlation_id == "corr-1"


async def test_request_commands():
    """request() with commands= routes through producer.request()."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.request = AsyncMock(return_value="cmd-result")

    result = await broker.request(b"do-it", commands="cmd-channel")
    assert result == "cmd-result"
    broker.config.producer.request.assert_awaited_once()
    cmd = broker.config.producer.request.call_args[0][0]
    assert cmd.destination == "cmd-channel"
    from kubemq_faststream.schemas import KubeMQPattern

    assert cmd.pattern == KubeMQPattern.COMMANDS


async def test_request_queries():
    """request() with queries= routes through producer.request()."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.request = AsyncMock(return_value="qry-result")

    result = await broker.request(b"ask", queries="qry-channel")
    assert result == "qry-result"
    cmd = broker.config.producer.request.call_args[0][0]
    assert cmd.destination == "qry-channel"
    from kubemq_faststream.schemas import KubeMQPattern

    assert cmd.pattern == KubeMQPattern.QUERIES


async def test_request_neither_commands_nor_queries_raises():
    """request() without commands= or queries= raises FeatureNotSupportedException."""
    from kubemq_faststream.schemas import FeatureNotSupportedException

    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    with pytest.raises(FeatureNotSupportedException, match="request"):
        await broker.request(b"hello")


async def test_request_not_connected_raises():
    """request() raises RuntimeError when broker not connected."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    with pytest.raises(RuntimeError, match="Broker not connected"):
        await broker.request(b"hello", commands="cmd")


async def test_request_with_timeout():
    """request() passes timeout to command."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.request = AsyncMock(return_value="result")

    await broker.request(b"ask", commands="cmd", timeout=60)
    cmd = broker.config.producer.request.call_args[0][0]
    assert cmd.timeout == 60


async def test_request_default_timeout():
    """request() uses default_cq_timeout when timeout not specified."""
    broker = KubeMQBroker("kubemq://localhost:50000", default_cq_timeout=45)

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.request = AsyncMock(return_value="result")

    await broker.request(b"ask", commands="cmd")
    cmd = broker.config.producer.request.call_args[0][0]
    assert cmd.timeout == 45


async def test_publish_batch_queues():
    """publish_batch() sends batch of queue messages."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.publish_batch = AsyncMock(return_value="batch-ok")

    result = await broker.publish_batch(b"msg1", b"msg2", b"msg3", queues="batch-queue")
    assert result == "batch-ok"
    broker.config.producer.publish_batch.assert_awaited_once()
    cmd = broker.config.producer.publish_batch.call_args[0][0]
    assert cmd.destination == "batch-queue"
    from kubemq_faststream.schemas import KubeMQPattern

    assert cmd.pattern == KubeMQPattern.QUEUES
    assert len(cmd.batch_bodies) == 3


async def test_publish_batch_not_connected_raises():
    """publish_batch() raises RuntimeError when broker not connected."""
    broker = KubeMQBroker("kubemq://localhost:50000")
    with pytest.raises(RuntimeError, match="Broker not connected"):
        await broker.publish_batch(b"msg1", queues="q")


async def test_publish_batch_with_headers():
    """publish_batch() forwards headers to command."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    with p1, p2, p3:
        await broker._connect()

    broker.config.producer.publish_batch = AsyncMock(return_value="ok")

    await broker.publish_batch(b"msg1", queues="q", headers={"k": "v"})
    cmd = broker.config.producer.publish_batch.call_args[0][0]
    assert cmd.headers == {"k": "v"}


async def test_start_injects_connection_into_subscribers():
    """start() injects connection into all subscribers with set_connection."""
    broker = KubeMQBroker("kubemq://localhost:50000")

    mock_pubsub = _make_mock_client()
    mock_queues = _make_mock_client()
    mock_cq = _make_mock_client()

    # Create mock subscribers
    mock_sub1 = MagicMock()
    mock_sub1.set_connection = MagicMock()
    mock_sub2 = MagicMock()
    mock_sub2.set_connection = MagicMock()

    # Patch subscribers property on the class, but restore it afterwards
    original_subscribers = type(broker).__dict__.get("subscribers")
    p1, p2, p3 = _patch_sdk_clients(mock_pubsub, mock_queues, mock_cq)
    try:
        type(broker).subscribers = property(lambda self: [mock_sub1, mock_sub2])
        with p1, p2, p3, patch.object(
            type(broker).__mro__[2], "start", new_callable=AsyncMock
        ):
            await broker.start()
    finally:
        # Restore original subscribers descriptor to avoid test pollution
        if original_subscribers is not None:
            type(broker).subscribers = original_subscribers
        else:
            delattr(type(broker), "subscribers")

    mock_sub1.set_connection.assert_called_once()
    mock_sub2.set_connection.assert_called_once()
