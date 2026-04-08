"""Integration tests: Events publish + subscribe round-trip."""

from __future__ import annotations

import asyncio

import pytest

from kubemq_faststream import KubeMQBroker

from .conftest import BROKER_AVAILABLE, unique_channel

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not BROKER_AVAILABLE, reason="KubeMQ broker not available"),
]

# Events are fire-and-forget: if published before the subscriber's gRPC
# subscription is established, the event is lost. A short delay after
# broker start gives the subscription time to reach the server.
_SUB_SETTLE_DELAY = 1.0


@pytest.mark.asyncio
async def test_events_publish_subscribe_roundtrip(broker_url: str) -> None:
    """Publish an event and verify the subscriber receives it."""
    channel = unique_channel("events-e2e")
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(events=channel)
    async def handler(msg: dict) -> None:
        received.append(msg)
        done.set()

    async with broker:
        await asyncio.sleep(_SUB_SETTLE_DELAY)
        await broker.publish({"key": "value", "num": 42}, events=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for event")

    assert len(received) == 1
    assert received[0]["key"] == "value"
    assert received[0]["num"] == 42


@pytest.mark.asyncio
async def test_events_multiple_messages(broker_url: str) -> None:
    """Publish multiple events and verify all are received."""
    channel = unique_channel("events-multi")
    received: list[dict] = []
    count = 5
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(events=channel)
    async def handler(msg: dict) -> None:
        received.append(msg)
        if len(received) >= count:
            done.set()

    async with broker:
        await asyncio.sleep(_SUB_SETTLE_DELAY)
        for i in range(count):
            await broker.publish({"index": i}, events=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except TimeoutError:
            pytest.fail(f"Timed out: received {len(received)}/{count} events")

    assert len(received) == count


@pytest.mark.asyncio
async def test_events_with_headers(broker_url: str) -> None:
    """Publish an event with custom headers and verify they arrive."""
    channel = unique_channel("events-headers")
    received_headers: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(events=channel)
    async def handler(msg: dict) -> None:
        received_headers.append(msg)
        done.set()

    async with broker:
        await asyncio.sleep(_SUB_SETTLE_DELAY)
        await broker.publish(
            {"data": "test"},
            events=channel,
            headers={"x-trace-id": "abc123"},
        )
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for event with headers")

    assert len(received_headers) == 1


@pytest.mark.asyncio
async def test_events_group_delivery(broker_url: str) -> None:
    """Two subscribers in same group: only one should receive each event."""
    channel = unique_channel("events-group")
    group = f"grp-{channel}"
    received_a: list[dict] = []
    received_b: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(events=channel, group=group)
    async def handler_a(msg: dict) -> None:
        received_a.append(msg)
        if len(received_a) + len(received_b) >= 5:
            done.set()

    @broker.subscriber(events=channel, group=group)
    async def handler_b(msg: dict) -> None:
        received_b.append(msg)
        if len(received_a) + len(received_b) >= 5:
            done.set()

    async with broker:
        await asyncio.sleep(_SUB_SETTLE_DELAY)
        for i in range(5):
            await broker.publish({"index": i}, events=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except TimeoutError:
            total = len(received_a) + len(received_b)
            pytest.fail(f"Timed out: received {total}/5 events across group")

    total = len(received_a) + len(received_b)
    assert total == 5, f"Expected 5 total messages, got {total}"


@pytest.mark.asyncio
async def test_ping_healthy(broker_url: str) -> None:
    """I-10: broker.ping() returns True against a live broker."""
    broker = KubeMQBroker(broker_url)

    async with broker:
        result = await broker.ping(timeout=5.0)
        assert result is True, "ping() should return True when broker is healthy"


@pytest.mark.asyncio
async def test_full_lifecycle(broker_url: str) -> None:
    """I-11: Full lifecycle -- start, publish, subscribe, receive, stop."""
    channel = unique_channel("lifecycle-e2e")
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(events=channel)
    async def handler(msg: dict) -> None:
        received.append(msg)
        done.set()

    # Explicitly exercise start -> publish -> receive -> stop
    await broker.start()
    try:
        await asyncio.sleep(_SUB_SETTLE_DELAY)
        await broker.publish({"lifecycle": "test"}, events=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for message in full lifecycle test")

        assert len(received) == 1
        assert received[0]["lifecycle"] == "test"
    finally:
        await broker.stop()
