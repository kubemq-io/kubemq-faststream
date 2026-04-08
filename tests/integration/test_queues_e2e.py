"""Integration tests: Queues send + receive + ack/nack."""

from __future__ import annotations

import asyncio

import pytest
from faststream.middlewares import AckPolicy

from kubemq_faststream import KubeMQBroker

from .conftest import BROKER_AVAILABLE, unique_channel

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not BROKER_AVAILABLE, reason="KubeMQ broker not available"),
]


@pytest.mark.asyncio
async def test_queues_send_receive_ack(broker_url: str) -> None:
    """Send a queue message, receive it, and verify auto-ack."""
    channel = unique_channel("q-ack")
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(queues=channel, ack_policy=AckPolicy.ACK)
    async def handler(msg: dict) -> None:
        received.append(msg)
        done.set()

    async with broker:
        await broker.publish({"order_id": "123"}, queues=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for queue message")

    assert len(received) == 1
    assert received[0]["order_id"] == "123"


@pytest.mark.asyncio
async def test_queues_multiple_messages(broker_url: str) -> None:
    """Send multiple queue messages and verify they are all received."""
    channel = unique_channel("q-multi")
    count = 5
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(queues=channel)
    async def handler(msg: dict) -> None:
        received.append(msg)
        if len(received) >= count:
            done.set()

    async with broker:
        for i in range(count):
            await broker.publish({"index": i}, queues=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except TimeoutError:
            pytest.fail(f"Timed out: received {len(received)}/{count} queue messages")

    assert len(received) == count


@pytest.mark.asyncio
async def test_queues_with_headers(broker_url: str) -> None:
    """Queue message with custom headers."""
    channel = unique_channel("q-headers")
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(queues=channel)
    async def handler(msg: dict) -> None:
        received.append(msg)
        done.set()

    async with broker:
        await broker.publish(
            {"data": "test"},
            queues=channel,
            headers={"x-priority": "high"},
        )
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for queue message with headers")

    assert len(received) == 1


@pytest.mark.asyncio
async def test_queues_nack_redelivery(broker_url: str) -> None:
    """Nack a queue message and verify it gets redelivered."""
    channel = unique_channel("q-nack")
    attempt_count = 0
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(queues=channel, ack_policy=AckPolicy.NACK_ON_ERROR)
    async def handler(msg: dict) -> None:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise ValueError("Simulated failure for redelivery test")
        received.append(msg)
        done.set()

    async with broker:
        await broker.publish({"item": "redelivery-test"}, queues=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except TimeoutError:
            pytest.fail(
                f"Timed out waiting for redelivery (attempts={attempt_count})"
            )

    assert attempt_count >= 2, "Message should have been redelivered at least once"
    assert len(received) == 1
    assert received[0]["item"] == "redelivery-test"


@pytest.mark.asyncio
async def test_queues_batch_publish(broker_url: str) -> None:
    """Publish a batch of queue messages and verify all received."""
    channel = unique_channel("q-batch")
    count = 3
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(queues=channel)
    async def handler(msg: dict) -> None:
        received.append(msg)
        if len(received) >= count:
            done.set()

    async with broker:
        await broker.publish_batch(
            {"item": 0},
            {"item": 1},
            {"item": 2},
            queues=channel,
        )
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except TimeoutError:
            pytest.fail(f"Timed out: received {len(received)}/{count} batch messages")

    assert len(received) == count
