"""Integration tests: EventsStore publish + subscribe with replay."""

from __future__ import annotations

import asyncio

import pytest

from kubemq_faststream import KubeMQBroker, StartPosition

from .conftest import BROKER_AVAILABLE, unique_channel

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not BROKER_AVAILABLE, reason="KubeMQ broker not available"),
]


@pytest.mark.asyncio
async def test_events_store_publish_subscribe(broker_url: str) -> None:
    """Publish to events_store and verify subscriber receives the message."""
    channel = unique_channel("es-basic")
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(events_store=channel, start_position=StartPosition.START_FROM_NEW)
    async def handler(msg: dict) -> None:
        received.append(msg)
        done.set()

    async with broker:
        # Small delay to let subscription establish
        await asyncio.sleep(1.0)
        await broker.publish({"event": "stored"}, events_store=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for events_store message")

    assert len(received) == 1
    assert received[0]["event"] == "stored"


@pytest.mark.asyncio
async def test_events_store_replay_from_first(broker_url: str) -> None:
    """Publish messages first, then subscribe with StartFromFirst to replay."""
    channel = unique_channel("es-replay")
    message_count = 3

    # Phase 1: publish messages using a plain broker (no subscriber)
    publisher_broker = KubeMQBroker(broker_url)
    async with publisher_broker:
        for i in range(message_count):
            await publisher_broker.publish(
                {"seq": i}, events_store=channel
            )
        # Brief delay to ensure messages are persisted
        await asyncio.sleep(1.0)

    # Phase 2: subscribe with StartFromFirst to replay all messages
    received: list[dict] = []
    done = asyncio.Event()

    replay_broker = KubeMQBroker(broker_url)

    @replay_broker.subscriber(
        events_store=channel,
        start_position=StartPosition.START_FROM_FIRST,
    )
    async def handler(msg: dict) -> None:
        received.append(msg)
        if len(received) >= message_count:
            done.set()

    async with replay_broker:
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except TimeoutError:
            pytest.fail(
                f"Timed out replaying: received {len(received)}/{message_count}"
            )

    assert len(received) == message_count
    sequences = [m["seq"] for m in received]
    assert sequences == list(range(message_count))


@pytest.mark.asyncio
async def test_events_store_replay_from_last(broker_url: str) -> None:
    """Subscribe with StartFromLast gets only the most recent stored event."""
    channel = unique_channel("es-last")

    # Publish several messages first
    pub_broker = KubeMQBroker(broker_url)
    async with pub_broker:
        for i in range(5):
            await pub_broker.publish({"seq": i}, events_store=channel)
        await asyncio.sleep(1.0)

    # Subscribe with StartFromLast
    received: list[dict] = []
    done = asyncio.Event()

    replay_broker = KubeMQBroker(broker_url)

    @replay_broker.subscriber(
        events_store=channel,
        start_position=StartPosition.START_FROM_LAST,
    )
    async def handler(msg: dict) -> None:
        received.append(msg)
        done.set()

    async with replay_broker:
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for last stored event")

    assert len(received) >= 1
    # The last published message should have seq=4
    assert received[0]["seq"] == 4


@pytest.mark.asyncio
async def test_events_store_with_group(broker_url: str) -> None:
    """EventsStore with group: group shares cursor position."""
    channel = unique_channel("es-group")
    group = f"grp-{channel}"
    received: list[dict] = []
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(
        events_store=channel,
        group=group,
        start_position=StartPosition.START_FROM_NEW,
    )
    async def handler(msg: dict) -> None:
        received.append(msg)
        done.set()

    async with broker:
        await asyncio.sleep(1.0)
        await broker.publish({"grouped": True}, events_store=channel)
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except TimeoutError:
            pytest.fail("Timed out waiting for grouped events_store message")

    assert len(received) == 1
    assert received[0]["grouped"] is True
