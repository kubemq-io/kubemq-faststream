"""Integration tests: Batch operations (publish_events_batch, request_batch)."""

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
async def test_publish_events_batch_e2e(broker_url: str) -> None:
    """Publish a batch of events and verify the subscriber receives all of them."""
    channel = unique_channel("batch-events-e2e")
    received: list[dict] = []
    count = 3
    done = asyncio.Event()

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(events=channel)
    async def handler(msg: dict) -> None:
        received.append(msg)
        if len(received) >= count:
            done.set()

    async with broker:
        await asyncio.sleep(_SUB_SETTLE_DELAY)
        await broker.publish_events_batch(
            {"idx": 0}, {"idx": 1}, {"idx": 2},
            events=channel,
        )
        try:
            await asyncio.wait_for(done.wait(), timeout=15.0)
        except TimeoutError:
            pytest.fail(f"Timed out: received {len(received)}/{count} batch events")

    assert len(received) == count


@pytest.mark.asyncio
async def test_request_batch_commands_e2e(broker_url: str) -> None:
    """Send a batch of commands and verify the handler is called for each."""
    channel = unique_channel("batch-cmd-e2e")
    handled: list[dict] = []

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(commands=channel)
    async def handler(msg: dict) -> None:
        handled.append(msg)

    async with broker:
        await asyncio.sleep(0.5)
        await broker.request_batch(
            {"action": "cmd1"}, {"action": "cmd2"},
            commands=channel,
            timeout=10,
        )

    assert len(handled) == 2


@pytest.mark.asyncio
async def test_request_batch_queries_e2e(broker_url: str) -> None:
    """Send a batch of queries and verify the handler is called for each."""
    channel = unique_channel("batch-qry-e2e")
    handled: list[dict] = []

    broker = KubeMQBroker(broker_url)

    @broker.subscriber(queries=channel)
    async def handler(msg: dict) -> dict:
        handled.append(msg)
        return {"echo": msg.get("q", "")}

    async with broker:
        await asyncio.sleep(0.5)
        await broker.request_batch(
            {"q": "q1"}, {"q": "q2"},
            queries=channel,
            timeout=10,
        )

    assert len(handled) == 2
