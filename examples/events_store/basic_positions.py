"""Events Store start positions: NEW, FIRST, and LAST.

Publishes several events to an events-store channel, then subscribes
with START_FROM_FIRST so the subscriber replays all stored messages.
A second subscriber uses START_FROM_LAST to receive only the most
recent event.  START_FROM_NEW (the default) would skip all historical
messages and only receive events published after the subscription
starts.

Usage:
    python examples/events_store/basic_positions.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.events_store.positions"


@broker.subscriber(
    events_store=CHANNEL,
    start_position=StartPosition.START_FROM_FIRST,
    group="first-group",
)
async def from_first(msg: dict) -> None:
    """Replay every stored event from the beginning."""
    print(f"[FIRST] Received: {msg}")


@broker.subscriber(
    events_store=CHANNEL,
    start_position=StartPosition.START_FROM_LAST,
    group="last-group",
)
async def from_last(msg: dict) -> None:
    """Receive only the most recent stored event."""
    print(f"[LAST] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish events and let subscribers replay them."""
    for i in range(1, 4):
        await broker.publish(
            {"order_id": i, "item": f"widget-{i}"},
            events_store=CHANNEL,
        )
        print(f"Published event {i}")

    await asyncio.sleep(3)
    print("Demo complete — START_FROM_FIRST replayed all, START_FROM_LAST got the latest")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
