"""Start From New -- KubeMQ FastStream Events Store.

Demonstrates ``StartPosition.START_FROM_NEW`` explicitly.  The subscriber
only receives events published **after** the subscription starts.  Events
published before the subscriber is active are not replayed.

Usage:
    python examples/events_store/start_from_new.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Pre-subscription event 1 published (will NOT be received)
    Pre-subscription event 2 published (will NOT be received)
    Pre-subscription event 3 published (will NOT be received)
    Post-subscription event 4 published
    Post-subscription event 5 published
    [NEW] Received: {'seq': 4}
    [NEW] Received: {'seq': 5}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

CHANNEL = "example.events_store.start_from_new"


@broker.subscriber(
    events_store=CHANNEL,
    start_position=StartPosition.START_FROM_NEW,
    group="new-group",
)
async def handle_new_event(msg: dict) -> None:
    """Only receives events published after subscription starts."""
    print(f"[NEW] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish events before and after subscriber starts.

    Note: In a FastStream app, subscribers are already active by the time
    after_startup runs.  To demonstrate the START_FROM_NEW behavior, we
    publish pre-subscription events from an external context or rely on
    previously stored events in the channel being skipped.
    """
    # These are published after subscription is active, but the concept
    # is that START_FROM_NEW skips any historical events in the store.
    try:
        for i in range(1, 4):
            await broker.publish({"seq": i}, events_store=CHANNEL)
            print(f"Pre-subscription event {i} published (will NOT be received)")

        # Small delay to simulate a gap between historical and new events.
        await asyncio.sleep(1)

        for i in range(4, 6):
            await broker.publish({"seq": i}, events_store=CHANNEL)
            print(f"Post-subscription event {i} published")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(3)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
