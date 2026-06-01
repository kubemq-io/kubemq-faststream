"""Loop Publish -- KubeMQ FastStream Events.

Publishes N events in a loop using individual ``broker.publish()`` calls.
Each publish is wrapped in its own try/except so that a single failure
does not stop the remaining messages.

This is the non-batch counterpart to ``batch_operations/events_batch.py``.

Usage:
    python examples/events/loop_publish.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published event 1/5
    Published event 2/5
    Published event 3/5
    Published event 4/5
    Published event 5/5
    Received: {'seq': 1}
    Received: {'seq': 2}
    Received: {'seq': 3}
    Received: {'seq': 4}
    Received: {'seq': 5}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

CHANNEL = "example.events.loop"
TOTAL_MESSAGES = 5


@broker.subscriber(events=CHANNEL)
async def handle_event(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish events one by one with per-message error handling."""
    for i in range(1, TOTAL_MESSAGES + 1):
        try:
            await broker.publish({"seq": i}, events=CHANNEL)
            print(f"Published event {i}/{TOTAL_MESSAGES}")
        except Exception as exc:
            print(f"Failed to publish event {i}: {exc}")
        await asyncio.sleep(0.3)

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
