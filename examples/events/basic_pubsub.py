"""Basic Pub/Sub -- KubeMQ FastStream Events.

Events are fire-and-forget: the publisher sends without waiting for
acknowledgment, and subscribers receive in real time.  Messages are
not persisted.

Usage:
    python examples/events/basic_pubsub.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published event #1
    Received event: {'seq': 1, 'type': 'notification'}
    Published event #2
    Received event: {'seq': 2, 'type': 'notification'}
    Published event #3
    Received event: {'seq': 3, 'type': 'notification'}
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


@broker.subscriber(events="example.events.basic")
async def handle_event(msg: dict) -> None:
    print(f"Received event: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish three events and observe subscriber output."""
    try:
        for i in range(1, 4):
            await broker.publish(
                {"seq": i, "type": "notification"},
                events="example.events.basic",
            )
            print(f"Published event #{i}")
            await asyncio.sleep(0.5)
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
