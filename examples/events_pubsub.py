"""Basic event publish + subscribe example.

Demonstrates fire-and-forget pub/sub messaging with KubeMQ Events.

Usage:
    python examples/events_pubsub.py

Requires a KubeMQ broker on localhost:50000 (or set KUBEMQ_ADDRESS).
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)


@broker.subscriber(events="notifications")
async def on_notification(msg: dict) -> None:
    """Handle incoming notification events."""
    print(f"Received notification: {msg}")


@app.after_startup
async def publish_events() -> None:
    """Publish sample events after the app starts."""
    for i in range(5):
        await broker.publish(
            {"type": "info", "message": f"Event #{i}"},
            events="notifications",
        )
        print(f"Published event #{i}")
        await asyncio.sleep(0.5)

    # Give time for messages to be received
    await asyncio.sleep(2)
    # Stop the app after demo
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
