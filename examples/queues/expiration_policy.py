"""Queue message expiration (TTL) policy.

Messages published with ``expiration_in_seconds`` are automatically
removed from the queue if not consumed within the specified time
window.  This example publishes two messages — one with a short
TTL and one without — to demonstrate the difference.

Usage:
    python examples/queues/expiration_policy.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.queues.expiration"


@broker.subscriber(queues=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Process queue messages that haven't expired."""
    print(f"Received (not expired): {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish messages with and without expiration."""
    await broker.publish(
        {"id": 1, "note": "expires in 5 seconds"},
        queues=CHANNEL,
        expiration_in_seconds=5,
    )
    print("Published message with 5-second TTL")

    await broker.publish(
        {"id": 2, "note": "no expiration"},
        queues=CHANNEL,
    )
    print("Published message without expiration")

    await broker.publish(
        {"id": 3, "note": "expires in 60 seconds"},
        queues=CHANNEL,
        expiration_in_seconds=60,
    )
    print("Published message with 60-second TTL")

    await asyncio.sleep(3)
    print("Demo complete — messages with short TTL expire if not consumed in time")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
