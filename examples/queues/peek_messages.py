"""Peek Messages -- KubeMQ FastStream Queues.

Demonstrates ``broker.peek_queue_messages()`` which inspects messages
in a queue without consuming them.  The messages remain in the queue
for a real subscriber to process later.

Usage:
    python examples/queues/peek_messages.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published 3 messages to queue
    Peeked messages: [...]
    Messages are still in the queue (not consumed by peek)
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

CHANNEL = "example.queues.peek"


@app.after_startup
async def run_demo() -> None:
    """Publish messages, then peek without consuming."""
    try:
        for i in range(1, 4):
            await broker.publish({"order_id": i}, queues=CHANNEL)
        print("Published 3 messages to queue")
    except Exception as exc:
        print(f"Publish error: {exc}")
        await app.stop()
        return

    await asyncio.sleep(1)

    # Peek at up to 3 messages without consuming them.
    try:
        result = await broker.peek_queue_messages(
            queues=CHANNEL,
            max_messages=3,
        )
        print(f"Peeked messages: {result}")
        print("Messages are still in the queue (not consumed by peek)")
    except Exception as exc:
        print(f"Peek error: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
