"""Acknowledge All -- KubeMQ FastStream Queues.

Demonstrates ``broker.ack_all_queue_messages()`` which acknowledges all
pending messages in a queue at once.  This is useful for clearing a
queue of messages that have already been processed or are no longer
needed.

Usage:
    python examples/queues/ack_all.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published 5 messages to queue
    Acknowledged all messages in queue
    Peek after ack_all: [...]
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

CHANNEL = "example.queues.ack_all"


@app.after_startup
async def run_demo() -> None:
    """Publish messages, ack all, then verify the queue is empty."""
    # Publish 5 messages to the queue.
    try:
        for i in range(1, 6):
            await broker.publish(
                {"task": i, "status": "pending"},
                queues=CHANNEL,
            )
        print("Published 5 messages to queue")
    except Exception as exc:
        print(f"Publish error: {exc}")
        await app.stop()
        return

    await asyncio.sleep(1)

    # Acknowledge all pending messages at once.
    try:
        await broker.ack_all_queue_messages(queues=CHANNEL)
        print("Acknowledged all messages in queue")
    except Exception as exc:
        print(f"Ack-all error: {exc}")

    await asyncio.sleep(1)

    # Peek to verify the queue is now empty.
    try:
        result = await broker.peek_queue_messages(
            queues=CHANNEL,
            max_messages=5,
        )
        print(f"Peek after ack_all: {result}")
    except Exception as exc:
        print(f"Peek error: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
