"""Queue Batch Send -- KubeMQ FastStream Batch Operations.

Publish multiple queue messages in a single batch call using
``broker.publish_batch()``. This sends all messages atomically,
reducing round-trips compared to individual publish calls.

Usage:
    python examples/batch_operations/queues_batch_send.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Batch of 10 messages sent to queue
    Received: {'item': 0, 'batch': True}
    Received: {'item': 1, 'batch': True}
    ...
    Total received: 10
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

CHANNEL = "example.batch.queues_send"
received_count = 0


@broker.subscriber(queues=CHANNEL)
async def handle(msg: dict) -> None:
    global received_count
    received_count += 1
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Send a batch of queue messages and observe delivery."""
    messages = [{"item": i, "batch": True} for i in range(10)]

    try:
        await broker.publish_batch(
            *messages,
            queues=CHANNEL,
            headers={"source": "batch-send-demo"},
        )
        print("Batch of 10 messages sent to queue")
    except Exception as exc:
        print(f"Batch send error: {exc}")

    await asyncio.sleep(5)
    print(f"Total received: {received_count}")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
