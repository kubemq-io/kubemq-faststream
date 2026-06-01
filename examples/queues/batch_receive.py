"""Batch Receive -- KubeMQ FastStream Queues.

Demonstrates a batch queue subscriber that receives up to
``max_messages`` messages at once.  The subscriber handler receives
a list of messages instead of a single message.

This example focuses on basic batch subscriber setup.  For a deeper
dive into batch acknowledgement semantics, see
``batch_operations/queues_batch_receive.py``.

Usage:
    python examples/queues/batch_receive.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published 5 messages to queue
    [BATCH] Received 5 messages: [{'task': 1}, {'task': 2}, {'task': 3}, {'task': 4}, {'task': 5}]
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

CHANNEL = "example.queues.batch_receive"


@broker.subscriber(
    queues=CHANNEL,
    batch=True,
    max_messages=5,
    wait_timeout=10,
)
async def handle_batch(msgs: list[dict]) -> None:
    """Process a batch of queue messages."""
    print(f"[BATCH] Received {len(msgs)} messages: {msgs}")


@app.after_startup
async def run_demo() -> None:
    """Publish several messages so the batch subscriber can collect them."""
    try:
        for i in range(1, 6):
            await broker.publish({"task": i}, queues=CHANNEL)
        print("Published 5 messages to queue")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(5)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
