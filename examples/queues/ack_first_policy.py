"""ACK First Policy -- KubeMQ FastStream Queues.

Demonstrates ``AckPolicy.ACK_FIRST`` where the message is acknowledged
**before** the handler runs.  This provides at-most-once delivery
semantics: if the handler fails, the message is NOT requeued.

Compare with ``AckPolicy.ACK`` (default) which acknowledges after the
handler completes successfully.

Usage:
    python examples/queues/ack_first_policy.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published task to queue
    [ACK_FIRST] Processing: {'task': 'send-email', 'to': 'user@example.com'}
    [ACK_FIRST] Done (message was already acked before handler ran)
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import AckPolicy, KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

CHANNEL = "example.queues.ack_first"


@broker.subscriber(queues=CHANNEL, ack_policy=AckPolicy.ACK_FIRST)
async def handle_task(msg: dict) -> None:
    """Handler runs after message is already acknowledged.

    If this handler raises an exception, the message will NOT be
    requeued because it was acked before execution.
    """
    print(f"[ACK_FIRST] Processing: {msg}")
    # Simulate work
    await asyncio.sleep(0.5)
    print("[ACK_FIRST] Done (message was already acked before handler ran)")


@app.after_startup
async def run_demo() -> None:
    """Publish a task to the queue."""
    try:
        await broker.publish(
            {"task": "send-email", "to": "user@example.com"},
            queues=CHANNEL,
        )
        print("Published task to queue")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(3)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
