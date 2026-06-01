"""Delayed message delivery for queues.

Messages published with ``delay_in_seconds`` become visible to
consumers only after the specified delay.  This is useful for
scheduling future work, retry back-off, or deferred processing.

Usage:
    python examples/queues/delay_policy.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
import time

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.queues.delay"


@broker.subscriber(queues=CHANNEL)
async def handle_delayed(msg: dict) -> None:
    """Process messages after their delay period."""
    now = time.time()
    published_at = msg.get("published_at", now)
    actual_delay = round(now - published_at, 1)
    print(f"Received after ~{actual_delay}s delay: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish messages with different delays."""
    now = time.time()

    await broker.publish(
        {"id": 1, "note": "immediate delivery", "published_at": now},
        queues=CHANNEL,
    )
    print("Published immediate message")

    await broker.publish(
        {"id": 2, "note": "delayed by 3 seconds", "published_at": now},
        queues=CHANNEL,
        delay_in_seconds=3,
    )
    print("Published message with 3-second delay")

    await broker.publish(
        {"id": 3, "note": "delayed by 5 seconds", "published_at": now},
        queues=CHANNEL,
        delay_in_seconds=5,
    )
    print("Published message with 5-second delay")

    await asyncio.sleep(7)
    print("Demo complete — delayed messages were delivered after their delay periods")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
