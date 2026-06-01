"""Dead-letter queue (DLQ) with max receive count.

When a message is delivered more than ``max_receive_count`` times
without being acknowledged, the broker automatically routes it to
the ``max_receive_queue`` (dead-letter queue).  A second subscriber
on the DLQ channel captures the failed messages.

Usage:
    python examples/queues/max_receive_dlq.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker
from kubemq_faststream.schemas import AckPolicy

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

MAIN_QUEUE = "example.queues.dlq_main"
DLQ_QUEUE = "example.queues.dlq_dead"


@broker.subscriber(queues=MAIN_QUEUE, ack_policy=AckPolicy.NACK_ON_ERROR)
async def handle_order(msg: dict) -> None:
    """Intentionally fail to trigger DLQ routing after max receive count."""
    print(f"[MAIN] Attempt to process: {msg}")
    raise ValueError("Simulated processing failure")


@broker.subscriber(queues=DLQ_QUEUE)
async def handle_dead_letter(msg: dict) -> None:
    """Capture messages that exceeded their max receive count."""
    print(f"[DLQ] Dead-letter received: {msg}")
    print("[DLQ] Message will be logged and investigated")


@app.after_startup
async def run_demo() -> None:
    """Publish a message with max_receive_count=3 and a DLQ target."""
    await broker.publish(
        {"order_id": 999, "item": "fragile-widget"},
        queues=MAIN_QUEUE,
        max_receive_count=3,
        max_receive_queue=DLQ_QUEUE,
    )
    print(f"Published order with max_receive_count=3, DLQ={DLQ_QUEUE}")

    await asyncio.sleep(5)
    print("Demo complete — after 3 failed attempts the message moved to the DLQ")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
