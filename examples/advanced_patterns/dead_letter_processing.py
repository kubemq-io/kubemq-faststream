"""Dead Letter Processing -- KubeMQ FastStream Advanced Patterns.

Main queue with ``max_receive_count=3`` and a dead-letter queue
(``max_receive_queue``). When a message fails processing three times,
the broker automatically routes it to the DLQ. A separate subscriber
on the DLQ captures and logs the failed messages for investigation.

Usage:
    python examples/advanced_patterns/dead_letter_processing.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published order to main queue with DLQ routing
    [main] Processing order: order-500 -- simulated failure
    [main] Processing order: order-500 -- simulated failure
    [main] Processing order: order-500 -- simulated failure
    [dlq] Dead-letter received: {'order_id': 'order-500', ...}
    [dlq] Logging failed order for manual review
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

MAIN_QUEUE = "example.advanced.dlq.main"
DLQ_QUEUE = "example.advanced.dlq"


@broker.subscriber(queues=MAIN_QUEUE, ack_policy=AckPolicy.NACK_ON_ERROR)
async def handle_order(msg: dict) -> None:
    """Intentionally fail to trigger DLQ routing after max receive count."""
    order_id = msg.get("order_id", "unknown")
    print(f"[main] Processing order: {order_id} -- simulated failure")
    raise ValueError(f"Cannot process order {order_id}")


@broker.subscriber(queues=DLQ_QUEUE)
async def handle_dead_letter(msg: dict) -> None:
    """Capture and log messages routed to the dead-letter queue."""
    print(f"[dlq] Dead-letter received: {msg}")
    print("[dlq] Logging failed order for manual review")


@app.after_startup
async def run_demo() -> None:
    """Publish a message with DLQ configuration."""
    try:
        await broker.publish(
            {"order_id": "order-500", "item": "premium-widget", "amount": 99.99},
            queues=MAIN_QUEUE,
            max_receive_count=3,
            max_receive_queue=DLQ_QUEUE,
        )
        print("Published order to main queue with DLQ routing")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(6)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
