"""Queue producer + consumer with acknowledgement.

Demonstrates point-to-point messaging with transactional ack/nack.

Usage:
    python examples/queues_worker.py

Requires a KubeMQ broker on localhost:50000 (or set KUBEMQ_ADDRESS).
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream
from faststream.middlewares import AckPolicy

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)


@broker.subscriber(queues="orders", ack_policy=AckPolicy.ACK)
async def process_order(order: dict) -> None:
    """Process an order from the queue.

    With AckPolicy.ACK, the message is acknowledged automatically
    after the handler completes successfully. If the handler raises
    an exception, the message is nacked (requeued for retry).
    """
    print(f"Processing order: {order}")
    # Simulate processing
    await asyncio.sleep(0.1)
    print(f"Order {order.get('id', 'unknown')} processed successfully")


@app.after_startup
async def produce_orders() -> None:
    """Send sample orders to the queue after startup."""
    orders = [
        {"id": "ORD-001", "item": "Widget", "qty": 5},
        {"id": "ORD-002", "item": "Gadget", "qty": 2},
        {"id": "ORD-003", "item": "Sprocket", "qty": 10},
    ]

    for order in orders:
        await broker.publish(order, queues="orders")
        print(f"Enqueued order: {order['id']}")

    # Give time for processing
    await asyncio.sleep(3)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
