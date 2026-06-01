"""Basic queue send and receive with auto-acknowledgement.

Sends a message to a queue channel and a subscriber receives it.
The default AckPolicy.ACK automatically acknowledges the message
after the handler completes successfully.

Usage:
    python examples/queues/basic_send_receive.py

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

CHANNEL = "example.queues.basic"


@broker.subscriber(queues=CHANNEL)
async def handle_task(msg: dict) -> None:
    """Process a queue message (auto-acked on success)."""
    print(f"Received task: {msg}")
    print(f"Processing order {msg.get('order_id')}...")


@app.after_startup
async def run_demo() -> None:
    """Send a message to the queue."""
    await broker.publish(
        {"order_id": 1001, "item": "laptop", "qty": 2},
        queues=CHANNEL,
    )
    print("Published order to queue")

    await asyncio.sleep(2)
    print("Demo complete — message was auto-acknowledged after handler success")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
