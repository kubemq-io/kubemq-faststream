"""Publisher Chain -- publish handler return value to multiple channels.

Stack two ``@broker.publisher()`` decorators on a single subscriber
so the handler's return value is automatically published to BOTH
output channels.  This is useful for fan-out scenarios where a
processed message needs to reach multiple downstream consumers.

Usage:
    python examples/publisher/publisher_chain.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [transform] Processing: {'item': 'order-001', 'amount': 42.50}
    [analytics] Received: {'item': 'order-001', 'amount': 42.5, 'processed': True}
    [archive] Received: {'item': 'order-001', 'amount': 42.5, 'processed': True}
    Demo complete
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

INPUT_CHANNEL = "example.publisher.chain.input"
ANALYTICS_CHANNEL = "example.publisher.chain.analytics"
ARCHIVE_CHANNEL = "example.publisher.chain.archive"


@broker.subscriber(events=ANALYTICS_CHANNEL)
async def analytics_consumer(msg: dict) -> None:
    """Consume from the analytics output channel."""
    print(f"[analytics] Received: {msg}")


@broker.subscriber(events=ARCHIVE_CHANNEL)
async def archive_consumer(msg: dict) -> None:
    """Consume from the archive output channel."""
    print(f"[archive] Received: {msg}")


@broker.publisher(events=ANALYTICS_CHANNEL)
@broker.publisher(events=ARCHIVE_CHANNEL)
@broker.subscriber(events=INPUT_CHANNEL)
async def transform(msg: dict) -> dict:
    """Process input and auto-publish to both analytics and archive channels."""
    print(f"[transform] Processing: {msg}")
    return {**msg, "processed": True}


@app.after_startup
async def run_demo() -> None:
    """Publish a message to the input channel and let it fan out."""
    try:
        await broker.publish(
            {"item": "order-001", "amount": 42.50},
            events=INPUT_CHANNEL,
        )
    except Exception as exc:
        print(f"Failed to publish: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
