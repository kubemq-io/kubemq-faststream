"""Publisher decorator that auto-publishes handler return values.

When ``@broker.publisher(events=...)`` is stacked on a subscriber,
the handler's return value is automatically published to the
specified output channel — no manual ``broker.publish()`` needed.

Usage:
    python examples/publisher/basic_publisher.py

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

INPUT_CHANNEL = "example.publisher.basic.input"
OUTPUT_CHANNEL = "example.publisher.basic.output"


@broker.subscriber(events=OUTPUT_CHANNEL)
async def receive_processed(msg: dict) -> None:
    """Consume the auto-published output."""
    print(f"[Output] Processed message received: {msg}")


@broker.publisher(events=OUTPUT_CHANNEL)
@broker.subscriber(events=INPUT_CHANNEL)
async def transform(msg: dict) -> dict:
    """Transform input and auto-publish the return value to the output channel."""
    print(f"[Transform] Input: {msg}")
    return {"original": msg, "processed": True, "uppercase_name": msg.get("name", "").upper()}


@app.after_startup
async def run_demo() -> None:
    """Publish a message to the input channel and let the pipeline run."""
    await broker.publish(
        {"name": "widget", "quantity": 5},
        events=INPUT_CHANNEL,
    )
    print("Published input — transform handler will auto-publish to output")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
