"""Publisher Headers -- attach default headers to all published messages.

Create a publisher via ``@broker.publisher(events="ch")`` and set its
``headers`` dict.  All messages auto-published through this publisher
carry those headers.  Useful for adding metadata like source, version,
or content-type to every outgoing message.

Usage:
    python examples/publisher/publisher_headers.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [transform] Processing: {'name': 'widget', 'quantity': 5}
    [output] Received message: {'name': 'widget', 'quantity': 5, 'processed': True}
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

INPUT_CHANNEL = "example.publisher.headers.input"
OUTPUT_CHANNEL = "example.publisher.headers.output"

# Create the publisher and set default headers
output_publisher = broker.publisher(events=OUTPUT_CHANNEL)
output_publisher.headers = {
    "x-source": "header-demo",
    "x-version": "1.0",
    "x-content-type": "application/json",
}


@broker.subscriber(events=OUTPUT_CHANNEL)
async def receive_output(msg: dict) -> None:
    """Consume messages that were auto-published with headers."""
    print(f"[output] Received message: {msg}")


@output_publisher
@broker.subscriber(events=INPUT_CHANNEL)
async def transform(msg: dict) -> dict:
    """Transform input and auto-publish to the output channel with headers."""
    print(f"[transform] Processing: {msg}")
    return {**msg, "processed": True}


@app.after_startup
async def run_demo() -> None:
    """Publish a message to the input channel."""
    try:
        await broker.publish(
            {"name": "widget", "quantity": 5},
            events=INPUT_CHANNEL,
        )
    except Exception as exc:
        print(f"Failed to publish: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
