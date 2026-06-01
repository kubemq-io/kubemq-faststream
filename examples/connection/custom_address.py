"""Custom Address -- KubeMQ FastStream Connection.

Shows how to specify a non-default broker address via the URL string.
When the target host is unreachable the connection fails fast with
a clear error.

Usage:
    python examples/connection/custom_address.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Connected to kubemq://localhost:50000
    Received: {'address': 'kubemq://localhost:50000'}
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


@broker.subscriber(events="example.connection.custom")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish a test message showing the connection address."""
    print(f"Connected to {KUBEMQ_ADDRESS}")
    await broker.publish(
        {"address": KUBEMQ_ADDRESS},
        events="example.connection.custom",
    )
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
