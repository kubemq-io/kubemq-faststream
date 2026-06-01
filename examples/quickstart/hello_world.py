"""Hello World Event -- KubeMQ FastStream Quickstart.

Minimal event publish and subscribe. The simplest possible KubeMQ
FastStream application -- a single publisher and subscriber.

Usage:
    python examples/quickstart/hello_world.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Received: {'greeting': 'Hello, KubeMQ!'}
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


@broker.subscriber(events="example.quickstart.hello")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish a greeting event after broker connects."""
    await broker.publish(
        {"greeting": "Hello, KubeMQ!"},
        events="example.quickstart.hello",
    )
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
