"""Basic Connection -- KubeMQ FastStream Connection.

Connect to a KubeMQ broker using the default URL and verify
connectivity with a ping health check.

Usage:
    python examples/connection/basic_connection.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Broker connected: True
    Received: {'source': 'basic_connection'}
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


@broker.subscriber(events="example.connection.basic")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Verify connection and publish a test message."""
    is_connected = await broker.ping(timeout=5.0)
    print(f"Broker connected: {is_connected}")

    await broker.publish({"source": "basic_connection"}, events="example.connection.basic")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
