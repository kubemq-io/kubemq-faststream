"""Custom Client ID -- KubeMQ FastStream Connection.

Demonstrates setting a custom ``client_id`` on the broker. By default
the client ID is derived from ``socket.gethostname()``.  Setting an
explicit ID helps identify connections in KubeMQ dashboard and logs.

Usage:
    python examples/connection/client_id.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Broker connected: True
    Received: {'source': 'my-service-orders'}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

# Set a custom client_id to identify this service in KubeMQ dashboard.
# Default would be socket.gethostname().
broker = KubeMQBroker(KUBEMQ_ADDRESS, client_id="my-service-orders")
app = FastStream(broker)


@broker.subscriber(events="example.connection.client_id")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Verify connection and publish a test message."""
    is_connected = await broker.ping(timeout=5.0)
    print(f"Broker connected: {is_connected}")

    try:
        await broker.publish(
            {"source": "my-service-orders"},
            events="example.connection.client_id",
        )
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
