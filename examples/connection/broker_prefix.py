"""Broker Prefix -- KubeMQ FastStream Connection.

Demonstrates the ``prefix=`` parameter on ``KubeMQBroker``.  When set,
all subscriber and publisher channel names are automatically prefixed.
A subscriber declared with ``events="prefix_orders"`` effectively
listens on ``example.connection.prefix_orders`` when the broker has
``prefix="example.connection."``.

This is useful for namespace isolation in multi-tenant or multi-service
deployments.

Usage:
    python examples/connection/broker_prefix.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Received on prefixed channel: {'item': 'widget', 'qty': 3}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

# All subscriber/publisher channels will be prefixed with "example.connection.".
broker = KubeMQBroker(KUBEMQ_ADDRESS, prefix="example.connection.")
app = FastStream(broker)


# Subscriber declares "prefix_orders" but actually listens on
# "example.connection.prefix_orders".
@broker.subscriber(events="prefix_orders")
async def handle_order(msg: dict) -> None:
    print(f"Received on prefixed channel: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish to the prefixed channel name."""
    try:
        # Publish must target the full prefixed channel name.
        await broker.publish(
            {"item": "widget", "qty": 3},
            events="example.connection.prefix_orders",
        )
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
