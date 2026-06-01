"""Dynamic channel routing at runtime.

Channel names are computed at runtime based on message content.
This pattern is useful when the target channel depends on business
logic — e.g. routing orders to region-specific channels.

Usage:
    python examples/publisher/dynamic_routing.py

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


@broker.subscriber(events="example.publisher.dynamic.us")
async def handle_us(msg: dict) -> None:
    """Handle US-region orders."""
    print(f"[US] Order routed here: {msg}")


@broker.subscriber(events="example.publisher.dynamic.eu")
async def handle_eu(msg: dict) -> None:
    """Handle EU-region orders."""
    print(f"[EU] Order routed here: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Route orders to region-specific channels dynamically."""
    orders = [
        {"order_id": "ORD-1", "region": "us", "total": 29.99},
        {"order_id": "ORD-2", "region": "eu", "total": 45.00},
        {"order_id": "ORD-3", "region": "us", "total": 12.50},
    ]

    for order in orders:
        channel = f"example.publisher.dynamic.{order['region']}"
        await broker.publish(order, events=channel)
        print(f"Routed order {order['order_id']} to {channel}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
