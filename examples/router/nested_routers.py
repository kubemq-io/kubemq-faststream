"""Nested routers for deeply modular applications.

A parent router includes a child router, and both are ultimately
included in the broker.  Prefixes compose, so a child subscriber
on ``"placed"`` under parent prefix ``"example.nested.orders."``
listens on ``"example.nested.orders.placed"``.

Usage:
    python examples/router/nested_routers.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, KubeMQRouter

logging.basicConfig(level=logging.INFO)

child_router = KubeMQRouter(prefix="example.nested.orders.")


@child_router.subscriber(events="placed")
async def on_order_placed(msg: dict) -> None:
    """Handle order-placed events (channel: example.nested.orders.placed)."""
    print(f"[Child] Order placed: {msg}")


@child_router.subscriber(events="shipped")
async def on_order_shipped(msg: dict) -> None:
    """Handle order-shipped events (channel: example.nested.orders.shipped)."""
    print(f"[Child] Order shipped: {msg}")


parent_router = KubeMQRouter()
parent_router.include_router(child_router)

broker = KubeMQBroker("kubemq://localhost:50000")
broker.include_router(parent_router)
app = FastStream(broker)


@app.after_startup
async def run_demo() -> None:
    """Publish to nested-router channels."""
    await broker.publish(
        {"order_id": "N-001", "items": 3},
        events="example.nested.orders.placed",
    )
    await broker.publish(
        {"order_id": "N-001", "carrier": "DHL"},
        events="example.nested.orders.shipped",
    )
    print("Published through nested router hierarchy")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
