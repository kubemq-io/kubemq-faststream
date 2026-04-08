"""Multi-router application composition.

Demonstrates how to use KubeMQRouter to organize handlers into
modular groups with prefix propagation.

Usage:
    python examples/router_composition.py

Requires a KubeMQ broker on localhost:50000 (or set KUBEMQ_ADDRESS).
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, KubeMQRouter

logging.basicConfig(level=logging.INFO)

# --- Orders router ---
orders_router = KubeMQRouter(prefix="orders.")


@orders_router.subscriber(events="created")
async def on_order_created(msg: dict) -> None:
    """Handle new order events (channel: orders.created)."""
    print(f"[Orders] New order created: {msg}")


@orders_router.subscriber(events="updated")
async def on_order_updated(msg: dict) -> None:
    """Handle order update events (channel: orders.updated)."""
    print(f"[Orders] Order updated: {msg}")


# --- Inventory router ---
inventory_router = KubeMQRouter(prefix="inventory.")


@inventory_router.subscriber(events="low-stock")
async def on_low_stock(msg: dict) -> None:
    """Handle low stock alerts (channel: inventory.low-stock)."""
    print(f"[Inventory] Low stock alert: {msg}")


@inventory_router.subscriber(events="restock")
async def on_restock(msg: dict) -> None:
    """Handle restock events (channel: inventory.restock)."""
    print(f"[Inventory] Restock: {msg}")


# --- Compose the app ---
broker = KubeMQBroker("kubemq://localhost:50000")
broker.include_router(orders_router)
broker.include_router(inventory_router)

app = FastStream(broker)


@app.after_startup
async def publish_events() -> None:
    """Publish events to all routed channels."""
    await asyncio.sleep(0.5)

    await broker.publish(
        {"order_id": "ORD-001", "total": 99.99},
        events="orders.created",
    )
    await broker.publish(
        {"order_id": "ORD-001", "status": "shipped"},
        events="orders.updated",
    )
    await broker.publish(
        {"sku": "WIDGET-A", "remaining": 5},
        events="inventory.low-stock",
    )
    await broker.publish(
        {"sku": "WIDGET-A", "added": 100},
        events="inventory.restock",
    )

    print("\nAll events published!")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
