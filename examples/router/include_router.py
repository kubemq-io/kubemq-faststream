"""Router inclusion with prefix propagation.

Shows how ``KubeMQRouter(prefix=...)`` prepends a namespace to all
channels registered on that router.  The subscriber decorates with
a short name (e.g. ``"created"``), and the prefix turns it into the
full channel ``"example.shop.created"``.

Usage:
    python examples/router/include_router.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, KubeMQRouter

logging.basicConfig(level=logging.INFO)

shop_router = KubeMQRouter(prefix="example.shop.")


@shop_router.subscriber(events="created")
async def on_created(msg: dict) -> None:
    """Handle item-created events (channel: example.shop.created)."""
    print(f"[Shop] Item created: {msg}")


@shop_router.subscriber(events="deleted")
async def on_deleted(msg: dict) -> None:
    """Handle item-deleted events (channel: example.shop.deleted)."""
    print(f"[Shop] Item deleted: {msg}")


broker = KubeMQBroker("kubemq://localhost:50000")
broker.include_router(shop_router)
app = FastStream(broker)


@app.after_startup
async def run_demo() -> None:
    """Publish events using the full prefixed channel names."""
    await broker.publish(
        {"item_id": "ITEM-1", "name": "Widget"},
        events="example.shop.created",
    )
    await broker.publish(
        {"item_id": "ITEM-2", "reason": "discontinued"},
        events="example.shop.deleted",
    )
    print("Published through prefix-routed channels")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
