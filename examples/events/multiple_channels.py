"""One application subscribing to multiple event channels.

Each channel has its own handler function, enabling clean separation of
concerns within a single process.

Usage:
    python examples/events/multiple_channels.py

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


@broker.subscriber(events="example.events.orders")
async def on_order(msg: dict) -> None:
    print(f"[Orders]  {msg}")


@broker.subscriber(events="example.events.payments")
async def on_payment(msg: dict) -> None:
    print(f"[Payments] {msg}")


@broker.subscriber(events="example.events.notifications")
async def on_notification(msg: dict) -> None:
    print(f"[Notifs]   {msg}")


@app.after_startup
async def run_demo() -> None:
    await broker.publish({"order_id": 1001}, events="example.events.orders")
    await broker.publish({"payment_id": "pay-42"}, events="example.events.payments")
    await broker.publish({"text": "Welcome!"}, events="example.events.notifications")
    print("Published to 3 different channels")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
