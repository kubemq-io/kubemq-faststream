"""Basic KubeMQRouter usage with subscriber and publisher registration.

Creates a KubeMQRouter, registers event subscribers on it, then
includes the router in the main broker.  The router groups related
handlers and can carry a shared prefix so channel names stay short
in handler code.

Usage:
    python examples/router/basic_router.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, KubeMQRouter

logging.basicConfig(level=logging.INFO)

router = KubeMQRouter()


@router.subscriber(events="example.router.basic.orders")
async def handle_order(msg: dict) -> None:
    """Process incoming order events."""
    print(f"[Router] Order received: {msg}")


@router.subscriber(events="example.router.basic.payments")
async def handle_payment(msg: dict) -> None:
    """Process incoming payment events."""
    print(f"[Router] Payment received: {msg}")


broker = KubeMQBroker("kubemq://localhost:50000")
broker.include_router(router)
app = FastStream(broker)


@app.after_startup
async def run_demo() -> None:
    """Publish events to channels registered via the router."""
    await broker.publish(
        {"order_id": "ORD-100", "amount": 49.99},
        events="example.router.basic.orders",
    )
    await broker.publish(
        {"payment_id": "PAY-200", "status": "completed"},
        events="example.router.basic.payments",
    )
    print("Published order and payment events through router-registered handlers")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
