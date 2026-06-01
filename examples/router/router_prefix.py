"""Router Prefix -- KubeMQ FastStream Router.

Use ``KubeMQRouter(prefix="orders.")`` to namespace all subscriber
channels. Subscribers registered on short names get the prefix
applied automatically.

Usage:
    python examples/router/router_prefix.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [orders] Received on orders.new: {'order_id': 1}
    [payments] Received on payments.received: {'payment_id': 'p-001'}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, KubeMQRouter

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

# Router with prefix -- all channels get "orders." prepended
orders_router = KubeMQRouter(prefix="orders.")


@orders_router.subscriber(queues="new")  # effective channel: orders.new
async def handle_order(msg: dict) -> None:
    print(f"[orders] Received on orders.new: {msg}")


# Second router with different prefix
payments_router = KubeMQRouter(prefix="payments.")


@payments_router.subscriber(events="received")  # effective channel: payments.received
async def handle_payment(msg: dict) -> None:
    print(f"[payments] Received on payments.received: {msg}")


broker.include_router(orders_router)
broker.include_router(payments_router)


@app.after_startup
async def run_demo() -> None:
    await broker.publish({"order_id": 1}, queues="orders.new")
    await broker.publish({"payment_id": "p-001"}, events="payments.received")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
