"""Scatter-gather: query multiple handlers and collect responses.

Sends a query request that a handler answers.  In a multi-service
deployment with ``group``-based load balancing, one of several
handlers processes each query.  This example shows the basic
request/response cycle that underpins scatter-gather architectures.

Usage:
    python examples/patterns/scatter_gather.py

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

QUERY_CH = "example.patterns.scatter.pricing"


@broker.subscriber(queries=QUERY_CH, group="pricing-workers")
async def pricing_handler(msg: dict) -> dict:
    """Return a price quote for the requested item."""
    item = msg.get("item", "unknown")
    print(f"[Pricing] Computing price for: {item}")
    prices = {"widget": 9.99, "gadget": 24.99, "gizmo": 14.99}
    return {"item": item, "price": prices.get(item, 0.0), "currency": "USD"}


@app.after_startup
async def run_demo() -> None:
    """Send multiple queries and gather responses."""
    items = ["widget", "gadget", "gizmo"]
    responses = []

    for item in items:
        resp = await broker.request(
            {"item": item},
            queries=QUERY_CH,
            timeout=10,
        )
        responses.append(resp)
        print(f"Got price for {item}: {resp}")

    print(f"\nGathered {len(responses)} responses")
    await asyncio.sleep(1)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
