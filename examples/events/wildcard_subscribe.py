"""Wildcard channel subscriptions matching multiple channels by pattern.

KubeMQ supports wildcard patterns like ``prefix.*`` and ``prefix.>``.
A subscriber using a wildcard receives events from all matching channels.

Usage:
    python examples/events/wildcard_subscribe.py

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


@broker.subscriber(events="example.events.wild.*")
async def handle_wildcard(msg: dict) -> None:
    print(f"Wildcard subscriber received: {msg}")


@app.after_startup
async def run_demo() -> None:
    channels = [
        "example.events.wild.orders",
        "example.events.wild.payments",
        "example.events.wild.alerts",
    ]

    for ch in channels:
        await broker.publish({"channel": ch, "data": "test"}, events=ch)
        print(f"Published to {ch}")
        await asyncio.sleep(0.3)

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
