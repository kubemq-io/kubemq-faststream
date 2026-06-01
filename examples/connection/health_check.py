"""Explicit health-check using ``broker.ping()`` with a timeout.

Demonstrates how to verify broker availability before publishing.
Returns ``True`` when the broker responds within the timeout, ``False``
otherwise.

Usage:
    python examples/connection/health_check.py

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


@broker.subscriber(events="example.connection.health")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    print("Checking broker health...")
    healthy = await broker.ping(timeout=5.0)
    print(f"Ping result: {healthy}")

    if healthy:
        await broker.publish({"health": "ok"}, events="example.connection.health")
        print("Published after health check passed")
    else:
        print("Broker is not healthy — skipping publish")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
