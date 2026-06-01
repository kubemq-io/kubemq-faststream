"""Consumer groups for load-balanced event distribution.

Two subscribers share the same ``group`` name on the same channel.
KubeMQ distributes each message to exactly one member of the group,
enabling horizontal scaling.

Usage:
    python examples/events/consumer_group.py

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


@broker.subscriber(events="example.events.group", group="workers")
async def worker_a(msg: dict) -> None:
    print(f"Worker-A received: {msg}")


@broker.subscriber(events="example.events.group", group="workers")
async def worker_b(msg: dict) -> None:
    print(f"Worker-B received: {msg}")


@app.after_startup
async def run_demo() -> None:
    print("Publishing 6 events to a consumer group of 2 workers...")
    for i in range(1, 7):
        await broker.publish({"task": i}, events="example.events.group")
        await asyncio.sleep(0.3)

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
