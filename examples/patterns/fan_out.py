"""Fan-out: broadcast vs load-balanced message distribution.

Without a ``group``, every subscriber receives every message (broadcast).
With a ``group``, only one subscriber in the group processes each
message (load-balanced).  This example shows both side by side.

Usage:
    python examples/patterns/fan_out.py

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

BROADCAST_CH = "example.patterns.fanout.broadcast"
LB_CH = "example.patterns.fanout.balanced"


@broker.subscriber(events=BROADCAST_CH)
async def broadcast_a(msg: dict) -> None:
    """Broadcast subscriber A — receives all messages."""
    print(f"[Broadcast-A] {msg}")


@broker.subscriber(events=BROADCAST_CH)
async def broadcast_b(msg: dict) -> None:
    """Broadcast subscriber B — also receives all messages."""
    print(f"[Broadcast-B] {msg}")


@broker.subscriber(events=LB_CH, group="workers")
async def worker_1(msg: dict) -> None:
    """Load-balanced worker 1 — receives some messages."""
    print(f"[Worker-1] {msg}")


@broker.subscriber(events=LB_CH, group="workers")
async def worker_2(msg: dict) -> None:
    """Load-balanced worker 2 — receives remaining messages."""
    print(f"[Worker-2] {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish to broadcast and load-balanced channels."""
    print("--- Broadcast (both subscribers receive each message) ---")
    for i in range(3):
        await broker.publish({"msg_id": i}, events=BROADCAST_CH)

    await asyncio.sleep(1)

    print("\n--- Load-balanced (one worker per message) ---")
    for i in range(4):
        await broker.publish({"task_id": i}, events=LB_CH)

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
