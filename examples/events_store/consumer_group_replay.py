"""Consumer group with events-store replay.

Combines consumer groups with start-position replay so that
multiple subscribers in the same group share the load while
still replaying historical events from the beginning.

Two subscribers join ``"workers"`` group on the same channel.
Events are load-balanced across the group members, and each
member starts by replaying from the first stored event.

Usage:
    python examples/events_store/consumer_group_replay.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.events_store.group_replay"
GROUP = "workers"


@broker.subscriber(
    events_store=CHANNEL,
    group=GROUP,
    start_position=StartPosition.START_FROM_FIRST,
)
async def worker_a(msg: dict) -> None:
    """Worker A in the consumer group."""
    print(f"[Worker-A] Received: {msg}")


@broker.subscriber(
    events_store=CHANNEL,
    group=GROUP,
    start_position=StartPosition.START_FROM_FIRST,
)
async def worker_b(msg: dict) -> None:
    """Worker B in the consumer group."""
    print(f"[Worker-B] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish events that are load-balanced across group members."""
    for i in range(1, 7):
        await broker.publish(
            {"task_id": i, "action": f"process-{i}"},
            events_store=CHANNEL,
        )
        print(f"Published task {i}")

    await asyncio.sleep(3)
    print("Demo complete — events were load-balanced across the consumer group")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
