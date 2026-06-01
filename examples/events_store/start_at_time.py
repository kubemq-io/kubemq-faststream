"""Replay events-store messages from a specific time or time delta.

Demonstrates two time-based start positions:

- START_AT_TIME_DELTA — replay events from N seconds ago.  The
  ``start_value`` is the number of seconds to look back.
- START_AT_TIME — replay events from an absolute Unix timestamp.
  The ``start_value`` is seconds since epoch.

Usage:
    python examples/events_store/start_at_time.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
import time

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.events_store.at_time"


@broker.subscriber(
    events_store=CHANNEL,
    start_position=StartPosition.START_AT_TIME_DELTA,
    start_value=60,
    group="delta-group",
)
async def from_last_60s(msg: dict) -> None:
    """Receive events published in the last 60 seconds."""
    print(f"[DELTA 60s] Received: {msg}")


@broker.subscriber(
    events_store=CHANNEL,
    start_position=StartPosition.START_AT_TIME,
    start_value=int(time.time()) - 30,
    group="abs-time-group",
)
async def from_absolute_time(msg: dict) -> None:
    """Receive events published since 30 seconds ago (absolute timestamp)."""
    print(f"[ABS TIME] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish events and let time-based subscribers replay them."""
    for i in range(1, 4):
        await broker.publish(
            {"event_id": i, "ts": time.time()},
            events_store=CHANNEL,
        )
        print(f"Published event {i}")

    await asyncio.sleep(3)
    print("Demo complete — time-based replay delivered matching events")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
