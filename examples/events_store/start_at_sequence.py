"""Resume an events-store subscription at a specific sequence number.

Publishes 5 events, then subscribes with START_AT_SEQUENCE starting
at sequence 3.  Only events 3, 4, and 5 are delivered to the handler.
This is useful for resuming processing from the last known position
after a consumer restart.

Usage:
    python examples/events_store/start_at_sequence.py

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

CHANNEL = "example.events_store.at_sequence"
RESUME_AT = 3


@broker.subscriber(
    events_store=CHANNEL,
    start_position=StartPosition.START_AT_SEQUENCE,
    start_value=RESUME_AT,
)
async def handle_from_seq(msg: dict) -> None:
    """Process events starting from sequence 3."""
    print(f"[SEQ>={RESUME_AT}] Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish 5 events, subscriber picks up from sequence 3."""
    for i in range(1, 6):
        await broker.publish(
            {"seq_demo": i, "data": f"payload-{i}"},
            events_store=CHANNEL,
        )
        print(f"Published event {i}")

    await asyncio.sleep(3)
    print(f"Demo complete — only events from sequence {RESUME_AT} onward were delivered")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
