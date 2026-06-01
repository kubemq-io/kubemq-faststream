"""Custom Message ID -- KubeMQ FastStream Events.

Publishes events with a custom ``message_id`` parameter.  Custom IDs
are useful for deduplication, idempotency checks, and log correlation.

Usage:
    python examples/events/message_id.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published event with message_id=evt-001
    Published event with message_id=evt-002
    Received event: {'order': 1}
    Received event: {'order': 2}
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

CHANNEL = "example.events.message_id"


@broker.subscriber(events=CHANNEL)
async def handle_event(msg: dict) -> None:
    print(f"Received event: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish events with explicit message IDs."""
    try:
        await broker.publish(
            {"order": 1},
            events=CHANNEL,
            message_id="evt-001",
        )
        print("Published event with message_id=evt-001")

        await broker.publish(
            {"order": 2},
            events=CHANNEL,
            message_id="evt-002",
        )
        print("Published event with message_id=evt-002")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
