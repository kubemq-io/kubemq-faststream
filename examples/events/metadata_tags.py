"""Publishing events with metadata and header tags.

The ``metadata`` parameter carries a string annotation, while ``headers``
is a string-to-string dict that travels with the message as gRPC tags.

Usage:
    python examples/events/metadata_tags.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream
from faststream.message import gen_cor_id

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)


@broker.subscriber(events="example.events.tags")
async def handle_event(msg: dict) -> None:
    print(f"Body: {msg}")


@app.after_startup
async def run_demo() -> None:
    await broker.publish(
        {"user": "alice", "action": "login"},
        events="example.events.tags",
        metadata="audit-trail",
        headers={
            "source": "auth-service",
            "priority": "high",
            "trace-id": gen_cor_id(),
        },
    )
    print("Published event with metadata='audit-trail' and 3 header tags")
    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
