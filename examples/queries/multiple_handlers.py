"""Multiple query handlers with group-based load balancing.

When multiple subscribers listen on the same query channel with the
same ``group`` name, queries are load-balanced across them.  Only
one handler processes each query.

Usage:
    python examples/queries/multiple_handlers.py

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

CHANNEL = "example.queries.multi"
GROUP = "query-workers"


@broker.subscriber(queries=CHANNEL, group=GROUP)
async def handler_a(msg: dict) -> dict:
    """Query handler A."""
    print(f"[Handler-A] Processing query: {msg}")
    return {"handler": "A", "result": msg.get("key", "none")}


@broker.subscriber(queries=CHANNEL, group=GROUP)
async def handler_b(msg: dict) -> dict:
    """Query handler B."""
    print(f"[Handler-B] Processing query: {msg}")
    return {"handler": "B", "result": msg.get("key", "none")}


@app.after_startup
async def run_demo() -> None:
    """Send several queries load-balanced across handlers."""
    for i in range(1, 5):
        print(f"\nSending query {i}...")
        response = await broker.request(
            {"key": f"data-{i}", "query_id": i},
            queries=CHANNEL,
        )
        print(f"Response from query {i}: {response}")

    await asyncio.sleep(2)
    print("\nDemo complete — queries were distributed across handlers")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
