"""Query timeout handling.

Demonstrates what happens when a query handler takes longer than
the caller's timeout.  The first query completes within the
deadline; the second simulates a slow handler that exceeds it.

Usage:
    python examples/queries/query_timeout.py

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

CHANNEL = "example.queries.timeout"


@broker.subscriber(queries=CHANNEL)
async def slow_query_handler(msg: dict) -> dict:
    """Simulate a query that may take too long."""
    delay = msg.get("delay_seconds", 0)
    print(f"Processing query (will take {delay}s)...")
    await asyncio.sleep(delay)
    return {"result": "computed", "delay": delay}


@app.after_startup
async def run_demo() -> None:
    """Send queries with different timeout scenarios."""
    print("--- Fast query (should succeed) ---")
    try:
        response = await broker.request(
            {"query": "fast_lookup", "delay_seconds": 1},
            queries=CHANNEL,
            timeout=10,
        )
        print(f"Success: {response}")
    except Exception as exc:
        print(f"Error: {exc}")

    print("\n--- Slow query (should timeout) ---")
    try:
        response = await broker.request(
            {"query": "slow_aggregation", "delay_seconds": 15},
            queries=CHANNEL,
            timeout=2,
        )
        print(f"Success: {response}")
    except Exception as exc:
        print(f"Timeout error (expected): {exc}")

    await asyncio.sleep(2)
    print("\nDemo complete — timeout errors are raised when deadline is exceeded")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
