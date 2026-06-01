"""Query response caching with cache_key and cache_ttl.

KubeMQ can cache query responses server-side.  When ``cache_key``
and ``cache_ttl`` are set on the request, the broker caches the
response for ``cache_ttl`` seconds.  Subsequent requests with the
same ``cache_key`` receive the cached response without invoking
the handler.

Usage:
    python examples/queries/cache_ttl.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
import time

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.queries.cache_ttl"

call_count = 0


@broker.subscriber(queries=CHANNEL)
async def handle_query(msg: dict) -> dict:
    """Handler that tracks invocation count."""
    global call_count  # noqa: PLW0603
    call_count += 1
    print(f"Handler invoked (call #{call_count})")
    return {
        "data": msg.get("key", "default"),
        "computed_at": time.time(),
        "call_count": call_count,
    }


@app.after_startup
async def run_demo() -> None:
    """Send queries with caching enabled."""
    print("--- First request (cache miss, handler invoked) ---")
    r1 = await broker.request(
        {"key": "config-v1"},
        queries=CHANNEL,
        cache_key="config-lookup",
        cache_ttl=30,
    )
    print(f"Response 1: {r1}")

    print("\n--- Second request (cache hit, handler NOT invoked) ---")
    r2 = await broker.request(
        {"key": "config-v1"},
        queries=CHANNEL,
        cache_key="config-lookup",
        cache_ttl=30,
    )
    print(f"Response 2: {r2}")

    print(f"\nHandler was invoked {call_count} time(s) — second call served from cache")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
