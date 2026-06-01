"""Detecting cache hits and misses in query responses.

When a cached response is returned, the KubeMQ response object
includes a ``CacheHit`` indicator.  This example sends the same
query twice and shows how the first call is a cache miss (handler
invoked) while the second is a cache hit (response from cache).

Usage:
    python examples/queries/cache_hit_miss.py

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

CHANNEL = "example.queries.cache_hit"

handler_calls = 0


@broker.subscriber(queries=CHANNEL)
async def lookup_product(msg: dict) -> dict:
    """Product lookup that should be cached."""
    global handler_calls  # noqa: PLW0603
    handler_calls += 1
    product_id = msg.get("product_id")
    print(f"[Handler] Looking up product {product_id} (call #{handler_calls})")
    return {
        "product_id": product_id,
        "name": "Widget Pro",
        "price": 29.99,
        "in_stock": True,
    }


@app.after_startup
async def run_demo() -> None:
    """Demonstrate cache hit vs miss."""
    cache_key = "product-42"
    cache_ttl = 60

    print("=== Request 1: Cache MISS (handler will be invoked) ===")
    r1 = await broker.request(
        {"product_id": 42},
        queries=CHANNEL,
        cache_key=cache_key,
        cache_ttl=cache_ttl,
    )
    print(f"Response: {r1}")
    print(f"Handler invocations so far: {handler_calls}")

    print("\n=== Request 2: Cache HIT (handler NOT invoked) ===")
    r2 = await broker.request(
        {"product_id": 42},
        queries=CHANNEL,
        cache_key=cache_key,
        cache_ttl=cache_ttl,
    )
    print(f"Response: {r2}")
    print(f"Handler invocations so far: {handler_calls}")

    print("\n=== Request 3: Different key — Cache MISS ===")
    r3 = await broker.request(
        {"product_id": 99},
        queries=CHANNEL,
        cache_key="product-99",
        cache_ttl=cache_ttl,
    )
    print(f"Response: {r3}")
    print(f"Handler invocations so far: {handler_calls}")

    await asyncio.sleep(2)
    print("\nDemo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
