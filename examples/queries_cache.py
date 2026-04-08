"""Query with caching example.

Demonstrates request-reply with data response and server-side caching.
Queries support cache_key and cache_ttl for automatic response caching
on the KubeMQ broker.

Usage:
    python examples/queries_cache.py

Requires a KubeMQ broker on localhost:50000 (or set KUBEMQ_ADDRESS).
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

# Track handler invocations to demonstrate caching
handler_calls = 0


@broker.subscriber(queries="product.lookup")
async def lookup_product(msg: dict) -> dict:
    """Look up product details by ID.

    Returns a dict that the broker caches if cache_key was set on the request.
    """
    global handler_calls
    handler_calls += 1

    product_id = msg.get("product_id", "unknown")
    print(f"[Handler] Looking up product {product_id} (call #{handler_calls})")

    # Simulate database lookup
    await asyncio.sleep(0.2)

    return {
        "product_id": product_id,
        "name": f"Product-{product_id}",
        "price": 29.99,
        "in_stock": True,
        "lookup_time": time.time(),
    }


@app.after_startup
async def run_queries() -> None:
    """Send queries with and without caching."""
    await asyncio.sleep(1.0)

    # Query 1: First call (no cache, handler invoked)
    print("\n--- Query 1: First call with cache_key ---")
    result1 = await broker.request(
        {"product_id": "SKU-100"},
        queries="product.lookup",
        timeout=10,
        cache_key="product:SKU-100",
        cache_ttl=30,  # Cache for 30 seconds
    )
    print(f"Result 1: {result1}")

    # Query 2: Same cache_key (may return cached response)
    print("\n--- Query 2: Same cache_key (may be cached) ---")
    result2 = await broker.request(
        {"product_id": "SKU-100"},
        queries="product.lookup",
        timeout=10,
        cache_key="product:SKU-100",
        cache_ttl=30,
    )
    print(f"Result 2: {result2}")

    # Query 3: Different product (no cache hit)
    print("\n--- Query 3: Different product ---")
    result3 = await broker.request(
        {"product_id": "SKU-200"},
        queries="product.lookup",
        timeout=10,
    )
    print(f"Result 3: {result3}")

    print(f"\nTotal handler invocations: {handler_calls}")
    await asyncio.sleep(1)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
