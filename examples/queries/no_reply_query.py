"""No-Reply Query -- fire-and-forget query with no response.

Subscribe to a query channel with ``no_reply=True`` so the handler
processes the query but does NOT send a response back to the caller.
The caller uses ``broker.publish(queries=...)`` instead of
``broker.request()`` since no reply is expected.  This is useful
for analytics or logging endpoints where no return data is needed.

Usage:
    python examples/queries/no_reply_query.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [handler] Processing analytics query: page_view for user user-42
    [caller] Query published (no response expected)
    [handler] Processing analytics query: button_click for user user-99
    [caller] Query published (no response expected)
    Demo complete
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

CHANNEL = "example.queries.noreply"


@broker.subscriber(queries=CHANNEL, no_reply=True)
async def handle_analytics(msg: dict) -> None:
    """Process the analytics query without returning a response."""
    event_type = msg.get("event", "unknown")
    user_id = msg.get("user_id", "unknown")
    print(f"[handler] Processing analytics query: {event_type} for user {user_id}")


@app.after_startup
async def run_demo() -> None:
    """Publish queries without waiting for a response."""
    queries_to_send = [
        {"event": "page_view", "user_id": "user-42", "page": "/home"},
        {"event": "button_click", "user_id": "user-99", "element": "signup-btn"},
    ]

    for query in queries_to_send:
        try:
            await broker.publish(query, queries=CHANNEL)
            print("[caller] Query published (no response expected)")
        except Exception as exc:
            print(f"[caller] Failed to publish query: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
