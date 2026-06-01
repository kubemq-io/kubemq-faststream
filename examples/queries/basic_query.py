"""Basic query RPC — send a query and receive response data.

Queries are the request-response pattern in KubeMQ.  Unlike commands
(fire-and-execute), queries return meaningful data payloads.  The
handler processes the query and returns structured response data.

Usage:
    python examples/queries/basic_query.py

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

CHANNEL = "example.queries.basic"


@broker.subscriber(queries=CHANNEL)
async def handle_query(msg: dict) -> dict:
    """Process the query and return data."""
    user_id = msg.get("user_id")
    print(f"Looking up user {user_id}...")
    return {
        "user_id": user_id,
        "name": "Alice",
        "email": "alice@example.com",
        "active": True,
    }


@app.after_startup
async def run_demo() -> None:
    """Send a query and print the response."""
    print("Sending query...")
    response = await broker.request(
        {"user_id": 42},
        queries=CHANNEL,
    )
    print(f"Query response: {response}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
