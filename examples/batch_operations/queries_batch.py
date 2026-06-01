"""Queries Batch Request -- KubeMQ FastStream Batch Operations.

Send multiple query requests in a single batch call using
``broker.request_batch()``. All queries are dispatched together
and data responses are collected. Each query is independently
processed by the subscriber.

Usage:
    python examples/batch_operations/queries_batch.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Looking up user 101
    Looking up user 102
    Looking up user 103
    Batch query responses: <response>
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

CHANNEL = "example.batch.queries"

# Simulated user database
USERS_DB = {
    101: {"name": "Alice", "email": "alice@example.com", "active": True},
    102: {"name": "Bob", "email": "bob@example.com", "active": True},
    103: {"name": "Carol", "email": "carol@example.com", "active": False},
}


@broker.subscriber(queries=CHANNEL)
async def handle_query(msg: dict) -> dict:
    """Look up a user by ID and return their profile."""
    user_id = msg.get("user_id")
    print(f"Looking up user {user_id}")
    user = USERS_DB.get(user_id, {"error": "not found"})
    return {"user_id": user_id, **user}


@app.after_startup
async def run_demo() -> None:
    """Send a batch of queries and inspect the responses."""
    queries_to_send = [
        {"user_id": 101},
        {"user_id": 102},
        {"user_id": 103},
    ]

    try:
        responses = await broker.request_batch(
            *queries_to_send,
            queries=CHANNEL,
            timeout=10,
            headers={"batch-id": "user-lookup-batch"},
            metadata="user-batch-query",
        )
        print(f"Batch query responses: {responses}")
    except Exception as exc:
        print(f"Batch query error: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
