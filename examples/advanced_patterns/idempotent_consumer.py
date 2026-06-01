"""Idempotent Consumer -- KubeMQ FastStream Advanced Patterns.

Tracks processed ``message_id`` values in an in-memory set to ensure
each unique message is processed exactly once.  The same message is
published twice with the same ``message_id``; the subscriber detects
the duplicate and skips processing.

Usage:
    python examples/advanced_patterns/idempotent_consumer.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published msg with message_id=order-abc-001
    Published msg with message_id=order-abc-001 (duplicate)
    [idempotent] Processing new message: order-abc-001
    [idempotent] Duplicate skipped: order-abc-001
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

CHANNEL = "example.advanced.idempotent"

# In-memory deduplication store (use Redis/DB in production)
processed_ids: set[str] = set()


@broker.subscriber(queues=CHANNEL)
async def idempotent_handler(msg: dict) -> None:
    """Process each unique message_id only once."""
    msg_id = msg.get("message_id", "")
    if not msg_id:
        print("[idempotent] No message_id in payload, processing anyway")
        return

    if msg_id in processed_ids:
        print(f"[idempotent] Duplicate skipped: {msg_id}")
        return

    processed_ids.add(msg_id)
    print(f"[idempotent] Processing new message: {msg_id}")


@app.after_startup
async def run_demo() -> None:
    """Publish the same message twice with identical message_id."""
    msg_id = "order-abc-001"
    payload = {"message_id": msg_id, "order": "widget-x", "quantity": 5}

    try:
        await broker.publish(
            payload,
            queues=CHANNEL,
            message_id=msg_id,
        )
        print(f"Published msg with message_id={msg_id}")

        await asyncio.sleep(1)

        # Publish duplicate with same message_id
        await broker.publish(
            payload,
            queues=CHANNEL,
            message_id=msg_id,
        )
        print(f"Published msg with message_id={msg_id} (duplicate)")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(3)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
