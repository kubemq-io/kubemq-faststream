"""Cross-pattern publishing from a single application.

Publishes the same logical data to different KubeMQ patterns — events
(fire-and-forget) and queues (reliable delivery) — showing how the
``broker.publish()`` keyword selects the target pattern.

Usage:
    python examples/publisher/publish_to_pattern.py

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

EVENT_CHANNEL = "example.publisher.pattern.notifications"
QUEUE_CHANNEL = "example.publisher.pattern.tasks"


@broker.subscriber(events=EVENT_CHANNEL)
async def on_notification(msg: dict) -> None:
    """Receive fire-and-forget notification."""
    print(f"[Event] Notification: {msg}")


@broker.subscriber(queues=QUEUE_CHANNEL)
async def on_task(msg: dict) -> None:
    """Receive reliable queued task."""
    print(f"[Queue] Task: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish to events and queues patterns from the same app."""
    payload = {"action": "user_signup", "user_id": "U-42"}

    await broker.publish(payload, events=EVENT_CHANNEL)
    print("Published to events pattern (fire-and-forget)")

    await broker.publish(payload, queues=QUEUE_CHANNEL)
    print("Published to queues pattern (reliable delivery)")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
