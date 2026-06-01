"""Priority Routing -- KubeMQ FastStream Advanced Patterns.

An event subscriber inspects incoming messages and routes them to
priority-specific queues (high, medium, low) based on the message
content.  Separate queue subscribers process each priority level
independently.

Usage:
    python examples/advanced_patterns/priority_routing.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published 3 tasks with different priorities
    [router] Routing task-1 to HIGH priority queue
    [router] Routing task-2 to LOW priority queue
    [router] Routing task-3 to MEDIUM priority queue
    [high] Processing high-priority: task-1
    [medium] Processing medium-priority: task-3
    [low] Processing low-priority: task-2
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

INGEST_CHANNEL = "example.advanced.priority.ingest"
HIGH_QUEUE = "example.advanced.priority.high"
MEDIUM_QUEUE = "example.advanced.priority.medium"
LOW_QUEUE = "example.advanced.priority.low"

PRIORITY_QUEUES = {
    "high": HIGH_QUEUE,
    "medium": MEDIUM_QUEUE,
    "low": LOW_QUEUE,
}


@broker.subscriber(events=INGEST_CHANNEL)
async def route_by_priority(msg: dict) -> None:
    """Route incoming events to priority-specific queues."""
    priority = msg.get("priority", "low").lower()
    target_queue = PRIORITY_QUEUES.get(priority, LOW_QUEUE)
    task_id = msg.get("task_id", "unknown")

    print(f"[router] Routing {task_id} to {priority.upper()} priority queue")
    try:
        await broker.publish(msg, queues=target_queue)
    except Exception as exc:
        print(f"[router] Failed to route {task_id}: {exc}")


@broker.subscriber(queues=HIGH_QUEUE)
async def handle_high(msg: dict) -> None:
    """Process high-priority tasks."""
    print(f"[high] Processing high-priority: {msg.get('task_id')}")


@broker.subscriber(queues=MEDIUM_QUEUE)
async def handle_medium(msg: dict) -> None:
    """Process medium-priority tasks."""
    print(f"[medium] Processing medium-priority: {msg.get('task_id')}")


@broker.subscriber(queues=LOW_QUEUE)
async def handle_low(msg: dict) -> None:
    """Process low-priority tasks."""
    print(f"[low] Processing low-priority: {msg.get('task_id')}")


@app.after_startup
async def run_demo() -> None:
    """Publish tasks with varying priorities."""
    tasks = [
        {"task_id": "task-1", "priority": "high", "data": "urgent-report"},
        {"task_id": "task-2", "priority": "low", "data": "cleanup-job"},
        {"task_id": "task-3", "priority": "medium", "data": "daily-sync"},
    ]

    for task in tasks:
        try:
            await broker.publish(task, events=INGEST_CHANNEL)
        except Exception as exc:
            print(f"Publish error: {exc}")

    print("Published 3 tasks with different priorities")

    await asyncio.sleep(4)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
