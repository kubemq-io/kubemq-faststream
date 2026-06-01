"""Retry with Exponential Backoff -- KubeMQ FastStream Advanced Patterns.

Demonstrates retry with exponential backoff using queue parameters:
``max_receive_count`` limits delivery attempts, ``max_receive_queue``
routes exhausted messages to a dead-letter queue, and ``delay_in_seconds``
spaces out retries with increasing delays.

Usage:
    python examples/advanced_patterns/retry_with_backoff.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published message with max_receive_count=5
    [retry] Attempt for task-1, simulating transient failure
    Published retry with delay=1s
    [retry] Attempt for task-1, simulating transient failure
    Published retry with delay=2s
    [retry] Attempt for task-1, processing succeeded on retry
    [dlq] Dead-letter received: ...
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

MAIN_QUEUE = "example.advanced.retry"
DLQ_QUEUE = "example.advanced.retry.dlq"

attempt_tracker: dict[str, int] = {}


@broker.subscriber(queues=MAIN_QUEUE)
async def handle_with_retry(msg: dict) -> None:
    """Process messages with simulated transient failures."""
    task_id = msg.get("task_id", "unknown")
    attempt_tracker[task_id] = attempt_tracker.get(task_id, 0) + 1
    attempt = attempt_tracker[task_id]

    if attempt < 3:
        print(f"[retry] Attempt for {task_id}, simulating transient failure")
        # Re-publish with exponential backoff delay
        delay = 2 ** (attempt - 1)  # 1s, 2s, 4s, ...
        try:
            await broker.publish(
                msg,
                queues=MAIN_QUEUE,
                max_receive_count=5,
                max_receive_queue=DLQ_QUEUE,
                delay_in_seconds=delay,
            )
            print(f"Published retry with delay={delay}s")
        except Exception as exc:
            print(f"Retry publish failed: {exc}")
    else:
        print(f"[retry] Attempt for {task_id}, processing succeeded on retry")


@broker.subscriber(queues=DLQ_QUEUE)
async def handle_dead_letter(msg: dict) -> None:
    """Process messages that exceeded max receive count."""
    print(f"[dlq] Dead-letter received: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish a message with retry/backoff configuration."""
    try:
        await broker.publish(
            {"task_id": "task-1", "data": "process-me"},
            queues=MAIN_QUEUE,
            max_receive_count=5,
            max_receive_queue=DLQ_QUEUE,
        )
        print("Published message with max_receive_count=5")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(8)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
