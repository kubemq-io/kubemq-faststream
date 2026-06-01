"""Manual requeue of a queue message.

Uses AckPolicy.MANUAL to access the underlying SDK queue message
and call ``async_re_queue(channel)`` to send the message back to
the same (or a different) channel for retry.

Usage:
    python examples/queues/requeue.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream, context

from kubemq_faststream import KubeMQBroker
from kubemq_faststream.schemas import AckPolicy

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

CHANNEL = "example.queues.requeue"
RETRY_CHANNEL = "example.queues.requeue_retry"

attempt_counter: dict[str, int] = {}


@broker.subscriber(queues=CHANNEL, ack_policy=AckPolicy.MANUAL)
async def handle_with_requeue(msg: dict) -> None:
    """Try to process; requeue to retry channel on failure."""
    message = context.get("message")
    task_id = msg.get("task_id", "unknown")
    attempt_counter[task_id] = attempt_counter.get(task_id, 0) + 1

    print(f"[MAIN] Processing task {task_id} (attempt {attempt_counter[task_id]})")

    if attempt_counter[task_id] < 2:
        print(f"[MAIN] Simulated failure — requeuing task {task_id} to retry channel")
        queue_msg = message.raw_message.queue_msg
        if queue_msg:
            await queue_msg.async_re_queue(RETRY_CHANNEL)
        return

    await message.ack()
    print(f"[MAIN] Task {task_id} completed successfully")


@broker.subscriber(queues=RETRY_CHANNEL, ack_policy=AckPolicy.ACK)
async def handle_retry(msg: dict) -> None:
    """Process requeued messages."""
    print(f"[RETRY] Received requeued task: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Send a task that will be requeued on first attempt."""
    await broker.publish(
        {"task_id": "T-100", "payload": "important-work"},
        queues=CHANNEL,
    )
    print("Published task to queue")

    await asyncio.sleep(3)
    print("Demo complete — task was requeued to the retry channel")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
