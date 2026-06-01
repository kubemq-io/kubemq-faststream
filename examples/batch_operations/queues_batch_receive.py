"""Queue Batch Receive with Ack Semantics -- KubeMQ FastStream Batch Operations.

Deep-dive into batch queue subscriber acknowledgement semantics.
Demonstrates how different ``AckPolicy`` modes affect batch message
processing: auto-ack on success, nack-on-error for redelivery,
and manual ack/nack per message.

This example focuses on ack semantics for batch subscribers. For a
basic batch subscriber setup, see ``queues/batch_receive.py``.

Usage:
    python examples/batch_operations/queues_batch_receive.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published 5 messages to batch queue
    [BATCH-ACK] Received 5 messages, auto-acked on success
    [BATCH-MANUAL] Received 5 messages, manually acking each
    Demo complete
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream, context

from kubemq_faststream import AckPolicy, KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

CHANNEL_ACK = "example.batch.queues_recv_ack"
CHANNEL_MANUAL = "example.batch.queues_recv_manual"


@broker.subscriber(
    queues=CHANNEL_ACK,
    batch=True,
    max_messages=5,
    wait_timeout=10,
    ack_policy=AckPolicy.ACK,
)
async def handle_batch_auto_ack(msgs: list[dict]) -> None:
    """Batch subscriber with auto-ack: all messages acknowledged on success.

    If the handler raises an exception, none of the messages are acked
    and they become available for redelivery.
    """
    print(f"[BATCH-ACK] Received {len(msgs)} messages, auto-acked on success")
    for msg in msgs:
        logging.info("Processing (auto-ack): %s", msg)


@broker.subscriber(
    queues=CHANNEL_MANUAL,
    batch=True,
    max_messages=5,
    wait_timeout=10,
    ack_policy=AckPolicy.MANUAL,
)
async def handle_batch_manual(msgs: list[dict]) -> None:
    """Batch subscriber with manual ack: explicit control per message.

    Each message can be individually acked, nacked, or rejected.
    This provides fine-grained control when some messages in a batch
    may succeed while others fail.
    """
    message = context.get("message")
    print(f"[BATCH-MANUAL] Received {len(msgs)} messages, manually acking each")
    for msg in msgs:
        logging.info("Processing (manual): %s", msg)
    # Acknowledge the entire batch after processing all messages
    await message.ack()


@app.after_startup
async def run_demo() -> None:
    """Publish messages to both batch channels."""
    try:
        for i in range(1, 6):
            await broker.publish({"task": i, "mode": "auto-ack"}, queues=CHANNEL_ACK)
        for i in range(1, 6):
            await broker.publish({"task": i, "mode": "manual"}, queues=CHANNEL_MANUAL)
        print("Published 5 messages to batch queue")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(5)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
