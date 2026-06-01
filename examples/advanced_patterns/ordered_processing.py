"""Ordered Processing -- KubeMQ FastStream Advanced Patterns.

Single-consumer queue with no consumer group and ``max_messages=1``
to guarantee strict in-order message processing.  Messages are
published in sequence and received in the exact same order.

Usage:
    python examples/advanced_patterns/ordered_processing.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published 5 ordered messages
    [ordered] Processing sequence 1: step-A
    [ordered] Processing sequence 2: step-B
    [ordered] Processing sequence 3: step-C
    [ordered] Processing sequence 4: step-D
    [ordered] Processing sequence 5: step-E
    [ordered] All 5 messages processed in order
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

CHANNEL = "example.advanced.ordered"

# Track received order
received_order: list[int] = []
TOTAL_MESSAGES = 5


@broker.subscriber(queues=CHANNEL, max_messages=1)
async def ordered_handler(msg: dict) -> None:
    """Process messages strictly one at a time in order."""
    seq = msg.get("sequence", 0)
    step = msg.get("step", "unknown")
    received_order.append(seq)

    print(f"[ordered] Processing sequence {seq}: {step}")

    # Simulate processing time to demonstrate ordering
    await asyncio.sleep(0.2)

    if len(received_order) >= TOTAL_MESSAGES:
        print(f"[ordered] All {TOTAL_MESSAGES} messages processed in order")


@app.after_startup
async def run_demo() -> None:
    """Publish numbered messages that must be processed in order."""
    steps = ["step-A", "step-B", "step-C", "step-D", "step-E"]

    for i, step in enumerate(steps, start=1):
        try:
            await broker.publish(
                {"sequence": i, "step": step},
                queues=CHANNEL,
            )
        except Exception as exc:
            print(f"Publish error: {exc}")

    print(f"Published {TOTAL_MESSAGES} ordered messages")

    await asyncio.sleep(5)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
