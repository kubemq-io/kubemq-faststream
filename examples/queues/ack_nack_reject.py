"""Queue acknowledgement strategies: ACK, NACK_ON_ERROR, REJECT_ON_ERROR, and MANUAL.

Demonstrates all four AckPolicy modes on separate channels:

- ACK (default): auto-ack after handler completes successfully.
- NACK_ON_ERROR: requeue the message if the handler raises an exception.
- REJECT_ON_ERROR: reject the message (server-configured disposition) on error.
- MANUAL: explicit ack/nack/reject calls in the handler body.

Usage:
    python examples/queues/ack_nack_reject.py

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


@broker.subscriber(queues="example.queues.ack_auto", ack_policy=AckPolicy.ACK)
async def auto_ack_handler(msg: dict) -> None:
    """Auto-acknowledged after handler returns without error."""
    print(f"[ACK] Processed: {msg}")


@broker.subscriber(queues="example.queues.ack_nack", ack_policy=AckPolicy.NACK_ON_ERROR)
async def nack_on_error_handler(msg: dict) -> None:
    """Message is requeued if handler raises an exception."""
    print(f"[NACK_ON_ERROR] Processing: {msg}")
    if msg.get("fail"):
        raise ValueError("Simulated failure — message will be requeued")
    print(f"[NACK_ON_ERROR] Success: {msg}")


@broker.subscriber(queues="example.queues.ack_reject", ack_policy=AckPolicy.REJECT_ON_ERROR)
async def reject_on_error_handler(msg: dict) -> None:
    """Message is rejected (not requeued) if handler raises an exception."""
    print(f"[REJECT_ON_ERROR] Processing: {msg}")
    if msg.get("fail"):
        raise ValueError("Simulated failure — message will be rejected")
    print(f"[REJECT_ON_ERROR] Success: {msg}")


@broker.subscriber(queues="example.queues.ack_manual", ack_policy=AckPolicy.MANUAL)
async def manual_handler(msg: dict) -> None:
    """Explicit ack/nack/reject — full control over message disposition."""
    message = context.get("message")
    print(f"[MANUAL] Processing: {msg}")

    if msg.get("action") == "reject":
        await message.reject()
        print("[MANUAL] Message rejected")
    elif msg.get("action") == "nack":
        await message.nack()
        print("[MANUAL] Message nacked (requeued)")
    else:
        await message.ack()
        print("[MANUAL] Message acknowledged")


@app.after_startup
async def run_demo() -> None:
    """Publish test messages to each ack-policy channel."""
    await broker.publish({"task": "auto"}, queues="example.queues.ack_auto")
    await broker.publish({"task": "nack", "fail": False}, queues="example.queues.ack_nack")
    await broker.publish({"task": "reject", "fail": False}, queues="example.queues.ack_reject")
    await broker.publish({"action": "ack", "task": "manual"}, queues="example.queues.ack_manual")
    print("Published messages to all ack-policy channels")

    await asyncio.sleep(3)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
