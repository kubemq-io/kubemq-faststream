"""Correlation Tracking -- KubeMQ FastStream Advanced Patterns.

Passes a ``correlation_id`` through a multi-pattern chain:
event -> queue -> command.  Each subscriber forwards the same
correlation_id to the next stage, demonstrating end-to-end
request tracing across different messaging patterns.

Usage:
    python examples/advanced_patterns/correlation_tracking.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [start] Published event with correlation_id=txn-7890
    [stage-1] Event received, forwarding to queue (corr=txn-7890)
    [stage-2] Queue received, forwarding to command (corr=txn-7890)
    [stage-3] Command received, final processing (corr=txn-7890)
    [tracking] Correlation chain complete for txn-7890
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

EVENT_CHANNEL = "example.advanced.corr.events"
QUEUE_CHANNEL = "example.advanced.corr.queue"
COMMAND_CHANNEL = "example.advanced.corr.command"

CORRELATION_ID = "txn-7890"


@broker.subscriber(events=EVENT_CHANNEL)
async def stage_1_event(msg: dict) -> None:
    """Stage 1: receive event, forward to queue with same correlation_id."""
    corr_id = msg.get("correlation_id", "unknown")
    print(f"[stage-1] Event received, forwarding to queue (corr={corr_id})")

    try:
        await broker.publish(
            {"stage": 2, "correlation_id": corr_id, "data": msg.get("data")},
            queues=QUEUE_CHANNEL,
            correlation_id=corr_id,
        )
    except Exception as exc:
        print(f"[stage-1] Forward failed: {exc}")


@broker.subscriber(queues=QUEUE_CHANNEL)
async def stage_2_queue(msg: dict) -> None:
    """Stage 2: receive from queue, forward to command with same correlation_id."""
    corr_id = msg.get("correlation_id", "unknown")
    print(f"[stage-2] Queue received, forwarding to command (corr={corr_id})")

    try:
        await broker.publish(
            {"stage": 3, "correlation_id": corr_id, "data": msg.get("data")},
            commands=COMMAND_CHANNEL,
            correlation_id=corr_id,
        )
    except Exception as exc:
        print(f"[stage-2] Forward failed: {exc}")


@broker.subscriber(commands=COMMAND_CHANNEL)
async def stage_3_command(msg: dict) -> None:
    """Stage 3: final processing with correlation_id tracking."""
    corr_id = msg.get("correlation_id", "unknown")
    print(f"[stage-3] Command received, final processing (corr={corr_id})")
    print(f"[tracking] Correlation chain complete for {corr_id}")


@app.after_startup
async def run_demo() -> None:
    """Start the correlation chain by publishing an event."""
    try:
        await broker.publish(
            {"stage": 1, "correlation_id": CORRELATION_ID, "data": "order-payload"},
            events=EVENT_CHANNEL,
            correlation_id=CORRELATION_ID,
        )
        print(f"[start] Published event with correlation_id={CORRELATION_ID}")
    except Exception as exc:
        print(f"Publish error: {exc}")

    await asyncio.sleep(5)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
