"""Mixed Batch Pipeline -- KubeMQ FastStream Batch Operations.

End-to-end batch processing pipeline that chains three stages:
1. Events batch: publish order events in bulk
2. Queue batch: enqueue validated orders for processing
3. Commands batch: dispatch fulfillment commands

Demonstrates how different KubeMQ patterns compose in a realistic
multi-stage batch workflow.

Usage:
    python examples/batch_operations/mixed_batch_pipeline.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Stage 1: Published 5 order events in batch
    Received order event: order-0
    ...
    Stage 2: Enqueued 5 orders via batch
    Processing queued order: order-0
    ...
    Stage 3: Sent 5 fulfillment commands in batch
    Fulfilling order: order-0
    ...
    Pipeline complete
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

EVENTS_CHANNEL = "example.batch.pipeline.events"
QUEUES_CHANNEL = "example.batch.pipeline.queues"
COMMANDS_CHANNEL = "example.batch.pipeline.commands"

# Track pipeline progress
pipeline_state: dict[str, list[str]] = {
    "events_received": [],
    "queues_processed": [],
    "commands_fulfilled": [],
}


@broker.subscriber(events=EVENTS_CHANNEL)
async def handle_order_event(msg: dict) -> None:
    """Stage 1 subscriber: receive order notification events."""
    order_id = msg.get("order_id", "unknown")
    print(f"Received order event: {order_id}")
    pipeline_state["events_received"].append(order_id)


@broker.subscriber(queues=QUEUES_CHANNEL)
async def handle_queued_order(msg: dict) -> None:
    """Stage 2 subscriber: process validated orders from the queue."""
    order_id = msg.get("order_id", "unknown")
    print(f"Processing queued order: {order_id}")
    pipeline_state["queues_processed"].append(order_id)


@broker.subscriber(commands=COMMANDS_CHANNEL)
async def handle_fulfillment_command(msg: dict) -> dict:
    """Stage 3 subscriber: execute fulfillment command and return result."""
    order_id = msg.get("order_id", "unknown")
    print(f"Fulfilling order: {order_id}")
    pipeline_state["commands_fulfilled"].append(order_id)
    return {"order_id": order_id, "status": "fulfilled"}


@app.after_startup
async def run_pipeline() -> None:
    """Execute the three-stage batch pipeline."""
    orders = [{"order_id": f"order-{i}", "amount": (i + 1) * 10} for i in range(5)]

    # Stage 1: Publish order events in batch
    try:
        await broker.publish_events_batch(
            *orders,
            events=EVENTS_CHANNEL,
            headers={"stage": "notification"},
            metadata="pipeline-events",
        )
        print("Stage 1: Published 5 order events in batch")
    except Exception as exc:
        print(f"Stage 1 error: {exc}")

    await asyncio.sleep(2)

    # Stage 2: Enqueue validated orders via batch
    try:
        await broker.publish_batch(
            *orders,
            queues=QUEUES_CHANNEL,
            headers={"stage": "processing"},
        )
        print("Stage 2: Enqueued 5 orders via batch")
    except Exception as exc:
        print(f"Stage 2 error: {exc}")

    await asyncio.sleep(3)

    # Stage 3: Send fulfillment commands in batch
    fulfillment_cmds = [{"order_id": f"order-{i}", "action": "fulfill"} for i in range(5)]
    try:
        responses = await broker.request_batch(
            *fulfillment_cmds,
            commands=COMMANDS_CHANNEL,
            timeout=10,
            headers={"stage": "fulfillment"},
            metadata="pipeline-commands",
        )
        print("Stage 3: Sent 5 fulfillment commands in batch")
        logging.info("Command responses: %s", responses)
    except Exception as exc:
        print(f"Stage 3 error: {exc}")

    await asyncio.sleep(1)
    print("Pipeline complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
