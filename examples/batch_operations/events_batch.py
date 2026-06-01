"""Events Batch Publish -- KubeMQ FastStream Batch Operations.

Publish multiple events in a single batch call using
``broker.publish_events_batch()``. Compares individual publish
with batch publish for throughput demonstration.

Usage:
    python examples/batch_operations/events_batch.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Published 20 events individually in 0.xxxs
    Published 20 events in batch in 0.xxxs
    Total received: 40
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

received_count = 0


@broker.subscriber(events="example.batch.events")
async def handle(msg: dict) -> None:
    global received_count
    received_count += 1


@app.after_startup
async def run_demo() -> None:
    global received_count

    # Individual publish
    start = time.monotonic()
    for i in range(20):
        await broker.publish({"order_id": i}, events="example.batch.events")
    individual_time = time.monotonic() - start
    print(f"Published 20 events individually in {individual_time:.3f}s")

    await asyncio.sleep(1)

    # Batch publish
    messages = [{"order_id": i + 20} for i in range(20)]
    start = time.monotonic()
    await broker.publish_events_batch(
        *messages,
        events="example.batch.events",
        headers={"source": "batch"},
        metadata="batch-demo",
    )
    batch_time = time.monotonic() - start
    print(f"Published 20 events in batch in {batch_time:.3f}s")

    await asyncio.sleep(2)
    print(f"Total received: {received_count}")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
