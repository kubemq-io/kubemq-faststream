"""Fan-Out / Fan-In -- KubeMQ FastStream Advanced Patterns.

Distributes work items to multiple worker queues (fan-out), then
aggregates results using ``correlation_id`` tracking (fan-in).
A coordinator publishes tasks to N worker queues and collects
results from a shared results queue, matching by correlation_id.

Usage:
    python examples/advanced_patterns/fan_out_fan_in.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [fanout] Distributed 3 tasks with correlation_id=batch-001
    [worker-1] Processed chunk 1, result=10
    [worker-2] Processed chunk 2, result=20
    [worker-3] Processed chunk 3, result=30
    [fanin] Collected 3/3 results for batch-001
    [fanin] Aggregated total: 60
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

WORKER_QUEUES = [
    "example.advanced.fanout.worker1",
    "example.advanced.fanout.worker2",
    "example.advanced.fanout.worker3",
]
RESULTS_QUEUE = "example.advanced.fanout.results"

# Aggregation state
results_store: dict[str, list[int]] = {}
expected_count = len(WORKER_QUEUES)


@broker.subscriber(queues=WORKER_QUEUES[0])
async def worker_1(msg: dict) -> None:
    """Worker 1: process chunk and publish result."""
    result = msg.get("value", 0) * 10
    print(f"[worker-1] Processed chunk {msg.get('chunk')}, result={result}")
    try:
        await broker.publish(
            {"correlation_id": msg["correlation_id"], "result": result},
            queues=RESULTS_QUEUE,
            correlation_id=msg["correlation_id"],
        )
    except Exception as exc:
        print(f"[worker-1] Result publish failed: {exc}")


@broker.subscriber(queues=WORKER_QUEUES[1])
async def worker_2(msg: dict) -> None:
    """Worker 2: process chunk and publish result."""
    result = msg.get("value", 0) * 10
    print(f"[worker-2] Processed chunk {msg.get('chunk')}, result={result}")
    try:
        await broker.publish(
            {"correlation_id": msg["correlation_id"], "result": result},
            queues=RESULTS_QUEUE,
            correlation_id=msg["correlation_id"],
        )
    except Exception as exc:
        print(f"[worker-2] Result publish failed: {exc}")


@broker.subscriber(queues=WORKER_QUEUES[2])
async def worker_3(msg: dict) -> None:
    """Worker 3: process chunk and publish result."""
    result = msg.get("value", 0) * 10
    print(f"[worker-3] Processed chunk {msg.get('chunk')}, result={result}")
    try:
        await broker.publish(
            {"correlation_id": msg["correlation_id"], "result": result},
            queues=RESULTS_QUEUE,
            correlation_id=msg["correlation_id"],
        )
    except Exception as exc:
        print(f"[worker-3] Result publish failed: {exc}")


@broker.subscriber(queues=RESULTS_QUEUE)
async def aggregator(msg: dict) -> None:
    """Fan-in: collect results and aggregate when all workers report."""
    corr_id = msg.get("correlation_id", "unknown")
    result = msg.get("result", 0)

    if corr_id not in results_store:
        results_store[corr_id] = []
    results_store[corr_id].append(result)

    collected = len(results_store[corr_id])
    print(f"[fanin] Collected {collected}/{expected_count} results for {corr_id}")

    if collected >= expected_count:
        total = sum(results_store[corr_id])
        print(f"[fanin] Aggregated total: {total}")


@app.after_startup
async def run_demo() -> None:
    """Fan out work to workers with shared correlation_id."""
    correlation_id = "batch-001"
    chunks = [
        {"chunk": 1, "value": 1, "correlation_id": correlation_id},
        {"chunk": 2, "value": 2, "correlation_id": correlation_id},
        {"chunk": 3, "value": 3, "correlation_id": correlation_id},
    ]

    for i, chunk in enumerate(chunks):
        try:
            await broker.publish(
                chunk,
                queues=WORKER_QUEUES[i],
                correlation_id=correlation_id,
            )
        except Exception as exc:
            print(f"Fan-out publish failed: {exc}")

    print(f"[fanout] Distributed {len(chunks)} tasks with correlation_id={correlation_id}")

    await asyncio.sleep(5)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
