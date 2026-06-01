"""Competing Consumers -- load-balanced queue processing with group.

Publish 20 messages to a queue channel and register 3 subscribers
with the same ``group="workers"``.  KubeMQ delivers each message
to exactly one subscriber in the group, distributing the load
across all consumers.

Usage:
    python examples/patterns/competing_consumers.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [worker-1] Processing task 0
    [worker-2] Processing task 1
    [worker-3] Processing task 2
    ... (20 tasks distributed across 3 workers)
    --- Distribution ---
    worker-1: N tasks
    worker-2: N tasks
    worker-3: N tasks
    Total: 20/20
    Demo complete
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

CHANNEL = "example.patterns.competing.tasks"
NUM_TASKS = 20
worker_counts: dict[str, int] = {"worker-1": 0, "worker-2": 0, "worker-3": 0}


@broker.subscriber(queues=CHANNEL, group="workers")
async def worker_1(msg: dict) -> None:
    """Worker 1 -- processes its share of tasks."""
    task_id = msg.get("task_id", "?")
    worker_counts["worker-1"] += 1
    print(f"[worker-1] Processing task {task_id}")


@broker.subscriber(queues=CHANNEL, group="workers")
async def worker_2(msg: dict) -> None:
    """Worker 2 -- processes its share of tasks."""
    task_id = msg.get("task_id", "?")
    worker_counts["worker-2"] += 1
    print(f"[worker-2] Processing task {task_id}")


@broker.subscriber(queues=CHANNEL, group="workers")
async def worker_3(msg: dict) -> None:
    """Worker 3 -- processes its share of tasks."""
    task_id = msg.get("task_id", "?")
    worker_counts["worker-3"] += 1
    print(f"[worker-3] Processing task {task_id}")


@app.after_startup
async def run_demo() -> None:
    """Publish 20 tasks and observe competing consumer distribution."""
    for i in range(NUM_TASKS):
        try:
            await broker.publish(
                {"task_id": i, "payload": f"job-{i}"},
                queues=CHANNEL,
            )
        except Exception as exc:
            print(f"Failed to publish task {i}: {exc}")

    await asyncio.sleep(5)

    print("\n--- Distribution ---")
    for worker, count in worker_counts.items():
        print(f"{worker}: {count} tasks")
    print(f"Total: {sum(worker_counts.values())}/{NUM_TASKS}")
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
