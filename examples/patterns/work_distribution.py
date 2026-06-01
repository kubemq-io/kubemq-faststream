"""Work distribution: multiple queue consumers with group-based load balancing.

Multiple subscribers on the same queue channel with a shared ``group``
name split the workload evenly.  KubeMQ delivers each message to
exactly one consumer in the group.

Usage:
    python examples/patterns/work_distribution.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

QUEUE_CH = "example.patterns.workdist.tasks"
worker_counts: dict[str, int] = {"worker-1": 0, "worker-2": 0, "worker-3": 0}


@broker.subscriber(queues=QUEUE_CH, group="task-workers")
async def worker_1(msg: dict) -> None:
    """Worker 1 — processes a share of the queued tasks."""
    worker_counts["worker-1"] += 1
    print(f"[Worker-1] Task #{msg.get('task_id')}")


@broker.subscriber(queues=QUEUE_CH, group="task-workers")
async def worker_2(msg: dict) -> None:
    """Worker 2 — processes a share of the queued tasks."""
    worker_counts["worker-2"] += 1
    print(f"[Worker-2] Task #{msg.get('task_id')}")


@broker.subscriber(queues=QUEUE_CH, group="task-workers")
async def worker_3(msg: dict) -> None:
    """Worker 3 — processes a share of the queued tasks."""
    worker_counts["worker-3"] += 1
    print(f"[Worker-3] Task #{msg.get('task_id')}")


@app.after_startup
async def run_demo() -> None:
    """Publish tasks and observe load-balanced distribution."""
    num_tasks = 9
    for i in range(num_tasks):
        await broker.publish(
            {"task_id": i, "payload": f"job-{i}"},
            queues=QUEUE_CH,
        )

    await asyncio.sleep(3)
    print(f"\nWork distribution: {worker_counts}")
    print(f"Total tasks: {sum(worker_counts.values())}/{num_tasks}")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
