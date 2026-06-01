"""Pipeline chain: event -> queue -> command across patterns.

Demonstrates handler chaining where each stage publishes to the
next pattern.  An event triggers queue processing, which in turn
fires a command for final execution.

Usage:
    python examples/patterns/pipeline_chain.py

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

STAGE_1 = "example.patterns.pipeline.ingest"
STAGE_2 = "example.patterns.pipeline.process"
STAGE_3 = "example.patterns.pipeline.execute"


@broker.subscriber(events=STAGE_1)
async def stage_ingest(msg: dict) -> None:
    """Stage 1: Receive event and forward to queue for reliable processing."""
    print(f"[Stage 1 - Event] Ingested: {msg}")
    enriched = {**msg, "ingested": True}
    await broker.publish(enriched, queues=STAGE_2)


@broker.subscriber(queues=STAGE_2)
async def stage_process(msg: dict) -> None:
    """Stage 2: Process from queue and trigger a command."""
    print(f"[Stage 2 - Queue] Processed: {msg}")
    await broker.request(
        {**msg, "processed": True},
        commands=STAGE_3,
        timeout=10,
    )


@broker.subscriber(commands=STAGE_3)
async def stage_execute(msg: dict) -> None:
    """Stage 3: Execute the final command."""
    print(f"[Stage 3 - Command] Executed: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Kick off the pipeline with an initial event."""
    await broker.publish(
        {"order_id": "PIPE-001", "amount": 99.99},
        events=STAGE_1,
    )
    print("Pipeline started: event -> queue -> command")

    await asyncio.sleep(3)
    print("Pipeline complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
