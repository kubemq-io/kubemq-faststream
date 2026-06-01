"""All Five Patterns -- KubeMQ FastStream Quickstart.

Side-by-side demonstration of all five KubeMQ messaging patterns:
Events, Events Store, Queues, Commands, and Queries.

Usage:
    python examples/quickstart/five_patterns.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [Events] Received: {'pattern': 'events'}
    [Events Store] Received: {'pattern': 'events_store'}
    [Queues] Received: {'pattern': 'queues'}
    [Commands] Handling command: {'action': 'restart'}
    [Queries] Handling query: {'question': 'status?'}
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


@broker.subscriber(events="example.quickstart.events")
async def events_handler(msg: dict) -> None:
    print(f"[Events] Received: {msg}")


@broker.subscriber(events_store="example.quickstart.events_store")
async def events_store_handler(msg: dict) -> None:
    print(f"[Events Store] Received: {msg}")


@broker.subscriber(queues="example.quickstart.queues")
async def queues_handler(msg: dict) -> None:
    print(f"[Queues] Received: {msg}")


@broker.subscriber(commands="example.quickstart.commands")
async def commands_handler(msg: dict) -> None:
    print(f"[Commands] Handling command: {msg}")


@broker.subscriber(queries="example.quickstart.queries")
async def queries_handler(msg: dict) -> dict:
    print(f"[Queries] Handling query: {msg}")
    return {"status": "ok", "uptime": 42}


@app.after_startup
async def run_demo() -> None:
    """Demonstrate all five patterns."""
    await broker.publish({"pattern": "events"}, events="example.quickstart.events")
    await broker.publish(
        {"pattern": "events_store"}, events_store="example.quickstart.events_store"
    )
    await broker.publish({"pattern": "queues"}, queues="example.quickstart.queues")

    try:
        await broker.request(
            {"action": "restart"}, commands="example.quickstart.commands", timeout=10
        )
    except Exception as exc:
        print(f"Command error: {exc}")

    try:
        result = await broker.request(
            {"question": "status?"}, queries="example.quickstart.queries", timeout=10
        )
        print(f"[Queries] Response: {result}")
    except Exception as exc:
        print(f"Query error: {exc}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
