"""Full Application -- KubeMQ FastStream Quickstart.

Demonstrates events, events_store, queues, commands, and queries
in one self-contained file with lifecycle hooks (@app.on_startup,
@app.after_startup, @app.on_shutdown).

Usage:
    python examples/quickstart/full_app.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [Startup] Initializing resources...
    [Event]  {'type': 'notification'}
    [Store]  {'action': 'login'}
    [Queue]  {'task': 'send-email'}
    [Cmd]    Executing: {'command': 'clear-cache'}
    [Query]  {'question': 'meaning-of-life'}
    Query result: ...
    [Shutdown] Cleaning up resources...
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream
from faststream.middlewares import AckPolicy

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)


@broker.subscriber(events="example.quickstart.events")
async def on_event(msg: dict) -> None:
    print(f"[Event]  {msg}")


@broker.subscriber(
    events_store="example.quickstart.store",
    start_position=StartPosition.START_FROM_NEW,
)
async def on_store(msg: dict) -> None:
    print(f"[Store]  {msg}")


@broker.subscriber(queues="example.quickstart.queues", ack_policy=AckPolicy.ACK)
async def on_queue(msg: dict) -> None:
    print(f"[Queue]  {msg}")


@broker.subscriber(commands="example.quickstart.commands")
async def on_command(msg: dict) -> None:
    print(f"[Cmd]    Executing: {msg}")


@broker.subscriber(queries="example.quickstart.queries")
async def on_query(msg: dict) -> dict:
    print(f"[Query]  {msg}")
    return {"answer": 42, "source": "full_app"}


@app.on_startup
async def on_startup() -> None:
    """Called before broker connects."""
    print("[Startup] Initializing resources...")


@app.after_startup
async def run_demo() -> None:
    """Called after broker connects -- run the demo."""
    await asyncio.sleep(1)

    print("\n--- Events ---")
    await broker.publish({"type": "notification"}, events="example.quickstart.events")
    await asyncio.sleep(0.5)

    print("\n--- Events Store ---")
    await broker.publish({"action": "login"}, events_store="example.quickstart.store")
    await asyncio.sleep(0.5)

    print("\n--- Queues ---")
    await broker.publish({"task": "send-email"}, queues="example.quickstart.queues")
    await asyncio.sleep(1)

    print("\n--- Commands ---")
    try:
        await broker.request(
            {"command": "clear-cache"}, commands="example.quickstart.commands", timeout=10
        )
    except Exception as exc:
        print(f"Command error: {exc}")

    print("\n--- Queries ---")
    try:
        result = await broker.request(
            {"question": "meaning-of-life"},
            queries="example.quickstart.queries",
            timeout=10,
        )
        print(f"Query result: {result}")
    except Exception as exc:
        print(f"Query error: {exc}")

    await asyncio.sleep(1)
    await app.stop()


@app.on_shutdown
async def on_shutdown() -> None:
    """Called when the app is shutting down."""
    print("[Shutdown] Cleaning up resources...")


if __name__ == "__main__":
    asyncio.run(app.run())
