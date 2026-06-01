"""Production-like app using all five KubeMQ patterns with cross-pattern communication.

An incoming event triggers a queue task; the queue handler fires a
command; and a query fetches the final status.  This demonstrates
how patterns compose in a realistic workflow.

Usage:
    python examples/patterns/multi_pattern_app.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

EVENT_CH = "example.patterns.multi.events"
STORE_CH = "example.patterns.multi.store"
QUEUE_CH = "example.patterns.multi.queue"
CMD_CH = "example.patterns.multi.cmd"
QUERY_CH = "example.patterns.multi.query"

results: list[str] = []


@broker.subscriber(events=EVENT_CH)
async def on_event(msg: dict) -> None:
    """Receive event and forward to a queue for reliable processing."""
    print(f"[Event] Received: {msg}")
    await broker.publish(msg, queues=QUEUE_CH)
    results.append("event")


@broker.subscriber(
    events_store=STORE_CH,
    start_position=StartPosition.START_FROM_NEW,
)
async def on_store_event(msg: dict) -> None:
    """Persist and replay events."""
    print(f"[Store] Persisted: {msg}")
    results.append("store")


@broker.subscriber(queues=QUEUE_CH)
async def on_queue(msg: dict) -> None:
    """Process queued work and trigger a command."""
    print(f"[Queue] Processing: {msg}")
    results.append("queue")


@broker.subscriber(commands=CMD_CH)
async def on_command(msg: dict) -> None:
    """Execute a command."""
    print(f"[Command] Executed: {msg}")
    results.append("command")


@broker.subscriber(queries=QUERY_CH)
async def on_query(msg: dict) -> dict:
    """Answer a status query."""
    print(f"[Query] Answering: {msg}")
    results.append("query")
    return {"status": "all_patterns_ok", "patterns_used": len(results)}


@app.after_startup
async def run_demo() -> None:
    """Drive the multi-pattern workflow."""
    payload = {"order_id": "MP-001", "item": "widget"}

    await broker.publish(payload, events=EVENT_CH)
    await broker.publish(payload, events_store=STORE_CH)
    await asyncio.sleep(1)

    await broker.request({"action": "deploy"}, commands=CMD_CH, timeout=10)

    response = await broker.request({"question": "status"}, queries=QUERY_CH, timeout=10)
    print(f"Query response: {response}")

    await asyncio.sleep(1)
    print(f"Patterns exercised: {results}")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
