"""Complete application using all KubeMQ messaging patterns.

Demonstrates events, events_store, queues, commands, and queries
in a single application.

Usage:
    python examples/full_app.py

Requires a KubeMQ broker on localhost:50000 (or set KUBEMQ_ADDRESS).
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream
from faststream.middlewares import AckPolicy

from kubemq_faststream import KubeMQBroker, StartPosition

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)


# --- Events: Fire-and-forget notifications ---
@broker.subscriber(events="app.logs")
async def on_log(msg: dict) -> None:
    level = msg.get("level", "INFO")
    message = msg.get("message", "")
    print(f"[LOG {level}] {message}")


# --- Events Store: Persistent audit trail ---
@broker.subscriber(
    events_store="app.audit",
    start_position=StartPosition.START_FROM_NEW,
)
async def on_audit(msg: dict) -> None:
    action = msg.get("action", "unknown")
    user = msg.get("user", "system")
    print(f"[AUDIT] {user} performed '{action}'")


# --- Queues: Reliable task processing ---
@broker.subscriber(queues="app.tasks", ack_policy=AckPolicy.ACK)
async def process_task(msg: dict) -> None:
    task_type = msg.get("type", "unknown")
    print(f"[TASK] Processing: {task_type}")
    await asyncio.sleep(0.1)
    print(f"[TASK] Completed: {task_type}")


# --- Commands: Execute operations ---
@broker.subscriber(commands="app.control")
async def handle_control(msg: dict) -> None:
    command = msg.get("command", "unknown")
    print(f"[COMMAND] Executing: {command}")
    await asyncio.sleep(0.2)
    print(f"[COMMAND] Done: {command}")


# --- Queries: Fetch data ---
@broker.subscriber(queries="app.status")
async def get_status(msg: dict) -> dict:
    component = msg.get("component", "all")
    print(f"[QUERY] Status request for: {component}")
    return {
        "component": component,
        "status": "healthy",
        "version": "1.0.0",
        "uptime_seconds": 3600,
    }


@app.after_startup
async def demo() -> None:
    """Run through all messaging patterns."""
    await asyncio.sleep(1.0)

    # 1. Events
    print("\n=== Events ===")
    await broker.publish(
        {"level": "INFO", "message": "Application started"},
        events="app.logs",
    )
    await asyncio.sleep(0.5)

    # 2. Events Store
    print("\n=== Events Store ===")
    await broker.publish(
        {"action": "login", "user": "admin"},
        events_store="app.audit",
    )
    await asyncio.sleep(0.5)

    # 3. Queues
    print("\n=== Queues ===")
    await broker.publish(
        {"type": "send-email", "to": "user@example.com"},
        queues="app.tasks",
    )
    await asyncio.sleep(1.0)

    # 4. Commands
    print("\n=== Commands ===")
    await broker.request(
        {"command": "clear-cache"},
        commands="app.control",
        timeout=10,
    )

    # 5. Queries
    print("\n=== Queries ===")
    result = await broker.request(
        {"component": "database"},
        queries="app.status",
        timeout=10,
    )
    print(f"Status response: {result}")

    print("\n=== Demo complete ===")
    await asyncio.sleep(1)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
