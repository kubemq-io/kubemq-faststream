"""Basic command RPC — send a command and receive the execution result.

Commands use request/response semantics: the caller sends a command
and blocks until the subscriber processes it and returns a result.
The handler's return value is sent back to the caller.

Usage:
    python examples/commands/basic_command.py

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

CHANNEL = "example.commands.basic"


@broker.subscriber(commands=CHANNEL)
async def handle_command(msg: dict) -> dict:
    """Execute the command and return a result."""
    action = msg.get("action", "unknown")
    print(f"Executing command: {action}")
    return {"status": "executed", "action": action, "success": True}


@app.after_startup
async def run_demo() -> None:
    """Send a command and inspect the response."""
    print("Sending command...")
    response = await broker.request(
        {"action": "create_user", "name": "Alice"},
        commands=CHANNEL,
    )
    print(f"Command response: {response}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
