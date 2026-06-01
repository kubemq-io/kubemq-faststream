"""Commands Batch Request -- KubeMQ FastStream Batch Operations.

Send multiple command requests in a single batch call using
``broker.request_batch()``. All commands are dispatched together
and responses are collected. Each command is independently processed
by the subscriber.

Usage:
    python examples/batch_operations/commands_batch.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    Executing command: deploy service-1
    Executing command: deploy service-2
    Executing command: deploy service-3
    Batch command responses: <response>
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

CHANNEL = "example.batch.commands"


@broker.subscriber(commands=CHANNEL)
async def handle_command(msg: dict) -> dict:
    """Execute a deployment command and return the result."""
    action = msg.get("action", "unknown")
    target = msg.get("target", "unknown")
    print(f"Executing command: {action} {target}")
    return {"status": "executed", "action": action, "target": target, "success": True}


@app.after_startup
async def run_demo() -> None:
    """Send a batch of commands and inspect the responses."""
    commands_to_send = [
        {"action": "deploy", "target": "service-1"},
        {"action": "deploy", "target": "service-2"},
        {"action": "deploy", "target": "service-3"},
    ]

    try:
        responses = await broker.request_batch(
            *commands_to_send,
            commands=CHANNEL,
            timeout=10,
            headers={"batch-id": "deploy-batch-001"},
            metadata="deployment-batch",
        )
        print(f"Batch command responses: {responses}")
    except Exception as exc:
        print(f"Batch command error: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
