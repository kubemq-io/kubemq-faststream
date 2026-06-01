"""No-Reply Command -- fire-and-forget command with no response.

Subscribe to a command channel with ``no_reply=True`` so the handler
executes the command but does NOT send a response back to the caller.
The caller uses ``broker.publish(commands=...)`` instead of
``broker.request()`` since no reply is expected.

Usage:
    python examples/commands/no_reply_command.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [handler] Executing fire-and-forget command: restart_service
    [caller] Command published (no response expected)
    [handler] Executing fire-and-forget command: clear_cache
    [caller] Command published (no response expected)
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

CHANNEL = "example.commands.noreply"


@broker.subscriber(commands=CHANNEL, no_reply=True)
async def handle_command(msg: dict) -> None:
    """Execute the command without sending a response."""
    action = msg.get("action", "unknown")
    print(f"[handler] Executing fire-and-forget command: {action}")


@app.after_startup
async def run_demo() -> None:
    """Publish commands without waiting for a response."""
    commands_to_send = [
        {"action": "restart_service", "target": "web-server-01"},
        {"action": "clear_cache", "target": "cache-node-03"},
    ]

    for cmd in commands_to_send:
        try:
            await broker.publish(cmd, commands=CHANNEL)
            print("[caller] Command published (no response expected)")
        except Exception as exc:
            print(f"[caller] Failed to publish command: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
