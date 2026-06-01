"""Multiple command handlers with group-based load balancing.

When multiple subscribers listen on the same command channel with the
same ``group`` name, the broker load-balances commands across them.
Only one handler processes each command.

Usage:
    python examples/commands/multiple_handlers.py

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

CHANNEL = "example.commands.multi"
GROUP = "cmd-workers"


@broker.subscriber(commands=CHANNEL, group=GROUP)
async def handler_a(msg: dict) -> dict:
    """Command handler A."""
    print(f"[Handler-A] Executing: {msg}")
    return {"handler": "A", "status": "executed"}


@broker.subscriber(commands=CHANNEL, group=GROUP)
async def handler_b(msg: dict) -> dict:
    """Command handler B."""
    print(f"[Handler-B] Executing: {msg}")
    return {"handler": "B", "status": "executed"}


@app.after_startup
async def run_demo() -> None:
    """Send several commands that are load-balanced across handlers."""
    for i in range(1, 5):
        print(f"\nSending command {i}...")
        response = await broker.request(
            {"command_id": i, "action": f"task-{i}"},
            commands=CHANNEL,
        )
        print(f"Response from command {i}: {response}")

    await asyncio.sleep(2)
    print("\nDemo complete — commands were distributed across handlers")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
