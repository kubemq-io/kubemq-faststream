"""Command timeout handling.

Demonstrates what happens when a command handler takes longer than
the caller's timeout.  The first command completes within the
deadline; the second simulates a slow handler that exceeds the
timeout, causing the caller to receive a timeout error.

Usage:
    python examples/commands/timeout_handling.py

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

CHANNEL = "example.commands.timeout"


@broker.subscriber(commands=CHANNEL)
async def slow_handler(msg: dict) -> dict:
    """Simulate a handler that may exceed the timeout."""
    delay = msg.get("delay_seconds", 0)
    print(f"Handler working for {delay}s...")
    await asyncio.sleep(delay)
    return {"status": "done", "delay": delay}


@app.after_startup
async def run_demo() -> None:
    """Send commands with different timeout scenarios."""
    print("--- Fast command (should succeed) ---")
    try:
        response = await broker.request(
            {"action": "fast_task", "delay_seconds": 1},
            commands=CHANNEL,
            timeout=10,
        )
        print(f"Success: {response}")
    except Exception as exc:
        print(f"Error: {exc}")

    print("\n--- Slow command (should timeout) ---")
    try:
        response = await broker.request(
            {"action": "slow_task", "delay_seconds": 15},
            commands=CHANNEL,
            timeout=2,
        )
        print(f"Success: {response}")
    except Exception as exc:
        print(f"Timeout error (expected): {exc}")

    await asyncio.sleep(2)
    print("\nDemo complete — timeout errors are raised when deadline is exceeded")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
