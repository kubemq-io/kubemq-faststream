"""Clear error when the broker is unreachable.

Attempts to connect to a non-existent broker address, demonstrating
the fail-fast behavior with an informative error message.

Usage:
    python examples/error_handling/connection_error.py

Requires:
    No broker needed (this example demonstrates the failure case)
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://nonexistent-host:50000")
app = FastStream(broker)


@broker.subscriber(events="example.error.connfail")
async def handle_message(msg: dict) -> None:
    print(f"Received: {msg}")


@app.after_startup
async def run_demo() -> None:
    await app.stop()


if __name__ == "__main__":
    print("Attempting to connect to a non-existent broker...")
    try:
        asyncio.run(app.run())
    except Exception as exc:
        print(f"\nConnection failed (expected): {type(exc).__name__}: {exc}")
        print("The app exits cleanly with a descriptive error.")
